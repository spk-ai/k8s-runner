package server

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

func pvcFixture(t *testing.T) (*runnerv1.StartWorkloadRequest, *corev1.PersistentVolumeClaim) {
	t.Helper()
	req := &runnerv1.StartWorkloadRequest{
		WorkloadId: uuid.NewString(), Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Labels: map[string]string{"agent-instance-id": "instance-a", "agent-id": "agent-a", "thread-id": "thread-new", workloadKeyLabelKey: "workload-new"},
		Volumes: []*runnerv1.VolumeSpec{{Name: "workspace", PersistentName: "workspace", Kind: runnerv1.VolumeKind_VOLUME_KIND_NAMED,
			Size: "1Gi", Labels: map[string]string{volumeKeyLabelKey: "volume-a"}}},
	}
	labels, err := buildLabels(uuid.NewString(), nil, req.Labels)
	if err != nil {
		t.Fatal(err)
	}
	delete(labels, workloadIDLabelKey)
	labels[volumeKeyLabelKey] = "volume-a"
	labels[workloadKeyLabelKey], labels["thread-id"] = "workload-old", "thread-old"
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "workspace", Namespace: "default", UID: types.UID(uuid.NewString()), ResourceVersion: "1", Labels: labels},
		Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: ptr.To("local-path"), Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}}},
	}
	return req, claim
}

func pvcTestServer(client *fake.Clientset) *Server {
	return New(Options{Clientset: client, Namespace: "default", StorageSize: "1Gi", Logger: zap.NewNop(),
		Catalog: config.Catalog{StorageClasses: []config.StorageClassEntry{{Name: "fast", StorageClassName: "fast-ssd"}, {Name: "default", StorageClassName: ""}}}})
}

func assertPVCUnchanged(t *testing.T, client *fake.Clientset, expected *corev1.PersistentVolumeClaim) {
	t.Helper()
	current, err := client.CoreV1().PersistentVolumeClaims(expected.Namespace).Get(context.Background(), expected.Name, metav1.GetOptions{})
	if err != nil || !reflect.DeepEqual(current, expected) {
		t.Fatalf("claim was changed or removed: %v", err)
	}
}

func TestPVCReuseRejectsConflictingClaims(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*runnerv1.StartWorkloadRequest, *corev1.PersistentVolumeClaim)
	}{
		{"other-volume", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			p.Labels[volumeKeyLabelKey] = "volume-b"
		}},
		{"missing-volume-key", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			delete(p.Labels, volumeKeyLabelKey)
		}},
		{"foreign-manager", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			p.Labels[managedByLabelKey] = "other"
		}},
		{"garbage-collection-owner", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "old-pod", UID: types.UID(uuid.NewString())}}
		}},
		{"missing-manager", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			delete(p.Labels, managedByLabelKey)
		}},
		{"foreign-workload-manager", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			p.Labels[workloadManagedByLabelKey] = "other"
		}},
		{"foreign-instance", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			p.Labels["agent-instance-id"] = "instance-b"
		}},
		{"missing-instance", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			delete(p.Labels, "agent-instance-id")
		}},
		{"omitted-instance", func(r *runnerv1.StartWorkloadRequest, _ *corev1.PersistentVolumeClaim) {
			delete(r.Labels, "agent-instance-id")
		}},
		{"foreign-agent", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			p.Labels["agent-id"] = "agent-b"
		}},
		{"sandbox-owner", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			p.Labels["sandbox-id"] = "sandbox-b"
		}},
		{"sandbox-user", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			p.Labels["sandbox-owner-id"] = "user-b"
		}},
		{"terminating", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			now := metav1.Now()
			p.DeletionTimestamp = &now
		}},
		{"lost", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			p.Status.Phase = corev1.ClaimLost
		}},
		{"block-device", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			p.Spec.VolumeMode = ptr.To(corev1.PersistentVolumeBlock)
		}},
		{"read-only", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			p.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}
		}},
		{"too-small", func(_ *runnerv1.StartWorkloadRequest, p *corev1.PersistentVolumeClaim) {
			p.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("512Mi")
		}},
		{"explicit-class-mismatch", func(r *runnerv1.StartWorkloadRequest, _ *corev1.PersistentVolumeClaim) {
			r.Volumes[0].StorageClass = "fast"
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			req, claim := pvcFixture(t)
			tt.mutate(req, claim)
			client := fake.NewSimpleClientset(claim.DeepCopy())
			_, err := pvcTestServer(client).StartWorkload(context.Background(), req)
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("expected FailedPrecondition before mounting a conflicting PVC, got %v", err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" || action.GetResource().Resource != "persistentvolumeclaims" {
					t.Fatalf("conflict caused a write or unrelated request: %s %s", action.GetVerb(), action.GetResource().Resource)
				}
			}
			assertPVCUnchanged(t, client, claim)
		})
	}
}

func TestPVCReuseValidatesRequestsBeforeLookup(t *testing.T) {
	for _, invalid := range []string{"missing-key", "empty-key", "workload-key-only", "override-owner", "empty-owner", "invalid-label", "invalid-size", "zero-size", "unknown-class"} {
		for _, existing := range []bool{false, true} {
			name := invalid + "/new"
			if existing {
				name = invalid + "/existing"
			}
			t.Run(name, func(t *testing.T) {
				req, claim := pvcFixture(t)
				switch invalid {
				case "missing-key":
					delete(req.Volumes[0].Labels, volumeKeyLabelKey)
				case "empty-key":
					req.Volumes[0].Labels[volumeKeyLabelKey] = ""
				case "workload-key-only":
					req.Labels[volumeKeyLabelKey] = "volume-a"
					req.Volumes[0].Labels = nil
				case "override-owner":
					req.Volumes[0].Labels["agent-instance-id"] = "instance-b"
				case "empty-owner":
					req.Labels["agent-instance-id"] = ""
				case "invalid-label":
					req.Volumes[0].Labels["bad label"] = "value"
				case "invalid-size":
					req.Volumes[0].Size = "invalid"
				case "zero-size":
					req.Volumes[0].Size = "0"
				case "unknown-class":
					req.Volumes[0].StorageClass = "unknown"
				}
				client := fake.NewSimpleClientset()
				if existing {
					client = fake.NewSimpleClientset(claim)
				}
				_, err := pvcTestServer(client).StartWorkload(context.Background(), req)
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("expected invalid request before PVC lookup, got %v", err)
				}
				if len(client.Actions()) != 0 {
					t.Fatalf("invalid request reached Kubernetes: %#v", client.Actions())
				}
			})
		}
	}
}

func TestPVCReuseRetainsSameOwnerAcrossWorkloads(t *testing.T) {
	for _, kind := range []string{"agent", "sandbox", "larger", "explicit-class", "cluster-default", "single-pod"} {
		t.Run(kind, func(t *testing.T) {
			req, claim := pvcFixture(t)
			switch kind {
			case "sandbox":
				for _, labels := range []map[string]string{req.Labels, claim.Labels} {
					delete(labels, "agent-instance-id")
					delete(labels, "agent-id")
					labels["sandbox-id"], labels["sandbox-owner-id"] = "sandbox-a", "user-a"
				}
			case "larger":
				claim.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("2Gi")
			case "explicit-class":
				req.Volumes[0].StorageClass = "fast"
				claim.Spec.StorageClassName = ptr.To("fast-ssd")
			case "cluster-default":
				req.Volumes[0].StorageClass = "default"
			case "single-pod":
				claim.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}
			}
			req.Volumes[0].Size = "1024Mi"
			client := fake.NewSimpleClientset(claim.DeepCopy())
			server := pvcTestServer(client)
			for turn := 0; turn < 2; turn++ {
				req.WorkloadId = uuid.NewString()
				req.Labels[workloadKeyLabelKey], req.Labels["thread-id"] = uuid.NewString(), uuid.NewString()
				resp, err := server.StartWorkload(context.Background(), req)
				if err != nil {
					t.Fatal(err)
				}
				pod, err := client.CoreV1().Pods("default").Get(context.Background(), podNameFromID(resp.Id), metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if len(pod.Spec.Volumes) != 1 || pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != claim.Name {
					t.Fatal("wrong workspace mounted")
				}
				assertPVCUnchanged(t, client, claim)
			}
			for _, action := range client.Actions() {
				if action.GetResource().Resource == "persistentvolumeclaims" && action.GetVerb() != "get" {
					t.Fatal("reuse must not relabel, resize or recreate the claim")
				}
			}
		})
	}
}

func TestPVCReuseChecksCreateRace(t *testing.T) {
	for _, sameOwner := range []bool{false, true} {
		name := "foreign-owner"
		if sameOwner {
			name = "same-owner"
		}
		t.Run(name, func(t *testing.T) {
			req, claim := pvcFixture(t)
			if !sameOwner {
				claim.Labels[volumeKeyLabelKey] = "volume-b"
			}
			client := fake.NewSimpleClientset(claim.DeepCopy())
			reads := 0
			client.PrependReactor("get", "persistentvolumeclaims", func(kubetesting.Action) (bool, runtime.Object, error) {
				reads++
				if reads == 1 {
					return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "persistentvolumeclaims"}, claim.Name)
				}
				return false, nil, nil
			})
			_, err := pvcTestServer(client).StartWorkload(context.Background(), req)
			if sameOwner && err != nil || !sameOwner && status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("create race result: %v", err)
			}
			if reads != 2 {
				t.Fatalf("expected a fresh ownership check after AlreadyExists, got %d reads", reads)
			}
			assertPVCUnchanged(t, client, claim)
		})
	}
}

func TestPVCReuseChecksAdmittedIdentity(t *testing.T) {
	req, claim := pvcFixture(t)
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "persistentvolumeclaims", func(action kubetesting.Action) (bool, runtime.Object, error) {
		admitted := action.(kubetesting.CreateAction).GetObject().(*corev1.PersistentVolumeClaim).DeepCopy()
		admitted.Labels = maps.Clone(admitted.Labels)
		admitted.Labels[volumeKeyLabelKey] = "foreign-volume"
		admitted.UID = claim.UID
		if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), admitted, "default"); err != nil {
			t.Fatal(err)
		}
		return true, admitted, nil
	})
	_, err := pvcTestServer(client).StartWorkload(context.Background(), req)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("changed admitted owner was accepted: %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource != "persistentvolumeclaims" || action.GetVerb() == "delete" {
			t.Fatal("admission mismatch must not mount or delete the claim")
		}
	}
}

func TestPVCReuseDoesNotTreatOtherErrorsAsConflicts(t *testing.T) {
	for _, stage := range []string{"get", "create"} {
		t.Run(stage, func(t *testing.T) {
			req, _ := pvcFixture(t)
			client := fake.NewSimpleClientset()
			client.PrependReactor(stage, "persistentvolumeclaims", func(kubetesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewServiceUnavailable("unavailable")
			})
			_, err := pvcTestServer(client).StartWorkload(context.Background(), req)
			if err == nil || !strings.Contains(err.Error(), "unavailable") {
				t.Fatalf("API failure lost: %v", err)
			}
			want := 1
			if stage == "create" {
				want = 2
			}
			if len(client.Actions()) != want {
				t.Fatalf("unexpected recovery or Pod creation after uncertain API failure: %#v", client.Actions())
			}
		})
	}
}

func TestPVCReuseDoesNotAdoptAfterDeniedCreate(t *testing.T) {
	req, claim := pvcFixture(t)
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "persistentvolumeclaims", func(kubetesting.Action) (bool, runtime.Object, error) {
		if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), claim.DeepCopy(), "default"); err != nil {
			t.Fatal(err)
		}
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "persistentvolumeclaims"}, claim.Name, errors.New("exceeded quota"))
	})
	_, err := pvcTestServer(client).StartWorkload(context.Background(), req)
	if status.Code(err) != codes.PermissionDenied || len(client.Actions()) != 2 {
		t.Fatalf("a denied create must not trigger claim adoption or Pod creation: %v", err)
	}
	assertPVCUnchanged(t, client, claim)
}
