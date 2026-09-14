package server

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/version"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

var preparedPodResource = schema.GroupVersionResource{Version: "v1", Resource: "pods"}
var preparedClaimResource = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}

func preparedTestServer(client *fake.Clientset) *Server {
	return New(Options{Clientset: client, Namespace: "default", StorageSize: "1Mi", Logger: zap.NewNop()})
}

func preparedTestRequest() *runnerv1.PrepareWorkloadRequest {
	return &runnerv1.PrepareWorkloadRequest{BackendId: testVolumeBackend, Workload: &runnerv1.StartWorkloadRequest{
		WorkloadId: uuid.NewString(), Labels: map[string]string{"agent-instance-id": "owner-1", "agent-id": "class-1", "managed-by": "agents-orchestrator"},
		Main:    &runnerv1.ContainerSpec{Image: "fixture.invalid/no-execution", Mounts: []*runnerv1.VolumeMount{{Volume: "workspace", MountPath: "/workspace"}}},
		Volumes: []*runnerv1.VolumeSpec{{Name: "workspace", PersistentName: "workspace", Kind: runnerv1.VolumeKind_VOLUME_KIND_NAMED, Labels: map[string]string{volumeKeyLabelKey: "volume-1"}}},
	}}
}

func preparedTestClient() *fake.Clientset {
	client := fake.NewSimpleClientset(volumeTestNamespace())
	client.Discovery().(*fakediscovery.FakeDiscovery).FakedServerVersion = &version.Info{GitVersion: "v1.33.1"}
	client.PrependReactor("create", "*", func(action kubetesting.Action) (bool, runtime.Object, error) {
		object := action.(kubetesting.CreateAction).GetObject()
		accessor, err := meta.Accessor(object)
		if err != nil {
			return true, nil, err
		}
		accessor.SetUID(types.UID(uuid.NewString()))
		accessor.SetResourceVersion("1")
		return false, nil, nil
	})
	return client
}

func TestPreparedWorkloadRequiresStableSchedulingGateSupport(t *testing.T) {
	for _, value := range []string{"v1.25.0", "v1.29.9", "v1.30.0-alpha.1", "unknown", ""} {
		t.Run(value, func(t *testing.T) {
			client := preparedTestClient()
			client.Discovery().(*fakediscovery.FakeDiscovery).FakedServerVersion = &version.Info{GitVersion: value}
			_, err := preparedTestServer(client).PrepareWorkload(context.Background(), preparedTestRequest())
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("unconfirmed gate support: %v", err)
			}
			assertNoVolumeMutation(t, client)
		})
	}
}

func prepareTestWorkload(t *testing.T) (*Server, *fake.Clientset, *runnerv1.PrepareWorkloadResponse) {
	t.Helper()
	client := preparedTestClient()
	s := preparedTestServer(client)
	response, err := s.PrepareWorkload(context.Background(), preparedTestRequest())
	if err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	return s, client, response
}

func preparedTestPod(t *testing.T, client *fake.Clientset, binding *runnerv1.WorkloadBinding) *corev1.Pod {
	t.Helper()
	pod, err := client.CoreV1().Pods("default").Get(context.Background(), podNameFromID(binding.WorkloadId), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return pod
}

func preparedTestClaim(t *testing.T, client *fake.Clientset) *corev1.PersistentVolumeClaim {
	t.Helper()
	pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "workspace", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return pvc
}

func TestPreparedWorkloadStartsGated(t *testing.T) {
	_, client, response := prepareTestWorkload(t)
	binding := response.Binding
	pod := preparedTestPod(t, client, binding)
	pvc := preparedTestClaim(t, client)
	if pod.Spec.NodeName != "" || !hasPreparedGate(pod) || pod.Annotations[preparedStateAnnotation] != "prepared" ||
		binding.InstanceUid != string(pod.UID) || len(binding.Volumes) != 1 || binding.Volumes[0].InstanceUid != string(pvc.UID) || binding.BackendId != testVolumeBackend {
		t.Fatalf("preparation lost its gate or physical identities: %v", binding)
	}
	if len(pvc.Finalizers) != 0 {
		t.Fatal("preparation must not claim execution before activation")
	}
}

func TestPreparedSecretsAreOwnedByExactPod(t *testing.T) {
	for _, failure := range []string{"", "replacement", "forbidden"} {
		t.Run(failure, func(t *testing.T) {
			client := preparedTestClient()
			req := preparedTestRequest()
			req.Workload.InlineFiles = map[string][]byte{"/fixture-marker": []byte("not-a-credential")}
			req.Workload.Main.InlineFileMounts = []*runnerv1.InlineFileMount{{Path: "/fixture-marker"}}
			if failure != "" {
				client.PrependReactor("patch", "secrets", func(action kubetesting.Action) (bool, runtime.Object, error) {
					name := action.(kubetesting.PatchAction).GetName()
					if failure == "forbidden" {
						return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, name, errors.New("fixture"))
					}
					object, err := client.Tracker().Get(action.GetResource(), "default", name)
					if err != nil {
						return true, nil, err
					}
					object.(*corev1.Secret).UID = types.UID(uuid.NewString())
					if err := client.Tracker().Update(action.GetResource(), object, "default"); err != nil {
						return true, nil, err
					}
					return false, nil, nil
				})
			}
			response, err := preparedTestServer(client).PrepareWorkload(context.Background(), req)
			if (err != nil) != (failure != "") {
				t.Fatalf("secret ownership result: %v", err)
			}
			pod, err := client.CoreV1().Pods("default").Get(context.Background(), podNameFromID(req.Workload.WorkloadId), metav1.GetOptions{})
			if err != nil || !hasPreparedGate(pod) {
				t.Fatalf("secret binding failure allowed execution: %v", err)
			}
			secrets, err := client.CoreV1().Secrets("default").List(context.Background(), metav1.ListOptions{})
			if err != nil || len(secrets.Items) != 1 {
				t.Fatalf("temporary secret lost: %v", err)
			}
			owners := secrets.Items[0].OwnerReferences
			if failure == "" {
				if response.Binding.InstanceUid != string(pod.UID) || len(owners) != 1 || owners[0].Kind != "Pod" || owners[0].UID != pod.UID || owners[0].Name != pod.Name {
					t.Fatal("secret bound to wrong incarnation")
				}
			} else if response != nil || len(owners) != 0 {
				t.Fatal("foreign/rejected secret ownership accepted")
			}
		})
	}
}

func TestPreparedResumeNeverCreatesMissingOrReplacedVolume(t *testing.T) {
	for _, failure := range []string{"missing", "replacement", "terminating", "lost-between-reads", "wrong-backend"} {
		t.Run(failure, func(t *testing.T) {
			s, client, first := prepareTestWorkload(t)
			pvc := preparedTestClaim(t, client)
			req := preparedTestRequest()
			req.ExpectedVolumes = first.Binding.Volumes
			switch failure {
			case "missing":
				_ = client.Tracker().Delete(preparedClaimResource, "default", pvc.Name)
			case "replacement":
				pvc.UID = types.UID(uuid.NewString())
				_ = client.Tracker().Update(preparedClaimResource, pvc, "default")
			case "terminating":
				now := metav1.Now()
				pvc.DeletionTimestamp = &now
				_ = client.Tracker().Update(preparedClaimResource, pvc, "default")
			case "lost-between-reads":
				reads := 0
				client.PrependReactor("get", "persistentvolumeclaims", func(kubetesting.Action) (bool, runtime.Object, error) {
					reads++
					if reads == 2 {
						_ = client.Tracker().Delete(preparedClaimResource, "default", pvc.Name)
					}
					return false, nil, nil
				})
			case "wrong-backend":
				req.BackendId = "other"
				req.ExpectedVolumes = nil
			}
			client.ClearActions()
			if response, err := s.PrepareWorkload(context.Background(), req); err == nil || response != nil {
				t.Fatal("unsafe resume was prepared")
			}
			assertNoVolumeMutation(t, client)
		})
	}
}

func TestPreparedWorkloadRejectsInvalidBindings(t *testing.T) {
	cases := map[string]func(*runnerv1.WorkloadBinding){
		"workload":         func(b *runnerv1.WorkloadBinding) { b.WorkloadId = "" },
		"uid":              func(b *runnerv1.WorkloadBinding) { b.InstanceUid = "" },
		"noncanonical-uid": func(b *runnerv1.WorkloadBinding) { b.InstanceUid = " " + b.InstanceUid },
		"backend":          func(b *runnerv1.WorkloadBinding) { b.BackendId = "" },
		"mixed-backend":    func(b *runnerv1.WorkloadBinding) { b.Volumes[0].BackendId = "other" },
		"duplicate":        func(b *runnerv1.WorkloadBinding) { b.Volumes = append(b.Volumes, b.Volumes[0]) },
		"volume-uid":       func(b *runnerv1.WorkloadBinding) { b.Volumes[0].InstanceUid = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s, client, preparation := prepareTestWorkload(t)
			mutate(preparation.Binding)
			_, activateErr := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: preparation.Binding})
			_, removeErr := s.RemovePreparedWorkload(context.Background(), &runnerv1.RemovePreparedWorkloadRequest{Expected: preparation.Binding})
			if status.Code(activateErr) != codes.InvalidArgument || status.Code(removeErr) != codes.InvalidArgument || len(client.Actions()) != 0 {
				t.Fatalf("invalid contract reached Kubernetes: %v %v", activateErr, removeErr)
			}
		})
	}
}

func TestPreparedActivationRejectsChangedObjects(t *testing.T) {
	for _, failure := range []string{"pod-uid", "pod-owner", "pod-volume", "pod-terminating", "missing-gate", "scheduled", "claim-uid", "claim-owner", "claim-terminating", "missing-claim", "namespace"} {
		t.Run(failure, func(t *testing.T) {
			s, client, preparation := prepareTestWorkload(t)
			pod, pvc := preparedTestPod(t, client, preparation.Binding), preparedTestClaim(t, client)
			now := metav1.Now()
			switch failure {
			case "pod-uid":
				pod.UID = types.UID(uuid.NewString())
			case "pod-owner":
				pod.Labels[workloadIDLabelKey] = uuid.NewString()
			case "pod-volume":
				pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "other"
			case "pod-terminating":
				pod.DeletionTimestamp = &now
			case "missing-gate":
				pod.Spec.SchedulingGates = nil
			case "scheduled":
				pod.Spec.NodeName = "node"
			case "claim-uid":
				pvc.UID = types.UID(uuid.NewString())
			case "claim-owner":
				pvc.Labels["agent-instance-id"] = "other"
			case "claim-terminating":
				pvc.DeletionTimestamp = &now
			case "namespace":
				ns := volumeTestNamespace()
				ns.UID = "replacement"
				_ = client.Tracker().Update(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}, ns, "")
			}
			_ = client.Tracker().Update(preparedPodResource, pod, "default")
			_ = client.Tracker().Update(preparedClaimResource, pvc, "default")
			if failure == "missing-claim" {
				_ = client.Tracker().Delete(preparedClaimResource, "default", pvc.Name)
			}
			client.ClearActions()
			if result, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: preparation.Binding}); err == nil || result != nil {
				t.Fatal("changed identity activated")
			}
			assertNoVolumeMutation(t, client)
		})
	}
}

func TestPreparedActivationHoldsVolumesAndIsIdempotentAcrossRestart(t *testing.T) {
	s, client, preparation := prepareTestWorkload(t)
	pod, pvc := preparedTestPod(t, client, preparation.Binding), preparedTestClaim(t, client)
	pvc.Finalizers = []string{"example.org/retain", "kubernetes.io/pvc-protection"}
	pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: "example.org/review"})
	_ = client.Tracker().Update(preparedPodResource, pod, "default")
	_ = client.Tracker().Update(preparedClaimResource, pvc, "default")
	resp, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: preparation.Binding})
	if err != nil || !proto.Equal(resp.GetBinding(), preparation.Binding) {
		t.Fatalf("activate: %v", err)
	}
	pod, pvc = preparedTestPod(t, client, preparation.Binding), preparedTestClaim(t, client)
	if hasPreparedGate(pod) || len(pod.Spec.SchedulingGates) != 1 || pod.Spec.SchedulingGates[0].Name != "example.org/review" ||
		!slices.Equal(pvc.Finalizers, []string{"example.org/retain", "kubernetes.io/pvc-protection", preparedHoldPrefix + preparation.Binding.InstanceUid}) {
		t.Fatal("activation lost a gate or volume protection")
	}
	client.ClearActions()
	if _, err := preparedTestServer(client).ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: preparation.Binding}); err != nil {
		t.Fatal(err)
	}
	assertNoVolumeMutation(t, client)
}

func TestPreparedActivationCASRejectsReplacementAndRevisionRaces(t *testing.T) {
	for _, resource := range []string{"pods", "persistentvolumeclaims"} {
		for _, race := range []string{"replacement", "revision", "terminating"} {
			t.Run(resource+"/"+race, func(t *testing.T) {
				s, client, preparation := prepareTestWorkload(t)
				client.PrependReactor("patch", resource, func(action kubetesting.Action) (bool, runtime.Object, error) {
					patch := action.(kubetesting.PatchAction)
					object, err := client.Tracker().Get(action.GetResource(), "default", patch.GetName())
					if err != nil {
						return true, nil, err
					}
					accessor, _ := meta.Accessor(object)
					if race == "replacement" {
						accessor.SetUID(types.UID(uuid.NewString()))
					} else {
						accessor.SetResourceVersion("2")
					}
					if race == "terminating" {
						now := metav1.Now()
						accessor.SetDeletionTimestamp(&now)
					}
					if err := client.Tracker().Update(action.GetResource(), object, "default"); err != nil {
						return true, nil, err
					}
					return false, nil, nil
				})
				if _, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: preparation.Binding}); err == nil {
					t.Fatal("racing PATCH succeeded")
				}
				pod := preparedTestPod(t, client, preparation.Binding)
				if !hasPreparedGate(pod) {
					t.Fatal("racing activation removed gate")
				}
				pvc := preparedTestClaim(t, client)
				held := slices.Contains(pvc.Finalizers, preparedHoldPrefix+preparation.Binding.InstanceUid)
				if held != (resource == "pods") {
					t.Fatal("hold acquisition/retention mismatch")
				}
			})
		}
	}
}

func TestPreparedLostActivationReplyRetainsHoldAndDoesNotReplay(t *testing.T) {
	s, client, preparation := prepareTestWorkload(t)
	client.PrependReactor("patch", "pods", func(action kubetesting.Action) (bool, runtime.Object, error) {
		object, err := client.Tracker().Get(preparedPodResource, "default", podNameFromID(preparation.Binding.WorkloadId))
		if err != nil {
			return true, nil, err
		}
		pod := object.(*corev1.Pod)
		pod.Spec.SchedulingGates, pod.Annotations[preparedStateAnnotation] = nil, "active"
		if err := client.Tracker().Update(preparedPodResource, pod, "default"); err != nil {
			return true, nil, err
		}
		return true, nil, apierrors.NewTimeoutError("fixture lost reply", 1)
	})
	if _, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: preparation.Binding}); err == nil {
		t.Fatal("lost reply reported success")
	}
	if !slices.Contains(preparedTestClaim(t, client).Finalizers, preparedHoldPrefix+preparation.Binding.InstanceUid) {
		t.Fatal("uncertain activation dropped hold")
	}
	client.ClearActions()
	if _, err := preparedTestServer(client).ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: preparation.Binding}); err != nil {
		t.Fatal(err)
	}
	assertNoVolumeMutation(t, client)
}

func TestPreparedRemovalRequiresAbsenceBeforeReleasingOnlyOwnHold(t *testing.T) {
	s, client, preparation := prepareTestWorkload(t)
	pvc := preparedTestClaim(t, client)
	pvc.Finalizers = []string{"example.org/retain"}
	_ = client.Tracker().Update(preparedClaimResource, pvc, "default")
	if _, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: preparation.Binding}); err != nil {
		t.Fatal(err)
	}
	request := &runnerv1.RemovePreparedWorkloadRequest{Expected: preparation.Binding}
	response, err := s.RemovePreparedWorkload(context.Background(), request)
	if err != nil || response.GetState() != runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_PENDING {
		t.Fatalf("delete acknowledgement is not absence: %v", err)
	}
	if !slices.Contains(preparedTestClaim(t, client).Finalizers, preparedHoldPrefix+preparation.Binding.InstanceUid) {
		t.Fatal("DELETE released protection too early")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "pods" {
			preconditions := action.(kubetesting.DeleteAction).GetDeleteOptions().Preconditions
			if preconditions == nil || preconditions.UID == nil || string(*preconditions.UID) != preparation.Binding.InstanceUid || preconditions.ResourceVersion == nil {
				t.Fatal("delete omitted incarnation/revision")
			}
		}
	}
	response, err = preparedTestServer(client).RemovePreparedWorkload(context.Background(), request)
	if err != nil || response.GetState() != runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT {
		t.Fatalf("confirm absence: %v", err)
	}
	if !slices.Equal(preparedTestClaim(t, client).Finalizers, []string{"example.org/retain"}) {
		t.Fatal("cleanup removed another controller's finalizer")
	}
	if _, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: preparation.Binding}); status.Code(err) != codes.NotFound {
		t.Fatalf("late activation: %v", err)
	}
}

func TestPreparedRemovalRejectsWrongBackendOrPodIncarnation(t *testing.T) {
	for _, mismatch := range []string{"backend", "pod", "forbidden"} {
		t.Run(mismatch, func(t *testing.T) {
			s, client, preparation := prepareTestWorkload(t)
			if mismatch == "backend" {
				preparation.Binding.BackendId = "other"
				preparation.Binding.Volumes[0].BackendId = "other"
			} else if mismatch == "pod" {
				pod := preparedTestPod(t, client, preparation.Binding)
				pod.UID = types.UID(uuid.NewString())
				_ = client.Tracker().Update(preparedPodResource, pod, "default")
			} else {
				client.PrependReactor("get", "pods", func(kubetesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "fixture", errors.New("fixture"))
				})
			}
			client.ClearActions()
			if _, err := s.RemovePreparedWorkload(context.Background(), &runnerv1.RemovePreparedWorkloadRequest{Expected: preparation.Binding}); err == nil {
				t.Fatal("wrong/unknown target removed")
			}
			assertNoVolumeMutation(t, client)
		})
	}
}

func TestPreparedWorkloadRefusesLegacyRemoval(t *testing.T) {
	s, client, preparation := prepareTestWorkload(t)
	_, stopErr := s.StopWorkload(context.Background(), &runnerv1.StopWorkloadRequest{WorkloadId: preparation.Binding.WorkloadId})
	_, removeErr := s.RemoveWorkload(context.Background(), &runnerv1.RemoveWorkloadRequest{WorkloadId: preparation.Binding.WorkloadId, Force: true})
	if status.Code(stopErr) != codes.FailedPrecondition || status.Code(removeErr) != codes.FailedPrecondition {
		t.Fatalf("legacy target accepted: %v %v", stopErr, removeErr)
	}
	assertNoVolumeMutation(t, client)
}

func TestPreparedVolumeSerializesActivationsAcrossPods(t *testing.T) {
	s, client, first := prepareTestWorkload(t)
	req := preparedTestRequest()
	req.ExpectedVolumes = first.Binding.Volumes
	second, err := s.PrepareWorkload(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: first.Binding}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: second.Binding}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("overlapping activation: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := s.RemovePreparedWorkload(context.Background(), &runnerv1.RemovePreparedWorkloadRequest{Expected: first.Binding}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: second.Binding}); err != nil {
		t.Fatal(err)
	}
	if first.Binding.InstanceUid == second.Binding.InstanceUid || first.Binding.Volumes[0].InstanceUid != second.Binding.Volumes[0].InstanceUid || hasPreparedGate(preparedTestPod(t, client, second.Binding)) {
		t.Fatal("resume lost its own identity or workspace")
	}
}
