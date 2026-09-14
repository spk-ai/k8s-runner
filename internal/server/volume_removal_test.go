package server

import (
	"context"
	"errors"
	"maps"
	"strconv"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

func checkedVolumePVC() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "vol-1", Namespace: "default", UID: "original-uid", ResourceVersion: "17",
		Labels: map[string]string{
			managedByLabelKey: managedByLabelValue, volumeKeyLabelKey: "volume-1",
			workloadManagedByLabelKey: workloadManagedByLabelValue,
			"managed-by":              "agents-orchestrator", "agent-instance-id": "instance-1", "agent-id": "class-1",
		},
	}}
}

func checkedVolumeRequest(pvc *corev1.PersistentVolumeClaim) *runnerv1.RemoveVolumeCheckedRequest {
	return &runnerv1.RemoveVolumeCheckedRequest{Expected: &runnerv1.VolumeListItem{
		InstanceId: pvc.Name, InstanceUid: string(pvc.UID), VolumeKey: pvc.Labels[volumeKeyLabelKey],
		IdentityLabels: maps.Clone(pvc.Labels),
	}}
}

func assertNoVolumeMutation(t *testing.T, clientset *fake.Clientset) {
	t.Helper()
	for _, action := range clientset.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("unexpected mutation: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func TestLegacyVolumeDeletionFailsClosed(t *testing.T) {
	for _, present := range []bool{false, true} {
		for _, force := range []bool{false, true} {
			t.Run(strconv.FormatBool(present)+"/force="+strconv.FormatBool(force), func(t *testing.T) {
				var objects []runtime.Object
				if present {
					objects = append(objects, checkedVolumePVC())
				}
				server, clientset := storageServer(t, objects...)
				resp, err := server.RemoveVolume(context.Background(), &runnerv1.RemoveVolumeRequest{VolumeName: "vol-1", Force: force})
				if status.Code(err) != codes.FailedPrecondition || resp != nil {
					t.Fatalf("legacy deletion accepted: %v, %v", resp, err)
				}
				if len(clientset.Actions()) != 0 {
					t.Fatal("legacy request contacted Kubernetes")
				}
			})
		}
	}
}

func TestRemoveWorkloadCannotBypassCheckedVolumeDeletion(t *testing.T) {
	server, clientset := storageServer(t, checkedVolumePVC())
	resp, err := server.RemoveWorkload(context.Background(), &runnerv1.RemoveWorkloadRequest{
		WorkloadId: "workload-1", RemoveVolumes: true, Force: true,
	})
	if status.Code(err) != codes.FailedPrecondition || resp != nil || len(clientset.Actions()) != 0 {
		t.Fatalf("must reject before deleting Pod, Secrets or volumes: %v, %v, actions=%v", resp, err, clientset.Actions())
	}
}

func TestRemoveVolumeCheckedRejectsIncompleteTarget(t *testing.T) {
	tests := map[string]func(*runnerv1.RemoveVolumeCheckedRequest){
		"missing target":  func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected = nil },
		"missing name":    func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.InstanceId = "" },
		"invalid name":    func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.InstanceId = "../other" },
		"padded name":     func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.InstanceId += " " },
		"missing uid":     func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.InstanceUid = "" },
		"padded uid":      func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.InstanceUid += " " },
		"missing key":     func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.VolumeKey = "" },
		"invalid key":     func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.VolumeKey = "a/b" },
		"missing labels":  func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.IdentityLabels = nil },
		"foreign manager": func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.IdentityLabels[managedByLabelKey] = "foreign" },
		"mismatched key":  func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.IdentityLabels[volumeKeyLabelKey] = "other" },
		"turn label":      func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.IdentityLabels[workloadIDLabelKey] = "turn-1" },
		"unknown label":   func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.IdentityLabels["secret"] = "not-identity" },
		"invalid label":   func(r *runnerv1.RemoveVolumeCheckedRequest) { r.Expected.IdentityLabels["agent-id"] = " " },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			server, clientset := storageServer(t)
			req := checkedVolumeRequest(checkedVolumePVC())
			mutate(req)
			resp, err := server.RemoveVolumeChecked(context.Background(), req)
			if resp != nil || status.Code(err) != codes.InvalidArgument || len(clientset.Actions()) != 0 {
				t.Fatalf("invalid target reached backend: %v, %v, actions=%v", resp, err, clientset.Actions())
			}
		})
	}
	server, clientset := storageServer(t)
	if resp, err := server.RemoveVolumeChecked(context.Background(), nil); resp != nil || status.Code(err) != codes.InvalidArgument || len(clientset.Actions()) != 0 {
		t.Fatalf("nil request: %v, %v", resp, err)
	}
}

func TestRemoveVolumeCheckedDoesNotRetarget(t *testing.T) {
	tests := map[string]func(*corev1.PersistentVolumeClaim){
		"replacement uid":    func(p *corev1.PersistentVolumeClaim) { p.UID = "replacement" },
		"missing uid":        func(p *corev1.PersistentVolumeClaim) { p.UID = "" },
		"missing revision":   func(p *corev1.PersistentVolumeClaim) { p.ResourceVersion = "" },
		"foreign manager":    func(p *corev1.PersistentVolumeClaim) { p.Labels[managedByLabelKey] = "foreign" },
		"foreign key":        func(p *corev1.PersistentVolumeClaim) { p.Labels[volumeKeyLabelKey] = "other" },
		"missing ownership":  func(p *corev1.PersistentVolumeClaim) { delete(p.Labels, "agent-instance-id") },
		"different instance": func(p *corev1.PersistentVolumeClaim) { p.Labels["agent-instance-id"] = "other" },
		"different class":    func(p *corev1.PersistentVolumeClaim) { p.Labels["agent-id"] = "other" },
		"new sandbox owner":  func(p *corev1.PersistentVolumeClaim) { p.Labels["sandbox-id"] = "other" },
		"controller owner":   func(p *corev1.PersistentVolumeClaim) { p.OwnerReferences = []metav1.OwnerReference{{UID: "pod-uid"}} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			pvc := checkedVolumePVC()
			req := checkedVolumeRequest(pvc)
			mutate(pvc)
			server, clientset := storageServer(t, pvc)
			resp, err := server.RemoveVolumeChecked(context.Background(), req)
			if resp != nil || status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("target mismatch was accepted: %v, %v", resp, err)
			}
			assertNoVolumeMutation(t, clientset)
		})
	}
}

func TestRemoveVolumeCheckedUsesAtomicPreconditions(t *testing.T) {
	pvc := checkedVolumePVC()
	req := checkedVolumeRequest(pvc)
	pvc.Labels[workloadIDLabelKey] = "later-turn"
	pvc.Labels[workloadKeyLabelKey] = "later-workload"
	pvc.Labels["diagnostic"] = "not-persistent-identity"
	pvc.ResourceVersion = "42"
	server, clientset := storageServer(t, pvc)
	deletes := 0
	clientset.PrependReactor("delete", "persistentvolumeclaims", func(action kubetesting.Action) (bool, runtime.Object, error) {
		deletes++
		options := action.(kubetesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != pvc.UID || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != "42" {
			t.Fatalf("missing fresh atomic preconditions: %+v", options)
		}
		if options.GracePeriodSeconds != nil {
			t.Fatal("checked deletion must not force termination")
		}
		return false, nil, nil
	})
	resp, err := server.RemoveVolumeChecked(context.Background(), req)
	if err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING || deletes != 1 {
		t.Fatalf("delete: %v, %v, calls=%d", resp, err, deletes)
	}
	resp, err = server.RemoveVolumeChecked(context.Background(), req)
	if err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT || deletes != 1 {
		t.Fatalf("absence confirmation: %v, %v", resp, err)
	}
	pvc.UID = "replacement"
	if _, err := clientset.CoreV1().PersistentVolumeClaims("default").Create(context.Background(), pvc, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	resp, err = server.RemoveVolumeChecked(context.Background(), req)
	if resp != nil || status.Code(err) != codes.FailedPrecondition || deletes != 1 {
		t.Fatalf("late retry deleted a replacement: %v, %v", resp, err)
	}
}

func TestRemoveVolumeCheckedBackendFailuresNeverConfirmAbsence(t *testing.T) {
	resource := schema.GroupResource{Resource: "persistentvolumeclaims"}
	for _, stage := range []string{"get", "delete"} {
		for name, failure := range map[string]struct {
			err  error
			code codes.Code
		}{
			"conflict":    {apierrors.NewConflict(resource, "vol-1", errors.New("changed")), codes.Aborted},
			"forbidden":   {apierrors.NewForbidden(resource, "vol-1", errors.New("denied")), codes.PermissionDenied},
			"unavailable": {errors.New("lost response"), codes.Internal},
		} {
			t.Run(stage+"/"+name, func(t *testing.T) {
				pvc := checkedVolumePVC()
				server, clientset := storageServer(t, pvc)
				calls := 0
				clientset.PrependReactor(stage, "persistentvolumeclaims", func(kubetesting.Action) (bool, runtime.Object, error) {
					calls++
					return true, nil, failure.err
				})
				resp, err := server.RemoveVolumeChecked(context.Background(), checkedVolumeRequest(pvc))
				if resp != nil || status.Code(err) != failure.code || calls != 1 {
					t.Fatalf("failure retried/confirmed: %v, %v, calls=%d", resp, err, calls)
				}
			})
		}
	}
	server, clientset := storageServer(t, checkedVolumePVC())
	clientset.PrependReactor("delete", "persistentvolumeclaims", func(kubetesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(resource, "vol-1")
	})
	resp, err := server.RemoveVolumeChecked(context.Background(), checkedVolumeRequest(checkedVolumePVC()))
	if err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING {
		t.Fatalf("delete-time NotFound is not an absence observation: %v, %v", resp, err)
	}
}

func TestVolumeInventoryIncludesOnlyPersistentIdentity(t *testing.T) {
	pvc := checkedVolumePVC()
	expected := checkedVolumeRequest(pvc).Expected
	pvc.Labels[workloadIDLabelKey] = "turn-1"
	pvc.Labels[workloadKeyLabelKey] = "workload-1"
	pvc.Labels["private"] = "excluded"
	pvc.Annotations = map[string]string{"private": "excluded"}
	server, _ := storageServer(t, pvc)
	resp, err := server.ListVolumes(context.Background(), &runnerv1.ListVolumesRequest{})
	if err != nil || len(resp.GetVolumes()) != 1 || !proto.Equal(expected, resp.Volumes[0]) {
		t.Fatalf("inventory identity: %v, %v", resp, err)
	}
}
