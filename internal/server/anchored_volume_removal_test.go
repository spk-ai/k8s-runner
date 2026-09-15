package server

import (
	"context"
	"maps"
	"testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

func anchoredRemovalFixture(t *testing.T, sandbox bool) (*Server, *fake.Clientset, *runnerv1.VolumeListItem) {
	t.Helper()
	client := preparedTestClient()
	s := preparedTestServer(client)
	a := reserveTestAnchor(t, s, anchorTestIntent(runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, sandbox))
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "volume-" + a.ResourceId, Namespace: "default", UID: types.UID(uuid.NewString()),
		ResourceVersion: "17", Labels: maps.Clone(a.IdentityLabels),
	}}
	if err := attachResourceAnchor(&pvc.ObjectMeta, a); err != nil {
		t.Fatal(err)
	}
	if err := client.Tracker().Add(pvc); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	return s, client, &runnerv1.VolumeListItem{InstanceId: pvc.Name, InstanceUid: string(pvc.UID),
		VolumeKey: a.ResourceId, BackendId: a.BackendId, IdentityLabels: maps.Clone(a.IdentityLabels), Anchor: a}
}

func TestAnchoredVolumeRemovalRejectsInvalidTargets(t *testing.T) {
	for name, mutate := range map[string]func(*runnerv1.RemoveVolumeAnchoredRequest){
		"no-target":   func(r *runnerv1.RemoveVolumeAnchoredRequest) { r.Expected = nil },
		"no-owner":    func(r *runnerv1.RemoveVolumeAnchoredRequest) { r.Expected.Anchor = nil },
		"bad-pvc-uid": func(r *runnerv1.RemoveVolumeAnchoredRequest) { r.Expected.InstanceUid = "not-a-uid" },
		"wrong-kind": func(r *runnerv1.RemoveVolumeAnchoredRequest) {
			r.Expected.Anchor.Kind = runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD
		},
		"wrong-backend":   func(r *runnerv1.RemoveVolumeAnchoredRequest) { r.Expected.Anchor.BackendId = "other" },
		"unknown-request": func(r *runnerv1.RemoveVolumeAnchoredRequest) { r.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown-target":  func(r *runnerv1.RemoveVolumeAnchoredRequest) { r.Expected.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			s, client, expected := anchoredRemovalFixture(t, false)
			r := &runnerv1.RemoveVolumeAnchoredRequest{Expected: expected}
			mutate(r)
			if resp, err := s.RemoveVolumeAnchored(context.Background(), r); resp != nil || status.Code(err) != codes.InvalidArgument || len(client.Actions()) != 0 {
				t.Fatalf("invalid target reached Kubernetes: %v", err)
			}
		})
	}
	if _, err := preparedTestServer(preparedTestClient()).RemoveVolumeAnchored(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal("nil removal request accepted")
	}
}

func TestAnchoredVolumeRemovalPreservesHeldAndTerminatingPVC(t *testing.T) {
	for _, held := range []bool{true, false} {
		t.Run(map[bool]string{true: "workload-hold", false: "ordinary-finalizer"}[held], func(t *testing.T) {
			s, client, expected := anchoredRemovalFixture(t, false)
			ctx := context.Background()
			pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, expected.InstanceId, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pvc.Finalizers = []string{"fixture.invalid/retain"}
			if held {
				pvc.Finalizers = []string{preparedHoldPrefix + uuid.NewString()}
			}
			if err := client.Tracker().Update(preparedClaimResource, pvc, "default"); err != nil {
				t.Fatal(err)
			}
			client.PrependReactor("delete", "persistentvolumeclaims", func(action kubetesting.Action) (bool, runtime.Object, error) {
				if held {
					t.Fatal("held PVC deletion attempted")
				}
				now := metav1.Now()
				pvc.DeletionTimestamp = &now
				return true, nil, client.Tracker().Update(preparedClaimResource, pvc, "default")
			})
			client.ClearActions()
			for i := 0; i < 2; i++ {
				resp, err := s.RemoveVolumeAnchored(ctx, &runnerv1.RemoveVolumeAnchoredRequest{Expected: expected})
				if err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING {
					t.Fatalf("held retirement: %v", err)
				}
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" && action.GetResource().Resource == "configmaps" {
					t.Fatal("owner deleted before finalized PVC absence")
				}
			}
			if held {
				assertNoVolumeMutation(t, client)
			}
		})
	}
}

func TestAnchoredVolumeRetirementMarkerRejectsPreparation(t *testing.T) {
	s, client, expected := anchoredRemovalFixture(t, false)
	ctx := context.Background()
	if _, err := s.RemoveVolumeAnchored(ctx, &runnerv1.RemoveVolumeAnchoredRequest{Expected: expected}); err != nil {
		t.Fatal(err)
	}
	owner, err := client.CoreV1().ConfigMaps("default").Get(ctx, resourceAnchorName(expected.Anchor), metav1.GetOptions{})
	if err != nil || owner.Annotations[volumeRetirementPVCAnnotation] != expected.InstanceUid {
		t.Fatal("retirement did not pin original PVC")
	}
	if err := s.requireResourceAnchor(ctx, expected.Anchor); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("retiring owner authorized preparation")
	}
	intent := proto.Clone(expected.Anchor).(*runnerv1.ResourceAnchor)
	intent.InstanceUid = ""
	if _, err := s.ReserveResourceAnchor(ctx, &runnerv1.ReserveResourceAnchorRequest{Intent: intent}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("retiring owner was reserved again")
	}
	if err := matchResourceAnchor(owner, expected.Anchor, "default"); err == nil {
		t.Fatal("unchanged active-owner validator accepted retirement marker")
	}
}

func TestAnchoredVolumeRemovalLostAcknowledgement(t *testing.T) {
	for _, operation := range []string{"marker", "pvc-delete", "owner-delete"} {
		t.Run(operation, func(t *testing.T) {
			s, client, expected := anchoredRemovalFixture(t, false)
			ctx := context.Background()
			request := &runnerv1.RemoveVolumeAnchoredRequest{Expected: expected}
			resource := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
			verb := "delete"
			if operation == "marker" {
				verb = "patch"
			}
			if operation == "pvc-delete" {
				resource = preparedClaimResource
			}
			if operation == "owner-delete" {
				if _, err := s.RemoveVolumeAnchored(ctx, request); err != nil {
					t.Fatal(err)
				}
			}
			lost := false
			client.PrependReactor(verb, resource.Resource, func(action kubetesting.Action) (bool, runtime.Object, error) {
				if lost {
					return false, nil, nil
				}
				lost = true
				name := resourceAnchorName(expected.Anchor)
				if operation == "marker" {
					obj, err := client.Tracker().Get(resource, "default", name)
					if err != nil {
						t.Fatal(err)
					}
					owner := obj.(*corev1.ConfigMap)
					owner.Annotations[resourceAnchorVersionAnnotation] = volumeRetirementVersion
					owner.Annotations[volumeRetirementPVCAnnotation] = expected.InstanceUid
					if err := client.Tracker().Update(resource, owner, "default"); err != nil {
						t.Fatal(err)
					}
				} else {
					if operation == "pvc-delete" {
						name = expected.InstanceId
					}
					if err := client.Tracker().Delete(resource, "default", name); err != nil {
						t.Fatal(err)
					}
				}
				return true, nil, apierrors.NewTimeoutError("lost fixture ACK", 0)
			})
			client.ClearActions()
			if resp, err := s.RemoveVolumeAnchored(ctx, request); err == nil || resp != nil || !lost {
				t.Fatal("lost ACK was treated as confirmation")
			}
			if operation == "marker" {
				for _, action := range client.Actions() {
					if action.GetVerb() == "delete" {
						t.Fatal("lost marker ACK authorized deletion")
					}
				}
			}
			s = preparedTestServer(client)
			settled := false
			for i := 0; i < 3; i++ {
				resp, err := s.RemoveVolumeAnchored(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				if resp.State == runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT {
					settled = true
					break
				}
			}
			if !settled {
				t.Fatal("fresh reads failed to recover original retirement")
			}
		})
	}
}

func TestAnchoredVolumeRemovalLateChildrenAreNotAdopted(t *testing.T) {
	for _, mode := range []string{"active-owner", "retiring-owner", "absent-owner", "foreign-owner"} {
		t.Run(mode, func(t *testing.T) {
			s, client, expected := anchoredRemovalFixture(t, false)
			ctx := context.Background()
			pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, expected.InstanceId, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			request := &runnerv1.RemoveVolumeAnchoredRequest{Expected: expected}
			if mode != "active-owner" {
				if _, err := s.RemoveVolumeAnchored(ctx, request); err != nil {
					t.Fatal(err)
				}
				if mode == "absent-owner" {
					if _, err := s.RemoveVolumeAnchored(ctx, request); err != nil {
						t.Fatal(err)
					}
				}
			}
			pvc.UID, pvc.ResourceVersion = types.UID(uuid.NewString()), "24"
			if mode == "foreign-owner" {
				pvc.OwnerReferences[0].UID = types.UID(uuid.NewString())
			}
			if mode == "active-owner" {
				if err := client.Tracker().Update(preparedClaimResource, pvc, "default"); err != nil {
					t.Fatal(err)
				}
			} else if err := client.Tracker().Add(pvc); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			resp, err := s.RemoveVolumeAnchored(ctx, request)
			if mode == "active-owner" || mode == "foreign-owner" {
				if resp != nil || status.Code(err) != codes.FailedPrecondition {
					t.Fatal("replacement adopted")
				}
				assertNoVolumeMutation(t, client)
				return
			}
			if err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING {
				t.Fatal("late child falsely absent")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" && action.GetResource().Resource == "persistentvolumeclaims" {
					t.Fatal("late child directly deleted by discovered identity")
				}
			}
			if resp, err := s.RemoveVolumeAnchored(ctx, request); err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING {
				t.Fatal("owner absence substituted for late child cleanup")
			}
			// The fake has no GC. Real collection is verified by the live fixture.
			if err := client.Tracker().Delete(preparedClaimResource, "default", pvc.Name); err != nil {
				t.Fatal(err)
			}
			if resp, err := s.RemoveVolumeAnchored(ctx, request); err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT {
				t.Fatal("observed late-child absence did not settle")
			}
		})
	}
}

func TestAnchoredVolumeRemovalSeparatesPVCAndOwnerAbsence(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		t.Run(map[bool]string{false: "agent", true: "sandbox"}[sandbox], func(t *testing.T) {
			s, client, expected := anchoredRemovalFixture(t, sandbox)
			ctx := context.Background()
			for step, state := range []runnerv1.VolumeRemovalState{
				runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING,
				runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING,
				runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT,
				runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT,
			} {
				response, err := s.RemoveVolumeAnchored(ctx, &runnerv1.RemoveVolumeAnchoredRequest{Expected: expected})
				if err != nil || response.GetState() != state || response.GetBackendId() != expected.BackendId || !proto.Equal(response.GetAnchor(), expected.Anchor) {
					t.Fatalf("step %d: incorrect retirement evidence: %v, %v", step, response, err)
				}
				_, err = client.CoreV1().PersistentVolumeClaims("default").Get(ctx, expected.InstanceId, metav1.GetOptions{})
				if !apierrors.IsNotFound(err) {
					t.Fatal("exact PVC was not retired")
				}
				_, err = client.CoreV1().ConfigMaps("default").Get(ctx, resourceAnchorName(expected.Anchor), metav1.GetOptions{})
				if step == 0 && err != nil || step > 0 && !apierrors.IsNotFound(err) {
					t.Fatal("owner removal did not follow separate PVC absence")
				}
			}
			deletes := 0
			for _, action := range client.Actions() {
				if action.GetVerb() != "delete" {
					continue
				}
				deletes++
				options := action.(kubetesting.DeleteAction).GetDeleteOptions()
				uid := expected.InstanceUid
				if action.GetResource().Resource == "configmaps" {
					uid = expected.Anchor.InstanceUid
				}
				if options.Preconditions == nil || options.Preconditions.UID == nil || string(*options.Preconditions.UID) != uid ||
					options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion == "" || options.GracePeriodSeconds != nil {
					t.Fatal("retirement lacked exact conditional non-forced deletion")
				}
			}
			if deletes != 2 {
				t.Fatalf("wanted one PVC and one owner deletion, got %d", deletes)
			}
		})
	}
}
