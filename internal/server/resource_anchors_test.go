package server

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

func anchorTestIntent(kind runnerv1.ResourceAnchorKind, sandbox bool) *runnerv1.ResourceAnchor {
	a := &runnerv1.ResourceAnchor{Kind: kind, ResourceId: uuid.NewString(), BackendId: testVolumeBackend,
		IdentityLabels: map[string]string{managedByLabelKey: managedByLabelValue, workloadManagedByLabelKey: "agents-orchestrator", "managed-by": "agents-orchestrator"}}
	if sandbox {
		a.IdentityLabels["sandbox-id"], a.IdentityLabels["sandbox-owner-id"] = uuid.NewString(), uuid.NewString()
	} else {
		a.IdentityLabels["agent-instance-id"], a.IdentityLabels["agent-id"] = uuid.NewString(), uuid.NewString()
		if kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD {
			a.IdentityLabels["thread-id"] = uuid.NewString()
		}
	}
	if kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME {
		a.IdentityLabels[volumeKeyLabelKey] = a.ResourceId
	}
	return a
}

func reserveTestAnchor(t *testing.T, s *Server, intent *runnerv1.ResourceAnchor) *runnerv1.ResourceAnchor {
	t.Helper()
	response, err := s.ReserveResourceAnchor(context.Background(), &runnerv1.ReserveResourceAnchorRequest{Intent: intent})
	if err != nil || response.GetAnchor() == nil || !validPreparedID(response.Anchor.InstanceUid) {
		t.Fatalf("metadata reservation failed: %v", err)
	}
	actual := proto.Clone(response.Anchor).(*runnerv1.ResourceAnchor)
	actual.InstanceUid = ""
	if !proto.Equal(actual, intent) {
		t.Fatal("reservation changed durable intent")
	}
	return response.Anchor
}

func assertAnchorOnlyActions(t *testing.T, client *fake.Clientset) {
	t.Helper()
	for _, action := range client.Actions() {
		resource := action.GetResource().Resource
		if resource != "configmaps" && !(action.GetVerb() == "get" && resource == "namespaces") {
			t.Fatalf("metadata reservation touched %s %s", action.GetVerb(), resource)
		}
	}
}

func TestResourceAnchorReservationIsMetadataOnly(t *testing.T) {
	for _, kind := range []runnerv1.ResourceAnchorKind{runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME} {
		for _, sandbox := range []bool{false, true} {
			t.Run(kind.String()+map[bool]string{false: "/agent", true: "/sandbox"}[sandbox], func(t *testing.T) {
				client := preparedTestClient()
				s := preparedTestServer(client)
				intent := anchorTestIntent(kind, sandbox)
				anchor := reserveTestAnchor(t, s, intent)
				if !proto.Equal(anchor, reserveTestAnchor(t, preparedTestServer(client), intent)) {
					t.Fatal("metadata retry replaced its owner UID")
				}
				assertAnchorOnlyActions(t, client)
			})
		}
	}
}

func TestResourceAnchorLostCreateReplyRetainsIdentity(t *testing.T) {
	client := preparedTestClient()
	intent := anchorTestIntent(runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, false)
	uid := types.UID(uuid.NewString())
	client.PrependReactor("create", "configmaps", func(action kubetesting.Action) (bool, runtime.Object, error) {
		cm := action.(kubetesting.CreateAction).GetObject().(*corev1.ConfigMap)
		cm.UID, cm.ResourceVersion = uid, "1"
		if err := client.Tracker().Add(cm); err != nil {
			t.Fatal(err)
		}
		return true, nil, errors.New("fixture lost create reply")
	})
	if _, err := preparedTestServer(client).ReserveResourceAnchor(context.Background(), &runnerv1.ReserveResourceAnchorRequest{Intent: intent}); err == nil {
		t.Fatal("lost metadata reply reported success")
	}
	anchor := reserveTestAnchor(t, preparedTestServer(client), intent)
	if anchor.InstanceUid != string(uid) {
		t.Fatal("metadata recovery adopted another owner generation")
	}
	assertAnchorOnlyActions(t, client)
}

func TestResourceAnchorRejectsInvalidOrConflictingIntent(t *testing.T) {
	for _, change := range []string{"kind", "id", "uid", "backend", "manager", "extra-label", "mixed-owner", "missing-owner", "missing-thread", "volume-key", "unknown-fields"} {
		t.Run(change, func(t *testing.T) {
			client := preparedTestClient()
			intent := anchorTestIntent(runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, false)
			switch change {
			case "kind":
				intent.Kind = runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_UNSPECIFIED
			case "id":
				intent.ResourceId = "not-uuid"
			case "uid":
				intent.InstanceUid = uuid.NewString()
			case "backend":
				intent.BackendId = "wrong-backend"
			case "manager":
				intent.IdentityLabels[managedByLabelKey] = "other"
			case "extra-label":
				intent.IdentityLabels["private-data"] = "not-identity"
			case "mixed-owner":
				intent.IdentityLabels["sandbox-id"] = uuid.NewString()
			case "missing-owner":
				delete(intent.IdentityLabels, "agent-instance-id")
			case "missing-thread":
				delete(intent.IdentityLabels, "thread-id")
			case "volume-key":
				intent.IdentityLabels[volumeKeyLabelKey] = uuid.NewString()
			case "unknown-fields":
				intent.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
			}
			if response, err := preparedTestServer(client).ReserveResourceAnchor(context.Background(), &runnerv1.ReserveResourceAnchorRequest{Intent: intent}); err == nil || response != nil {
				t.Fatal("invalid metadata intent accepted")
			}
			assertNoVolumeMutation(t, client)
		})
	}
	client := preparedTestClient()
	intent := anchorTestIntent(runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, false)
	reserveTestAnchor(t, preparedTestServer(client), intent)
	intent.IdentityLabels = maps.Clone(intent.IdentityLabels)
	intent.IdentityLabels["agent-instance-id"] = uuid.NewString()
	client.ClearActions()
	if response, err := preparedTestServer(client).ReserveResourceAnchor(context.Background(), &runnerv1.ReserveResourceAnchorRequest{Intent: intent}); err == nil || response != nil {
		t.Fatal("existing reservation retargeted to another owner")
	}
	assertNoVolumeMutation(t, client)
}

func TestResourceAnchorRejectsCorruptNativeIdentity(t *testing.T) {
	for _, change := range []string{"nil", "uid", "revision", "mutable", "owner", "labels", "data", "invalid-json", "oversize", "pod-pin", "activation-without-pin", "activation-other-pod", "deleting"} {
		t.Run(change, func(t *testing.T) {
			client := preparedTestClient()
			s := preparedTestServer(client)
			intent := anchorTestIntent(runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, false)
			anchor := reserveTestAnchor(t, s, intent)
			cm, err := client.CoreV1().ConfigMaps("default").Get(context.Background(), resourceAnchorName(anchor), metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "nil":
				client.PrependReactor("get", "configmaps", func(kubetesting.Action) (bool, runtime.Object, error) { return true, nil, nil })
			case "uid":
				cm.UID = types.UID(uuid.NewString())
			case "revision":
				cm.ResourceVersion = ""
			case "mutable":
				cm.Immutable = nil
			case "owner":
				cm.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "foreign", UID: types.UID(uuid.NewString())}}
			case "labels":
				cm.Labels["agent-instance-id"] = uuid.NewString()
			case "data":
				cm.Data["unexpected"] = "fixture"
			case "invalid-json":
				cm.Data["identity.json"] = "invalid"
			case "oversize":
				cm.Data["identity.json"] = strings.Repeat(" ", 16*1024+1)
			case "pod-pin":
				cm.Annotations[resourceAnchorPodAnnotation] = "invalid"
			case "activation-without-pin":
				cm.Annotations[resourceAnchorActivationAnnotation] = uuid.NewString()
			case "activation-other-pod":
				cm.Annotations[resourceAnchorPodAnnotation], cm.Annotations[resourceAnchorActivationAnnotation] = uuid.NewString(), uuid.NewString()
			case "deleting":
				now := metav1.Now()
				cm.DeletionTimestamp = &now
			}
			if err := client.Tracker().Update(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, cm, "default"); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			if err := s.requireResourceAnchor(context.Background(), anchor); err == nil {
				t.Fatal("invalid bound anchor remained authoritative")
			}
			if change != "uid" {
				if response, err := s.ReserveResourceAnchor(context.Background(), &runnerv1.ReserveResourceAnchorRequest{Intent: intent}); err == nil || response != nil {
					t.Fatal("invalid native anchor was adopted")
				}
			}
			if change != "deleting" {
				if response, err := s.RemoveWorkloadAnchor(context.Background(), &runnerv1.RemoveWorkloadAnchorRequest{Expected: anchor}); err == nil || response != nil {
					t.Fatal("invalid bound anchor was deleted")
				}
			}
			assertNoVolumeMutation(t, client)
		})
	}
}

func TestResourceAnchorRemovalRequiresConfirmedPodLookup(t *testing.T) {
	client := preparedTestClient()
	s := preparedTestServer(client)
	anchor := reserveTestAnchor(t, s, anchorTestIntent(runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, false))
	client.PrependReactor("get", "pods", func(kubetesting.Action) (bool, runtime.Object, error) { return true, nil, nil })
	client.ClearActions()
	if response, err := s.RemoveWorkloadAnchor(context.Background(), &runnerv1.RemoveWorkloadAnchorRequest{Expected: anchor}); err == nil || response != nil {
		t.Fatal("unconfirmed Pod lookup allowed anchor revocation")
	}
	assertNoVolumeMutation(t, client)
}

func TestResourceAnchorRemovalPinsUIDAndCannotDeleteVolumeAnchor(t *testing.T) {
	client := preparedTestClient()
	s := preparedTestServer(client)
	volume := reserveTestAnchor(t, s, anchorTestIntent(runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, false))
	workload := reserveTestAnchor(t, s, anchorTestIntent(runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, false))
	for _, expected := range []*runnerv1.ResourceAnchor{volume, proto.Clone(workload).(*runnerv1.ResourceAnchor)} {
		if expected.Kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD {
			expected.InstanceUid = uuid.NewString()
		}
		client.ClearActions()
		if response, err := s.RemoveWorkloadAnchor(context.Background(), &runnerv1.RemoveWorkloadAnchorRequest{Expected: expected}); err == nil || response != nil {
			t.Fatal("wrong scope or UID permitted anchor deletion")
		}
		assertNoVolumeMutation(t, client)
	}
	client.ClearActions()
	removed, err := s.RemoveWorkloadAnchor(context.Background(), &runnerv1.RemoveWorkloadAnchorRequest{Expected: workload})
	if err != nil || !proto.Equal(removed.GetAnchor(), workload) || removed.GetState() != runnerv1.ResourceAnchorRemovalState_RESOURCE_ANCHOR_REMOVAL_STATE_PENDING {
		t.Fatalf("exact anchor deletion failed: %v", err)
	}
	deletes := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			options := action.(kubetesting.DeleteAction).GetDeleteOptions()
			if action.GetResource().Resource != "configmaps" || options.Preconditions == nil || options.Preconditions.UID == nil || string(*options.Preconditions.UID) != workload.InstanceUid || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion == "" {
				t.Fatal("anchor deletion lacked exact atomic preconditions")
			}
			deletes++
		}
	}
	if deletes != 1 {
		t.Fatal("anchor deletion not observed")
	}
	removed, err = preparedTestServer(client).RemoveWorkloadAnchor(context.Background(), &runnerv1.RemoveWorkloadAnchorRequest{Expected: workload})
	if err != nil || !proto.Equal(removed.GetAnchor(), workload) || removed.GetState() != runnerv1.ResourceAnchorRemovalState_RESOURCE_ANCHOR_REMOVAL_STATE_ABSENT {
		t.Fatalf("anchor absence was not recoverable: %v", err)
	}
	intent := proto.Clone(volume).(*runnerv1.ResourceAnchor)
	intent.InstanceUid = ""
	if !proto.Equal(volume, reserveTestAnchor(t, s, intent)) {
		t.Fatal("workload cleanup changed persistent volume anchor")
	}
	if _, err := s.RemoveWorkloadAnchor(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal("nil anchor removal accepted")
	}
}
