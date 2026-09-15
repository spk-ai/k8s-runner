package server

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

func anchoredTestRequest(t *testing.T, s *Server, sandbox, zero bool) *runnerv1.PrepareAnchoredWorkloadRequest {
	t.Helper()
	p := preparedTestRequest()
	work := anchorTestIntent(runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, sandbox)
	p.Workload.WorkloadId, p.Workload.Labels = work.ResourceId, maps.Clone(work.IdentityLabels)
	delete(p.Workload.Labels, managedByLabelKey)
	delete(p.Workload.Labels, workloadManagedByLabelKey)
	req := &runnerv1.PrepareAnchoredWorkloadRequest{Preparation: p, WorkloadAnchor: reserveTestAnchor(t, s, work)}
	if zero {
		p.Workload.Volumes, p.Workload.Main.Mounts = nil, nil
		return req
	}
	volume := &runnerv1.ResourceAnchor{Kind: runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, ResourceId: uuid.NewString(), BackendId: p.BackendId, IdentityLabels: volumeIdentityLabels(work.IdentityLabels)}
	volume.IdentityLabels[volumeKeyLabelKey] = volume.ResourceId
	p.Workload.Volumes[0].Labels[volumeKeyLabelKey] = volume.ResourceId
	req.VolumeAnchors = []*runnerv1.ResourceAnchor{reserveTestAnchor(t, s, volume)}
	return req
}

func prepareAnchoredTest(t *testing.T, s *Server, req *runnerv1.PrepareAnchoredWorkloadRequest) *runnerv1.WorkloadBinding {
	t.Helper()
	response, err := s.PrepareAnchoredWorkload(context.Background(), req)
	if err != nil || response.GetBinding() == nil || !proto.Equal(response.Binding.Anchor, req.WorkloadAnchor) {
		t.Fatalf("anchored preparation failed: %v", err)
	}
	return response.Binding
}

func TestAnchoredWorkloadSeparatesComputeAndPersistentVolumeOwners(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, zero := range []bool{false, true} {
			t.Run(map[bool]string{false: "agent", true: "sandbox"}[sandbox]+map[bool]string{false: "/volume", true: "/zero"}[zero], func(t *testing.T) {
				client := preparedTestClient()
				s := preparedTestServer(client)
				req := anchoredTestRequest(t, s, sandbox, zero)
				first := prepareAnchoredTest(t, s, req)
				pod := preparedTestPod(t, client, first)
				if !reflect.DeepEqual(pod.OwnerReferences, resourceAnchorOwners(req.WorkloadAnchor)) || !hasPreparedGate(pod) {
					t.Fatal("Pod lacked atomic workload owner or execution gate")
				}
				if !zero {
					claim := preparedTestClaim(t, client)
					if len(first.Volumes) != 1 || !proto.Equal(first.Volumes[0].Anchor, req.VolumeAnchors[0]) || !reflect.DeepEqual(claim.OwnerReferences, resourceAnchorOwners(req.VolumeAnchors[0])) || claim.OwnerReferences[0].UID == pod.OwnerReferences[0].UID {
						t.Fatal("persistent workspace was tied to transient compute lifetime")
					}
				}
				if _, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: first}); err != nil {
					t.Fatal(err)
				}
				if _, err := s.RemoveWorkloadAnchor(context.Background(), &runnerv1.RemoveWorkloadAnchorRequest{Expected: req.WorkloadAnchor}); err == nil {
					t.Fatal("anchor revocation replaced exact removal of potentially executing Pod")
				}
				for _, expected := range []runnerv1.PreparedWorkloadRemovalState{runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_PENDING, runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT} {
					removed, err := s.RemovePreparedWorkload(context.Background(), &runnerv1.RemovePreparedWorkloadRequest{Expected: first})
					if err != nil || removed.GetState() != expected {
						t.Fatalf("anchored exact Pod removal failed: %v", err)
					}
				}
				if _, err := s.RemoveWorkloadAnchor(context.Background(), &runnerv1.RemoveWorkloadAnchorRequest{Expected: req.WorkloadAnchor}); err != nil {
					t.Fatal(err)
				}
				intent := proto.Clone(req.WorkloadAnchor).(*runnerv1.ResourceAnchor)
				intent.ResourceId, intent.InstanceUid = uuid.NewString(), ""
				req.WorkloadAnchor = reserveTestAnchor(t, s, intent)
				req.Preparation.Workload.WorkloadId, req.Preparation.ExpectedVolumes = intent.ResourceId, first.Volumes
				second := prepareAnchoredTest(t, preparedTestServer(client), req)
				if second.InstanceUid == first.InstanceUid || len(second.Volumes) != len(first.Volumes) || !zero && !proto.Equal(second.Volumes[0], first.Volumes[0]) {
					t.Fatal("follow-up did not preserve only the persistent workspace")
				}
			})
		}
	}
}

func TestAnchoredWorkloadRevokedOrMissingAnchorCannotCreateResources(t *testing.T) {
	for _, which := range []string{"workload", "volume", "replaced-workload", "missing-volume-set", "wrong-volume-owner"} {
		t.Run(which, func(t *testing.T) {
			client := preparedTestClient()
			s := preparedTestServer(client)
			req := anchoredTestRequest(t, s, false, false)
			switch which {
			case "workload", "replaced-workload":
				if _, err := s.RemoveWorkloadAnchor(context.Background(), &runnerv1.RemoveWorkloadAnchorRequest{Expected: req.WorkloadAnchor}); err != nil {
					t.Fatal(err)
				}
				if which == "replaced-workload" {
					intent := proto.Clone(req.WorkloadAnchor).(*runnerv1.ResourceAnchor)
					intent.InstanceUid = ""
					if reserveTestAnchor(t, s, intent).InstanceUid == req.WorkloadAnchor.InstanceUid {
						t.Fatal("fixture did not replace owner generation")
					}
				}
			case "volume":
				if err := client.CoreV1().ConfigMaps("default").Delete(context.Background(), resourceAnchorName(req.VolumeAnchors[0]), metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			case "missing-volume-set":
				req.VolumeAnchors = nil
			case "wrong-volume-owner":
				req.VolumeAnchors[0].IdentityLabels["agent-instance-id"] = uuid.NewString()
			}
			client.ClearActions()
			if response, err := s.PrepareAnchoredWorkload(context.Background(), req); err == nil || response != nil {
				t.Fatal("unverified anchor authorized resource creation")
			}
			assertNoVolumeMutation(t, client)
		})
	}
}

func TestAnchoredVolumesCannotBeSilentlyDowngraded(t *testing.T) {
	client := preparedTestClient()
	s := preparedTestServer(client)
	req := anchoredTestRequest(t, s, false, false)
	binding := prepareAnchoredTest(t, s, req)
	client.ClearActions()
	if _, err := s.RemoveVolumeBound(context.Background(), &runnerv1.RemoveVolumeBoundRequest{Expected: binding.Volumes[0]}); err == nil {
		t.Fatal("unanchored deletion API accepted anchored volume")
	}
	req.Preparation.Workload.WorkloadId = uuid.NewString()
	if _, err := s.PrepareWorkload(context.Background(), req.Preparation); err == nil {
		t.Fatal("legacy preparation adopted anchored workspace")
	}
	assertNoVolumeMutation(t, client)
	listed, err := s.ListVolumes(context.Background(), &runnerv1.ListVolumesRequest{})
	if err != nil || len(listed.GetVolumes()) != 1 || !proto.Equal(listed.Volumes[0], binding.Volumes[0]) {
		t.Fatalf("inventory dropped immutable volume anchor: %v", err)
	}
	if err := client.CoreV1().PersistentVolumeClaims("default").Delete(context.Background(), binding.Volumes[0].InstanceId, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	if _, err := s.RemoveVolumeBound(context.Background(), &runnerv1.RemoveVolumeBoundRequest{Expected: binding.Volumes[0]}); err == nil {
		t.Fatal("missing PVC bypassed anchored volume lifetime")
	}
	assertNoVolumeMutation(t, client)
}

func TestAnchoredRevocationWinsBeforeActivationClaim(t *testing.T) {
	client := preparedTestClient()
	s := preparedTestServer(client)
	req := anchoredTestRequest(t, s, false, false)
	binding := prepareAnchoredTest(t, s, req)
	// Reactors run under a client-local lock. A second client shares the API
	// state without reentering that lock during the competing handler.
	peer := fake.NewSimpleClientset()
	peer.PrependReactor("*", "*", kubetesting.ObjectReaction(client.Tracker()))
	competing := preparedTestServer(peer)
	intercepted := false
	client.PrependReactor("patch", "configmaps", func(kubetesting.Action) (bool, runtime.Object, error) {
		intercepted = true
		if _, err := competing.RemoveWorkloadAnchor(context.Background(), &runnerv1.RemoveWorkloadAnchorRequest{Expected: req.WorkloadAnchor}); err != nil {
			t.Fatal(err)
		}
		return false, nil, nil
	})
	client.ClearActions()
	if response, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: binding}); err == nil || response != nil || !intercepted {
		t.Fatal("activation did not contend with revocation on its native anchor")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" && action.GetResource().Resource != "configmaps" {
			t.Fatal("revoked activation mutated a Pod or PVC")
		}
	}
	if !hasPreparedGate(preparedTestPod(t, client, binding)) {
		t.Fatal("revoked preparation became executable")
	}
}

func TestAnchoredActivationClaimPreventsStaleRevocation(t *testing.T) {
	client := preparedTestClient()
	s := preparedTestServer(client)
	req := anchoredTestRequest(t, s, false, false)
	binding := prepareAnchoredTest(t, s, req)
	peer := fake.NewSimpleClientset()
	peer.PrependReactor("*", "*", kubetesting.ObjectReaction(client.Tracker()))
	competing := preparedTestServer(peer)
	resource := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	intercepted, conflicts := false, 0
	client.PrependReactor("get", "pods", func(action kubetesting.Action) (bool, runtime.Object, error) {
		if intercepted {
			return false, nil, nil
		}
		intercepted = true
		before, err := client.Tracker().Get(preparedPodResource, "default", action.(kubetesting.GetAction).GetName())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := competing.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: binding}); err != nil {
			t.Fatal(err)
		}
		obj, err := client.Tracker().Get(resource, "default", resourceAnchorName(req.WorkloadAnchor))
		if err != nil {
			t.Fatal(err)
		}
		cm := obj.(*corev1.ConfigMap)
		// The simple tracker does not implement API-server revision increments.
		cm.ResourceVersion = "2"
		if err := client.Tracker().Update(resource, cm, "default"); err != nil {
			t.Fatal(err)
		}
		return true, before, nil
	})
	client.PrependReactor("delete", "configmaps", func(action kubetesting.Action) (bool, runtime.Object, error) {
		op := action.(kubetesting.DeleteAction)
		obj, err := client.Tracker().Get(resource, "default", op.GetName())
		if err != nil {
			t.Fatal(err)
		}
		cm, preconditions := obj.(*corev1.ConfigMap), op.GetDeleteOptions().Preconditions
		if preconditions == nil || preconditions.ResourceVersion == nil || *preconditions.ResourceVersion != cm.ResourceVersion {
			conflicts++
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, op.GetName(), nil)
		}
		return false, nil, nil
	})
	if response, err := s.RemoveWorkloadAnchor(context.Background(), &runnerv1.RemoveWorkloadAnchorRequest{Expected: req.WorkloadAnchor}); err == nil || response != nil || !intercepted || conflicts != 1 {
		t.Fatal("stale revocation did not conflict with committed activation claim")
	}
	cm, err := client.CoreV1().ConfigMaps("default").Get(context.Background(), resourceAnchorName(req.WorkloadAnchor), metav1.GetOptions{})
	if err != nil || cm.Annotations["agyn.io/anchor-activation"] != binding.InstanceUid {
		t.Fatal("activation was not durably pinned before releasing the gate")
	}
}

func TestAnchoredPreparationPinsOnePodBeforeCredentials(t *testing.T) {
	client := preparedTestClient()
	s := preparedTestServer(client)
	req := anchoredTestRequest(t, s, false, false)
	req.Preparation.Workload.InlineFiles = map[string][]byte{"/marker": []byte("fixture")}
	req.Preparation.Workload.Main.InlineFileMounts = []*runnerv1.InlineFileMount{{Path: "/marker"}}
	checked := false
	client.PrependReactor("create", "secrets", func(action kubetesting.Action) (bool, runtime.Object, error) {
		secret := action.(kubetesting.CreateAction).GetObject().(*corev1.Secret)
		obj, err := client.Tracker().Get(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, "default", resourceAnchorName(req.WorkloadAnchor))
		if err != nil || len(secret.OwnerReferences) != 1 || obj.(*corev1.ConfigMap).Annotations[resourceAnchorPodAnnotation] != string(secret.OwnerReferences[0].UID) {
			t.Fatal("credentials preceded durable Pod-incarnation selection")
		}
		checked = true
		return false, nil, nil
	})
	binding := prepareAnchoredTest(t, s, req)
	if !checked {
		t.Fatal("credential boundary not exercised")
	}
	for i := 0; i < 2; i++ {
		if _, err := s.RemovePreparedWorkload(context.Background(), &runnerv1.RemovePreparedWorkloadRequest{Expected: binding}); err != nil {
			t.Fatal(err)
		}
	}
	client.ClearActions()
	if response, err := preparedTestServer(client).PrepareAnchoredWorkload(context.Background(), req); err == nil || response != nil {
		t.Fatal("consumed anchor authorized replacement Pod creation")
	}
	assertNoVolumeMutation(t, client)
}

func TestAnchoredLostClaimReplyRetainsExactPodAuthority(t *testing.T) {
	for _, activation := range []bool{false, true} {
		t.Run(map[bool]string{false: "selection", true: "activation"}[activation], func(t *testing.T) {
			client := preparedTestClient()
			s := preparedTestServer(client)
			req := anchoredTestRequest(t, s, false, false)
			req.Preparation.Workload.InlineFiles = map[string][]byte{"/marker": []byte("fixture")}
			req.Preparation.Workload.Main.InlineFileMounts = []*runnerv1.InlineFileMount{{Path: "/marker"}}
			var binding *runnerv1.WorkloadBinding
			if activation {
				binding = prepareAnchoredTest(t, s, req)
			}
			committed := false
			client.PrependReactor("patch", "configmaps", func(action kubetesting.Action) (bool, runtime.Object, error) {
				if committed {
					return false, nil, nil
				}
				handled, _, err := kubetesting.ObjectReaction(client.Tracker())(action)
				if err != nil || !handled {
					t.Fatalf("claim fixture failed to commit: %v", err)
				}
				committed = true
				return true, nil, errors.New("fixture lost claim reply")
			})
			client.ClearActions()
			if activation {
				if response, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: binding}); err == nil || response != nil {
					t.Fatal("lost activation claim reply reported success")
				}
			} else {
				if response, err := s.PrepareAnchoredWorkload(context.Background(), req); err == nil || response != nil {
					t.Fatal("lost selection reply reported preparation success")
				}
			}
			if !committed {
				t.Fatal("claim write boundary not reached")
			}
			for _, action := range client.Actions() {
				if action.Matches("patch", "pods") || action.Matches("patch", "persistentvolumeclaims") || action.Matches("create", "secrets") {
					t.Fatal("unacknowledged claim authorized downstream writes")
				}
			}
			pod, err := client.CoreV1().Pods("default").Get(context.Background(), podNameFromID(req.Preparation.Workload.WorkloadId), metav1.GetOptions{})
			if err != nil || !hasPreparedGate(pod) {
				t.Fatal("lost reply removed the execution gate")
			}
			if activation {
				if _, err := preparedTestServer(client).RemoveWorkloadAnchor(context.Background(), &runnerv1.RemoveWorkloadAnchorRequest{Expected: req.WorkloadAnchor}); err == nil {
					t.Fatal("lost activation claim bypassed exact Pod retirement")
				}
				if response, err := preparedTestServer(client).ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: binding}); err != nil || !proto.Equal(response.GetBinding(), binding) {
					t.Fatalf("exact activation claim recovery failed: %v", err)
				}
			} else {
				client.ClearActions()
				if response, err := preparedTestServer(client).PrepareAnchoredWorkload(context.Background(), req); err == nil || response != nil {
					t.Fatal("lost selection reply allowed preparation replay")
				}
				assertNoVolumeMutation(t, client)
				observation, err := preparedTestServer(client).ObserveWorkloadPreparation(context.Background(), &runnerv1.ObserveWorkloadPreparationRequest{WorkloadId: req.Preparation.Workload.WorkloadId, BackendId: req.Preparation.BackendId})
				if err != nil || !proto.Equal(observation.GetBinding(), preparedBindingFromPod(t, pod)) {
					t.Fatalf("lost selection was not discoverable for removal: %v", err)
				}
			}
		})
	}
}
