package server

import (
	"context"
	"maps"
	"testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
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

var revocationConfigMaps = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

func revokeRequest(req *runnerv1.PrepareAnchoredWorkloadRequest) *runnerv1.RevokeWorkloadPreparationRequest {
	return &runnerv1.RevokeWorkloadPreparationRequest{WorkloadAnchor: req.WorkloadAnchor, VolumeAnchors: req.VolumeAnchors}
}

func revokePreparation(t *testing.T, s *Server, req *runnerv1.PrepareAnchoredWorkloadRequest) *runnerv1.PreparationRevocation {
	t.Helper()
	response, err := s.RevokeWorkloadPreparation(context.Background(), revokeRequest(req))
	if err != nil || response.GetRevocation() == nil {
		t.Fatalf("preparation revocation failed: %v", err)
	}
	if _, err := canonicalPreparationRevocation(response.Revocation, true); err != nil {
		t.Fatal(err)
	}
	return response.Revocation
}

func assertReadOnlyRevocation(t *testing.T, client *fake.Clientset) {
	t.Helper()
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("observation mutated native state: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func TestPreparationRevocationBeforeFirstCreate(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, zero := range []bool{false, true} {
			t.Run(map[bool]string{false: "agent", true: "sandbox"}[sandbox]+map[bool]string{false: "/volume", true: "/zero"}[zero], func(t *testing.T) {
				client := preparedTestClient()
				s := preparedTestServer(client)
				req := anchoredTestRequest(t, s, sandbox, zero)
				receipt := revokePreparation(t, s, req)
				if receipt.SelectedPodUid != "" || !proto.Equal(receipt.WorkloadAnchor, req.WorkloadAnchor) {
					t.Fatal("empty first provision invented a Pod binding")
				}
				client.ClearActions()
				s = preparedTestServer(client)
				if !proto.Equal(receipt, revokePreparation(t, s, req)) {
					t.Fatal("restart changed the immutable native receipt")
				}
				assertReadOnlyRevocation(t, client)
				response, err := s.ObservePreparationRevocation(context.Background(), &runnerv1.ObservePreparationRevocationRequest{Expected: receipt})
				if err != nil || response.GetState() != runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_POD_ABSENT ||
					!proto.Equal(response.Revocation, receipt) || len(response.Volumes) != 0 || len(response.AbsentVolumeIds) != len(req.VolumeAnchors) {
					t.Fatalf("first provision observation: %v", err)
				}
				assertReadOnlyRevocation(t, client)
				intent := proto.Clone(req.WorkloadAnchor).(*runnerv1.ResourceAnchor)
				intent.InstanceUid = ""
				if _, err := s.ReserveResourceAnchor(context.Background(), &runnerv1.ReserveResourceAnchorRequest{Intent: intent}); status.Code(err) != codes.FailedPrecondition {
					t.Fatal("revoked workload ID was reserved again")
				}
				if _, err := s.PrepareAnchoredWorkload(context.Background(), req); err == nil {
					t.Fatal("revoked owner authorized preparation")
				}
				assertNoVolumeMutation(t, client)
			})
		}
	}
}

func TestPreparationRevocationWaitsForGCAndPreservesWorkspace(t *testing.T) {
	client := preparedTestClient()
	s := preparedTestServer(client)
	req := anchoredTestRequest(t, s, false, false)
	binding := prepareAnchoredTest(t, s, req)
	before := preparedTestClaim(t, client).DeepCopy()
	receipt := revokePreparation(t, s, req)
	if receipt.SelectedPodUid != binding.InstanceUid {
		t.Fatal("receipt lost the earlier native Pod selection")
	}
	client.ClearActions()
	response, err := s.ObservePreparationRevocation(context.Background(), &runnerv1.ObservePreparationRevocationRequest{Expected: receipt})
	if err != nil || response.GetState() != runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_PENDING || len(response.Volumes) != 0 || len(response.AbsentVolumeIds) != 0 {
		t.Fatalf("owner deletion substituted for child cleanup: %v", err)
	}
	assertReadOnlyRevocation(t, client)
	// The fake has no garbage collector; native GC is tested separately.
	if err := client.Tracker().Delete(preparedPodResource, "default", podNameFromID(binding.WorkloadId)); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	response, err = preparedTestServer(client).ObservePreparationRevocation(context.Background(), &runnerv1.ObservePreparationRevocationRequest{Expected: receipt})
	if err != nil || response.GetState() != runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_POD_ABSENT || len(response.Volumes) != 1 || !proto.Equal(response.Volumes[0], binding.Volumes[0]) || len(response.AbsentVolumeIds) != 0 {
		t.Fatalf("exact retained workspace was not recovered: %v", err)
	}
	assertReadOnlyRevocation(t, client)
	after := preparedTestClaim(t, client)
	if !maps.Equal(before.Annotations, after.Annotations) || before.UID != after.UID || before.ResourceVersion != after.ResourceVersion {
		t.Fatal("revocation changed the persistent workspace")
	}
}

func TestPreparationRevocationNeverInfersUnactivatedFromAbsence(t *testing.T) {
	for _, phase := range []string{"missing-owner", "claimed-gated-pod", "claimed-absent-pod", "running-pod"} {
		t.Run(phase, func(t *testing.T) {
			client := preparedTestClient()
			s := preparedTestServer(client)
			req := anchoredTestRequest(t, s, false, false)
			if phase == "missing-owner" {
				if err := client.Tracker().Delete(revocationConfigMaps, "default", resourceAnchorName(req.WorkloadAnchor)); err != nil {
					t.Fatal(err)
				}
			} else {
				binding := prepareAnchoredTest(t, s, req)
				if phase == "running-pod" {
					pod := preparedTestPod(t, client, binding)
					pod.Spec.NodeName = "fixture-node"
					if err := client.Tracker().Update(preparedPodResource, pod, "default"); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := s.claimAnchorPod(context.Background(), binding, true); err != nil {
						t.Fatal(err)
					}
					if phase == "claimed-absent-pod" {
						if err := client.Tracker().Delete(preparedPodResource, "default", podNameFromID(binding.WorkloadId)); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			client.ClearActions()
			if response, err := s.RevokeWorkloadPreparation(context.Background(), revokeRequest(req)); response != nil || status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("unknown/claimed execution was declared unactivated: %v", err)
			}
			assertReadOnlyRevocation(t, client)
		})
	}
}

func TestPreparationRevocationLostAcknowledgements(t *testing.T) {
	for _, checkpoint := range []string{"claim", "receipt", "delete"} {
		t.Run(checkpoint, func(t *testing.T) {
			client := preparedTestClient()
			s := preparedTestServer(client)
			req := anchoredTestRequest(t, s, true, false)
			lost := false
			verb := map[string]string{"claim": "patch", "receipt": "create", "delete": "delete"}[checkpoint]
			client.PrependReactor(verb, "configmaps", func(action kubetesting.Action) (bool, runtime.Object, error) {
				if lost {
					return false, nil, nil
				}
				lost = true
				switch checkpoint {
				case "claim":
					value, err := client.Tracker().Get(revocationConfigMaps, "default", resourceAnchorName(req.WorkloadAnchor))
					if err != nil {
						t.Fatal(err)
					}
					cm := value.(*corev1.ConfigMap)
					data, err := protojson.Marshal(&runnerv1.PreparationRevocation{WorkloadAnchor: req.WorkloadAnchor, VolumeAnchors: req.VolumeAnchors})
					if err != nil {
						t.Fatal(err)
					}
					cm.Annotations[resourceAnchorVersionAnnotation], cm.Annotations[preparationRevocationAnnotation], cm.ResourceVersion = preparationRevokedVersion, string(data), "2"
					if err := client.Tracker().Update(revocationConfigMaps, cm, "default"); err != nil {
						t.Fatal(err)
					}
				case "receipt":
					cm := action.(kubetesting.CreateAction).GetObject().(*corev1.ConfigMap)
					cm.UID, cm.ResourceVersion = types.UID(uuid.NewString()), "1"
					if err := client.Tracker().Add(cm); err != nil {
						t.Fatal(err)
					}
				case "delete":
					if _, err := client.Tracker().Get(revocationConfigMaps, "default", preparationRevocationName(req.WorkloadAnchor)); err != nil {
						t.Fatal("native owner deletion preceded durable receipt")
					}
					if err := client.Tracker().Delete(revocationConfigMaps, "default", resourceAnchorName(req.WorkloadAnchor)); err != nil {
						t.Fatal(err)
					}
				}
				return true, nil, apierrors.NewTimeoutError("fixture committed operation lost ACK", 0)
			})
			client.ClearActions()
			if response, err := s.RevokeWorkloadPreparation(context.Background(), revokeRequest(req)); response != nil || err == nil || !lost {
				t.Fatal("lost acknowledgement was reported as a receipt")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" && checkpoint != "delete" {
					t.Fatal("unacknowledged claim/receipt authorized owner deletion")
				}
			}
			receipt := revokePreparation(t, preparedTestServer(client), req)
			if !proto.Equal(receipt, revokePreparation(t, preparedTestServer(client), req)) {
				t.Fatal("lost-ACK recovery replaced revocation identity")
			}
			if _, err := client.CoreV1().ConfigMaps("default").Get(context.Background(), resourceAnchorName(req.WorkloadAnchor), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatal("owner was not revoked")
			}
		})
	}
}

func TestPreparationRevocationRejectsChangedReceipt(t *testing.T) {
	for _, change := range []string{"uid", "owner", "selected-pod", "missing-volume", "unknown-wire", "missing-record", "corrupt-record", "replaced-owner"} {
		t.Run(change, func(t *testing.T) {
			client := preparedTestClient()
			s := preparedTestServer(client)
			req := anchoredTestRequest(t, s, false, false)
			receipt := revokePreparation(t, s, req)
			request := &runnerv1.ObservePreparationRevocationRequest{Expected: proto.Clone(receipt).(*runnerv1.PreparationRevocation)}
			switch change {
			case "uid":
				request.Expected.InstanceUid = uuid.NewString()
			case "owner":
				request.Expected.WorkloadAnchor.InstanceUid = uuid.NewString()
			case "selected-pod":
				request.Expected.SelectedPodUid = uuid.NewString()
			case "missing-volume":
				request.Expected.VolumeAnchors = nil
			case "unknown-wire":
				request.Expected.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "missing-record":
				if err := client.Tracker().Delete(revocationConfigMaps, "default", preparationRevocationName(req.WorkloadAnchor)); err != nil {
					t.Fatal(err)
				}
			case "corrupt-record":
				cm, err := client.CoreV1().ConfigMaps("default").Get(context.Background(), preparationRevocationName(req.WorkloadAnchor), metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				cm.Data["revocation.json"] = "{}"
				if err := client.Tracker().Update(revocationConfigMaps, cm, "default"); err != nil {
					t.Fatal(err)
				}
			case "replaced-owner":
				immutable := true
				if err := client.Tracker().Add(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: resourceAnchorName(req.WorkloadAnchor), Namespace: "default", UID: types.UID(uuid.NewString()), ResourceVersion: "1"}, Immutable: &immutable}); err != nil {
					t.Fatal(err)
				}
			}
			client.ClearActions()
			if response, err := s.ObservePreparationRevocation(context.Background(), request); err == nil || response != nil {
				t.Fatal("unverified receipt produced cleanup evidence")
			}
			assertReadOnlyRevocation(t, client)
		})
	}
}

func TestPreparationRevocationWinsActivationRace(t *testing.T) {
	client := preparedTestClient()
	s := preparedTestServer(client)
	req := anchoredTestRequest(t, s, false, false)
	binding := prepareAnchoredTest(t, s, req)
	peer := fake.NewSimpleClientset()
	peer.PrependReactor("*", "*", kubetesting.ObjectReaction(client.Tracker()))
	peer.PrependReactor("create", "configmaps", func(action kubetesting.Action) (bool, runtime.Object, error) {
		cm := action.(kubetesting.CreateAction).GetObject().(*corev1.ConfigMap)
		cm.UID, cm.ResourceVersion = types.UID(uuid.NewString()), "1"
		return false, nil, nil
	})
	var receipt *runnerv1.PreparationRevocation
	client.PrependReactor("patch", "configmaps", func(kubetesting.Action) (bool, runtime.Object, error) {
		receipt = revokePreparation(t, preparedTestServer(peer), req)
		return false, nil, nil
	})
	client.ClearActions()
	if response, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: binding}); response != nil || err == nil || receipt == nil {
		t.Fatal("activation escaped committed preparation revocation")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" && action.GetResource().Resource != "configmaps" {
			t.Fatal("losing activation mutated a Pod or workspace")
		}
	}
	if !hasPreparedGate(preparedTestPod(t, client, binding)) {
		t.Fatal("revoked Pod became executable")
	}
	if !proto.Equal(receipt, revokePreparation(t, preparedTestServer(peer), req)) {
		t.Fatal("revocation proof changed after a competing activation")
	}
}

func TestPreparationRevocationLosesToActivationCAS(t *testing.T) {
	client := preparedTestClient()
	s := preparedTestServer(client)
	req := anchoredTestRequest(t, s, true, false)
	binding := prepareAnchoredTest(t, s, req)
	peer := fake.NewSimpleClientset()
	peer.PrependReactor("*", "*", kubetesting.ObjectReaction(client.Tracker()))
	intercepted := false
	client.PrependReactor("get", "pods", func(action kubetesting.Action) (bool, runtime.Object, error) {
		if intercepted {
			return false, nil, nil
		}
		intercepted = true
		before, err := client.Tracker().Get(preparedPodResource, "default", action.(kubetesting.GetAction).GetName())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := preparedTestServer(peer).ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: binding}); err != nil {
			t.Fatal(err)
		}
		object, err := client.Tracker().Get(revocationConfigMaps, "default", resourceAnchorName(req.WorkloadAnchor))
		if err != nil {
			t.Fatal(err)
		}
		cm := object.(*corev1.ConfigMap)
		// The tracker applies JSON Patch tests but does not advance native revisions.
		cm.ResourceVersion = "2"
		if err := client.Tracker().Update(revocationConfigMaps, cm, "default"); err != nil {
			t.Fatal(err)
		}
		return true, before, nil
	})
	client.ClearActions()
	if response, err := s.RevokeWorkloadPreparation(context.Background(), revokeRequest(req)); response != nil || err == nil || !intercepted {
		t.Fatal("stale revocation ignored the activation owner revision")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" || action.GetVerb() == "delete" {
			t.Fatal("losing revocation created a receipt or deleted a resource")
		}
	}
	if _, err := client.Tracker().Get(revocationConfigMaps, "default", preparationRevocationName(req.WorkloadAnchor)); !apierrors.IsNotFound(err) {
		t.Fatal("claimed activation received an unactivated receipt")
	}
}

func TestPreparationRevocationRejectsUnverifiedInventory(t *testing.T) {
	for _, change := range []string{"nil-list", "continued-list", "duplicate", "foreign-owner", "missing-anchor", "replaced-anchor", "missing-uid", "deleting", "lost", "held"} {
		t.Run(change, func(t *testing.T) {
			client := preparedTestClient()
			s := preparedTestServer(client)
			req := anchoredTestRequest(t, s, false, false)
			binding := prepareAnchoredTest(t, s, req)
			receipt := revokePreparation(t, s, req)
			if err := client.Tracker().Delete(preparedPodResource, "default", podNameFromID(binding.WorkloadId)); err != nil {
				t.Fatal(err)
			}
			pvc := preparedTestClaim(t, client)
			switch change {
			case "nil-list":
				client.PrependReactor("list", "persistentvolumeclaims", func(kubetesting.Action) (bool, runtime.Object, error) { return true, nil, nil })
			case "continued-list":
				client.PrependReactor("list", "persistentvolumeclaims", func(kubetesting.Action) (bool, runtime.Object, error) {
					return true, &corev1.PersistentVolumeClaimList{ListMeta: metav1.ListMeta{Continue: "remaining"}, Items: []corev1.PersistentVolumeClaim{*pvc}}, nil
				})
			case "duplicate":
				second := pvc.DeepCopy()
				second.Name, second.UID = "duplicate", types.UID(uuid.NewString())
				if err := client.Tracker().Add(second); err != nil {
					t.Fatal(err)
				}
			case "foreign-owner":
				pvc.OwnerReferences[0].UID = types.UID(uuid.NewString())
			case "missing-anchor", "replaced-anchor":
				name := resourceAnchorName(req.VolumeAnchors[0])
				object, err := client.Tracker().Get(revocationConfigMaps, "default", name)
				if err != nil {
					t.Fatal(err)
				}
				if err := client.Tracker().Delete(revocationConfigMaps, "default", name); err != nil {
					t.Fatal(err)
				}
				if change == "replaced-anchor" {
					object.(*corev1.ConfigMap).UID = types.UID(uuid.NewString())
					if err := client.Tracker().Add(object); err != nil {
						t.Fatal(err)
					}
				}
			case "missing-uid":
				pvc.UID = ""
			case "deleting":
				now := metav1.Now()
				pvc.DeletionTimestamp = &now
			case "lost":
				pvc.Status.Phase = corev1.ClaimLost
			case "held":
				pvc.Finalizers = append(pvc.Finalizers, preparedHoldPrefix+uuid.NewString())
			}
			if err := client.Tracker().Update(preparedClaimResource, pvc, "default"); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			if response, err := s.ObservePreparationRevocation(context.Background(), &runnerv1.ObservePreparationRevocationRequest{Expected: receipt}); response != nil || err == nil {
				t.Fatal("unverified inventory produced cleanup evidence")
			}
			assertReadOnlyRevocation(t, client)
		})
	}
}
