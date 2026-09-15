package server

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	jsonpatch "github.com/evanphx/json-patch"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

var adoptionCMResource = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

func adoptionTestFixture(t *testing.T, sandbox bool) (*fake.Clientset, *runnerv1.ReserveVolumeAnchorAdoptionRequest, *corev1.PersistentVolumeClaim) {
	t.Helper()
	client := preparedTestClient()
	// The fake API evaluates the actual JSON Patch preconditions and advances RV.
	client.PrependReactor("patch", "*", func(action kubetesting.Action) (bool, runtime.Object, error) {
		patch := action.(kubetesting.PatchAction)
		current, err := client.Tracker().Get(patch.GetResource(), patch.GetNamespace(), patch.GetName())
		if err != nil {
			return true, nil, err
		}
		data, err := json.Marshal(current)
		if err != nil {
			return true, nil, err
		}
		if patch.GetPatchType() != types.JSONPatchType {
			t.Fatal("adoption did not use UID/RV-conditional JSON Patch")
		}
		operations, err := jsonpatch.DecodePatch(patch.GetPatch())
		if err != nil {
			return true, nil, err
		}
		data, err = operations.Apply(data)
		if err != nil {
			return true, nil, apierrors.NewConflict(patch.GetResource().GroupResource(), patch.GetName(), err)
		}
		updated := current.DeepCopyObject()
		if err := json.Unmarshal(data, updated); err != nil {
			return true, nil, err
		}
		metadata, err := meta.Accessor(updated)
		if err != nil {
			return true, nil, err
		}
		revision, err := strconv.Atoi(metadata.GetResourceVersion())
		if err != nil {
			return true, nil, err
		}
		metadata.SetResourceVersion(strconv.Itoa(revision + 1))
		return true, updated, client.Tracker().Update(patch.GetResource(), updated, patch.GetNamespace())
	})
	client.PrependReactor("list", "pods", func(action kubetesting.Action) (bool, runtime.Object, error) {
		list, err := client.Tracker().List(preparedPodResource, corev1.SchemeGroupVersion.WithKind("Pod"), action.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		list.(*corev1.PodList).ResourceVersion = "1"
		return true, list, nil
	})
	intent := anchorTestIntent(runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, sandbox)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "workspace", Namespace: "default", UID: types.UID(uuid.NewString()), ResourceVersion: "1",
		Labels: maps.Clone(intent.IdentityLabels), Annotations: map[string]string{"example.org/keep": "original"}, Finalizers: []string{"example.org/retain", "kubernetes.io/pvc-protection"}},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-original", AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Mi")}}}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	if err := client.Tracker().Add(pvc); err != nil {
		t.Fatal(err)
	}
	previous, err := preparedVolume(pvc, testVolumeBackend)
	if err != nil {
		t.Fatal(err)
	}
	return client, &runnerv1.ReserveVolumeAnchorAdoptionRequest{Id: uuid.NewString(), Expected: previous, Intent: intent}, pvc.DeepCopy()
}

func reserveAdoptionTest(t *testing.T, client *fake.Clientset, req *runnerv1.ReserveVolumeAnchorAdoptionRequest) *runnerv1.VolumeAnchorAdoption {
	t.Helper()
	response, err := preparedTestServer(client).ReserveVolumeAnchorAdoption(context.Background(), req)
	if err != nil || response.GetAdoption() == nil {
		t.Fatalf("adoption reservation failed: %v", err)
	}
	return response.Adoption
}

func applyAdoptionTest(t *testing.T, client *fake.Clientset, a *runnerv1.VolumeAnchorAdoption) *runnerv1.VolumeListItem {
	t.Helper()
	response, err := preparedTestServer(client).ApplyVolumeAnchorAdoption(context.Background(), &runnerv1.ApplyVolumeAnchorAdoptionRequest{Adoption: a})
	if err != nil || response.GetState() != runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_APPLIED {
		t.Fatalf("adoption apply failed: %v", err)
	}
	return response.Volume
}

func finalizeAdoptionTest(t *testing.T, client *fake.Clientset, a *runnerv1.VolumeAnchorAdoption) *runnerv1.VolumeListItem {
	t.Helper()
	response, err := preparedTestServer(client).FinalizeVolumeAnchorAdoption(context.Background(), &runnerv1.FinalizeVolumeAnchorAdoptionRequest{Adoption: a})
	if err != nil || response.GetState() != runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_READY {
		t.Fatalf("adoption finalize failed: %v", err)
	}
	return response.Volume
}

func assertAdoptionReadsOnly(t *testing.T, client *fake.Clientset) {
	t.Helper()
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("unexpected mutation: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func TestVolumeAnchorAdoptionPreservesOriginalStorage(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		t.Run(map[bool]string{false: "agent", true: "sandbox"}[sandbox], func(t *testing.T) {
			client, req, original := adoptionTestFixture(t, sandbox)
			input := proto.Clone(req)
			a := reserveAdoptionTest(t, client, req)
			if !proto.Equal(input, req) || !proto.Equal(a.Previous, req.Expected) || a.InstanceUid == a.Anchor.InstanceUid || a.InstanceUid == a.Previous.InstanceUid || a.Anchor.InstanceUid == a.Previous.InstanceUid {
				t.Fatal("adoption changed input or conflated journal, owner and original PVC identities")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" && action.GetResource().Resource != "configmaps" {
					t.Fatal("reservation changed storage or created compute")
				}
			}
			if !reflect.DeepEqual(original, preparedTestClaim(t, client)) {
				t.Fatal("reserve changed original PVC")
			}
			client.ClearActions()
			if !proto.Equal(a, reserveAdoptionTest(t, client, req)) {
				t.Fatal("retry changed reservation")
			}
			assertAdoptionReadsOnly(t, client)
			if _, err := preparedTestServer(client).FinalizeVolumeAnchorAdoption(context.Background(), &runnerv1.FinalizeVolumeAnchorAdoptionRequest{Adoption: a}); err == nil {
				t.Fatal("finalized before apply")
			}
			binding := applyAdoptionTest(t, client, a)
			if !proto.Equal(binding.Anchor, a.Anchor) || binding.InstanceUid != string(original.UID) {
				t.Fatal("apply changed storage identity")
			}
			applied := preparedTestClaim(t, client)
			if validatePVCReuseSpec(applied, original) == nil || !slices.Contains(applied.Finalizers, volumeAdoptionHoldPrefix+a.Id) {
				t.Fatal("applied adoption became reusable before finalization")
			}
			if !reflect.DeepEqual(applied.Spec, original.Spec) || !reflect.DeepEqual(applied.Labels, original.Labels) || applied.Annotations["example.org/keep"] != "original" {
				t.Fatal("apply changed original specification or unrelated metadata")
			}
			client.ClearActions()
			applyAdoptionTest(t, client, a)
			assertAdoptionReadsOnly(t, client)
			if !proto.Equal(binding, finalizeAdoptionTest(t, client, a)) {
				t.Fatal("finalization changed bound target")
			}
			ready := preparedTestClaim(t, client)
			if !reflect.DeepEqual(ready.Spec, original.Spec) || !slices.Equal(ready.Finalizers, original.Finalizers) || validatePVCReuseSpec(ready, original) != nil {
				t.Fatal("finalization changed storage or unrelated finalizers")
			}
			client.ClearActions()
			finalizeAdoptionTest(t, client, a)
			assertAdoptionReadsOnly(t, client)
		})
	}
}

func TestVolumeAnchorAdoptionMissingReceiptNeverRecreated(t *testing.T) {
	for _, stage := range []string{"reserved", "applied", "ready"} {
		t.Run(stage, func(t *testing.T) {
			client, req, _ := adoptionTestFixture(t, false)
			a := reserveAdoptionTest(t, client, req)
			if stage != "reserved" {
				applyAdoptionTest(t, client, a)
			}
			if stage == "ready" {
				finalizeAdoptionTest(t, client, a)
			}
			if err := client.Tracker().Delete(adoptionCMResource, "default", adoptionJournalName(a.Previous)); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			if response, err := preparedTestServer(client).ReserveVolumeAnchorAdoption(context.Background(), req); err == nil || response != nil {
				t.Fatal("lost durable journal was recreated")
			}
			assertAdoptionReadsOnly(t, client)
		})
	}
}

func TestVolumeAnchorAdoptionRejectsInputWithoutMutation(t *testing.T) {
	for _, change := range []string{"nil", "operation", "uid", "key", "bound-intent", "backend", "owner", "labels", "unknown", "unknown-volume", "unknown-anchor"} {
		t.Run(change, func(t *testing.T) {
			client, req, _ := adoptionTestFixture(t, false)
			switch change {
			case "nil":
				req = nil
			case "operation":
				req.Id = "not-a-uuid"
			case "uid":
				req.Expected.InstanceUid = uuid.NewString()
			case "key":
				req.Expected.VolumeKey = uuid.NewString()
			case "bound-intent":
				req.Intent.InstanceUid = uuid.NewString()
			case "backend":
				req.Intent.BackendId, req.Expected.BackendId = "wrong-backend", "wrong-backend"
			case "owner":
				req.Intent.IdentityLabels["agent-id"], req.Expected.IdentityLabels["agent-id"] = "not-uuid", "not-uuid"
			case "labels":
				req.Expected.IdentityLabels["unapproved"] = "value"
			case "unknown":
				req.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
			case "unknown-volume":
				req.Expected.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
			case "unknown-anchor":
				req.Intent.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
			}
			if response, err := preparedTestServer(client).ReserveVolumeAnchorAdoption(context.Background(), req); err == nil || response != nil {
				t.Fatal("invalid adoption input accepted")
			}
			assertAdoptionReadsOnly(t, client)
		})
	}
}

func TestVolumeAnchorAdoptionRejectsActiveWriters(t *testing.T) {
	for _, stage := range []string{"reserve", "apply", "finalize"} {
		for _, blocker := range []string{"pod", "terminal-pod", "deleting-pod", "hold", "nil-list", "partial-list", "remaining", "unversioned-list", "list-error"} {
			t.Run(stage+"/"+blocker, func(t *testing.T) {
				client, req, _ := adoptionTestFixture(t, false)
				var a *runnerv1.VolumeAnchorAdoption
				if stage != "reserve" {
					a = reserveAdoptionTest(t, client, req)
				}
				if stage == "finalize" {
					applyAdoptionTest(t, client, a)
				}
				switch blocker {
				case "pod", "terminal-pod", "deleting-pod":
					pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unmanaged-writer", Namespace: "default", UID: types.UID(uuid.NewString()), ResourceVersion: "1"}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "workspace"}}}}}}
					if blocker == "terminal-pod" {
						pod.Status.Phase = corev1.PodSucceeded
					}
					if blocker == "deleting-pod" {
						now := metav1.Now()
						pod.DeletionTimestamp = &now
					}
					if err := client.Tracker().Add(pod); err != nil {
						t.Fatal(err)
					}
				case "hold":
					pvc := preparedTestClaim(t, client)
					pvc.Finalizers = append(pvc.Finalizers, preparedHoldPrefix+uuid.NewString())
					if err := client.Tracker().Update(preparedClaimResource, pvc, "default"); err != nil {
						t.Fatal(err)
					}
				default:
					client.PrependReactor("list", "pods", func(kubetesting.Action) (bool, runtime.Object, error) {
						if blocker == "nil-list" {
							return true, nil, nil
						}
						if blocker == "list-error" {
							return true, nil, errors.New("inventory unavailable")
						}
						list := &corev1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: "1"}}
						if blocker == "partial-list" {
							list.Continue = "next"
						}
						if blocker == "remaining" {
							count := int64(1)
							list.RemainingItemCount = &count
						}
						if blocker == "unversioned-list" {
							list.ResourceVersion = ""
						}
						return true, list, nil
					})
				}
				client.ClearActions()
				var err error
				s := preparedTestServer(client)
				switch stage {
				case "reserve":
					_, err = s.ReserveVolumeAnchorAdoption(context.Background(), req)
				case "apply":
					_, err = s.ApplyVolumeAnchorAdoption(context.Background(), &runnerv1.ApplyVolumeAnchorAdoptionRequest{Adoption: a})
				case "finalize":
					_, err = s.FinalizeVolumeAnchorAdoption(context.Background(), &runnerv1.FinalizeVolumeAnchorAdoptionRequest{Adoption: a})
				}
				if err == nil {
					t.Fatal("unconfirmed writer drain accepted")
				}
				assertAdoptionReadsOnly(t, client)
			})
		}
	}
}

func TestVolumeAnchorAdoptionPendingReuseGuard(t *testing.T) {
	for _, state := range []string{"applied", "unknown", "ready-held", "empty-held"} {
		t.Run(state, func(t *testing.T) {
			_, _, original := adoptionTestFixture(t, false)
			pvc := original.DeepCopy()
			pvc.Annotations[volumeAdoptionStateAnnotation] = state
			if strings.HasSuffix(state, "-held") {
				pvc.Annotations[volumeAdoptionStateAnnotation] = ""
				if state == "ready-held" {
					pvc.Annotations[volumeAdoptionStateAnnotation] = "ready"
				}
				pvc.Finalizers = append(pvc.Finalizers, volumeAdoptionHoldPrefix+uuid.NewString())
			}
			if status.Code(validatePVCReuseSpec(pvc, original)) != codes.FailedPrecondition {
				t.Fatal("pending migration became reusable")
			}
		})
	}
}

func TestVolumeAnchorAdoptionReadyRequiresCompletePVCMetadata(t *testing.T) {
	for _, change := range []string{"state", "journal", "journal-format", "owner", "anchor"} {
		t.Run(change, func(t *testing.T) {
			client, req, original := adoptionTestFixture(t, false)
			a := reserveAdoptionTest(t, client, req)
			applyAdoptionTest(t, client, a)
			finalizeAdoptionTest(t, client, a)
			pvc := preparedTestClaim(t, client)
			switch change {
			case "state":
				delete(pvc.Annotations, volumeAdoptionStateAnnotation)
			case "journal":
				delete(pvc.Annotations, volumeAdoptionJournalAnnotation)
			case "journal-format":
				pvc.Annotations[volumeAdoptionJournalAnnotation] = "not-a-uid"
			case "owner":
				pvc.OwnerReferences = nil
			case "anchor":
				delete(pvc.Annotations, resourceAnchorAnnotation)
			}
			if err := validatePVCReuseSpec(pvc, original); err == nil {
				t.Fatal("partial adoption metadata authorized reuse")
			}
		})
	}
}

func TestVolumeAnchorAdoptionLostRepliesResumeExactIdentities(t *testing.T) {
	for _, checkpoint := range []string{"owner-create", "journal-create", "journal-pin", "apply", "finalize-owner", "finalize-pvc"} {
		t.Run(checkpoint, func(t *testing.T) {
			client, req, original := adoptionTestFixture(t, false)
			var a *runnerv1.VolumeAnchorAdoption
			if checkpoint == "apply" || strings.HasPrefix(checkpoint, "finalize") {
				a = reserveAdoptionTest(t, client, req)
			}
			if strings.HasPrefix(checkpoint, "finalize") {
				applyAdoptionTest(t, client, a)
			}
			verb, target := "create", "configmaps"
			if checkpoint == "journal-pin" || checkpoint == "finalize-owner" {
				verb = "patch"
			}
			if checkpoint == "apply" || checkpoint == "finalize-pvc" {
				verb, target = "patch", "persistentvolumeclaims"
			}
			chain := slices.Clone(client.ReactionChain)
			lost := false
			client.PrependReactor(verb, target, func(action kubetesting.Action) (bool, runtime.Object, error) {
				if lost {
					return false, nil, nil
				}
				if checkpoint == "owner-create" || checkpoint == "journal-create" {
					name := action.(kubetesting.CreateAction).GetObject().(*corev1.ConfigMap).Name
					if strings.HasPrefix(name, "volume-adoption-") != (checkpoint == "journal-create") {
						return false, nil, nil
					}
				}
				for _, reactor := range chain {
					if !reactor.Handles(action) {
						continue
					}
					handled, _, err := reactor.React(action)
					if !handled {
						continue
					}
					if err != nil {
						return true, nil, err
					}
					lost = true
					return true, nil, errors.New("fixture lost committed response")
				}
				t.Fatal("fixture failed to commit original action")
				return true, nil, errors.New("no reactor")
			})
			s := preparedTestServer(client)
			var err error
			switch checkpoint {
			case "apply":
				_, err = s.ApplyVolumeAnchorAdoption(context.Background(), &runnerv1.ApplyVolumeAnchorAdoptionRequest{Adoption: a})
			case "finalize-owner", "finalize-pvc":
				_, err = s.FinalizeVolumeAnchorAdoption(context.Background(), &runnerv1.FinalizeVolumeAnchorAdoptionRequest{Adoption: a})
			default:
				_, err = s.ReserveVolumeAnchorAdoption(context.Background(), req)
			}
			if !lost || err == nil {
				t.Fatal("lost reply was not exercised or reported success")
			}
			owner, err := client.Tracker().Get(adoptionCMResource, "default", resourceAnchorName(req.Intent))
			if err != nil {
				t.Fatal(err)
			}
			ownerUID := owner.(*corev1.ConfigMap).UID
			if checkpoint == "finalize-owner" && validatePVCReuseSpec(preparedTestClaim(t, client), original) == nil {
				t.Fatal("owner activation alone permitted PVC reuse")
			}
			recovered := reserveAdoptionTest(t, client, req)
			if recovered.Anchor.InstanceUid != string(ownerUID) || a != nil && !proto.Equal(recovered, a) {
				t.Fatal("recovery replaced a committed journal or owner")
			}
			if checkpoint != "finalize-pvc" {
				applyAdoptionTest(t, client, recovered)
			}
			finalizeAdoptionTest(t, client, recovered)
			pvc := preparedTestClaim(t, client)
			if pvc.UID != original.UID || !reflect.DeepEqual(pvc.Spec, original.Spec) || !slices.Equal(pvc.Finalizers, original.Finalizers) {
				t.Fatal("lost reply recovery changed original storage")
			}
		})
	}
}

func TestVolumeAnchorAdoptionRejectsChangedNativeEvidence(t *testing.T) {
	for _, stage := range []string{"reserved", "applied", "ready"} {
		for _, change := range []string{"missing-journal", "journal-uid", "journal-rv", "journal-mutable", "journal-owner", "journal-data", "missing-owner", "owner-uid", "owner-intent", "owner-journal", "owner-deleting", "nil-owner", "missing-pvc", "pvc-uid", "pvc-rv", "pvc-spec", "pvc-labels", "pvc-deleting", "pvc-unbound", "nil-pvc", "foreign-adoption-hold"} {
			t.Run(stage+"/"+change, func(t *testing.T) {
				client, req, _ := adoptionTestFixture(t, false)
				a := reserveAdoptionTest(t, client, req)
				if stage != "reserved" {
					applyAdoptionTest(t, client, a)
				}
				if stage == "ready" {
					finalizeAdoptionTest(t, client, a)
				}
				resource, name := adoptionCMResource, adoptionJournalName(a.Previous)
				if strings.Contains(change, "owner") && change != "journal-owner" {
					name = resourceAnchorName(a.Anchor)
				}
				if strings.Contains(change, "pvc") || change == "foreign-adoption-hold" {
					resource, name = preparedClaimResource, a.Previous.InstanceId
				}
				current, err := client.Tracker().Get(resource, "default", name)
				if err != nil {
					t.Fatal(err)
				}
				if strings.HasPrefix(change, "missing-") {
					if err := client.Tracker().Delete(resource, "default", name); err != nil {
						t.Fatal(err)
					}
				} else if strings.HasPrefix(change, "nil-") {
					client.PrependReactor("get", resource.Resource, func(action kubetesting.Action) (bool, runtime.Object, error) {
						return action.(kubetesting.GetAction).GetName() == name, nil, nil
					})
				} else {
					metadata, err := meta.Accessor(current)
					if err != nil {
						t.Fatal(err)
					}
					switch change {
					case "journal-uid", "owner-uid", "pvc-uid":
						metadata.SetUID(types.UID(uuid.NewString()))
					case "journal-rv", "pvc-rv":
						metadata.SetResourceVersion("")
					case "journal-mutable":
						current.(*corev1.ConfigMap).Immutable = nil
					case "journal-owner":
						metadata.SetOwnerReferences(resourceAnchorOwners(a.Anchor))
					case "journal-data":
						current.(*corev1.ConfigMap).Data["adoption.json"] = "{}"
					case "owner-intent":
						current.(*corev1.ConfigMap).Annotations[volumeAdoptionIntentAnnotation] = "{}"
					case "owner-journal":
						current.(*corev1.ConfigMap).Annotations[volumeAdoptionJournalAnnotation] = uuid.NewString()
					case "owner-deleting", "pvc-deleting":
						now := metav1.Now()
						metadata.SetDeletionTimestamp(&now)
					case "pvc-spec":
						current.(*corev1.PersistentVolumeClaim).Spec.VolumeName = "another-pv"
					case "pvc-labels":
						current.(*corev1.PersistentVolumeClaim).Labels["agent-id"] = uuid.NewString()
					case "pvc-unbound":
						current.(*corev1.PersistentVolumeClaim).Status.Phase = corev1.ClaimPending
					case "foreign-adoption-hold":
						metadata.SetFinalizers(append(metadata.GetFinalizers(), volumeAdoptionHoldPrefix+uuid.NewString()))
					}
					if err := client.Tracker().Update(resource, current, "default"); err != nil {
						t.Fatal(err)
					}
				}
				client.ClearActions()
				s := preparedTestServer(client)
				if _, err := s.ObserveVolumeAnchorAdoption(context.Background(), &runnerv1.ObserveVolumeAnchorAdoptionRequest{Adoption: a}); err == nil {
					t.Fatal("changed evidence observed as valid")
				}
				if _, err := s.ApplyVolumeAnchorAdoption(context.Background(), &runnerv1.ApplyVolumeAnchorAdoptionRequest{Adoption: a}); err == nil {
					t.Fatal("changed evidence applied")
				}
				if _, err := s.FinalizeVolumeAnchorAdoption(context.Background(), &runnerv1.FinalizeVolumeAnchorAdoptionRequest{Adoption: a}); err == nil {
					t.Fatal("changed evidence finalized")
				}
				if _, err := s.ReserveVolumeAnchorAdoption(context.Background(), req); err == nil {
					t.Fatal("changed evidence repaired implicitly")
				}
				assertAdoptionReadsOnly(t, client)
			})
		}
	}
}

func TestVolumeAnchorAdoptionCASNeverRetargets(t *testing.T) {
	for _, stage := range []string{"pin", "apply", "finalize-owner", "finalize-pvc"} {
		for _, change := range []string{"revision", "uid"} {
			t.Run(stage+"/"+change, func(t *testing.T) {
				client, req, _ := adoptionTestFixture(t, false)
				var a *runnerv1.VolumeAnchorAdoption
				if stage != "pin" {
					a = reserveAdoptionTest(t, client, req)
				}
				if strings.HasPrefix(stage, "finalize") {
					applyAdoptionTest(t, client, a)
				}
				target := "configmaps"
				if stage == "apply" || stage == "finalize-pvc" {
					target = "persistentvolumeclaims"
				}
				calls := 0
				client.PrependReactor("patch", target, func(action kubetesting.Action) (bool, runtime.Object, error) {
					calls++
					patch := action.(kubetesting.PatchAction)
					var operations []preparedPatchOperation
					if err := json.Unmarshal(patch.GetPatch(), &operations); err != nil {
						t.Fatal(err)
					}
					if len(operations) < 3 || operations[0].Op != "test" || operations[0].Path != "/metadata/uid" || operations[1].Op != "test" || operations[1].Path != "/metadata/resourceVersion" {
						t.Fatal("missing atomic UID/RV tests")
					}
					current, err := client.Tracker().Get(patch.GetResource(), patch.GetNamespace(), patch.GetName())
					if err != nil {
						t.Fatal(err)
					}
					metadata, err := meta.Accessor(current)
					if err != nil {
						t.Fatal(err)
					}
					if change == "uid" {
						metadata.SetUID(types.UID(uuid.NewString()))
					} else {
						metadata.SetResourceVersion("100")
					}
					if err := client.Tracker().Update(patch.GetResource(), current, patch.GetNamespace()); err != nil {
						t.Fatal(err)
					}
					return false, nil, nil
				})
				s := preparedTestServer(client)
				var err error
				switch stage {
				case "pin":
					_, err = s.ReserveVolumeAnchorAdoption(context.Background(), req)
				case "apply":
					_, err = s.ApplyVolumeAnchorAdoption(context.Background(), &runnerv1.ApplyVolumeAnchorAdoptionRequest{Adoption: a})
				default:
					_, err = s.FinalizeVolumeAnchorAdoption(context.Background(), &runnerv1.FinalizeVolumeAnchorAdoptionRequest{Adoption: a})
				}
				if err == nil || calls != 1 {
					t.Fatalf("CAS conflict retried or ignored: calls=%d err=%v", calls, err)
				}
			})
		}
	}
}

func adoptionWorkloadRequest(t *testing.T, client *fake.Clientset, a *runnerv1.VolumeAnchorAdoption) *runnerv1.PrepareAnchoredWorkloadRequest {
	t.Helper()
	work := &runnerv1.ResourceAnchor{Kind: runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, ResourceId: uuid.NewString(), BackendId: a.Anchor.BackendId, IdentityLabels: maps.Clone(a.Anchor.IdentityLabels)}
	delete(work.IdentityLabels, volumeKeyLabelKey)
	if work.IdentityLabels["agent-id"] != "" {
		work.IdentityLabels["thread-id"] = uuid.NewString()
	}
	p := preparedTestRequest()
	p.Workload.WorkloadId, p.Workload.Labels = work.ResourceId, maps.Clone(work.IdentityLabels)
	delete(p.Workload.Labels, managedByLabelKey)
	delete(p.Workload.Labels, workloadManagedByLabelKey)
	p.Workload.Volumes[0].Labels[volumeKeyLabelKey] = a.Previous.VolumeKey
	bound := proto.Clone(a.Previous).(*runnerv1.VolumeListItem)
	bound.Anchor = proto.Clone(a.Anchor).(*runnerv1.ResourceAnchor)
	p.ExpectedVolumes = []*runnerv1.VolumeListItem{bound}
	return &runnerv1.PrepareAnchoredWorkloadRequest{Preparation: p, VolumeAnchors: []*runnerv1.ResourceAnchor{a.Anchor}, WorkloadAnchor: reserveTestAnchor(t, preparedTestServer(client), work)}
}

func TestVolumeAnchorAdoptionFeedsExistingWorkloadLifecycle(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		t.Run(map[bool]string{false: "agent", true: "sandbox"}[sandbox], func(t *testing.T) {
			client, req, original := adoptionTestFixture(t, sandbox)
			a := reserveAdoptionTest(t, client, req)
			work := adoptionWorkloadRequest(t, client, a)
			for _, stage := range []string{"reserved", "applied", "owner-active"} {
				if stage == "applied" {
					applyAdoptionTest(t, client, a)
				}
				if stage == "owner-active" {
					owner, err := client.Tracker().Get(adoptionCMResource, "default", resourceAnchorName(a.Anchor))
					if err != nil {
						t.Fatal(err)
					}
					owner.(*corev1.ConfigMap).Annotations[resourceAnchorVersionAnnotation] = "v1"
					if err := client.Tracker().Update(adoptionCMResource, owner, "default"); err != nil {
						t.Fatal(err)
					}
				}
				client.ClearActions()
				if _, err := preparedTestServer(client).PrepareAnchoredWorkload(context.Background(), work); err == nil {
					t.Fatalf("%s adoption started compute", stage)
				}
				assertAdoptionReadsOnly(t, client)
			}
			bound := work.Preparation.ExpectedVolumes[0]
			client.ClearActions()
			if _, err := preparedTestServer(client).RemoveVolumeBound(context.Background(), &runnerv1.RemoveVolumeBoundRequest{Expected: req.Expected}); err == nil {
				t.Fatal("old removal deleted adopted PVC")
			}
			retiring, err := preparedTestServer(client).RemoveVolumeAnchored(context.Background(), &runnerv1.RemoveVolumeAnchoredRequest{Expected: bound})
			if err != nil || retiring.State != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING {
				t.Fatalf("adoption hold did not block retirement: %v", err)
			}
			assertAdoptionReadsOnly(t, client)
			finalizeAdoptionTest(t, client, a)
			for range 2 {
				binding := prepareAnchoredTest(t, preparedTestServer(client), work)
				if len(binding.Volumes) != 1 || !proto.Equal(binding.Volumes[0], bound) {
					t.Fatal("new workload changed adopted workspace")
				}
				if _, err := preparedTestServer(client).ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: binding}); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					if _, err := preparedTestServer(client).RemovePreparedWorkload(context.Background(), &runnerv1.RemovePreparedWorkloadRequest{Expected: binding}); err != nil {
						t.Fatal(err)
					}
				}
				work = adoptionWorkloadRequest(t, client, a)
			}
			pvc := preparedTestClaim(t, client)
			if pvc.UID != original.UID || !reflect.DeepEqual(pvc.Spec, original.Spec) || !slices.Equal(pvc.Finalizers, original.Finalizers) {
				t.Fatal("compute release changed durable workspace")
			}
			journal, err := client.Tracker().Get(adoptionCMResource, "default", adoptionJournalName(a.Previous))
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := readAdoptionJournal(journal.(*corev1.ConfigMap), "default")
			if err != nil || !proto.Equal(receipt, a) {
				t.Fatal("compute release changed adoption journal")
			}
		})
	}
}

func TestVolumeAnchorAdoptionRejectsAlteredReceipts(t *testing.T) {
	for _, change := range []string{"nil", "operation", "original-uid", "owner-uid", "journal-uid", "digest", "upper-digest", "unknown", "preexisting-anchor"} {
		t.Run(change, func(t *testing.T) {
			client, req, _ := adoptionTestFixture(t, false)
			a := reserveAdoptionTest(t, client, req)
			switch change {
			case "nil":
				a = nil
			case "operation":
				a.Id = uuid.NewString()
			case "original-uid":
				a.Previous.InstanceUid = uuid.NewString()
			case "owner-uid":
				a.Anchor.InstanceUid = uuid.NewString()
			case "journal-uid":
				a.InstanceUid = uuid.NewString()
			case "digest":
				a.PvcSpecSha256 = strings.Repeat("0", 64)
			case "upper-digest":
				a.PvcSpecSha256 = strings.ToUpper(a.PvcSpecSha256)
			case "unknown":
				a.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
			case "preexisting-anchor":
				a.Previous.Anchor = proto.Clone(a.Anchor).(*runnerv1.ResourceAnchor)
			}
			client.ClearActions()
			if _, err := preparedTestServer(client).ApplyVolumeAnchorAdoption(context.Background(), &runnerv1.ApplyVolumeAnchorAdoptionRequest{Adoption: a}); err == nil {
				t.Fatal("substituted receipt authorized apply")
			}
			assertAdoptionReadsOnly(t, client)
		})
	}
}

func TestVolumeAnchorAdoptionOwnerIntentCannotBeRepurposed(t *testing.T) {
	client, req, _ := adoptionTestFixture(t, false)
	a := reserveAdoptionTest(t, client, req)
	owner, err := client.Tracker().Get(adoptionCMResource, "default", resourceAnchorName(a.Anchor))
	if err != nil {
		t.Fatal(err)
	}
	intent := adoptionOwnerIntent(a)
	intent.Id = uuid.NewString()
	data, err := protojson.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	owner.(*corev1.ConfigMap).Annotations[volumeAdoptionIntentAnnotation] = string(data)
	if err := client.Tracker().Update(adoptionCMResource, owner, "default"); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	if _, err := preparedTestServer(client).ReserveVolumeAnchorAdoption(context.Background(), req); err == nil {
		t.Fatal("original operation used another adoption intent")
	}
	assertAdoptionReadsOnly(t, client)
}
