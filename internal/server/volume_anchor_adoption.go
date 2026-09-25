package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"reflect"
	"slices"
	"strings"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	volumeAdoptingVersion           = "volume-adopting-v1"
	volumeAdoptionVersion           = "volume-adoption-v1"
	volumeAdoptionIntentAnnotation  = "agyn.io/volume-adoption-intent"
	volumeAdoptionJournalAnnotation = "agyn.io/volume-adoption-journal"
	volumeAdoptionStateAnnotation   = "agyn.io/volume-adoption-state"
	volumeAdoptionHoldPrefix        = preparedHoldPrefix + "adopt-"
	maxVolumeAdoptionBytes          = 64 * 1024
)

func adoptionSpecHash(pvc *corev1.PersistentVolumeClaim) string {
	data, _ := json.Marshal(pvc.Spec)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func validateVolumeAdoptionInput(id string, previous *runnerv1.VolumeListItem, anchor *runnerv1.ResourceAnchor, bound bool) error {
	if !validPreparedID(id) || previous == nil || previous.Anchor != nil || !validPreparedID(previous.InstanceUid) ||
		len(previous.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "original_volume_adoption_identity_required")
	}
	if err := validateVolumeRemovalTarget(previous); err != nil {
		return err
	}
	if err := validateResourceAnchor(anchor, bound); err != nil {
		return err
	}
	if anchor.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME || anchor.ResourceId != previous.VolumeKey ||
		anchor.BackendId != previous.BackendId || !maps.Equal(anchor.IdentityLabels, previous.IdentityLabels) {
		return status.Error(codes.InvalidArgument, "volume_adoption_owner_mismatch")
	}
	for _, key := range []string{"agent-instance-id", "agent-id", "sandbox-id", "sandbox-owner-id"} {
		if value := anchor.IdentityLabels[key]; value != "" && !validPreparedID(value) {
			return status.Error(codes.InvalidArgument, "volume_adoption_owner_uuid_required")
		}
	}
	return nil
}

func validateVolumeAdoption(a *runnerv1.VolumeAnchorAdoption, journal bool) error {
	if a == nil || len(a.ProtoReflect().GetUnknown()) != 0 || journal && !validPreparedID(a.InstanceUid) || !journal && a.InstanceUid != "" {
		return status.Error(codes.InvalidArgument, "complete_volume_adoption_receipt_required")
	}
	if err := validateVolumeAdoptionInput(a.Id, a.Previous, a.Anchor, true); err != nil {
		return err
	}
	if digest, err := hex.DecodeString(a.PvcSpecSha256); err != nil || len(digest) != sha256.Size || strings.ToLower(a.PvcSpecSha256) != a.PvcSpecSha256 {
		return status.Error(codes.InvalidArgument, "volume_adoption_spec_digest_required")
	}
	return nil
}

func adoptionJournalName(previous *runnerv1.VolumeListItem) string {
	return "volume-adoption-" + previous.VolumeKey
}

func adoptionOwnerIntent(a *runnerv1.VolumeAnchorAdoption) *runnerv1.VolumeAnchorAdoption {
	intent := proto.Clone(a).(*runnerv1.VolumeAnchorAdoption)
	intent.InstanceUid, intent.Anchor.InstanceUid = "", ""
	return intent
}

func matchVolumeAdoptionOwner(owner *corev1.ConfigMap, a *runnerv1.VolumeAnchorAdoption, namespace string) (bool, error) {
	if owner == nil || owner.DeletionTimestamp != nil {
		return false, status.Error(codes.FailedPrecondition, "volume_adoption_owner_unavailable")
	}
	version := owner.Annotations[resourceAnchorVersionAnnotation]
	if version != "v1" && version != volumeAdoptingVersion {
		return false, status.Error(codes.FailedPrecondition, "volume_adoption_owner_state_invalid")
	}
	active := owner.DeepCopy()
	active.Annotations[resourceAnchorVersionAnnotation] = "v1"
	if err := matchResourceAnchor(active, a.Anchor, namespace); err != nil {
		return false, err
	}
	stored := &runnerv1.VolumeAnchorAdoption{}
	data := owner.Annotations[volumeAdoptionIntentAnnotation]
	if len(data) > maxVolumeAdoptionBytes || protojson.Unmarshal([]byte(data), stored) != nil || !proto.Equal(stored, adoptionOwnerIntent(a)) {
		return false, status.Error(codes.FailedPrecondition, "volume_adoption_owner_intent_changed")
	}
	return version == "v1", nil
}

// Pin the journal before acknowledging its receipt. A later missing journal is
// unresolved state, not permission to allocate another receipt incarnation.
func (s *Server) pinVolumeAdoptionJournal(ctx context.Context, owner *corev1.ConfigMap, a *runnerv1.VolumeAnchorAdoption) error {
	active, err := matchVolumeAdoptionOwner(owner, a, s.namespace)
	if err != nil {
		return err
	}
	if pinned := owner.Annotations[volumeAdoptionJournalAnnotation]; pinned != "" {
		if pinned != a.InstanceUid {
			return status.Error(codes.FailedPrecondition, "volume_adoption_owner_journal_changed")
		}
		return nil
	}
	if active {
		return status.Error(codes.FailedPrecondition, "volume_adoption_owner_journal_missing")
	}
	annotations := maps.Clone(owner.Annotations)
	annotations[volumeAdoptionJournalAnnotation] = a.InstanceUid
	updated, err := s.clientset.CoreV1().ConfigMaps(s.namespace).Patch(ctx, owner.Name, types.JSONPatchType,
		preparedObjectPatch(owner.ObjectMeta, preparedPatchOperation{"add", "/metadata/annotations", annotations}), metav1.PatchOptions{})
	if err != nil {
		return grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if _, err := matchVolumeAdoptionOwner(updated, a, s.namespace); err != nil {
		return err
	}
	if updated.Annotations[volumeAdoptionJournalAnnotation] != a.InstanceUid {
		return status.Error(codes.FailedPrecondition, "volume_adoption_owner_journal_unconfirmed")
	}
	return nil
}

func readAdoptionJournal(cm *corev1.ConfigMap, namespace string) (*runnerv1.VolumeAnchorAdoption, error) {
	if cm == nil || cm.Namespace != namespace || !validPreparedID(string(cm.UID)) || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil ||
		cm.Immutable == nil || !*cm.Immutable || len(cm.OwnerReferences) != 0 || len(cm.Data) != 1 || len(cm.BinaryData) != 0 ||
		cm.Annotations[resourceAnchorVersionAnnotation] != volumeAdoptionVersion {
		return nil, status.Error(codes.FailedPrecondition, "volume_adoption_journal_invalid")
	}
	a := &runnerv1.VolumeAnchorAdoption{}
	data := cm.Data["adoption.json"]
	if len(data) > maxVolumeAdoptionBytes || protojson.Unmarshal([]byte(data), a) != nil || validateVolumeAdoption(a, false) != nil ||
		cm.Name != adoptionJournalName(a.Previous) || !maps.Equal(cm.Labels, a.Anchor.IdentityLabels) {
		return nil, status.Error(codes.FailedPrecondition, "volume_adoption_journal_mismatch")
	}
	a.InstanceUid = string(cm.UID)
	return a, nil
}

func (s *Server) adoptionIdlePVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim, allowedHold string) error {
	if pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		return status.Error(codes.FailedPrecondition, "volume_adoption_requires_bound_storage")
	}
	for _, finalizer := range pvc.Finalizers {
		if strings.HasPrefix(finalizer, preparedHoldPrefix) && finalizer != allowedHold {
			return status.Error(codes.FailedPrecondition, "volume_adoption_has_workload_hold")
		}
	}
	pods, err := s.clientset.CoreV1().Pods(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if pods == nil || pods.ResourceVersion == "" || pods.Continue != "" || pods.RemainingItemCount != nil && *pods.RemainingItemCount != 0 {
		return status.Error(codes.FailedPrecondition, "volume_adoption_pod_inventory_incomplete")
	}
	for _, pod := range pods.Items {
		if pod.Namespace != s.namespace {
			return status.Error(codes.FailedPrecondition, "volume_adoption_pod_inventory_mismatch")
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name {
				return status.Error(codes.FailedPrecondition, "volume_adoption_has_pod_reference")
			}
		}
	}
	return nil
}

// ReserveVolumeAnchorAdoption creates metadata, never storage. The caller must hold
// durable owner-wide admission blocking and drain writers. Require the original
// Bound PVC, no workload holds and a complete versioned Pod inventory without
// references, including terminal/deleting/unmanaged Pods. Pin the journal UID on
// the owner before replying; missing pinned evidence or changed UID/spec requires
// reconciliation, never recreation.
// @see api::proto/agynio/api/runner/v1/runner
// @see runners::internal/server/volume_anchor_migration
// @see orchestrator::internal/volumemigration/coordinator
func (s *Server) ReserveVolumeAnchorAdoption(ctx context.Context, req *runnerv1.ReserveVolumeAnchorAdoptionRequest) (*runnerv1.ReserveVolumeAnchorAdoptionResponse, error) {
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "volume_adoption_request_required")
	}
	if err := validateVolumeAdoptionInput(req.Id, req.Expected, req.Intent, false); err != nil {
		return nil, err
	}
	if _, err := s.checkVolumeBackend(ctx, req.Expected.BackendId); err != nil {
		return nil, err
	}
	objects := s.clientset.CoreV1().ConfigMaps(s.namespace)
	journal, err := objects.Get(ctx, adoptionJournalName(req.Expected), metav1.GetOptions{})
	if err == nil {
		a, err := readAdoptionJournal(journal, s.namespace)
		if err != nil {
			return nil, err
		}
		intent := proto.Clone(a.Anchor).(*runnerv1.ResourceAnchor)
		intent.InstanceUid = ""
		if a.Id != req.Id || !proto.Equal(a.Previous, req.Expected) || !proto.Equal(intent, req.Intent) {
			return nil, status.Error(codes.FailedPrecondition, "volume_adoption_already_reserved")
		}
		owner, err := objects.Get(ctx, resourceAnchorName(a.Anchor), metav1.GetOptions{})
		if err != nil {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
		if err := s.pinVolumeAdoptionJournal(ctx, owner, a); err != nil {
			return nil, err
		}
		if _, _, _, err := s.observeVolumeAdoption(ctx, a); err != nil {
			return nil, err
		}
		return &runnerv1.ReserveVolumeAnchorAdoptionResponse{Adoption: a}, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	pvc, err := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace).Get(ctx, req.Expected.InstanceId, metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if err := matchPreparedPVC(pvc, req.Expected, s.namespace); err != nil {
		return nil, err
	}
	if pvc.Annotations[volumeAdoptionJournalAnnotation] != "" || pvc.Annotations[volumeAdoptionStateAnnotation] != "" {
		return nil, status.Error(codes.FailedPrecondition, "volume_adoption_history_missing")
	}
	if err := s.adoptionIdlePVC(ctx, pvc, ""); err != nil {
		return nil, err
	}
	a := &runnerv1.VolumeAnchorAdoption{Id: req.Id, Previous: proto.Clone(req.Expected).(*runnerv1.VolumeListItem), Anchor: proto.Clone(req.Intent).(*runnerv1.ResourceAnchor), PvcSpecSha256: adoptionSpecHash(pvc)}
	owner, err := objects.Get(ctx, resourceAnchorName(a.Anchor), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		identity, _ := protojson.Marshal(req.Intent)
		original, _ := protojson.Marshal(a)
		immutable := true
		owner, err = objects.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: resourceAnchorName(a.Anchor), Namespace: s.namespace,
			Labels: maps.Clone(a.Anchor.IdentityLabels), Annotations: map[string]string{resourceAnchorVersionAnnotation: volumeAdoptingVersion, volumeAdoptionIntentAnnotation: string(original)}},
			Immutable: &immutable, Data: map[string]string{"identity.json": string(identity)}}, metav1.CreateOptions{FieldValidation: metav1.FieldValidationStrict})
		if apierrors.IsAlreadyExists(err) {
			owner, err = objects.Get(ctx, resourceAnchorName(a.Anchor), metav1.GetOptions{})
		}
	}
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if owner == nil || owner.Annotations[volumeAdoptionJournalAnnotation] != "" {
		return nil, status.Error(codes.FailedPrecondition, "volume_adoption_journal_missing")
	}
	a.Anchor.InstanceUid = string(owner.UID)
	if active, err := matchVolumeAdoptionOwner(owner, a, s.namespace); err != nil {
		return nil, err
	} else if active {
		return nil, status.Error(codes.FailedPrecondition, "volume_adoption_journal_missing")
	}
	if err := validateVolumeAdoption(a, false); err != nil {
		return nil, err
	}
	data, err := protojson.Marshal(a)
	if err != nil || len(data) > maxVolumeAdoptionBytes {
		return nil, status.Error(codes.InvalidArgument, "volume_adoption_receipt_too_large")
	}
	immutable := true
	journal, err = objects.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: adoptionJournalName(a.Previous), Namespace: s.namespace,
		Labels: maps.Clone(a.Anchor.IdentityLabels), Annotations: map[string]string{resourceAnchorVersionAnnotation: volumeAdoptionVersion}}, Immutable: &immutable,
		Data: map[string]string{"adoption.json": string(data)}}, metav1.CreateOptions{FieldValidation: metav1.FieldValidationStrict})
	if apierrors.IsAlreadyExists(err) {
		journal, err = objects.Get(ctx, adoptionJournalName(a.Previous), metav1.GetOptions{})
	}
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	recorded, err := readAdoptionJournal(journal, s.namespace)
	if err != nil {
		return nil, err
	}
	a.InstanceUid = recorded.InstanceUid
	if !proto.Equal(a, recorded) {
		return nil, status.Error(codes.FailedPrecondition, "volume_adoption_receipt_changed")
	}
	if err := s.pinVolumeAdoptionJournal(ctx, owner, a); err != nil {
		return nil, err
	}
	if _, _, _, err := s.observeVolumeAdoption(ctx, a); err != nil {
		return nil, err
	}
	return &runnerv1.ReserveVolumeAnchorAdoptionResponse{Adoption: a}, nil
}

func (s *Server) observeVolumeAdoption(ctx context.Context, value *runnerv1.VolumeAnchorAdoption) (*corev1.ConfigMap, *corev1.PersistentVolumeClaim, *runnerv1.ObserveVolumeAnchorAdoptionResponse, error) {
	if err := validateVolumeAdoption(value, true); err != nil {
		return nil, nil, nil, err
	}
	a := proto.Clone(value).(*runnerv1.VolumeAnchorAdoption)
	if _, err := s.checkVolumeBackend(ctx, a.Previous.BackendId); err != nil {
		return nil, nil, nil, err
	}
	objects := s.clientset.CoreV1().ConfigMaps(s.namespace)
	journal, err := objects.Get(ctx, adoptionJournalName(a.Previous), metav1.GetOptions{})
	if err != nil {
		return nil, nil, nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	stored, err := readAdoptionJournal(journal, s.namespace)
	if err != nil {
		return nil, nil, nil, err
	}
	if !proto.Equal(stored, a) {
		return nil, nil, nil, status.Error(codes.FailedPrecondition, "volume_adoption_receipt_changed")
	}
	owner, err := objects.Get(ctx, resourceAnchorName(a.Anchor), metav1.GetOptions{})
	if err != nil {
		return nil, nil, nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	active, err := matchVolumeAdoptionOwner(owner, a, s.namespace)
	if err != nil {
		return nil, nil, nil, err
	}
	if owner.Annotations[volumeAdoptionJournalAnnotation] != a.InstanceUid {
		return nil, nil, nil, status.Error(codes.FailedPrecondition, "volume_adoption_owner_journal_changed")
	}
	pvc, err := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace).Get(ctx, a.Previous.InstanceId, metav1.GetOptions{})
	if err != nil {
		return nil, nil, nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if pvc == nil || pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" || adoptionSpecHash(pvc) != a.PvcSpecSha256 {
		return nil, nil, nil, status.Error(codes.FailedPrecondition, "volume_adoption_storage_changed")
	}
	bound := proto.Clone(a.Previous).(*runnerv1.VolumeListItem)
	state := runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_RESERVED
	hold := volumeAdoptionHoldPrefix + a.Id
	for _, finalizer := range pvc.Finalizers {
		if strings.HasPrefix(finalizer, volumeAdoptionHoldPrefix) && finalizer != hold {
			return nil, nil, nil, status.Error(codes.FailedPrecondition, "volume_adoption_foreign_hold")
		}
	}
	if len(pvc.OwnerReferences) > 0 || pvc.Annotations[resourceAnchorAnnotation] != "" {
		bound.Anchor = proto.Clone(a.Anchor).(*runnerv1.ResourceAnchor)
		if pvc.Annotations[volumeAdoptionJournalAnnotation] != a.InstanceUid {
			return nil, nil, nil, status.Error(codes.FailedPrecondition, "volume_adoption_pvc_receipt_changed")
		}
		switch pvc.Annotations[volumeAdoptionStateAnnotation] {
		case "applied":
			if !slices.Contains(pvc.Finalizers, hold) {
				return nil, nil, nil, status.Error(codes.FailedPrecondition, "volume_adoption_hold_missing")
			}
			state = runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_APPLIED
		case "ready":
			if !active || slices.Contains(pvc.Finalizers, hold) {
				return nil, nil, nil, status.Error(codes.FailedPrecondition, "volume_adoption_finalization_incomplete")
			}
			state = runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_READY
		default:
			return nil, nil, nil, status.Error(codes.FailedPrecondition, "volume_adoption_pvc_state_invalid")
		}
	} else if active || slices.Contains(pvc.Finalizers, hold) || pvc.Annotations[volumeAdoptionJournalAnnotation] != "" || pvc.Annotations[volumeAdoptionStateAnnotation] != "" {
		return nil, nil, nil, status.Error(codes.FailedPrecondition, "volume_adoption_partial_pvc_metadata")
	}
	if err := matchPreparedPVC(pvc, bound, s.namespace); err != nil {
		return nil, nil, nil, err
	}
	current, err := objects.Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if current == nil || current.UID != owner.UID || current.ResourceVersion != owner.ResourceVersion {
		return nil, nil, nil, status.Error(codes.Aborted, "volume_adoption_owner_changed_during_observation")
	}
	if _, err := s.checkVolumeBackend(ctx, a.Previous.BackendId); err != nil {
		return nil, nil, nil, err
	}
	return owner, pvc, &runnerv1.ObserveVolumeAnchorAdoptionResponse{Adoption: a, Volume: bound, State: state}, nil
}

// ObserveVolumeAnchorAdoption is read-only and matches the journal, owner, original
// PVC spec/UID and live backend. RESERVED/APPLIED/READY are distinct; missing or
// changed evidence is an error, not absence or permission to rebind.
func (s *Server) ObserveVolumeAnchorAdoption(ctx context.Context, req *runnerv1.ObserveVolumeAnchorAdoptionRequest) (*runnerv1.ObserveVolumeAnchorAdoptionResponse, error) {
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "volume_adoption_request_required")
	}
	_, _, response, err := s.observeVolumeAdoption(ctx, req.Adoption)
	return response, err
}

// ApplyVolumeAnchorAdoption attaches the exact owner, receipt and migration hold
// atomically with PVC UID/resource-version tests, preserving spec and unrelated
// metadata. APPLIED is not reusable: persist that binding in the registry under
// the owner block before finalization may remove this operation's hold.
func (s *Server) ApplyVolumeAnchorAdoption(ctx context.Context, req *runnerv1.ApplyVolumeAnchorAdoptionRequest) (*runnerv1.ApplyVolumeAnchorAdoptionResponse, error) {
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "volume_adoption_request_required")
	}
	_, pvc, observed, err := s.observeVolumeAdoption(ctx, req.Adoption)
	if err != nil {
		return nil, err
	}
	if observed.State == runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_RESERVED {
		if err := s.adoptionIdlePVC(ctx, pvc, ""); err != nil {
			return nil, err
		}
		updated := pvc.DeepCopy()
		if err := attachResourceAnchor(&updated.ObjectMeta, observed.Adoption.Anchor); err != nil {
			return nil, err
		}
		updated.Annotations[volumeAdoptionJournalAnnotation] = observed.Adoption.InstanceUid
		updated.Annotations[volumeAdoptionStateAnnotation] = "applied"
		updated.Finalizers = append(updated.Finalizers, volumeAdoptionHoldPrefix+observed.Adoption.Id)
		patched, err := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace).Patch(ctx, pvc.Name, types.JSONPatchType,
			preparedObjectPatch(pvc.ObjectMeta, preparedPatchOperation{"add", "/metadata/annotations", updated.Annotations},
				preparedPatchOperation{"add", "/metadata/ownerReferences", updated.OwnerReferences}, preparedPatchOperation{"add", "/metadata/finalizers", updated.Finalizers}), metav1.PatchOptions{})
		if err != nil {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
		if patched == nil || !reflect.DeepEqual(patched.Spec, pvc.Spec) {
			return nil, status.Error(codes.FailedPrecondition, "volume_adoption_patch_changed_storage")
		}
		_, _, observed, err = s.observeVolumeAdoption(ctx, observed.Adoption)
		if err != nil {
			return nil, err
		}
		if observed.State != runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_APPLIED && observed.State != runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_READY {
			return nil, status.Error(codes.FailedPrecondition, "volume_adoption_apply_unconfirmed")
		}
	}
	return &runnerv1.ApplyVolumeAnchorAdoptionResponse{Adoption: observed.Adoption, Volume: observed.Volume, State: observed.State}, nil
}

// FinalizeVolumeAnchorAdoption activates owner metadata, rechecks storage/drain,
// then marks READY and removes only its hold. Partial finalization remains blocked;
// once READY, retries only observe. Adoption never allocates, resizes or deletes
// PVCs/Pods, supplies credentials or retries a turn.
func (s *Server) FinalizeVolumeAnchorAdoption(ctx context.Context, req *runnerv1.FinalizeVolumeAnchorAdoptionRequest) (*runnerv1.FinalizeVolumeAnchorAdoptionResponse, error) {
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "volume_adoption_request_required")
	}
	owner, pvc, observed, err := s.observeVolumeAdoption(ctx, req.Adoption)
	if err != nil {
		return nil, err
	}
	if observed.State == runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_RESERVED {
		return nil, status.Error(codes.FailedPrecondition, "volume_adoption_must_be_applied")
	}
	if observed.State != runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_READY {
		if err := s.adoptionIdlePVC(ctx, pvc, volumeAdoptionHoldPrefix+observed.Adoption.Id); err != nil {
			return nil, err
		}
		if owner.Annotations[resourceAnchorVersionAnnotation] != "v1" {
			_, err := s.clientset.CoreV1().ConfigMaps(s.namespace).Patch(ctx, owner.Name, types.JSONPatchType,
				preparedObjectPatch(owner.ObjectMeta, preparedPatchOperation{"add", "/metadata/annotations/agyn.io~1resource-anchor-version", "v1"}), metav1.PatchOptions{})
			if err != nil {
				return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
			}
			_, pvc, observed, err = s.observeVolumeAdoption(ctx, observed.Adoption)
			if err != nil {
				return nil, err
			}
			if observed.State == runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_READY {
				return &runnerv1.FinalizeVolumeAnchorAdoptionResponse{Adoption: observed.Adoption, Volume: observed.Volume, State: observed.State}, nil
			}
			if err := s.adoptionIdlePVC(ctx, pvc, volumeAdoptionHoldPrefix+observed.Adoption.Id); err != nil {
				return nil, err
			}
		}
		updated := pvc.DeepCopy()
		updated.Annotations[volumeAdoptionStateAnnotation] = "ready"
		updated.Finalizers = slices.DeleteFunc(updated.Finalizers, func(f string) bool { return f == volumeAdoptionHoldPrefix+observed.Adoption.Id })
		patched, err := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace).Patch(ctx, pvc.Name, types.JSONPatchType,
			preparedObjectPatch(pvc.ObjectMeta, preparedPatchOperation{"add", "/metadata/annotations", updated.Annotations},
				preparedPatchOperation{"add", "/metadata/finalizers", updated.Finalizers}), metav1.PatchOptions{})
		if err != nil {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
		if patched == nil || !reflect.DeepEqual(patched.Spec, pvc.Spec) {
			return nil, status.Error(codes.FailedPrecondition, "volume_adoption_finalization_changed_storage")
		}
		_, _, observed, err = s.observeVolumeAdoption(ctx, observed.Adoption)
		if err != nil {
			return nil, err
		}
		if observed.State != runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_READY {
			return nil, status.Error(codes.FailedPrecondition, "volume_adoption_finalization_unconfirmed")
		}
	}
	return &runnerv1.FinalizeVolumeAnchorAdoptionResponse{Adoption: observed.Adoption, Volume: observed.Volume, State: observed.State}, nil
}
