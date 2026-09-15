package server

import (
	"context"
	"maps"
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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

const preparationRevokedVersion = "preparation-revoked-v1"
const preparationRevocationAnnotation = "agyn.io/preparation-revocation"
const preparationRevocationVersionAnnotation = "agyn.io/preparation-revocation-version"
const maxPreparationRevocationBytes = 128 * 1024

func preparationRevocationName(anchor *runnerv1.ResourceAnchor) string {
	return "preparation-revocation-" + anchor.ResourceId
}

func canonicalPreparationRevocation(value *runnerv1.PreparationRevocation, bound bool) (*runnerv1.PreparationRevocation, error) {
	if value == nil || len(value.ProtoReflect().GetUnknown()) != 0 || len(value.VolumeAnchors) > maxPreparedVolumes ||
		bound && !validPreparedID(value.InstanceUid) || !bound && value.InstanceUid != "" ||
		value.SelectedPodUid != "" && !validPreparedID(value.SelectedPodUid) {
		return nil, status.Error(codes.InvalidArgument, "complete_preparation_revocation_required")
	}
	work := value.WorkloadAnchor
	if err := validateResourceAnchor(work, true); err != nil {
		return nil, err
	}
	if work.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD {
		return nil, status.Error(codes.InvalidArgument, "workload_anchor_required")
	}
	seen := map[string]bool{}
	for _, volume := range value.VolumeAnchors {
		if err := validateResourceAnchor(volume, true); err != nil {
			return nil, err
		}
		owner := maps.Clone(volume.IdentityLabels)
		delete(owner, volumeKeyLabelKey)
		if volume.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME || volume.BackendId != work.BackendId ||
			seen[volume.ResourceId] || !maps.Equal(owner, volumeIdentityLabels(work.IdentityLabels)) {
			return nil, status.Error(codes.InvalidArgument, "revocation_volume_owner_mismatch")
		}
		seen[volume.ResourceId] = true
	}
	copy := proto.Clone(value).(*runnerv1.PreparationRevocation)
	slices.SortFunc(copy.VolumeAnchors, func(a, b *runnerv1.ResourceAnchor) int { return strings.Compare(a.ResourceId, b.ResourceId) })
	if data, err := protojson.Marshal(copy); err != nil || len(data) > maxPreparationRevocationBytes {
		return nil, status.Error(codes.InvalidArgument, "preparation_revocation_too_large")
	}
	return copy, nil
}

func samePreparationRevocationIntent(a, b *runnerv1.PreparationRevocation) bool {
	a, b = proto.Clone(a).(*runnerv1.PreparationRevocation), proto.Clone(b).(*runnerv1.PreparationRevocation)
	a.InstanceUid, a.SelectedPodUid, b.InstanceUid, b.SelectedPodUid = "", "", "", ""
	return proto.Equal(a, b)
}

func (s *Server) readPreparationRevocation(ctx context.Context, expected *runnerv1.PreparationRevocation) (*runnerv1.PreparationRevocation, error) {
	cm, err := s.clientset.CoreV1().ConfigMaps(s.namespace).Get(ctx, preparationRevocationName(expected.WorkloadAnchor), metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if cm == nil || cm.Name != preparationRevocationName(expected.WorkloadAnchor) || cm.Namespace != s.namespace || !validPreparedID(string(cm.UID)) ||
		cm.ResourceVersion == "" || cm.DeletionTimestamp != nil || cm.Immutable == nil || !*cm.Immutable || len(cm.OwnerReferences) != 0 ||
		cm.Annotations[preparationRevocationVersionAnnotation] != "v1" || !maps.Equal(cm.Labels, expected.WorkloadAnchor.IdentityLabels) || len(cm.Data) != 1 || len(cm.BinaryData) != 0 {
		return nil, status.Error(codes.FailedPrecondition, "preparation_revocation_record_mismatch")
	}
	stored := &runnerv1.PreparationRevocation{}
	data := cm.Data["revocation.json"]
	if len(data) > maxPreparationRevocationBytes || protojson.Unmarshal([]byte(data), stored) != nil {
		return nil, status.Error(codes.FailedPrecondition, "preparation_revocation_record_invalid")
	}
	value, err := canonicalPreparationRevocation(stored, false)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "preparation_revocation_record_invalid")
	}
	value.InstanceUid = string(cm.UID)
	if !samePreparationRevocationIntent(value, expected) || expected.InstanceUid != "" && !proto.Equal(value, expected) {
		return nil, status.Error(codes.FailedPrecondition, "preparation_revocation_identity_changed")
	}
	return value, nil
}

func matchRevokedPreparationOwner(cm *corev1.ConfigMap, expected *runnerv1.PreparationRevocation, namespace string) error {
	if cm == nil || cm.Annotations[resourceAnchorVersionAnnotation] != preparationRevokedVersion || cm.Annotations[resourceAnchorActivationAnnotation] != "" {
		return status.Error(codes.FailedPrecondition, "preparation_revocation_claim_required")
	}
	active := cm.DeepCopy()
	active.Annotations[resourceAnchorVersionAnnotation] = "v1"
	if err := matchResourceAnchor(active, expected.WorkloadAnchor, namespace); err != nil {
		return err
	}
	stored := &runnerv1.PreparationRevocation{}
	data := cm.Annotations[preparationRevocationAnnotation]
	if len(data) > maxPreparationRevocationBytes || protojson.Unmarshal([]byte(data), stored) != nil {
		return status.Error(codes.FailedPrecondition, "preparation_revocation_claim_invalid")
	}
	value, err := canonicalPreparationRevocation(stored, false)
	if err != nil || !samePreparationRevocationIntent(value, expected) || value.SelectedPodUid != cm.Annotations[resourceAnchorPodAnnotation] ||
		expected.InstanceUid != "" && expected.SelectedPodUid != value.SelectedPodUid {
		return status.Error(codes.FailedPrecondition, "preparation_revocation_claim_changed")
	}
	return nil
}

func (s *Server) validateRevocationPod(pod *corev1.Pod, expected *runnerv1.PreparationRevocation) error {
	if pod == nil || pod.Annotations[preparationRecoveryAnnotation] != preparationRecoveryVersion {
		return status.Error(codes.FailedPrecondition, "revocation_pod_credential_ownership_required")
	}
	stored := &runnerv1.WorkloadBinding{}
	data := pod.Annotations[preparedBindingAnnotation]
	if len(data) > 64*1024 || protojson.Unmarshal([]byte(data), stored) != nil || stored.InstanceUid != "" ||
		stored.WorkloadId != expected.WorkloadAnchor.ResourceId || stored.BackendId != expected.WorkloadAnchor.BackendId || !proto.Equal(stored.Anchor, expected.WorkloadAnchor) {
		return status.Error(codes.FailedPrecondition, "revocation_pod_binding_mismatch")
	}
	stored.InstanceUid = string(pod.UID)
	binding, err := canonicalBinding(stored)
	if err != nil {
		return err
	}
	if err := s.matchPreparedPod(pod, binding); err != nil {
		return err
	}
	anchors := &runnerv1.PreparationRevocation{WorkloadAnchor: binding.Anchor}
	for _, volume := range binding.Volumes {
		anchors.VolumeAnchors = append(anchors.VolumeAnchors, volume.Anchor)
	}
	actual, err := canonicalPreparationRevocation(anchors, false)
	if err != nil || !samePreparationRevocationIntent(actual, expected) {
		return status.Error(codes.FailedPrecondition, "revocation_pod_volume_set_changed")
	}
	state := pod.Annotations[preparedStateAnnotation]
	if state != "preparing" && state != "prepared" || !hasPreparedGate(pod) || pod.Spec.NodeName != "" || pod.Status.Phase != "" && pod.Status.Phase != corev1.PodPending {
		return status.Error(codes.FailedPrecondition, "preparation_not_proven_unactivated")
	}
	statuses := append(slices.Clone(pod.Status.InitContainerStatuses), pod.Status.ContainerStatuses...)
	statuses = append(statuses, pod.Status.EphemeralContainerStatuses...)
	for _, container := range statuses {
		if container.State.Running != nil || container.State.Terminated != nil || container.LastTerminationState.Terminated != nil {
			return status.Error(codes.FailedPrecondition, "preparation_has_execution_evidence")
		}
	}
	return nil
}

func (s *Server) RevokeWorkloadPreparation(ctx context.Context, req *runnerv1.RevokeWorkloadPreparationRequest) (*runnerv1.RevokeWorkloadPreparationResponse, error) {
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "preparation_revocation_intent_required")
	}
	intent, err := canonicalPreparationRevocation(&runnerv1.PreparationRevocation{WorkloadAnchor: req.WorkloadAnchor, VolumeAnchors: req.VolumeAnchors}, false)
	if err != nil {
		return nil, err
	}
	anchor := intent.WorkloadAnchor
	if _, err := s.checkVolumeBackend(ctx, anchor.BackendId); err != nil {
		return nil, err
	}
	receipt, err := s.readPreparationRevocation(ctx, intent)
	if err != nil && status.Code(err) != codes.NotFound {
		return nil, err
	}
	objects := s.clientset.CoreV1().ConfigMaps(s.namespace)
	owner, err := objects.Get(ctx, resourceAnchorName(anchor), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if receipt == nil {
			return nil, status.Error(codes.FailedPrecondition, "preparation_revocation_unproven")
		}
	} else if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	} else {
		if owner == nil {
			return nil, status.Error(codes.FailedPrecondition, "preparation_revocation_owner_missing")
		}
		if owner.Annotations[resourceAnchorVersionAnnotation] == preparationRevokedVersion {
			if err := matchRevokedPreparationOwner(owner, intent, s.namespace); err != nil {
				return nil, err
			}
		} else {
			if err := matchResourceAnchor(owner, anchor, s.namespace); err != nil {
				return nil, err
			}
			if receipt != nil || owner.DeletionTimestamp != nil || owner.Annotations[resourceAnchorActivationAnnotation] != "" || owner.Annotations[preparationRevocationAnnotation] != "" {
				return nil, status.Error(codes.FailedPrecondition, "unclaimed_activation_required_for_revocation")
			}
		}
		intent.SelectedPodUid = owner.Annotations[resourceAnchorPodAnnotation]
		pod, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, podNameFromID(anchor.ResourceId), metav1.GetOptions{})
		if err == nil {
			if err := s.validateRevocationPod(pod, intent); err != nil {
				return nil, err
			}
		} else if !apierrors.IsNotFound(err) {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
		if owner.Annotations[resourceAnchorVersionAnnotation] != preparationRevokedVersion {
			data, err := protojson.Marshal(intent)
			if err != nil {
				return nil, status.Error(codes.Internal, "preparation_revocation_encoding_failed")
			}
			// Activation and revocation linearize on the same owner UID/revision.
			// Old preparation/activation readers reject this non-active version.
			owner, err = objects.Patch(ctx, owner.Name, types.JSONPatchType, preparedObjectPatch(owner.ObjectMeta,
				preparedPatchOperation{"add", "/metadata/annotations/agyn.io~1resource-anchor-version", preparationRevokedVersion},
				preparedPatchOperation{"add", "/metadata/annotations/agyn.io~1preparation-revocation", string(data)}), metav1.PatchOptions{})
			if err != nil {
				return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
			}
			if err := matchRevokedPreparationOwner(owner, intent, s.namespace); err != nil {
				return nil, err
			}
		}
		if receipt == nil {
			data, err := protojson.Marshal(intent)
			if err != nil {
				return nil, status.Error(codes.Internal, "preparation_revocation_encoding_failed")
			}
			immutable := true
			_, err = objects.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: preparationRevocationName(anchor), Namespace: s.namespace,
				Labels: maps.Clone(anchor.IdentityLabels), Annotations: map[string]string{preparationRevocationVersionAnnotation: "v1"}},
				Immutable: &immutable, Data: map[string]string{"revocation.json": string(data)}}, metav1.CreateOptions{FieldValidation: metav1.FieldValidationStrict})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
			}
			receipt, err = s.readPreparationRevocation(ctx, intent)
			if err != nil {
				return nil, err
			}
		}
		if err := matchRevokedPreparationOwner(owner, receipt, s.namespace); err != nil {
			return nil, err
		}
		// The independent immutable receipt survives deletion and lost replies.
		// No DELETE is issued until the stored receipt has been read and matched.
		if owner.DeletionTimestamp == nil {
			background := metav1.DeletePropagationBackground
			if err := objects.Delete(ctx, owner.Name, metav1.DeleteOptions{PropagationPolicy: &background, Preconditions: &metav1.Preconditions{UID: &owner.UID, ResourceVersion: &owner.ResourceVersion}}); err != nil && !apierrors.IsNotFound(err) {
				return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
			}
		}
	}
	if _, err := s.checkVolumeBackend(ctx, anchor.BackendId); err != nil {
		return nil, err
	}
	return &runnerv1.RevokeWorkloadPreparationResponse{Revocation: receipt}, nil
}

func (s *Server) ObservePreparationRevocation(ctx context.Context, req *runnerv1.ObservePreparationRevocationRequest) (*runnerv1.ObservePreparationRevocationResponse, error) {
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "preparation_revocation_receipt_required")
	}
	expected, err := canonicalPreparationRevocation(req.Expected, true)
	if err != nil {
		return nil, err
	}
	anchor := expected.WorkloadAnchor
	if _, err := s.checkVolumeBackend(ctx, anchor.BackendId); err != nil {
		return nil, err
	}
	receipt, err := s.readPreparationRevocation(ctx, expected)
	if err != nil {
		return nil, err
	}
	response := &runnerv1.ObservePreparationRevocationResponse{Revocation: receipt, State: runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_PENDING}
	absent := func() (bool, error) {
		owner, err := s.clientset.CoreV1().ConfigMaps(s.namespace).Get(ctx, resourceAnchorName(anchor), metav1.GetOptions{})
		if err == nil {
			return false, matchRevokedPreparationOwner(owner, expected, s.namespace)
		}
		if !apierrors.IsNotFound(err) {
			return false, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
		pod, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, podNameFromID(anchor.ResourceId), metav1.GetOptions{})
		if err == nil {
			return false, s.validateRevocationPod(pod, expected)
		}
		if !apierrors.IsNotFound(err) {
			return false, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
		return true, nil
	}
	if ok, err := absent(); !ok || err != nil {
		if err != nil {
			return nil, err
		}
		return response, nil
	}
	var found []*runnerv1.VolumeListItem
	var missing []string
	for _, volume := range expected.VolumeAnchors {
		if err := s.requireResourceAnchor(ctx, volume); err != nil {
			return nil, err
		}
		list, err := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace).List(ctx, metav1.ListOptions{Limit: 2, LabelSelector: labels.Set{volumeKeyLabelKey: volume.ResourceId}.AsSelector().String()})
		if err != nil {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
		if list == nil || list.Continue != "" || len(list.Items) > 1 {
			return nil, status.Error(codes.FailedPrecondition, "revocation_volume_inventory_incomplete_or_ambiguous")
		}
		if len(list.Items) == 0 {
			missing = append(missing, volume.ResourceId)
			continue
		}
		pvc := &list.Items[0]
		if !validPreparedID(string(pvc.UID)) || pvc.Namespace != s.namespace || pvc.ResourceVersion == "" || pvc.DeletionTimestamp != nil || pvc.Status.Phase == corev1.ClaimLost ||
			slices.ContainsFunc(pvc.Finalizers, func(f string) bool { return strings.HasPrefix(f, preparedHoldPrefix) }) {
			return nil, status.Error(codes.FailedPrecondition, "revocation_volume_not_recoverable")
		}
		if err := matchAnchoredMetadata(pvc.ObjectMeta, volume); err != nil {
			return nil, err
		}
		item, err := preparedVolume(pvc, anchor.BackendId)
		if err != nil {
			return nil, err
		}
		if err := validateVolumeRemovalTarget(item); err != nil {
			return nil, err
		}
		found = append(found, item)
	}
	if ok, err := absent(); !ok || err != nil {
		if err != nil {
			return nil, err
		}
		return response, nil
	}
	if _, err := s.readPreparationRevocation(ctx, expected); err != nil {
		return nil, err
	}
	if _, err := s.checkVolumeBackend(ctx, anchor.BackendId); err != nil {
		return nil, err
	}
	response.State, response.Volumes, response.AbsentVolumeIds = runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_POD_ABSENT, found, missing
	return response, nil
}
