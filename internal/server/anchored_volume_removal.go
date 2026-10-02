package server

import (
	"context"
	"maps"
	"strings"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const volumeRetirementVersion = "volume-retiring-v1"
const volumeRetirementPVCAnnotation = "agyn.io/retiring-pvc-uid"

func matchVolumeRetirementOwner(owner *corev1.ConfigMap, expected *runnerv1.VolumeListItem, namespace string) (bool, error) {
	if owner != nil && owner.Annotations[resourceAnchorVersionAnnotation] == volumeRetirementVersion {
		if owner.Annotations[volumeRetirementPVCAnnotation] != expected.InstanceUid {
			return false, status.Error(codes.FailedPrecondition, "volume_retirement_target_changed")
		}
		original := owner.DeepCopy()
		original.Annotations[resourceAnchorVersionAnnotation] = "v1"
		return true, matchResourceAnchor(original, expected.Anchor, namespace)
	}
	if owner != nil && owner.Annotations[volumeRetirementPVCAnnotation] != "" {
		return false, status.Error(codes.FailedPrecondition, "volume_retirement_state_invalid")
	}
	return false, matchResourceAnchor(owner, expected.Anchor, namespace)
}

func (s *Server) claimVolumeRetirement(ctx context.Context, owner *corev1.ConfigMap, expected *runnerv1.VolumeListItem) (*corev1.ConfigMap, error) {
	// Both old and new preparation readers reject this non-active version.
	// The exact PVC UID also prevents another retirement request retargeting it.
	patched, err := s.clientset.CoreV1().ConfigMaps(s.namespace).Patch(ctx, owner.Name, types.JSONPatchType,
		preparedObjectPatch(owner.ObjectMeta,
			preparedPatchOperation{"add", "/metadata/annotations/agyn.io~1resource-anchor-version", volumeRetirementVersion},
			preparedPatchOperation{"add", "/metadata/annotations/agyn.io~1retiring-pvc-uid", expected.InstanceUid}), metav1.PatchOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	retiring, err := matchVolumeRetirementOwner(patched, expected, s.namespace)
	if err != nil {
		return nil, err
	}
	if !retiring || patched.DeletionTimestamp != nil {
		return nil, status.Error(codes.FailedPrecondition, "volume_retirement_claim_unconfirmed")
	}
	return patched, nil
}

// RemoveVolumeAnchored requires durable registry intent and excluded admission.
// Pin the original PVC UID on its exact owner, delete conditionally, observe PVC
// absence, then retire the owner. Holds return PENDING; ABSENT needs both objects
// absent in the pinned backend. Never retarget replacement owners or different-UID
// late children; observe owner-based GC. This is not between-turn compute release.
// @see runners::internal/server/anchored_volume_removal
// @see orchestrator::internal/reconciler/anchored_volume_removal
func (s *Server) RemoveVolumeAnchored(ctx context.Context, req *runnerv1.RemoveVolumeAnchoredRequest) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
	if err := validateVolumeRemovalTarget(req.GetExpected()); err != nil {
		return nil, err
	}
	if req.Expected.Anchor == nil || !validPreparedID(req.Expected.InstanceUid) || len(req.ProtoReflect().GetUnknown()) != 0 || len(req.Expected.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "anchored_volume_target_required")
	}
	expected := proto.Clone(req.Expected).(*runnerv1.VolumeListItem)
	if _, err := s.checkVolumeBackend(ctx, expected.BackendId); err != nil {
		return nil, err
	}
	response := func(state runnerv1.VolumeRemovalState) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
		backend, err := s.checkVolumeBackend(ctx, expected.BackendId)
		if err != nil {
			return nil, err
		}
		return &runnerv1.RemoveVolumeAnchoredResponse{State: state, BackendId: backend, Anchor: expected.Anchor}, nil
	}
	owners := s.clientset.CoreV1().ConfigMaps(s.namespace)
	owner, err := owners.Get(ctx, resourceAnchorName(expected.Anchor), metav1.GetOptions{})
	retiring := false
	if err == nil {
		retiring, err = matchVolumeRetirementOwner(owner, expected, s.namespace)
		if err != nil {
			return nil, err
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	} else {
		owner = nil
	}
	retireOwner := func() (*runnerv1.RemoveVolumeAnchoredResponse, error) {
		if owner == nil {
			return response(runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT)
		}
		if owner.DeletionTimestamp == nil {
			if !retiring {
				owner, err = s.claimVolumeRetirement(ctx, owner, expected)
				if err != nil {
					return nil, err
				}
			}
			background := metav1.DeletePropagationBackground
			err := owners.Delete(ctx, owner.Name, metav1.DeleteOptions{PropagationPolicy: &background, Preconditions: &metav1.Preconditions{
				UID: &owner.UID, ResourceVersion: &owner.ResourceVersion,
			}})
			if err != nil && !apierrors.IsNotFound(err) {
				return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
			}
		}
		return response(runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING)
	}
	claims := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace)
	pvc, err := claims.Get(ctx, expected.InstanceId, metav1.GetOptions{})
	if err == nil {
		if pvc == nil || pvc.Name != expected.InstanceId || pvc.Namespace != s.namespace || !validPreparedID(string(pvc.UID)) ||
			pvc.ResourceVersion == "" || !maps.Equal(volumeIdentityLabels(pvc.Labels), expected.IdentityLabels) {
			return nil, status.Error(codes.FailedPrecondition, "anchored_volume_instance_mismatch")
		}
		if err := matchAnchoredMetadata(pvc.ObjectMeta, expected.Anchor); err != nil {
			return nil, err
		}
		if string(pvc.UID) != expected.InstanceUid && owner != nil && owner.DeletionTimestamp == nil && !retiring {
			return nil, status.Error(codes.FailedPrecondition, "anchored_volume_instance_mismatch")
		}
		for _, finalizer := range pvc.Finalizers {
			if strings.HasPrefix(finalizer, preparedHoldPrefix) {
				return response(runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING)
			}
		}
		if string(pvc.UID) != expected.InstanceUid {
			// A late child of this revoked incarnation is not a new binding.
			// Do not delete by its discovered UID; observe owner-based GC instead.
			if owner != nil && owner.DeletionTimestamp == nil {
				return retireOwner()
			}
			return response(runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING)
		}
		if pvc.DeletionTimestamp == nil {
			if owner != nil && owner.DeletionTimestamp == nil && !retiring {
				owner, err = s.claimVolumeRetirement(ctx, owner, expected)
				if err != nil {
					return nil, err
				}
			}
			err := claims.Delete(ctx, pvc.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{
				UID: &pvc.UID, ResourceVersion: &pvc.ResourceVersion,
			}})
			if err != nil && !apierrors.IsNotFound(err) {
				return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
			}
		}
		// Keep the owner until a separate read confirms the exact PVC is gone.
		// GC must not bypass the conditional PVC deletion or a workload hold.
		return response(runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING)
	}
	if !apierrors.IsNotFound(err) {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	return retireOwner()
}
