package server

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
)

// Workload IDs, workload_key and thread-id change across starts of the same
// persistent owner. Only persistent identity participates in claim reuse.
var pvcIdentityLabelKeys = [...]string{
	managedByLabelKey, workloadManagedByLabelKey, volumeKeyLabelKey, "managed-by",
	"agent-instance-id", "agent-id", "sandbox-id", "sandbox-owner-id",
}

func validatePVCReuse(existing, desired *corev1.PersistentVolumeClaim) error {
	if existing != nil && (len(existing.OwnerReferences) != 0 || existing.Annotations[resourceAnchorAnnotation] != "") {
		return status.Errorf(codes.FailedPrecondition, "pvc_unexpected_owner_reference: %s", existing.Name)
	}
	return validatePVCReuseSpec(existing, desired)
}

// validatePVCReuseSpec preserves owner/key identity across workload/thread changes.
// Reuse never repairs labels, resizes claims or changes an implicit storage class.
// Adoption requires READY with no migration hold; an active owner alone is not
// enough. Validation is neither caller authentication nor a storage fence.
func validatePVCReuseSpec(existing, desired *corev1.PersistentVolumeClaim) error {
	if existing == nil || existing.Name != desired.Name || existing.Namespace != desired.Namespace {
		return status.Error(codes.FailedPrecondition, "pvc_identity_mismatch")
	}
	if existing.DeletionTimestamp != nil || existing.Status.Phase == corev1.ClaimLost {
		return status.Errorf(codes.FailedPrecondition, "pvc_not_reusable: %s", existing.Name)
	}
	state, journal := existing.Annotations[volumeAdoptionStateAnnotation], existing.Annotations[volumeAdoptionJournalAnnotation]
	if state != "" || journal != "" {
		if state != "ready" || !validPreparedID(journal) || len(existing.OwnerReferences) != 1 || existing.Annotations[resourceAnchorAnnotation] == "" {
			return status.Error(codes.FailedPrecondition, "pvc_anchor_adoption_incomplete")
		}
	}
	for _, finalizer := range existing.Finalizers {
		if strings.HasPrefix(finalizer, volumeAdoptionHoldPrefix) {
			return status.Error(codes.FailedPrecondition, "pvc_anchor_adoption_incomplete")
		}
	}
	for _, key := range pvcIdentityLabelKeys {
		actual, present := existing.Labels[key]
		expected, required := desired.Labels[key]
		if present != required || actual != expected {
			return status.Errorf(codes.FailedPrecondition, "pvc_identity_mismatch: %s (%s)", existing.Name, key)
		}
	}
	if existing.Spec.VolumeMode != nil && *existing.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		return status.Errorf(codes.FailedPrecondition, "pvc_volume_mode_mismatch: %s", existing.Name)
	}
	if len(existing.Spec.AccessModes) != 1 || existing.Spec.AccessModes[0] != corev1.ReadWriteOnce && existing.Spec.AccessModes[0] != corev1.ReadWriteOncePod {
		return status.Errorf(codes.FailedPrecondition, "pvc_access_mode_mismatch: %s", existing.Name)
	}
	actualSize := existing.Spec.Resources.Requests[corev1.ResourceStorage]
	if actualSize.Cmp(desired.Spec.Resources.Requests[corev1.ResourceStorage]) < 0 {
		return status.Errorf(codes.FailedPrecondition, "pvc_storage_size_mismatch: %s", existing.Name)
	}
	// An unspecified class delegates initial allocation to the cluster. Reuse
	// must retain that choice, even if the cluster default has since changed.
	if desired.Spec.StorageClassName != nil && (existing.Spec.StorageClassName == nil || *existing.Spec.StorageClassName != *desired.Spec.StorageClassName) {
		return status.Errorf(codes.FailedPrecondition, "pvc_storage_class_mismatch: %s", existing.Name)
	}
	return nil
}
