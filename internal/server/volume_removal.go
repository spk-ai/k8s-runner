package server

import (
	"context"
	"maps"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

// Workload and turn identifiers change while a persistent workspace is idle.
// Keep only the labels that identify its manager and durable owner.
var volumeRemovalIdentityKeys = [...]string{
	managedByLabelKey, workloadManagedByLabelKey, volumeKeyLabelKey,
	"managed-by", "agent-instance-id", "agent-id", "sandbox-id", "sandbox-owner-id",
}

func volumeIdentityLabels(labels map[string]string) map[string]string {
	identity := make(map[string]string)
	for _, key := range volumeRemovalIdentityKeys {
		if value, exists := labels[key]; exists {
			identity[key] = value
		}
	}
	return identity
}

func validateVolumeRemovalTarget(expected *runnerv1.VolumeListItem) error {
	if expected == nil || expected.GetInstanceId() == "" || len(validation.IsDNS1123Subdomain(expected.GetInstanceId())) != 0 {
		return status.Error(codes.InvalidArgument, "valid_volume_instance_id_required")
	}
	uid := expected.GetInstanceUid()
	if uid == "" || strings.TrimSpace(uid) != uid || len(uid) > 256 {
		return status.Error(codes.InvalidArgument, "volume_instance_uid_required")
	}
	key := expected.GetVolumeKey()
	if key == "" || len(validation.IsValidLabelValue(key)) != 0 {
		return status.Error(codes.InvalidArgument, "valid_volume_key_required")
	}
	identity := expected.GetIdentityLabels()
	if identity[managedByLabelKey] != managedByLabelValue || identity[volumeKeyLabelKey] != key || !maps.Equal(identity, volumeIdentityLabels(identity)) {
		return status.Error(codes.InvalidArgument, "persistent_volume_identity_required")
	}
	for _, value := range identity {
		if value == "" || len(validation.IsValidLabelValue(value)) != 0 {
			return status.Error(codes.InvalidArgument, "invalid_volume_identity_label")
		}
	}
	return nil
}

func (s *Server) RemoveVolumeChecked(ctx context.Context, req *runnerv1.RemoveVolumeCheckedRequest) (*runnerv1.RemoveVolumeCheckedResponse, error) {
	expected := req.GetExpected()
	if err := validateVolumeRemovalTarget(expected); err != nil {
		return nil, err
	}
	claims := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace)
	pvc, err := claims.Get(ctx, expected.InstanceId, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return &runnerv1.RemoveVolumeCheckedResponse{State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT}, nil
	}
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if string(pvc.UID) != expected.InstanceUid || pvc.ResourceVersion == "" || len(pvc.OwnerReferences) != 0 || !maps.Equal(volumeIdentityLabels(pvc.Labels), expected.IdentityLabels) {
		return nil, status.Error(codes.FailedPrecondition, "volume_instance_identity_mismatch")
	}
	if pvc.DeletionTimestamp == nil {
		// A fresh GET supplies the resource version, but cannot replace the
		// durable UID/owner target. Conflicts are returned without retargeting.
		err := claims.Delete(ctx, pvc.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{
			UID: &pvc.UID, ResourceVersion: &pvc.ResourceVersion,
		}})
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
	}
	// Even a successful DELETE can leave a mounted/finalized claim present.
	// Only a subsequent GET returning NotFound confirms absence.
	return &runnerv1.RemoveVolumeCheckedResponse{State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING}, nil
}
