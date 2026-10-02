package server

import (
	"context"
	"slices"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const preparationRecoveryAnnotation = "agyn.io/preparation-recovery"
const preparationRecoveryVersion = "pod-owned-secrets/v1"

// ObserveWorkloadPreparation discovers a lost reply for retirement only. Require
// gated, unscheduled, unexecuted compute with the atomic Pod-owned Secret marker;
// validate every named claim and bounded owner labels across stable Pod/backend
// reads without mutations or Secret reads. The controller must persist REMOVING
// and this binding before cleanup. NotFound/Unimplemented and old ownership
// semantics do not release admission or permit another preparation.
// @see orchestrator::internal/reconciler/prepared_recovery
func (s *Server) ObserveWorkloadPreparation(ctx context.Context, req *runnerv1.ObserveWorkloadPreparationRequest) (*runnerv1.ObserveWorkloadPreparationResponse, error) {
	if !validPreparedID(req.GetWorkloadId()) || !validPreparedBackend(req.GetBackendId()) {
		return nil, status.Error(codes.InvalidArgument, "preparation_intent_and_backend_required")
	}
	if _, err := s.checkVolumeBackend(ctx, req.BackendId); err != nil {
		return nil, err
	}
	pods := s.clientset.CoreV1().Pods(s.namespace)
	pod, err := pods.Get(ctx, podNameFromID(req.WorkloadId), metav1.GetOptions{})
	if err != nil {
		// In particular, NotFound does not prove an in-flight create is over.
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if pod.Annotations[preparationRecoveryAnnotation] != preparationRecoveryVersion {
		return nil, status.Error(codes.FailedPrecondition, "prepared_credential_ownership_unconfirmed")
	}
	stored := &runnerv1.WorkloadBinding{}
	data := pod.Annotations[preparedBindingAnnotation]
	if len(data) > 64*1024 || protojson.Unmarshal([]byte(data), stored) != nil || stored.InstanceUid != "" || stored.WorkloadId != req.WorkloadId || stored.BackendId != req.BackendId {
		return nil, status.Error(codes.FailedPrecondition, "prepared_intent_binding_mismatch")
	}
	stored.InstanceUid = string(pod.UID)
	binding, err := canonicalBinding(stored)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "prepared_intent_binding_invalid")
	}
	if err := s.matchPreparedPod(pod, binding); err != nil {
		return nil, err
	}
	state := pod.Annotations[preparedStateAnnotation]
	if state != "preparing" && state != "prepared" || !hasPreparedGate(pod) || pod.Spec.NodeName != "" || pod.Status.Phase != "" && pod.Status.Phase != corev1.PodPending {
		return nil, status.Error(codes.FailedPrecondition, "preparation_not_proven_unactivated")
	}
	statuses := append(slices.Clone(pod.Status.InitContainerStatuses), pod.Status.ContainerStatuses...)
	statuses = append(statuses, pod.Status.EphemeralContainerStatuses...)
	for _, container := range statuses {
		if container.State.Running != nil || container.State.Terminated != nil || container.LastTerminationState.Terminated != nil {
			return nil, status.Error(codes.FailedPrecondition, "preparation_has_execution_evidence")
		}
	}
	for _, target := range binding.Volumes {
		claim, err := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace).Get(ctx, target.InstanceId, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil, status.Error(codes.FailedPrecondition, "prepared_volume_missing")
		}
		if err != nil {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
		if err := matchPreparedPVC(claim, target, s.namespace); err != nil {
			return nil, err
		}
		if claim.Status.Phase == corev1.ClaimLost || claim.DeletionTimestamp != nil {
			return nil, status.Error(codes.FailedPrecondition, "prepared_volume_not_recoverable")
		}
	}
	current, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if err := s.matchPreparedPod(current, binding); err != nil {
		return nil, err
	}
	if current.ResourceVersion != pod.ResourceVersion {
		return nil, status.Error(codes.Aborted, "preparation_observation_changed")
	}
	if _, err := s.checkVolumeBackend(ctx, req.BackendId); err != nil {
		return nil, err
	}
	identity := map[string]string{}
	for _, key := range []string{managedByLabelKey, workloadManagedByLabelKey, "managed-by", "agent-instance-id", "agent-id", "thread-id", "sandbox-id", "sandbox-owner-id"} {
		if value, ok := pod.Labels[key]; ok {
			identity[key] = value
		}
	}
	return &runnerv1.ObserveWorkloadPreparationResponse{Binding: binding, IdentityLabels: identity, ResourceVersion: pod.ResourceVersion,
		SetupComplete: state == "prepared", RemovalPending: pod.DeletionTimestamp != nil}, nil
}
