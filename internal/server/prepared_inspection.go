package server

import (
	"context"
	"slices"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// InspectPreparedWorkload reads a known binding, never probes by activation. Match
// backend, Pod/claim UIDs, owners and active holds across a stable Pod revision.
// Unactivated compute must be gated and unexecuted; snapshot changes require another
// read, not repair. Activation and container readiness are separate observations.
func (s *Server) InspectPreparedWorkload(ctx context.Context, req *runnerv1.InspectPreparedWorkloadRequest) (*runnerv1.InspectPreparedWorkloadResponse, error) {
	expected, err := canonicalBinding(req.GetExpected())
	if err != nil {
		return nil, err
	}
	if _, err := s.checkVolumeBackend(ctx, expected.BackendId); err != nil {
		return nil, err
	}
	pods := s.clientset.CoreV1().Pods(s.namespace)
	pod, err := pods.Get(ctx, podNameFromID(expected.WorkloadId), metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if err := s.matchPreparedPod(pod, expected); err != nil {
		return nil, err
	}
	active := pod.Annotations[preparedStateAnnotation] == "active"
	if active && hasPreparedGate(pod) || !active && (pod.Annotations[preparedStateAnnotation] != "prepared" || !hasPreparedGate(pod) || pod.Spec.NodeName != "") {
		return nil, status.Error(codes.FailedPrecondition, "prepared_workload_state_inconsistent")
	}
	if !active {
		for _, container := range append(slices.Clone(pod.Status.InitContainerStatuses), pod.Status.ContainerStatuses...) {
			if container.State.Running != nil || container.State.Terminated != nil || container.LastTerminationState.Terminated != nil {
				return nil, status.Error(codes.FailedPrecondition, "unactivated_workload_executed")
			}
		}
	}
	for _, target := range expected.Volumes {
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
		if claim.Status.Phase == corev1.ClaimLost || claim.DeletionTimestamp != nil || active && !slices.Contains(claim.Finalizers, preparedHoldPrefix+expected.InstanceUid) {
			return nil, status.Error(codes.FailedPrecondition, "prepared_volume_protection_unconfirmed")
		}
	}
	// Return one validated Pod snapshot, not a second name-only inspection that
	// could report a replacement. A changed Pod requires another read-only try.
	current, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if err := s.matchPreparedPod(current, expected); err != nil {
		return nil, err
	}
	if current.ResourceVersion != pod.ResourceVersion {
		return nil, status.Error(codes.Aborted, "prepared_workload_observation_changed")
	}
	if _, err := s.checkVolumeBackend(ctx, expected.BackendId); err != nil {
		return nil, err
	}
	workload, err := inspectWorkloadPod(expected.WorkloadId, pod)
	if err != nil {
		return nil, err
	}
	return &runnerv1.InspectPreparedWorkloadResponse{Binding: expected, Workload: workload, Activated: active,
		RemovalPending: pod.DeletionTimestamp != nil, ResourceVersion: pod.ResourceVersion}, nil
}
