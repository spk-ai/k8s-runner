package server

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

func (s *Server) GetWorkloadLabels(ctx context.Context, req *runnerv1.GetWorkloadLabelsRequest) (*runnerv1.GetWorkloadLabelsResponse, error) {
	workloadID := strings.TrimSpace(req.GetWorkloadId())
	if workloadID == "" {
		return nil, status.Error(codes.InvalidArgument, "workload_id_required")
	}

	podName := podNameFromID(workloadID)

	pod, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}

	return &runnerv1.GetWorkloadLabelsResponse{Labels: pod.Labels}, nil
}

func (s *Server) ListWorkloads(ctx context.Context, _ *runnerv1.ListWorkloadsRequest) (*runnerv1.ListWorkloadsResponse, error) {
	selector := labels.Set(map[string]string{managedByLabelKey: managedByLabelValue}).AsSelector().String()
	list, err := s.clientset.CoreV1().Pods(s.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}

	workloads := make([]*runnerv1.WorkloadListItem, 0, len(list.Items))
	for _, pod := range list.Items {
		workloadKey, ok := pod.Labels[workloadKeyLabelKey]
		if !ok {
			continue
		}
		workloads = append(workloads, &runnerv1.WorkloadListItem{
			InstanceId:  pod.Name,
			WorkloadKey: workloadKey,
		})
	}

	return &runnerv1.ListWorkloadsResponse{Workloads: workloads}, nil
}

func (s *Server) FindWorkloadsByLabels(ctx context.Context, req *runnerv1.FindWorkloadsByLabelsRequest) (*runnerv1.FindWorkloadsByLabelsResponse, error) {
	selector := labels.Set(req.GetLabels()).AsSelector().String()
	list, err := s.clientset.CoreV1().Pods(s.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}

	ids := make([]string, 0, len(list.Items))
	for _, pod := range list.Items {
		if !req.GetAll() && pod.Status.Phase != corev1.PodRunning {
			continue
		}
		ids = append(ids, workloadIDFromPod(&pod))
	}

	return &runnerv1.FindWorkloadsByLabelsResponse{TargetIds: ids}, nil
}

func (s *Server) ListWorkloadsByVolume(ctx context.Context, req *runnerv1.ListWorkloadsByVolumeRequest) (*runnerv1.ListWorkloadsByVolumeResponse, error) {
	volumeName := strings.TrimSpace(req.GetVolumeName())
	if volumeName == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_name_required")
	}

	selector := labels.Set(map[string]string{managedByLabelKey: managedByLabelValue}).AsSelector().String()
	list, err := s.clientset.CoreV1().Pods(s.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}

	ids := make([]string, 0)
	for _, pod := range list.Items {
		if podUsesPVC(&pod, volumeName) {
			ids = append(ids, workloadIDFromPod(&pod))
		}
	}

	return &runnerv1.ListWorkloadsByVolumeResponse{TargetIds: ids}, nil
}

func (s *Server) ListVolumes(ctx context.Context, _ *runnerv1.ListVolumesRequest) (*runnerv1.ListVolumesResponse, error) {
	backend, err := s.volumeBackendID(ctx)
	if err != nil {
		return nil, err
	}
	selector := labels.Set(map[string]string{managedByLabelKey: managedByLabelValue}).AsSelector().String()
	list, err := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}

	volumes := make([]*runnerv1.VolumeListItem, 0, len(list.Items))
	keys := make(map[string]struct{}, len(list.Items))
	for _, pvc := range list.Items {
		volumeKey := pvc.Labels[volumeKeyLabelKey]
		// Callers use absence to finalize volume records. A partial or
		// ambiguous inventory is not evidence that a managed disk is gone.
		if volumeKey == "" || strings.TrimSpace(volumeKey) != volumeKey {
			return nil, status.Error(codes.FailedPrecondition, "volume_inventory_missing_key")
		}
		if _, exists := keys[volumeKey]; exists {
			return nil, status.Error(codes.FailedPrecondition, "volume_inventory_duplicate_key")
		}
		keys[volumeKey] = struct{}{}
		volumes = append(volumes, &runnerv1.VolumeListItem{
			InstanceId:     pvc.Name,
			VolumeKey:      volumeKey,
			InstanceUid:    string(pvc.UID),
			IdentityLabels: volumeIdentityLabels(pvc.Labels),
			BackendId:      backend,
		})
	}

	if _, err := s.checkVolumeBackend(ctx, backend); err != nil {
		return nil, err
	}
	return &runnerv1.ListVolumesResponse{Volumes: volumes, BackendId: backend}, nil
}

func podUsesPVC(pod *corev1.Pod, pvcName string) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		if volume.PersistentVolumeClaim.ClaimName == pvcName {
			return true
		}
	}
	return false
}
