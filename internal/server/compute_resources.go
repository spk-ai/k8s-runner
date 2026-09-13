package server

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

func containerResources(value *runnerv1.ComputeResources) (corev1.ResourceRequirements, error) {
	if value == nil {
		return corev1.ResourceRequirements{}, nil
	}
	resources, err := (config.ComputeResources{
		RequestsCPU: value.GetRequestsCpu(), RequestsMemory: value.GetRequestsMemory(),
		LimitsCPU: value.GetLimitsCpu(), LimitsMemory: value.GetLimitsMemory(),
	}).ResourceRequirements()
	if err != nil {
		return corev1.ResourceRequirements{}, status.Errorf(codes.InvalidArgument, "invalid_container_resources: %s", err)
	}
	return resources, nil
}

// Validate before creating any Kubernetes objects, including PVCs and secrets.
func validateComputeResources(req *runnerv1.StartWorkloadRequest, required bool, supporting *config.ComputeResources) (corev1.ResourceRequirements, error) {
	var defaults corev1.ResourceRequirements
	if required {
		if supporting == nil {
			return defaults, status.Error(codes.FailedPrecondition, "compute_resources_not_configured")
		}
		var err error
		defaults, err = supporting.ResourceRequirements()
		if err != nil {
			return defaults, status.Error(codes.FailedPrecondition, "invalid_supporting_container_resources")
		}
		if req.GetMain().GetResources() == nil {
			return defaults, status.Error(codes.InvalidArgument, "main_container_resources_required")
		}
	}
	containers := append([]*runnerv1.ContainerSpec{req.GetMain()}, req.GetSidecars()...)
	containers = append(containers, req.GetInitContainers()...)
	for _, container := range containers {
		if container.GetResources() == nil {
			continue
		}
		if !required {
			return defaults, status.Error(codes.InvalidArgument, "compute_resources_capability_required")
		}
		if _, err := containerResources(container.GetResources()); err != nil {
			return defaults, err
		}
	}
	return defaults, nil
}

// Includes restartable init containers and capability-injected containers.
func applySupportingResources(containers, initContainers []corev1.Container, defaults corev1.ResourceRequirements) {
	for _, group := range [][]corev1.Container{containers, initContainers} {
		for index := range group {
			if len(group[index].Resources.Requests) == 0 && len(group[index].Resources.Limits) == 0 {
				group[index].Resources = *defaults.DeepCopy()
			}
		}
	}
}
