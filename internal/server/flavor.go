package server

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/agynio/k8s-runner/internal/config"
)

// workloadResources is one flavor resolved into what each kind of container in
// the pod gets. A flavor names a workload size; splitting it across the pod is
// this runner's business, so the split lives here rather than on the wire.
type workloadResources struct {
	main    *corev1.ResourceRequirements
	sidecar *corev1.ResourceRequirements
}

// resolveFlavor turns the requested catalog entry name into the resources its
// containers run with. An empty name leaves the workload unsized, which is what
// a caller that predates flavors sends.
func (s *Server) resolveFlavor(name string) (workloadResources, error) {
	if name == "" {
		return workloadResources{}, nil
	}
	flavor, ok := s.catalog.FlavorFor(name)
	if !ok {
		return workloadResources{}, fmt.Errorf("unknown_flavor: %s", name)
	}
	main, err := containerResources(flavor.Resources)
	if err != nil {
		return workloadResources{}, fmt.Errorf("flavor %s: %w", name, err)
	}
	sidecar, err := containerResources(flavor.SidecarResources)
	if err != nil {
		return workloadResources{}, fmt.Errorf("flavor %s sidecarResources: %w", name, err)
	}
	return workloadResources{main: main, sidecar: sidecar}, nil
}

// containerResources converts a declared size into Kubernetes requirements.
// Nothing declared yields nil, which leaves the container unsized rather than
// pinning it to zero.
func containerResources(declared config.ComputeResources) (*corev1.ResourceRequirements, error) {
	if declared.IsZero() {
		return nil, nil
	}
	requestsCPU, err := resource.ParseQuantity(declared.RequestsCPU)
	if err != nil {
		return nil, fmt.Errorf("requestsCpu %q: %w", declared.RequestsCPU, err)
	}
	requestsMemory, err := resource.ParseQuantity(declared.RequestsMemory)
	if err != nil {
		return nil, fmt.Errorf("requestsMemory %q: %w", declared.RequestsMemory, err)
	}
	limitsCPU, err := resource.ParseQuantity(declared.LimitsCPU)
	if err != nil {
		return nil, fmt.Errorf("limitsCpu %q: %w", declared.LimitsCPU, err)
	}
	limitsMemory, err := resource.ParseQuantity(declared.LimitsMemory)
	if err != nil {
		return nil, fmt.Errorf("limitsMemory %q: %w", declared.LimitsMemory, err)
	}
	return &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    requestsCPU,
			corev1.ResourceMemory: requestsMemory,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    limitsCPU,
			corev1.ResourceMemory: limitsMemory,
		},
	}, nil
}
