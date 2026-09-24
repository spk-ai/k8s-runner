package server

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

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
	main, err := flavorContainerResources(flavor.Resources)
	if err != nil {
		return workloadResources{}, fmt.Errorf("flavor %s: %w", name, err)
	}
	sidecar, err := flavorContainerResources(flavor.SidecarResources)
	if err != nil {
		return workloadResources{}, fmt.Errorf("flavor %s sidecarResources: %w", name, err)
	}
	return workloadResources{main: main, sidecar: sidecar}, nil
}

// flavorContainerResources converts a declared size into Kubernetes requirements.
// Nothing declared yields nil, which leaves the container unsized rather than
// pinning it to zero.
func flavorContainerResources(declared config.ComputeResources) (*corev1.ResourceRequirements, error) {
	if declared.IsZero() {
		return nil, nil
	}
	resources, err := declared.ResourceRequirements()
	if err != nil {
		return nil, err
	}
	return &resources, nil
}
