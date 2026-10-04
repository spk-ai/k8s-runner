package server

import (
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"

	"github.com/agynio/k8s-runner/internal/config"
)

// workloadResources is one flavor resolved into what each kind of container in
// the pod gets. A flavor names a workload size; splitting it across the pod is
// this runner's business, so the split lives here rather than on the wire.
type workloadResources struct {
	main    *corev1.ResourceRequirements
	sidecar *corev1.ResourceRequirements
	// devices go on the main container only, as equal requests and limits;
	// supplementalGroups on the Pod. Both are nil for a flavor without them,
	// which leaves its Pods exactly as they were before flavors had devices.
	devices            corev1.ResourceList
	supplementalGroups []int64
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
	devices, err := flavor.DeviceResources()
	if err != nil {
		return workloadResources{}, fmt.Errorf("flavor %s devices: %w", name, err)
	}
	if err := flavor.ValidGroups(); err != nil {
		return workloadResources{}, fmt.Errorf("flavor %s: %w", name, err)
	}
	return workloadResources{main: main, sidecar: sidecar, devices: devices, supplementalGroups: slices.Clone(flavor.SupplementalGroups)}, nil
}

// applyDevices adds the flavor's devices to the main container after it is
// sized, whether by the flavor or by explicit bounds. Kubernetes does not
// overcommit extended resources, so requests equal limits.
func applyDevices(container *corev1.Container, devices corev1.ResourceList) {
	if len(devices) == 0 {
		return
	}
	if container.Resources.Requests == nil {
		container.Resources.Requests = corev1.ResourceList{}
	}
	if container.Resources.Limits == nil {
		container.Resources.Limits = corev1.ResourceList{}
	}
	for name, quantity := range devices {
		container.Resources.Requests[name] = quantity.DeepCopy()
		container.Resources.Limits[name] = quantity.DeepCopy()
	}
}

// applySupplementalGroups runs after the Pod security profile has set the Pod
// security context, under either profile. A group is how a non-root workload
// opens a device node owned by that host group; the restricted Pod Security
// standard does not constrain supplemental groups, and nothing here relaxes a
// field it does.
func applySupplementalGroups(pod *corev1.Pod, groups []int64) {
	if len(groups) == 0 {
		return
	}
	if pod.Spec.SecurityContext == nil {
		pod.Spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	pod.Spec.SecurityContext.SupplementalGroups = append(slices.Clone(pod.Spec.SecurityContext.SupplementalGroups), groups...)
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
