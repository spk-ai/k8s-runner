package server

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

const (
	// podSecurityAnnotation records the profile a Pod was built under, so an
	// activation after a runner restart or policy change can refuse a Pod built
	// by a looser runner without trusting caller metadata.
	podSecurityAnnotation       = "agyn.dev/pod-security-profile"
	podSecurityRestrictedV1     = "restricted-v1"
	readOnlyRootFilesystemKey   = "read_only_root_filesystem"
	readOnlyRootFilesystemValue = "true"
)

// validateRequestedSecurity runs before any Secret, PVC or Pod write, so a
// refused request leaves nothing behind. The allowlist applies under every
// profile: the "none" profile only leaves the Pod's security fields unset, it
// does not grant capabilities the operator did not list.
func (s *Server) validateRequestedSecurity(req *runnerv1.StartWorkloadRequest, plan capabilityPlan) error {
	if s.podSecurity.Restricted() && plan.dockerImplementation != "" {
		return status.Error(codes.FailedPrecondition, "capability_forbidden_by_pod_security: docker")
	}
	containers := append([]*runnerv1.ContainerSpec{req.GetMain()}, req.GetSidecars()...)
	containers = append(containers, req.GetInitContainers()...)
	for _, container := range containers {
		for _, requested := range container.GetRequiredCapabilities() {
			if strings.TrimSpace(requested) == "" {
				continue
			}
			name := config.NormalizeCapability(requested)
			if !s.podSecurity.AllowsCapability(name) {
				return status.Errorf(codes.InvalidArgument, "required_capability_not_allowed: %s", name)
			}
		}
		if value, ok := container.GetAdditionalProperties()[readOnlyRootFilesystemKey]; ok && value != "true" && value != "false" {
			return status.Errorf(codes.InvalidArgument, "invalid_%s: %s", readOnlyRootFilesystemKey, value)
		}
	}
	return nil
}

// applyPodSecurity hardens the Pod after every container, including the
// capability plan's injected ones, exists and before it is created. Caller
// input can tighten (read_only_root_filesystem=true, allowlisted adds) but has
// no field that loosens what is set here.
func (s *Server) applyPodSecurity(pod *corev1.Pod) {
	if !s.podSecurity.Restricted() {
		return
	}
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{
		RunAsNonRoot:        ptr.To(true),
		RunAsUser:           ptr.To(s.podSecurity.RunAsUser),
		RunAsGroup:          ptr.To(s.podSecurity.RunAsGroup),
		FSGroup:             ptr.To(s.podSecurity.FSGroup),
		FSGroupChangePolicy: ptr.To(corev1.FSGroupChangeOnRootMismatch),
		SeccompProfile:      &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	// Service links put every Service's address in the environment, which
	// task workloads must not use (their traffic goes through the overlay).
	pod.Spec.EnableServiceLinks = ptr.To(false)
	for i := range pod.Spec.InitContainers {
		restrictContainer(&pod.Spec.InitContainers[i])
	}
	for i := range pod.Spec.Containers {
		restrictContainer(&pod.Spec.Containers[i])
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[podSecurityAnnotation] = podSecurityRestrictedV1
}

func restrictContainer(container *corev1.Container) {
	if container.SecurityContext == nil {
		container.SecurityContext = &corev1.SecurityContext{}
	}
	sc := container.SecurityContext
	sc.Privileged = nil
	sc.AllowPrivilegeEscalation = ptr.To(false)
	sc.RunAsNonRoot = ptr.To(true)
	if sc.Capabilities == nil {
		sc.Capabilities = &corev1.Capabilities{}
	}
	sc.Capabilities.Drop = []corev1.Capability{"ALL"}
}

// podMatchesSecurity is the activation recheck: a prepared Pod must have been
// built under the profile now in force. It reads only the immutable Pod.
func (s *Server) podMatchesSecurity(pod *corev1.Pod) bool {
	if !s.podSecurity.Restricted() {
		return true
	}
	sc := pod.Spec.SecurityContext
	return pod.Annotations[podSecurityAnnotation] == podSecurityRestrictedV1 &&
		sc != nil && sc.RunAsNonRoot != nil && *sc.RunAsNonRoot &&
		sc.SeccompProfile != nil && sc.SeccompProfile.Type == corev1.SeccompProfileTypeRuntimeDefault
}
