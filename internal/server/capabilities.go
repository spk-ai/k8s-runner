package server

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

const (
	dockerCapability              = config.CapabilityDocker
	dockerSidecarName             = "docker-daemon"
	dockerDataVolumeName          = "docker-data"
	dockerRunVolumeName           = "docker-run"
	dockerTunVolumeName           = "docker-tun"
	dockerSubidVolumeName         = "docker-subid"
	dockerRootlessImage           = "docker:27-dind-rootless"
	dockerPrivilegedImage         = "docker:27-dind"
	dockerSubidInitContainerName  = "docker-subid-init"
	dockerSubidMountPath          = "/subid"
	dockerTLSCertDirEnvName       = "DOCKER_TLS_CERTDIR"
	dockerTLSCertDirDisabledValue = ""
	dockerHostEnvName             = "DOCKER_HOST"
	dockerHostEnvValue            = "tcp://localhost:2375"
	dockerRootlessDataMountPath   = "/home/rootless/.local/share"
	dockerRootlessRunMountPath    = "/run/user/1000"
	dockerTunDevicePath           = "/dev/net/tun"
	dockerPrivilegedDataMountPath = "/var/lib/docker"
	dockerSubuidMountPath         = "/etc/subuid"
	dockerSubgidMountPath         = "/etc/subgid"
	dockerSubuidFileName          = "subuid"
	dockerSubgidFileName          = "subgid"
	dockerSubidInitScript         = `set -eu

write_subid() {
  file="$1"
  length="$2"
  : > "$file"
  if [ "$length" -le 1 ]; then
    return
  fi
  max=$((length - 1))
  if [ "$max" -le 0 ]; then
    return
  fi
  if [ "$max" -lt 999 ]; then
    first_end=$max
  else
    first_end=999
  fi
  if [ "$first_end" -ge 1 ]; then
    echo "rootless:1:$first_end" >> "$file"
  fi
  if [ "$max" -ge 1001 ]; then
    second_start=1001
    second_count=$((max - second_start + 1))
    if [ "$second_count" -gt 0 ]; then
      echo "rootless:$second_start:$second_count" >> "$file"
    fi
  fi
}

uid_length=$(awk 'NR==1 {print $3}' /proc/self/uid_map)
gid_length=$(awk 'NR==1 {print $3}' /proc/self/gid_map)

write_subid "/subid/subuid" "$uid_length"
write_subid "/subid/subgid" "$gid_length"
`
	dockerAppArmorLegacyAnnotationKey   = "container.apparmor.security.beta.kubernetes.io/" + dockerSidecarName
	dockerSeccompPodAnnotationKey       = "seccomp.security.alpha.kubernetes.io/pod"
	dockerSeccompContainerAnnotationKey = "container.seccomp.security.alpha.kubernetes.io/" + dockerSidecarName
	dockerSecurityProfileUnconfined     = "unconfined"
)

type capabilityPlan struct {
	dockerImplementation config.DockerImplementation
	runtimeClassName     *string
	computeResources     bool
}

func resolveCapabilityPlan(req *runnerv1.StartWorkloadRequest, implementations config.CapabilityImplementations) (capabilityPlan, error) {
	plan := capabilityPlan{}
	capabilities, err := normalizeCapabilities(req.GetCapabilities())
	if err != nil {
		return plan, err
	}
	if len(capabilities) == 0 {
		return plan, nil
	}

	containerNames := collectContainerNames(req)
	volumeNames := collectVolumeNames(req.GetVolumes())

	for _, capability := range capabilities {
		switch capability {
		case config.CapabilityComputeResources:
			plan.computeResources = true
		case dockerCapability:
			implementation := implementations.Docker
			if implementation == "" {
				return plan, status.Error(codes.InvalidArgument, "docker_capability_not_configured")
			}
			if !isSupportedDockerImplementation(implementation) {
				return plan, status.Errorf(codes.InvalidArgument, "unknown_docker_implementation: %s", implementation)
			}
			if err := validateDockerInjection(containerNames, volumeNames, implementation); err != nil {
				return plan, err
			}
			plan.dockerImplementation = implementation
			plan.runtimeClassName = dockerRuntimeClassName(implementation)
		default:
			return plan, status.Errorf(codes.InvalidArgument, "unknown_capability: %s", capability)
		}
	}
	return plan, nil
}

func (plan capabilityPlan) apply(containers *[]corev1.Container, initContainers *[]corev1.Container, volumes *[]corev1.Volume, sidecarNames *[]string) *bool {
	if plan.dockerImplementation == "" {
		return nil
	}
	if len(*containers) == 0 {
		panic("capability plan requires main container")
	}

	(*containers)[0].Env = upsertEnvVar((*containers)[0].Env, dockerHostEnvName, dockerHostEnvValue)
	*containers = append(*containers, dockerSidecarContainer(plan.dockerImplementation))
	*sidecarNames = append(*sidecarNames, dockerSidecarName)
	*volumes = append(*volumes, dockerVolumes(plan.dockerImplementation)...)
	if plan.dockerImplementation == config.DockerImplementationRootless {
		*initContainers = append([]corev1.Container{dockerSubidInitContainer()}, *initContainers...)
		hostUsers := false
		return &hostUsers
	}
	return nil
}

func normalizeCapabilities(capabilities []string) ([]string, error) {
	if len(capabilities) == 0 {
		return nil, nil
	}
	unique := make(map[string]struct{}, len(capabilities))
	normalized := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		name := strings.ToLower(strings.TrimSpace(capability))
		if name == "" {
			return nil, status.Error(codes.InvalidArgument, "capability_required")
		}
		if _, exists := unique[name]; exists {
			continue
		}
		unique[name] = struct{}{}
		normalized = append(normalized, name)
	}
	return normalized, nil
}

func collectContainerNames(req *runnerv1.StartWorkloadRequest) map[string]struct{} {
	names := make(map[string]struct{})
	addName := func(spec *runnerv1.ContainerSpec, fallback string) {
		if spec == nil {
			return
		}
		name := containerName(spec, fallback)
		if name == "" {
			return
		}
		names[name] = struct{}{}
	}
	addName(req.Main, "main")
	for idx, sidecar := range req.Sidecars {
		addName(sidecar, fmt.Sprintf("sidecar-%d", idx+1))
	}
	for idx, initContainer := range req.InitContainers {
		addName(initContainer, fmt.Sprintf("init-%d", idx+1))
	}
	return names
}

func collectVolumeNames(volumes []*runnerv1.VolumeSpec) map[string]struct{} {
	names := make(map[string]struct{}, len(volumes))
	for _, volume := range volumes {
		if volume == nil {
			continue
		}
		name := strings.TrimSpace(volume.Name)
		if name == "" {
			continue
		}
		names[name] = struct{}{}
	}
	return names
}

func containerName(spec *runnerv1.ContainerSpec, fallback string) string {
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return fallback
	}
	return name
}

func validateDockerInjection(containerNames, volumeNames map[string]struct{}, implementation config.DockerImplementation) error {
	if _, exists := containerNames[dockerSidecarName]; exists {
		return status.Errorf(codes.InvalidArgument, "capability_container_name_conflict: %s", dockerSidecarName)
	}
	if implementation == config.DockerImplementationRootless {
		if _, exists := containerNames[dockerSubidInitContainerName]; exists {
			return status.Errorf(codes.InvalidArgument, "capability_container_name_conflict: %s", dockerSubidInitContainerName)
		}
	}
	if _, exists := volumeNames[dockerDataVolumeName]; exists {
		return status.Errorf(codes.InvalidArgument, "capability_volume_name_conflict: %s", dockerDataVolumeName)
	}
	if implementation == config.DockerImplementationRootless {
		if _, exists := volumeNames[dockerRunVolumeName]; exists {
			return status.Errorf(codes.InvalidArgument, "capability_volume_name_conflict: %s", dockerRunVolumeName)
		}
		if _, exists := volumeNames[dockerTunVolumeName]; exists {
			return status.Errorf(codes.InvalidArgument, "capability_volume_name_conflict: %s", dockerTunVolumeName)
		}
		if _, exists := volumeNames[dockerSubidVolumeName]; exists {
			return status.Errorf(codes.InvalidArgument, "capability_volume_name_conflict: %s", dockerSubidVolumeName)
		}
	}
	return nil
}

func dockerSidecarContainer(implementation config.DockerImplementation) corev1.Container {
	env := []corev1.EnvVar{{Name: dockerTLSCertDirEnvName, Value: dockerTLSCertDirDisabledValue}}
	switch implementation {
	case config.DockerImplementationRootless:
		allowPrivilegeEscalation := true
		seccompProfile := &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}
		appArmorProfile := &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeUnconfined}
		procMount := corev1.UnmaskedProcMount
		return corev1.Container{
			Name:  dockerSidecarName,
			Image: dockerRootlessImage,
			Env:   env,
			VolumeMounts: []corev1.VolumeMount{
				{Name: dockerDataVolumeName, MountPath: dockerRootlessDataMountPath},
				{Name: dockerRunVolumeName, MountPath: dockerRootlessRunMountPath},
				{Name: dockerTunVolumeName, MountPath: dockerTunDevicePath},
				{Name: dockerSubidVolumeName, MountPath: dockerSubuidMountPath, SubPath: dockerSubuidFileName, ReadOnly: true},
				{Name: dockerSubidVolumeName, MountPath: dockerSubgidMountPath, SubPath: dockerSubgidFileName, ReadOnly: true},
			},
			// Rootless dockerd launches a nested runc; default RuntimeDefault
			// seccomp/AppArmor profiles block mount-related syscalls (notably
			// mounting proc), so unconfined is required for docker run to work.
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: &allowPrivilegeEscalation,
				SeccompProfile:           seccompProfile,
				AppArmorProfile:          appArmorProfile,
				ProcMount:                &procMount,
			},
		}
	case config.DockerImplementationPrivileged, config.DockerImplementationKataQemu, config.DockerImplementationKataFc:
		privileged := true
		return corev1.Container{
			Name:  dockerSidecarName,
			Image: dockerPrivilegedImage,
			Env:   env,
			VolumeMounts: []corev1.VolumeMount{
				{Name: dockerDataVolumeName, MountPath: dockerPrivilegedDataMountPath},
			},
			SecurityContext: &corev1.SecurityContext{
				Privileged:               &privileged,
				AllowPrivilegeEscalation: &privileged,
			},
		}
	default:
		panic(fmt.Sprintf("unsupported docker implementation: %s", implementation))
	}
}

func dockerSubidInitContainer() corev1.Container {
	return corev1.Container{
		Name:    dockerSubidInitContainerName,
		Image:   dockerRootlessImage,
		Command: []string{"/bin/sh", "-c"},
		Args:    []string{dockerSubidInitScript},
		VolumeMounts: []corev1.VolumeMount{
			{Name: dockerSubidVolumeName, MountPath: dockerSubidMountPath},
		},
	}
}

func dockerVolumes(implementation config.DockerImplementation) []corev1.Volume {
	dataVolume := corev1.Volume{
		Name: dockerDataVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}
	switch implementation {
	case config.DockerImplementationRootless:
		runVolume := corev1.Volume{
			Name: dockerRunVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		}
		hostPathType := corev1.HostPathCharDev
		tunVolume := corev1.Volume{
			Name: dockerTunVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: dockerTunDevicePath,
					Type: &hostPathType,
				},
			},
		}
		subidVolume := corev1.Volume{
			Name: dockerSubidVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		}
		return []corev1.Volume{dataVolume, runVolume, tunVolume, subidVolume}
	case config.DockerImplementationPrivileged, config.DockerImplementationKataQemu, config.DockerImplementationKataFc:
		return []corev1.Volume{dataVolume}
	default:
		panic(fmt.Sprintf("unsupported docker implementation: %s", implementation))
	}
}

func isSupportedDockerImplementation(implementation config.DockerImplementation) bool {
	switch implementation {
	case config.DockerImplementationRootless,
		config.DockerImplementationPrivileged,
		config.DockerImplementationKataQemu,
		config.DockerImplementationKataFc:
		return true
	default:
		return false
	}
}

func dockerRuntimeClassName(implementation config.DockerImplementation) *string {
	switch implementation {
	case config.DockerImplementationRootless, config.DockerImplementationPrivileged:
		return nil
	case config.DockerImplementationKataQemu, config.DockerImplementationKataFc:
		name := string(implementation)
		return &name
	default:
		panic(fmt.Sprintf("unsupported docker implementation: %s", implementation))
	}
}

func upsertEnvVar(envs []corev1.EnvVar, name, value string) []corev1.EnvVar {
	result := make([]corev1.EnvVar, 0, len(envs)+1)
	for _, env := range envs {
		if env.Name == name {
			continue
		}
		result = append(result, env)
	}
	return append(result, corev1.EnvVar{Name: name, Value: value})
}
