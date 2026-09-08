package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

func TestBuildLabelsFiltersAndValidates(t *testing.T) {
	labels, err := buildLabels("uuid-1", map[string]string{
		"label.team": "core",
		"ignored":    "value",
	}, map[string]string{workloadKeyLabelKey: "workload-1"})
	if err != nil {
		t.Fatalf("buildLabels returned error: %v", err)
	}

	expected := map[string]string{
		managedByLabelKey:         managedByLabelValue,
		workloadManagedByLabelKey: workloadManagedByLabelValue,
		workloadIDLabelKey:        "uuid-1",
		workloadKeyLabelKey:       "workload-1",
		"team":                    "core",
	}
	if !reflect.DeepEqual(labels, expected) {
		t.Fatalf("labels mismatch: got %#v want %#v", labels, expected)
	}
}

func TestBuildLabelsRejectsInvalidKey(t *testing.T) {
	_, err := buildLabels("uuid-1", map[string]string{
		"label.bad key": "value",
	}, nil)
	if err == nil {
		t.Fatalf("expected error for invalid label key")
	}
}

func TestBuildLabelsRejectsInvalidExplicitLabel(t *testing.T) {
	_, err := buildLabels("uuid-1", nil, map[string]string{"bad key": "value"})
	if err == nil {
		t.Fatalf("expected error for invalid explicit label key")
	}
}

func TestBuildLabelsRejectsReservedLabel(t *testing.T) {
	for _, key := range []string{managedByLabelKey, workloadManagedByLabelKey} {
		_, err := buildLabels("uuid-1", nil, map[string]string{key: "override"})
		if err == nil {
			t.Fatalf("expected error for reserved label key %s", key)
		}
	}
}

func TestBuildDockerConfigJSON(t *testing.T) {
	payload, err := buildDockerConfigJSON("registry.example.com", "user", "pass")
	if err != nil {
		t.Fatalf("buildDockerConfigJSON returned error: %v", err)
	}

	var config dockerConfig
	if err := json.Unmarshal(payload, &config); err != nil {
		t.Fatalf("failed to unmarshal docker config: %v", err)
	}

	auth, ok := config.Auths["registry.example.com"]
	if !ok {
		t.Fatalf("expected registry auth entry")
	}
	if auth.Username != "user" {
		t.Fatalf("expected username 'user', got %q", auth.Username)
	}
	if auth.Password != "pass" {
		t.Fatalf("expected password 'pass', got %q", auth.Password)
	}
	expectedAuth := base64.StdEncoding.EncodeToString([]byte("user:pass"))
	if auth.Auth != expectedAuth {
		t.Fatalf("expected auth %q, got %q", expectedAuth, auth.Auth)
	}
}

func TestBuildContainersMapsMainSpec(t *testing.T) {
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{
			Name:       "main",
			Image:      "busybox",
			Entrypoint: "/bin/sh",
			Cmd:        []string{"echo", "hi"},
			WorkingDir: "/work",
		},
	}

	containers, initContainers, sidecars, err := buildContainers(req, nil, nil)
	if err != nil {
		t.Fatalf("buildContainers returned error: %v", err)
	}
	if len(sidecars) != 0 {
		t.Fatalf("expected no sidecars, got %d", len(sidecars))
	}
	if len(initContainers) != 0 {
		t.Fatalf("expected no init containers, got %d", len(initContainers))
	}
	if len(containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(containers))
	}

	main := containers[0]
	if main.Name != "main" {
		t.Fatalf("expected container name 'main', got %q", main.Name)
	}
	if !reflect.DeepEqual(main.Command, []string{"/bin/sh"}) {
		t.Fatalf("expected entrypoint command, got %#v", main.Command)
	}
	if !reflect.DeepEqual(main.Args, []string{"echo", "hi"}) {
		t.Fatalf("expected args to match, got %#v", main.Args)
	}
	if main.WorkingDir != "/work" {
		t.Fatalf("expected working dir to be /work, got %q", main.WorkingDir)
	}
}

func TestBuildContainerMapsRequiredCapabilities(t *testing.T) {
	container, err := buildContainer(&runnerv1.ContainerSpec{
		Name:                 "main",
		Image:                "busybox",
		RequiredCapabilities: []string{"NET_ADMIN"},
	}, "main", map[string]struct{}{}, nil)
	if err != nil {
		t.Fatalf("buildContainer returned error: %v", err)
	}
	if container.SecurityContext == nil || container.SecurityContext.Capabilities == nil {
		t.Fatalf("expected security context capabilities")
	}
	expected := []corev1.Capability{"NET_ADMIN"}
	if !reflect.DeepEqual(container.SecurityContext.Capabilities.Add, expected) {
		t.Fatalf("expected capabilities %#v, got %#v", expected, container.SecurityContext.Capabilities.Add)
	}
}

func TestBuildContainerOmitsSecurityContextWithoutRequiredCapabilities(t *testing.T) {
	container, err := buildContainer(&runnerv1.ContainerSpec{
		Name:  "main",
		Image: "busybox",
	}, "main", map[string]struct{}{}, nil)
	if err != nil {
		t.Fatalf("buildContainer returned error: %v", err)
	}
	if container.SecurityContext != nil {
		t.Fatalf("expected no security context when capabilities are nil")
	}

	container, err = buildContainer(&runnerv1.ContainerSpec{
		Name:                 "main",
		Image:                "busybox",
		RequiredCapabilities: []string{},
	}, "main", map[string]struct{}{}, nil)
	if err != nil {
		t.Fatalf("buildContainer returned error: %v", err)
	}
	if container.SecurityContext != nil {
		t.Fatalf("expected no security context when capabilities are empty")
	}
}

func TestBuildContainerOmitsSecurityContextForWhitespaceCapabilities(t *testing.T) {
	container, err := buildContainer(&runnerv1.ContainerSpec{
		Name:                 "main",
		Image:                "busybox",
		RequiredCapabilities: []string{" ", ""},
	}, "main", map[string]struct{}{}, nil)
	if err != nil {
		t.Fatalf("buildContainer returned error: %v", err)
	}
	if container.SecurityContext != nil {
		t.Fatalf("expected no security context when capabilities are whitespace-only")
	}
}

func TestBuildContainersRejectsDuplicateNames(t *testing.T) {
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Sidecars: []*runnerv1.ContainerSpec{
			{Name: "main", Image: "busybox"},
		},
	}

	_, _, _, err := buildContainers(req, nil, nil)
	if err == nil {
		t.Fatalf("expected duplicate container error")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "duplicate_container_name") {
		t.Fatalf("expected duplicate container error message, got %q", st.Message())
	}
}

func TestBuildContainersRejectsEntrypointWithSpaces(t *testing.T) {
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox", Entrypoint: "/bin/sh -c"},
	}

	_, _, _, err := buildContainers(req, nil, nil)
	if err == nil {
		t.Fatalf("expected entrypoint validation error")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "entrypoint_must_be_single_path") {
		t.Fatalf("expected entrypoint error message, got %q", st.Message())
	}
}

func TestBuildContainersMapsInitRestartPolicy(t *testing.T) {
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		InitContainers: []*runnerv1.ContainerSpec{
			{Name: "setup", Image: "alpine", AdditionalProperties: map[string]string{"restart_policy": "Always"}},
		},
	}

	_, initContainers, _, err := buildContainers(req, nil, nil)
	if err != nil {
		t.Fatalf("buildContainers returned error: %v", err)
	}
	if len(initContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(initContainers))
	}
	if initContainers[0].RestartPolicy == nil {
		t.Fatalf("expected init container restart policy to be set")
	}
	if *initContainers[0].RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Fatalf("expected restart policy Always, got %q", *initContainers[0].RestartPolicy)
	}
}

func TestBuildContainersOmitsInitRestartPolicy(t *testing.T) {
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		InitContainers: []*runnerv1.ContainerSpec{
			{Name: "setup", Image: "alpine"},
		},
	}

	_, initContainers, _, err := buildContainers(req, nil, nil)
	if err != nil {
		t.Fatalf("buildContainers returned error: %v", err)
	}
	if len(initContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(initContainers))
	}
	if initContainers[0].RestartPolicy != nil {
		t.Fatalf("expected init container restart policy to be nil")
	}
}

func TestBuildContainersRejectsInitDuplicateNameWithMain(t *testing.T) {
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		InitContainers: []*runnerv1.ContainerSpec{
			{Name: "main", Image: "alpine"},
		},
	}

	_, _, _, err := buildContainers(req, nil, nil)
	if err == nil {
		t.Fatalf("expected duplicate container error")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "duplicate_container_name") {
		t.Fatalf("expected duplicate container error message, got %q", st.Message())
	}
}

func TestBuildContainersRejectsInitDuplicateNames(t *testing.T) {
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		InitContainers: []*runnerv1.ContainerSpec{
			{Name: "setup", Image: "busybox"},
			{Name: "setup", Image: "alpine"},
		},
	}

	_, _, _, err := buildContainers(req, nil, nil)
	if err == nil {
		t.Fatalf("expected duplicate container error")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "duplicate_container_name") {
		t.Fatalf("expected duplicate container error message, got %q", st.Message())
	}
}

func TestStartWorkloadUsesProvidedWorkloadID(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	workloadID := "2db3f1b6-7d0b-4c28-8ed4-5eb9803ccf12"
	req := &runnerv1.StartWorkloadRequest{
		WorkloadId: workloadID,
		Main:       &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
	}

	resp, err := server.StartWorkload(ctx, req)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}
	if resp.GetId() != workloadID {
		t.Fatalf("expected workload id %q, got %q", workloadID, resp.GetId())
	}

	podName := podNameFromID(workloadID)
	pod, err := clientset.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pod created: %v", err)
	}
	if pod.Labels[workloadIDLabelKey] != workloadID {
		t.Fatalf("expected workload label %q, got %q", workloadID, pod.Labels[workloadIDLabelKey])
	}
	if pod.Labels[workloadManagedByLabelKey] != workloadManagedByLabelValue {
		t.Fatalf("expected workload managed-by label %q, got %q", workloadManagedByLabelValue, pod.Labels[workloadManagedByLabelKey])
	}
}

func TestStartWorkloadRejectsInvalidWorkloadID(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{
		WorkloadId: "not-a-uuid",
		Main:       &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
	}

	_, err := server.StartWorkload(ctx, req)
	if err == nil {
		t.Fatalf("expected error for invalid workload id")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "workload_id_invalid") {
		t.Fatalf("expected workload id invalid error, got %q", st.Message())
	}
}

func TestStartWorkloadBuildsInitContainers(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		InitContainers: []*runnerv1.ContainerSpec{
			{Image: "alpine"},
			{Name: "init-setup", Image: "busybox", Cmd: []string{"echo", "ready"}},
		},
	}

	resp, err := server.StartWorkload(ctx, req)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}
	if resp == nil || resp.Id == "" {
		t.Fatalf("expected response with id")
	}

	podName := podNameFromID(resp.Id)
	pod, err := clientset.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pod created: %v", err)
	}
	if len(pod.Spec.InitContainers) != 2 {
		t.Fatalf("expected 2 init containers, got %d", len(pod.Spec.InitContainers))
	}
	if pod.Spec.InitContainers[0].Name != "init-1" {
		t.Fatalf("expected fallback init container name, got %q", pod.Spec.InitContainers[0].Name)
	}
	if pod.Spec.InitContainers[0].Image != "alpine" {
		t.Fatalf("expected init container image alpine, got %q", pod.Spec.InitContainers[0].Image)
	}
	if pod.Spec.InitContainers[1].Name != "init-setup" {
		t.Fatalf("expected init container name init-setup, got %q", pod.Spec.InitContainers[1].Name)
	}
	if !reflect.DeepEqual(pod.Spec.InitContainers[1].Args, []string{"echo", "ready"}) {
		t.Fatalf("expected init container args, got %#v", pod.Spec.InitContainers[1].Args)
	}
}

func TestStartWorkloadInjectsDockerRootless(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
		CapabilityImplementations: config.CapabilityImplementations{
			Docker: config.DockerImplementationRootless,
		},
	})

	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{
		Main:         &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Capabilities: []string{dockerCapability},
	}

	resp, err := server.StartWorkload(ctx, req)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}

	podName := podNameFromID(resp.Id)
	pod, err := clientset.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pod created: %v", err)
	}

	if pod.Spec.HostUsers == nil || *pod.Spec.HostUsers {
		t.Fatalf("expected hostUsers false for rootless docker")
	}
	assertRuntimeClassNameUnset(t, pod.Spec.RuntimeClassName)
	if len(pod.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(pod.Spec.InitContainers))
	}
	initContainer := pod.Spec.InitContainers[0]
	if initContainer.Name != dockerSubidInitContainerName {
		t.Fatalf("expected init container %q, got %q", dockerSubidInitContainerName, initContainer.Name)
	}
	if initContainer.Image != dockerRootlessImage {
		t.Fatalf("expected subid init image %q, got %q", dockerRootlessImage, initContainer.Image)
	}
	assertVolumeMount(t, initContainer.VolumeMounts, dockerSubidVolumeName, dockerSubidMountPath)
	assertAnnotation(t, pod.Annotations, dockerAppArmorLegacyAnnotationKey, dockerSecurityProfileUnconfined)
	assertAnnotation(t, pod.Annotations, dockerSeccompPodAnnotationKey, dockerSecurityProfileUnconfined)
	assertAnnotation(t, pod.Annotations, dockerSeccompContainerAnnotationKey, dockerSecurityProfileUnconfined)

	main := findContainer(pod.Spec.Containers, "main")
	if main == nil {
		t.Fatal("expected main container")
	}
	assertEnvValue(t, main.Env, dockerHostEnvName, dockerHostEnvValue)

	sidecar := findContainer(pod.Spec.Containers, dockerSidecarName)
	if sidecar == nil {
		t.Fatal("expected docker sidecar container")
	}
	if sidecar.Image != dockerRootlessImage {
		t.Fatalf("expected rootless docker image %q, got %q", dockerRootlessImage, sidecar.Image)
	}
	if sidecar.SecurityContext == nil {
		t.Fatal("expected docker sidecar security context")
	}
	if sidecar.SecurityContext.AllowPrivilegeEscalation == nil || !*sidecar.SecurityContext.AllowPrivilegeEscalation {
		t.Fatalf("expected allowPrivilegeEscalation true for rootless docker")
	}
	if sidecar.SecurityContext.SeccompProfile == nil || sidecar.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeUnconfined {
		t.Fatalf("expected seccomp profile unconfined for rootless docker")
	}
	if sidecar.SecurityContext.AppArmorProfile == nil || sidecar.SecurityContext.AppArmorProfile.Type != corev1.AppArmorProfileTypeUnconfined {
		t.Fatalf("expected appArmor profile unconfined for rootless docker")
	}
	if sidecar.SecurityContext.ProcMount == nil || *sidecar.SecurityContext.ProcMount != corev1.UnmaskedProcMount {
		t.Fatalf("expected procMount unmasked for rootless docker")
	}
	assertEnvValue(t, sidecar.Env, dockerTLSCertDirEnvName, dockerTLSCertDirDisabledValue)
	assertVolumeMount(t, sidecar.VolumeMounts, dockerDataVolumeName, dockerRootlessDataMountPath)
	assertVolumeMount(t, sidecar.VolumeMounts, dockerRunVolumeName, dockerRootlessRunMountPath)
	assertVolumeMount(t, sidecar.VolumeMounts, dockerTunVolumeName, dockerTunDevicePath)
	assertVolumeMountWithSubPath(t, sidecar.VolumeMounts, dockerSubidVolumeName, dockerSubuidMountPath, dockerSubuidFileName)
	assertVolumeMountWithSubPath(t, sidecar.VolumeMounts, dockerSubidVolumeName, dockerSubgidMountPath, dockerSubgidFileName)
	assertEmptyDirVolume(t, pod.Spec.Volumes, dockerDataVolumeName)
	assertEmptyDirVolume(t, pod.Spec.Volumes, dockerRunVolumeName)
	assertHostPathVolume(t, pod.Spec.Volumes, dockerTunVolumeName, dockerTunDevicePath, corev1.HostPathCharDev)
	assertEmptyDirVolume(t, pod.Spec.Volumes, dockerSubidVolumeName)
	assertSidecarInstance(t, resp.GetContainers().GetSidecars(), dockerSidecarName)
}

func TestStartWorkloadInjectsDockerPrivileged(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
		CapabilityImplementations: config.CapabilityImplementations{
			Docker: config.DockerImplementationPrivileged,
		},
	})

	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{
		Main:         &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Capabilities: []string{dockerCapability},
	}

	resp, err := server.StartWorkload(ctx, req)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}

	podName := podNameFromID(resp.Id)
	pod, err := clientset.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pod created: %v", err)
	}

	if pod.Spec.HostUsers != nil {
		t.Fatalf("expected hostUsers to remain unset for privileged docker")
	}
	assertRuntimeClassNameUnset(t, pod.Spec.RuntimeClassName)
	assertPrivilegedDockerInjection(t, pod, resp)
}

func TestStartWorkloadInjectsDockerKataQemu(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
		CapabilityImplementations: config.CapabilityImplementations{
			Docker: config.DockerImplementationKataQemu,
		},
	})

	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{
		Main:         &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Capabilities: []string{dockerCapability},
	}

	resp, err := server.StartWorkload(ctx, req)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}

	podName := podNameFromID(resp.Id)
	pod, err := clientset.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pod created: %v", err)
	}

	if pod.Spec.HostUsers != nil {
		t.Fatalf("expected hostUsers to remain unset for kata docker")
	}
	assertRuntimeClassName(t, pod.Spec.RuntimeClassName, string(config.DockerImplementationKataQemu))
	assertPrivilegedDockerInjection(t, pod, resp)
}

func TestStartWorkloadInjectsDockerKataFc(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
		CapabilityImplementations: config.CapabilityImplementations{
			Docker: config.DockerImplementationKataFc,
		},
	})

	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{
		Main:         &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Capabilities: []string{dockerCapability},
	}

	resp, err := server.StartWorkload(ctx, req)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}

	podName := podNameFromID(resp.Id)
	pod, err := clientset.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pod created: %v", err)
	}

	if pod.Spec.HostUsers != nil {
		t.Fatalf("expected hostUsers to remain unset for kata docker")
	}
	assertRuntimeClassName(t, pod.Spec.RuntimeClassName, string(config.DockerImplementationKataFc))
	assertPrivilegedDockerInjection(t, pod, resp)
}

func TestStartWorkloadRejectsUnknownCapability(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	_, err := server.StartWorkload(ctx, &runnerv1.StartWorkloadRequest{
		Main:         &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Capabilities: []string{"unknown"},
	})
	if err == nil {
		t.Fatal("expected error for unknown capability")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "unknown_capability") {
		t.Fatalf("expected unknown capability error, got %q", st.Message())
	}
}

func TestStartWorkloadRejectsDockerCapabilityNotConfigured(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	_, err := server.StartWorkload(ctx, &runnerv1.StartWorkloadRequest{
		Main:         &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Capabilities: []string{dockerCapability},
	})
	if err == nil {
		t.Fatal("expected error for docker capability not configured")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "docker_capability_not_configured") {
		t.Fatalf("expected docker capability not configured error, got %q", st.Message())
	}
}

func TestStartWorkloadRejectsDockerContainerNameConflict(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
		CapabilityImplementations: config.CapabilityImplementations{
			Docker: config.DockerImplementationRootless,
		},
	})

	ctx := context.Background()
	_, err := server.StartWorkload(ctx, &runnerv1.StartWorkloadRequest{
		Main:         &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Sidecars:     []*runnerv1.ContainerSpec{{Name: dockerSidecarName, Image: "busybox"}},
		Capabilities: []string{dockerCapability},
	})
	if err == nil {
		t.Fatal("expected error for docker container name conflict")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "capability_container_name_conflict") {
		t.Fatalf("expected container name conflict error, got %q", st.Message())
	}
}

func TestStartWorkloadRejectsDockerSubidInitContainerNameConflict(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
		CapabilityImplementations: config.CapabilityImplementations{
			Docker: config.DockerImplementationRootless,
		},
	})

	ctx := context.Background()
	_, err := server.StartWorkload(ctx, &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		InitContainers: []*runnerv1.ContainerSpec{{
			Name:  dockerSubidInitContainerName,
			Image: "busybox",
		}},
		Capabilities: []string{dockerCapability},
	})
	if err == nil {
		t.Fatal("expected error for docker init container name conflict")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "capability_container_name_conflict") {
		t.Fatalf("expected container name conflict error, got %q", st.Message())
	}
}

func TestStartWorkloadRejectsDockerVolumeNameConflict(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
		CapabilityImplementations: config.CapabilityImplementations{
			Docker: config.DockerImplementationRootless,
		},
	})

	ctx := context.Background()
	_, err := server.StartWorkload(ctx, &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Volumes: []*runnerv1.VolumeSpec{
			{Name: dockerDataVolumeName, Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL},
		},
		Capabilities: []string{dockerCapability},
	})
	if err == nil {
		t.Fatal("expected error for docker volume name conflict")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "capability_volume_name_conflict") {
		t.Fatalf("expected volume name conflict error, got %q", st.Message())
	}
}

func TestStartWorkloadRejectsDockerTunVolumeNameConflict(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
		CapabilityImplementations: config.CapabilityImplementations{
			Docker: config.DockerImplementationRootless,
		},
	})

	ctx := context.Background()
	_, err := server.StartWorkload(ctx, &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Volumes: []*runnerv1.VolumeSpec{
			{Name: dockerTunVolumeName, Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL},
		},
		Capabilities: []string{dockerCapability},
	})
	if err == nil {
		t.Fatal("expected error for docker tun volume name conflict")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "capability_volume_name_conflict") {
		t.Fatalf("expected volume name conflict error, got %q", st.Message())
	}
}

func TestStartWorkloadRejectsDockerSubidVolumeNameConflict(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
		CapabilityImplementations: config.CapabilityImplementations{
			Docker: config.DockerImplementationRootless,
		},
	})

	ctx := context.Background()
	_, err := server.StartWorkload(ctx, &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Volumes: []*runnerv1.VolumeSpec{{
			Name: dockerSubidVolumeName,
			Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL,
		}},
		Capabilities: []string{dockerCapability},
	})
	if err == nil {
		t.Fatal("expected error for docker subid volume name conflict")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "capability_volume_name_conflict") {
		t.Fatalf("expected volume name conflict error, got %q", st.Message())
	}
}

func TestStartWorkloadRejectsUnknownDockerImplementation(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
		CapabilityImplementations: config.CapabilityImplementations{
			Docker: config.DockerImplementation("unsupported"),
		},
	})

	ctx := context.Background()
	_, err := server.StartWorkload(ctx, &runnerv1.StartWorkloadRequest{
		Main:         &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Capabilities: []string{dockerCapability},
	})
	if err == nil {
		t.Fatal("expected error for unknown docker implementation")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "unknown_docker_implementation") {
		t.Fatalf("expected unknown docker implementation error, got %q", st.Message())
	}
}

func TestStartWorkloadMapsDnsConfig(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		DnsConfig: &runnerv1.DnsConfig{
			Nameservers: []string{"127.0.0.1", "10.96.0.10"},
			Searches:    []string{"svc.cluster.local"},
		},
	}

	resp, err := server.StartWorkload(ctx, req)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}
	if resp == nil || resp.Id == "" {
		t.Fatalf("expected response with id")
	}

	podName := podNameFromID(resp.Id)
	pod, err := clientset.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pod created: %v", err)
	}
	if pod.Spec.DNSPolicy != corev1.DNSNone {
		t.Fatalf("expected DNSPolicy None, got %q", pod.Spec.DNSPolicy)
	}
	if pod.Spec.DNSConfig == nil {
		t.Fatalf("expected DNSConfig to be set")
	}
	if !reflect.DeepEqual(pod.Spec.DNSConfig.Nameservers, req.DnsConfig.Nameservers) {
		t.Fatalf("expected nameservers %#v, got %#v", req.DnsConfig.Nameservers, pod.Spec.DNSConfig.Nameservers)
	}
	if !reflect.DeepEqual(pod.Spec.DNSConfig.Searches, req.DnsConfig.Searches) {
		t.Fatalf("expected searches %#v, got %#v", req.DnsConfig.Searches, pod.Spec.DNSConfig.Searches)
	}
}

func TestStartWorkloadCreatesImagePullSecrets(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	credentials := []*runnerv1.ImagePullCredential{
		{Registry: "registry.example.com", Username: "user", Password: "pass"},
		{Registry: "registry.internal", Username: "robot", Password: "token"},
	}
	req := &runnerv1.StartWorkloadRequest{
		Main:                 &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		ImagePullCredentials: credentials,
	}

	resp, err := server.StartWorkload(ctx, req)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}
	if resp == nil || resp.Id == "" {
		t.Fatalf("expected response with id")
	}

	podName := podNameFromID(resp.Id)
	pod, err := clientset.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pod created: %v", err)
	}

	expectedNames := []string{
		fmt.Sprintf("workload-%s-pull-0", resp.Id),
		fmt.Sprintf("workload-%s-pull-1", resp.Id),
	}
	if len(pod.Spec.ImagePullSecrets) != len(expectedNames) {
		t.Fatalf("expected %d image pull secrets, got %d", len(expectedNames), len(pod.Spec.ImagePullSecrets))
	}
	gotNames := make([]string, 0, len(pod.Spec.ImagePullSecrets))
	for _, ref := range pod.Spec.ImagePullSecrets {
		gotNames = append(gotNames, ref.Name)
	}
	if !reflect.DeepEqual(gotNames, expectedNames) {
		t.Fatalf("expected image pull secret names %#v, got %#v", expectedNames, gotNames)
	}
	if pod.Annotations[secretAnnotationKey] != strings.Join(expectedNames, ",") {
		t.Fatalf("expected secret annotation %q, got %q", strings.Join(expectedNames, ","), pod.Annotations[secretAnnotationKey])
	}

	for idx, secretName := range expectedNames {
		secret, err := clientset.CoreV1().Secrets("default").Get(ctx, secretName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("expected secret %s: %v", secretName, err)
		}
		if secret.Type != corev1.SecretTypeDockerConfigJson {
			t.Fatalf("expected dockerconfigjson secret type, got %q", secret.Type)
		}
		if secret.Labels[managedByLabelKey] != managedByLabelValue {
			t.Fatalf("expected managed-by label %q, got %q", managedByLabelValue, secret.Labels[managedByLabelKey])
		}
		if secret.Labels[workloadIDLabelKey] != resp.Id {
			t.Fatalf("expected workload id label %q, got %q", resp.Id, secret.Labels[workloadIDLabelKey])
		}
		expectedConfig, err := buildDockerConfigJSON(credentials[idx].Registry, credentials[idx].Username, credentials[idx].Password)
		if err != nil {
			t.Fatalf("buildDockerConfigJSON returned error: %v", err)
		}
		if !reflect.DeepEqual(secret.Data[corev1.DockerConfigJsonKey], expectedConfig) {
			t.Fatalf("expected docker config data to match")
		}
	}
}

func TestStartWorkloadAppliesLabelsToPodAndPVC(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Labels: map[string]string{
			workloadKeyLabelKey: "workload-key-1",
			"team":              "core",
		},
		Volumes: []*runnerv1.VolumeSpec{
			{
				Name:           "data",
				Kind:           runnerv1.VolumeKind_VOLUME_KIND_NAMED,
				PersistentName: "pvc-data",
				Labels: map[string]string{
					volumeKeyLabelKey: "volume-key-1",
				},
			},
		},
	}

	resp, err := server.StartWorkload(ctx, req)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}
	if resp == nil || resp.Id == "" {
		t.Fatalf("expected response with id")
	}

	podName := podNameFromID(resp.Id)
	pod, err := clientset.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pod created: %v", err)
	}
	if pod.Labels[managedByLabelKey] != managedByLabelValue {
		t.Fatalf("expected managed-by label %q, got %q", managedByLabelValue, pod.Labels[managedByLabelKey])
	}
	if pod.Labels[workloadManagedByLabelKey] != workloadManagedByLabelValue {
		t.Fatalf("expected workload managed-by label %q, got %q", workloadManagedByLabelValue, pod.Labels[workloadManagedByLabelKey])
	}
	if pod.Labels[workloadIDLabelKey] != resp.Id {
		t.Fatalf("expected workload id label %q, got %q", resp.Id, pod.Labels[workloadIDLabelKey])
	}
	if pod.Labels[workloadKeyLabelKey] != "workload-key-1" {
		t.Fatalf("expected workload key label %q, got %q", "workload-key-1", pod.Labels[workloadKeyLabelKey])
	}
	if pod.Labels["team"] != "core" {
		t.Fatalf("expected team label %q, got %q", "core", pod.Labels["team"])
	}

	pvc, err := clientset.CoreV1().PersistentVolumeClaims("default").Get(ctx, "pvc-data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pvc created: %v", err)
	}
	if pvc.Labels[managedByLabelKey] != managedByLabelValue {
		t.Fatalf("expected pvc managed-by label %q, got %q", managedByLabelValue, pvc.Labels[managedByLabelKey])
	}
	if pvc.Labels[volumeKeyLabelKey] != "volume-key-1" {
		t.Fatalf("expected volume key label %q, got %q", "volume-key-1", pvc.Labels[volumeKeyLabelKey])
	}
}

func TestStartWorkloadHonorsVolumeSizeAndStorageClass(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Catalog: config.Catalog{StorageClasses: []config.StorageClassEntry{
			{Name: "fast", StorageClassName: "fast-ssd"},
			{Name: "standard", StorageClassName: ""},
		}},
		Logger: zap.NewNop(),
	})

	ctx := context.Background()
	resp, err := server.StartWorkload(ctx, &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Volumes: []*runnerv1.VolumeSpec{
			{
				Name:           "data",
				Kind:           runnerv1.VolumeKind_VOLUME_KIND_NAMED,
				PersistentName: "pvc-sized",
				Size:           "5Gi",
				StorageClass:   "fast",
			},
			{
				Name:           "scratch",
				Kind:           runnerv1.VolumeKind_VOLUME_KIND_NAMED,
				PersistentName: "pvc-default",
			},
			{
				Name:           "shared",
				Kind:           runnerv1.VolumeKind_VOLUME_KIND_NAMED,
				PersistentName: "pvc-cluster-default",
				StorageClass:   "standard",
			},
		},
	})
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}
	if resp == nil || resp.Id == "" {
		t.Fatalf("expected response with id")
	}

	sized, err := clientset.CoreV1().PersistentVolumeClaims("default").Get(ctx, "pvc-sized", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pvc created: %v", err)
	}
	if got := sized.Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "5Gi" {
		t.Fatalf("expected 5Gi storage request, got %s", got.String())
	}
	if sized.Spec.StorageClassName == nil || *sized.Spec.StorageClassName != "fast-ssd" {
		t.Fatalf("expected storage class fast-ssd, got %v", sized.Spec.StorageClassName)
	}

	fallback, err := clientset.CoreV1().PersistentVolumeClaims("default").Get(ctx, "pvc-default", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pvc created: %v", err)
	}
	if got := fallback.Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "1Gi" {
		t.Fatalf("expected 1Gi storage request, got %s", got.String())
	}
	if fallback.Spec.StorageClassName != nil {
		t.Fatalf("expected no storage class, got %q", *fallback.Spec.StorageClassName)
	}

	clusterDefault, err := clientset.CoreV1().PersistentVolumeClaims("default").Get(ctx, "pvc-cluster-default", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pvc created: %v", err)
	}
	if clusterDefault.Spec.StorageClassName != nil {
		t.Fatalf("expected cluster default storage class, got %q", *clusterDefault.Spec.StorageClassName)
	}
}

func TestStartWorkloadRejectsUnknownStorageClass(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	_, err := server.StartWorkload(context.Background(), &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Volumes: []*runnerv1.VolumeSpec{
			{
				Name:           "data",
				Kind:           runnerv1.VolumeKind_VOLUME_KIND_NAMED,
				PersistentName: "pvc-data",
				StorageClass:   "missing",
			},
		},
	})
	if err == nil {
		t.Fatalf("expected error for unknown storage class")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "unknown_storage_class") {
		t.Fatalf("expected unknown storage class error, got %q", st.Message())
	}
}

func TestStartWorkloadRejectsInvalidVolumeSize(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	_, err := server.StartWorkload(context.Background(), &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Volumes: []*runnerv1.VolumeSpec{
			{
				Name:           "data",
				Kind:           runnerv1.VolumeKind_VOLUME_KIND_NAMED,
				PersistentName: "pvc-data",
				Size:           "lots",
			},
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid volume size")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "invalid_volume_size") {
		t.Fatalf("expected invalid volume size error, got %q", st.Message())
	}
}

func TestStartWorkloadRejectsReservedVolumeLabel(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Volumes: []*runnerv1.VolumeSpec{
			{
				Name:           "data",
				Kind:           runnerv1.VolumeKind_VOLUME_KIND_NAMED,
				PersistentName: "pvc-data",
				Labels: map[string]string{
					managedByLabelKey: "override",
				},
			},
		},
	}

	_, err := server.StartWorkload(ctx, req)
	if err == nil {
		t.Fatalf("expected error for reserved volume label")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if !strings.Contains(st.Message(), "invalid_volume_label") {
		t.Fatalf("expected invalid volume label error, got %q", st.Message())
	}
}

func TestStartWorkloadNoCredentials(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
	}

	resp, err := server.StartWorkload(ctx, req)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}
	if resp == nil || resp.Id == "" {
		t.Fatalf("expected response with id")
	}

	podName := podNameFromID(resp.Id)
	pod, err := clientset.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pod created: %v", err)
	}
	if len(pod.Spec.ImagePullSecrets) != 0 {
		t.Fatalf("expected no image pull secrets, got %d", len(pod.Spec.ImagePullSecrets))
	}
	if _, ok := pod.Annotations[secretAnnotationKey]; ok {
		t.Fatalf("expected no secret annotation")
	}

	secrets, err := clientset.CoreV1().Secrets("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("expected secrets list: %v", err)
	}
	if len(secrets.Items) != 0 {
		t.Fatalf("expected no secrets, got %d", len(secrets.Items))
	}
}

func TestStartWorkloadRejectsIncompleteCredential(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		ImagePullCredentials: []*runnerv1.ImagePullCredential{
			{Username: "user", Password: "pass"},
		},
	}

	_, err := server.StartWorkload(ctx, req)
	if err == nil {
		t.Fatalf("expected error for incomplete credential")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
}

func TestStopWorkloadDeletesPullSecrets(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	startReq := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		ImagePullCredentials: []*runnerv1.ImagePullCredential{
			{Registry: "registry.example.com", Username: "user", Password: "pass"},
		},
	}

	resp, err := server.StartWorkload(ctx, startReq)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}

	_, err = server.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{WorkloadId: resp.Id})
	if err != nil {
		t.Fatalf("StopWorkload returned error: %v", err)
	}

	secretName := fmt.Sprintf("workload-%s-pull-0", resp.Id)
	_, err = clientset.CoreV1().Secrets("default").Get(ctx, secretName, metav1.GetOptions{})
	if err == nil || !apierrors.IsNotFound(err) {
		t.Fatalf("expected secret %s to be deleted", secretName)
	}
}

func TestRemoveWorkloadDeletesPullSecrets(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	startReq := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		ImagePullCredentials: []*runnerv1.ImagePullCredential{
			{Registry: "registry.example.com", Username: "user", Password: "pass"},
		},
	}

	resp, err := server.StartWorkload(ctx, startReq)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}

	_, err = server.RemoveWorkload(ctx, &runnerv1.RemoveWorkloadRequest{WorkloadId: resp.Id})
	if err != nil {
		t.Fatalf("RemoveWorkload returned error: %v", err)
	}

	secretName := fmt.Sprintf("workload-%s-pull-0", resp.Id)
	_, err = clientset.CoreV1().Secrets("default").Get(ctx, secretName, metav1.GetOptions{})
	if err == nil || !apierrors.IsNotFound(err) {
		t.Fatalf("expected secret %s to be deleted", secretName)
	}
}

func TestParsePVCAnnotation(t *testing.T) {
	if got := parsePVCAnnotation(nil); got != nil {
		t.Fatalf("expected nil for nil annotations, got %#v", got)
	}

	got := parsePVCAnnotation(map[string]string{pvcAnnotationKey: " pvc-a , , pvc-b "})
	expected := []string{"pvc-a", "pvc-b"}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected pvc list %#v, got %#v", expected, got)
	}
}

func TestParseSecretAnnotation(t *testing.T) {
	if got := parseSecretAnnotation(nil); got != nil {
		t.Fatalf("expected nil for nil annotations, got %#v", got)
	}

	got := parseSecretAnnotation(map[string]string{secretAnnotationKey: " secret-a , , secret-b "})
	expected := []string{"secret-a", "secret-b"}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected secret list %#v, got %#v", expected, got)
	}
}

func TestWorkloadContainersForPodOrdersAndMaps(t *testing.T) {
	initReason := "ImagePullBackOff"
	initMessage := "pull failed"
	mainStarted := metav1.NewTime(time.Date(2026, time.April, 24, 10, 0, 0, 0, time.UTC))
	sidecarStarted := metav1.NewTime(time.Date(2026, time.April, 24, 10, 5, 0, 0, time.UTC))
	sidecarFinished := metav1.NewTime(time.Date(2026, time.April, 24, 10, 6, 0, 0, time.UTC))

	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "init-setup", Image: "init-image"}},
			Containers:     []corev1.Container{{Name: "main", Image: "main-image"}, {Name: "sidecar", Image: "sidecar-image"}},
		},
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name:         "init-setup",
				Image:        "init-image@sha",
				ContainerID:  "containerd://init",
				RestartCount: 1,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason:  initReason,
					Message: initMessage,
				}},
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:        "main",
				Image:       "main-image@sha",
				ContainerID: "containerd://main",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{
					StartedAt: mainStarted,
				}},
			}, {
				Name:         "sidecar",
				Image:        "sidecar-image@sha",
				ContainerID:  "containerd://sidecar",
				RestartCount: 2,
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					Reason:     "Error",
					Message:    "boom",
					ExitCode:   137,
					StartedAt:  sidecarStarted,
					FinishedAt: sidecarFinished,
				}},
			}},
		},
	}

	containers := workloadContainersForPod(pod)
	if len(containers) != 3 {
		t.Fatalf("expected 3 containers, got %d", len(containers))
	}

	initContainer := containers[0]
	if initContainer.GetName() != "init-setup" {
		t.Fatalf("expected init container name, got %q", initContainer.GetName())
	}
	if initContainer.GetRole() != runnerv1.ContainerRole_CONTAINER_ROLE_INIT {
		t.Fatalf("expected init role, got %v", initContainer.GetRole())
	}
	if initContainer.GetStatus() != runnerv1.ContainerStatus_CONTAINER_STATUS_WAITING {
		t.Fatalf("expected init status waiting, got %v", initContainer.GetStatus())
	}
	if initContainer.Reason == nil || *initContainer.Reason != initReason {
		t.Fatalf("expected init reason %q, got %#v", initReason, initContainer.Reason)
	}
	if initContainer.Message == nil || *initContainer.Message != initMessage {
		t.Fatalf("expected init message %q, got %#v", initMessage, initContainer.Message)
	}
	if initContainer.ExitCode != nil {
		t.Fatalf("expected init exit code to be nil")
	}
	if initContainer.StartedAt != nil || initContainer.FinishedAt != nil {
		t.Fatalf("expected init timestamps to be nil")
	}
	if initContainer.GetRestartCount() != 1 {
		t.Fatalf("expected init restart count 1, got %d", initContainer.GetRestartCount())
	}
	if initContainer.GetImage() != "init-image@sha" {
		t.Fatalf("expected init image from status, got %q", initContainer.GetImage())
	}

	mainContainer := containers[1]
	if mainContainer.GetName() != "main" {
		t.Fatalf("expected main container name, got %q", mainContainer.GetName())
	}
	if mainContainer.GetRole() != runnerv1.ContainerRole_CONTAINER_ROLE_MAIN {
		t.Fatalf("expected main role, got %v", mainContainer.GetRole())
	}
	if mainContainer.GetStatus() != runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING {
		t.Fatalf("expected main status running, got %v", mainContainer.GetStatus())
	}
	if mainContainer.Reason != nil || mainContainer.Message != nil {
		t.Fatalf("expected main reason/message to be nil")
	}
	if mainContainer.StartedAt == nil {
		t.Fatalf("expected main started_at to be set")
	}
	if !mainContainer.StartedAt.AsTime().Equal(mainStarted.Time) {
		t.Fatalf("expected main started_at %v, got %v", mainStarted.Time, mainContainer.StartedAt.AsTime())
	}
	if mainContainer.GetImage() != "main-image@sha" {
		t.Fatalf("expected main image from status, got %q", mainContainer.GetImage())
	}

	sidecarContainer := containers[2]
	if sidecarContainer.GetName() != "sidecar" {
		t.Fatalf("expected sidecar container name, got %q", sidecarContainer.GetName())
	}
	if sidecarContainer.GetRole() != runnerv1.ContainerRole_CONTAINER_ROLE_SIDECAR {
		t.Fatalf("expected sidecar role, got %v", sidecarContainer.GetRole())
	}
	if sidecarContainer.GetStatus() != runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED {
		t.Fatalf("expected sidecar status terminated, got %v", sidecarContainer.GetStatus())
	}
	if sidecarContainer.ExitCode == nil || *sidecarContainer.ExitCode != 137 {
		t.Fatalf("expected sidecar exit code 137, got %#v", sidecarContainer.ExitCode)
	}
	if sidecarContainer.FinishedAt == nil {
		t.Fatalf("expected sidecar finished_at to be set")
	}
	if !sidecarContainer.FinishedAt.AsTime().Equal(sidecarFinished.Time) {
		t.Fatalf("expected sidecar finished_at %v, got %v", sidecarFinished.Time, sidecarContainer.FinishedAt.AsTime())
	}
}

func findContainer(containers []corev1.Container, name string) *corev1.Container {
	for idx := range containers {
		if containers[idx].Name == name {
			return &containers[idx]
		}
	}
	return nil
}

func findVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for idx := range volumes {
		if volumes[idx].Name == name {
			return &volumes[idx]
		}
	}
	return nil
}

func assertEnvValue(t *testing.T, envs []corev1.EnvVar, name, expected string) {
	t.Helper()
	for _, env := range envs {
		if env.Name != name {
			continue
		}
		if env.Value != expected {
			t.Fatalf("expected env %s=%q, got %q", name, expected, env.Value)
		}
		return
	}
	t.Fatalf("expected env %s to be set", name)
}

func assertPrivilegedDockerInjection(t *testing.T, pod *corev1.Pod, resp *runnerv1.StartWorkloadResponse) {
	t.Helper()
	main := findContainer(pod.Spec.Containers, "main")
	if main == nil {
		t.Fatal("expected main container")
	}
	assertEnvValue(t, main.Env, dockerHostEnvName, dockerHostEnvValue)

	sidecar := findContainer(pod.Spec.Containers, dockerSidecarName)
	if sidecar == nil {
		t.Fatal("expected docker sidecar container")
	}
	if sidecar.Image != dockerPrivilegedImage {
		t.Fatalf("expected privileged docker image %q, got %q", dockerPrivilegedImage, sidecar.Image)
	}
	if sidecar.SecurityContext == nil || sidecar.SecurityContext.Privileged == nil || !*sidecar.SecurityContext.Privileged {
		t.Fatalf("expected docker sidecar to be privileged")
	}
	if sidecar.SecurityContext.AllowPrivilegeEscalation == nil || !*sidecar.SecurityContext.AllowPrivilegeEscalation {
		t.Fatalf("expected docker sidecar to allow privilege escalation")
	}
	assertEnvValue(t, sidecar.Env, dockerTLSCertDirEnvName, dockerTLSCertDirDisabledValue)
	assertVolumeMount(t, sidecar.VolumeMounts, dockerDataVolumeName, dockerPrivilegedDataMountPath)
	assertEmptyDirVolume(t, pod.Spec.Volumes, dockerDataVolumeName)
	if findVolume(pod.Spec.Volumes, dockerRunVolumeName) != nil {
		t.Fatalf("expected no docker run volume for privileged docker")
	}
	if findVolume(pod.Spec.Volumes, dockerTunVolumeName) != nil {
		t.Fatalf("expected no docker tun volume for privileged docker")
	}
	assertSidecarInstance(t, resp.GetContainers().GetSidecars(), dockerSidecarName)
}

func assertRuntimeClassName(t *testing.T, runtimeClassName *string, expected string) {
	t.Helper()
	if runtimeClassName == nil {
		t.Fatalf("expected runtimeClassName %q", expected)
	}
	if *runtimeClassName != expected {
		t.Fatalf("expected runtimeClassName %q, got %q", expected, *runtimeClassName)
	}
}

func assertRuntimeClassNameUnset(t *testing.T, runtimeClassName *string) {
	t.Helper()
	if runtimeClassName != nil {
		t.Fatalf("expected runtimeClassName to be unset")
	}
}

func assertAnnotation(t *testing.T, annotations map[string]string, key, expected string) {
	t.Helper()
	value, ok := annotations[key]
	if !ok {
		t.Fatalf("expected annotation %s", key)
	}
	if value != expected {
		t.Fatalf("expected annotation %s=%q, got %q", key, expected, value)
	}
}

func assertVolumeMount(t *testing.T, mounts []corev1.VolumeMount, name, mountPath string) {
	t.Helper()
	for _, mount := range mounts {
		if mount.Name != name {
			continue
		}
		if mount.MountPath != mountPath {
			t.Fatalf("expected mount %s at %q, got %q", name, mountPath, mount.MountPath)
		}
		return
	}
	t.Fatalf("expected mount %s", name)
}

func assertVolumeMountWithSubPath(t *testing.T, mounts []corev1.VolumeMount, name, mountPath, subPath string) {
	t.Helper()
	for _, mount := range mounts {
		if mount.Name != name {
			continue
		}
		if mount.MountPath != mountPath {
			continue
		}
		if mount.SubPath != subPath {
			t.Fatalf("expected mount %s subPath %q, got %q", name, subPath, mount.SubPath)
		}
		return
	}
	t.Fatalf("expected mount %s at %q", name, mountPath)
}

func assertEmptyDirVolume(t *testing.T, volumes []corev1.Volume, name string) {
	t.Helper()
	volume := findVolume(volumes, name)
	if volume == nil {
		t.Fatalf("expected volume %s", name)
	}
	if volume.EmptyDir == nil {
		t.Fatalf("expected %s to be emptyDir volume", name)
	}
}

func assertHostPathVolume(t *testing.T, volumes []corev1.Volume, name, path string, hostPathType corev1.HostPathType) {
	t.Helper()
	volume := findVolume(volumes, name)
	if volume == nil {
		t.Fatalf("expected volume %s", name)
	}
	if volume.HostPath == nil {
		t.Fatalf("expected %s to be hostPath volume", name)
	}
	if volume.HostPath.Path != path {
		t.Fatalf("expected %s hostPath %q, got %q", name, path, volume.HostPath.Path)
	}
	if volume.HostPath.Type == nil {
		t.Fatalf("expected %s hostPath type", name)
	}
	if *volume.HostPath.Type != hostPathType {
		t.Fatalf("expected %s hostPath type %q, got %q", name, hostPathType, *volume.HostPath.Type)
	}
}

func assertSidecarInstance(t *testing.T, sidecars []*runnerv1.SidecarInstance, name string) {
	t.Helper()
	for _, sidecar := range sidecars {
		if sidecar.GetName() == name {
			return
		}
	}
	t.Fatalf("expected sidecar instance %s", name)
}

func TestStartWorkloadMountsInlineFiles(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{
			Name:             "main",
			Image:            "busybox",
			InlineFileMounts: []*runnerv1.InlineFileMount{{Path: "/etc/agyn/egress-ca/ca.crt"}},
		},
		Sidecars: []*runnerv1.ContainerSpec{{
			Name:             "sidecar",
			Image:            "busybox",
			InlineFileMounts: []*runnerv1.InlineFileMount{{Path: "/etc/agyn/egress-ca/ca.crt"}},
		}},
		InlineFiles: map[string][]byte{
			"/etc/agyn/egress-ca/ca.crt": []byte("cert"),
		},
	}

	resp, err := server.StartWorkload(ctx, req)
	if err != nil {
		t.Fatalf("StartWorkload returned error: %v", err)
	}

	pod, err := clientset.CoreV1().Pods("default").Get(ctx, podNameFromID(resp.Id), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected pod created: %v", err)
	}
	secretName := fmt.Sprintf("workload-%s-inline-files", resp.Id)
	if pod.Annotations[secretAnnotationKey] != secretName {
		t.Fatalf("expected inline secret annotation %q, got %q", secretName, pod.Annotations[secretAnnotationKey])
	}
	volume := findPodVolume(pod, inlineFilesVolumeName)
	if volume == nil || volume.Secret == nil || volume.Secret.SecretName != secretName {
		t.Fatalf("expected inline files secret volume")
	}
	assertInlineVolumeMount(t, pod.Spec.Containers[0], "/etc/agyn/egress-ca/ca.crt")
	assertInlineVolumeMount(t, pod.Spec.Containers[1], "/etc/agyn/egress-ca/ca.crt")

	secret, err := clientset.CoreV1().Secrets("default").Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected inline secret: %v", err)
	}
	if string(secret.Data["inline-file-0"]) != "cert" {
		t.Fatalf("expected inline file secret data")
	}
}

func TestStartWorkloadRejectsInvalidInlineFiles(t *testing.T) {
	tests := []struct {
		name string
		req  *runnerv1.StartWorkloadRequest
		want string
	}{
		{
			name: "mount without file",
			req: &runnerv1.StartWorkloadRequest{Main: &runnerv1.ContainerSpec{
				Name: "main", Image: "busybox", InlineFileMounts: []*runnerv1.InlineFileMount{{Path: "/file"}},
			}},
			want: "inline_file_not_defined",
		},
		{
			name: "unreferenced file",
			req: &runnerv1.StartWorkloadRequest{
				Main:        &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
				InlineFiles: map[string][]byte{"/file": []byte("data")},
			},
			want: "inline_file_not_referenced",
		},
		{
			name: "non absolute file",
			req: &runnerv1.StartWorkloadRequest{
				Main:        &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
				InlineFiles: map[string][]byte{"file": []byte("data")},
			},
			want: "inline_file_path_invalid",
		},
		{
			name: "unclean mount",
			req: &runnerv1.StartWorkloadRequest{
				Main:        &runnerv1.ContainerSpec{Name: "main", Image: "busybox", InlineFileMounts: []*runnerv1.InlineFileMount{{Path: "/dir/../file"}}},
				InlineFiles: map[string][]byte{"/file": []byte("data")},
			},
			want: "inline_file_path_invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			server := New(Options{Clientset: clientset, Namespace: "default", StorageSize: "1Gi", Logger: zap.NewNop()})
			_, err := server.StartWorkload(context.Background(), tt.req)
			if err == nil {
				t.Fatalf("expected error")
			}
			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.InvalidArgument {
				t.Fatalf("expected invalid argument, got %v", err)
			}
			if !strings.Contains(st.Message(), tt.want) {
				t.Fatalf("expected %q in error, got %q", tt.want, st.Message())
			}
		})
	}
}

func findPodVolume(pod *corev1.Pod, name string) *corev1.Volume {
	for idx := range pod.Spec.Volumes {
		if pod.Spec.Volumes[idx].Name == name {
			return &pod.Spec.Volumes[idx]
		}
	}
	return nil
}

func assertInlineVolumeMount(t *testing.T, container corev1.Container, mountPath string) {
	t.Helper()
	for _, mount := range container.VolumeMounts {
		if mount.Name == inlineFilesVolumeName && mount.MountPath == mountPath {
			if mount.SubPath == "" {
				t.Fatalf("expected inline file subPath")
			}
			if !mount.ReadOnly {
				t.Fatalf("expected inline file read-only mount")
			}
			return
		}
	}
	t.Fatalf("container %s missing inline mount %s", container.Name, mountPath)
}

// Pull credentials live as secrets alongside every other workload's in the
// shared workload namespace, so a workload must not be able to read the API and
// enumerate its neighbours'.
func TestWorkloadPodDoesNotMountAServiceAccountToken(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	server := New(Options{Clientset: clientset, Namespace: "default", StorageSize: "1Gi", Logger: zap.NewNop()})

	workloadID := "3f1c8a52-1a2b-4c3d-9e8f-0a1b2c3d4e5f"
	if _, err := server.StartWorkload(context.Background(), &runnerv1.StartWorkloadRequest{
		WorkloadId: workloadID,
		Main:       &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
	}); err != nil {
		t.Fatalf("StartWorkload: %v", err)
	}

	pods, err := clientset.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(pods.Items) != 1 {
		t.Fatalf("pods = %v, err = %v", pods, err)
	}
	automount := pods.Items[0].Spec.AutomountServiceAccountToken
	if automount == nil || *automount {
		t.Fatal("expected automountServiceAccountToken to be false")
	}
}

// One Secret per workload: every catalog image resolves to the same proxy host,
// so one auths entry covers the whole Pod.
func TestSingleCredentialProducesOneUnindexedSecret(t *testing.T) {
	server := New(Options{Clientset: fake.NewSimpleClientset(), Namespace: "default", StorageSize: "1Gi", Logger: zap.NewNop()})

	workloadID := "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d"
	refs, names, err := server.buildImagePullSecrets(context.Background(), workloadID, []*runnerv1.ImagePullCredential{
		{Registry: "registry.agyn.dev", Username: "w-1", Password: "secret"},
	})
	if err != nil {
		t.Fatalf("buildImagePullSecrets: %v", err)
	}
	want := "workload-" + workloadID + "-pull"
	if len(refs) != 1 || refs[0].Name != want {
		t.Fatalf("refs = %+v, want a single %q", refs, want)
	}
	if len(names) != 1 || names[0] != want {
		t.Fatalf("names = %v, want [%s]", names, want)
	}
}
