package server

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

// Pods written here are evaluated by the pinned Kubernetes v1.35 Pod Security
// "restricted" evaluator in hack/podcheck (a separate module, so this module's
// Kubernetes libraries are not upgraded for a test). Regenerate with
// UPDATE_POD_SECURITY_FIXTURES=1 after an intended Pod shape change.
const podSecurityFixtureDir = "testdata/pod-security"

const fixtureWorkloadID = "00000000-0000-4000-8000-000000000001"

func restrictedPolicy(allowed ...string) config.PodSecurity {
	return config.PodSecurity{Profile: config.PodSecurityRestricted, RunAsUser: 10001, RunAsGroup: 10001, FSGroup: 10001, AllowedCapabilities: allowed}
}

func podSecurityServer(client *fake.Clientset, policy config.PodSecurity) *Server {
	return New(Options{Clientset: client, Namespace: "default", StorageSize: "1Gi", Logger: zap.NewNop(), PodSecurity: policy,
		CapabilityImplementations: config.CapabilityImplementations{Docker: config.DockerImplementationRootless}})
}

// explicitProxyTaskRequest is the shape agents-orchestrator assembles in
// WORKLOAD_NETWORK_MODE=explicit-proxy: enrol, restartable overlay sidecar,
// platform inits, readiness wait, main and an MCP sidecar, with the egress CA
// as an inline file and a persistent workspace.
func explicitProxyTaskRequest() *runnerv1.StartWorkloadRequest {
	readOnly := map[string]string{readOnlyRootFilesystemKey: "true"}
	identity := []*runnerv1.VolumeMount{{Volume: "ziti-identity", MountPath: "/netfoundry"}}
	agynBin := []*runnerv1.VolumeMount{{Volume: "agyn-bin", MountPath: "/agyn"}}
	ca := []*runnerv1.InlineFileMount{{Path: "/etc/ssl/certs/ca-certificates.crt"}}
	return &runnerv1.StartWorkloadRequest{
		WorkloadId: fixtureWorkloadID,
		Main: &runnerv1.ContainerSpec{Name: "agent-main", Image: "workspace.invalid/agent", Cmd: []string{"/agyn/bin/agynd"},
			Mounts: append([]*runnerv1.VolumeMount{{Volume: "vol-workspace", MountPath: "/workspace"}}, agynBin...), InlineFileMounts: ca},
		Sidecars: []*runnerv1.ContainerSpec{{Name: "mcp-00000001", Image: "mcp.invalid/server", Cmd: []string{"/bin/sh", "-c", "serve"}, InlineFileMounts: ca}},
		InitContainers: []*runnerv1.ContainerSpec{
			{Name: "ziti-enroll", Image: "proxy.invalid/workload-proxy", Entrypoint: "/app/workload-proxy", Cmd: []string{"enroll"}, Mounts: identity, AdditionalProperties: readOnly},
			{Name: "ziti-sidecar", Image: "proxy.invalid/workload-proxy", Entrypoint: "/app/workload-proxy", Cmd: []string{"serve"}, Mounts: identity,
				AdditionalProperties: map[string]string{"restart_policy": "Always", readOnlyRootFilesystemKey: "true"}},
			{Name: "agynd-cli-init", Image: "init.invalid/agynd", Mounts: agynBin, AdditionalProperties: readOnly},
			{Name: "agent-runtime", Image: "init.invalid/runtime", Mounts: agynBin, AdditionalProperties: readOnly},
			{Name: "ziti-wait", Image: "proxy.invalid/workload-proxy", Entrypoint: "/app/workload-proxy", Cmd: []string{"wait"}, AdditionalProperties: readOnly},
		},
		Volumes: []*runnerv1.VolumeSpec{
			{Name: "agyn-bin", Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL},
			{Name: "ziti-identity", Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL},
			{Name: "vol-workspace", Kind: runnerv1.VolumeKind_VOLUME_KIND_NAMED, PersistentName: "pv-fixture-workspace", Labels: map[string]string{volumeKeyLabelKey: "fixture-volume"}},
		},
		InlineFiles: map[string][]byte{"/etc/ssl/certs/ca-certificates.crt": []byte("bundle")},
	}
}

func startFixturePod(t *testing.T, s *Server, client *fake.Clientset, req *runnerv1.StartWorkloadRequest) *corev1.Pod {
	t.Helper()
	resp, err := s.StartWorkload(context.Background(), req)
	if err != nil {
		t.Fatalf("StartWorkload: %v", err)
	}
	pod, err := client.CoreV1().Pods("default").Get(context.Background(), podNameFromID(resp.Id), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("pod not created: %v", err)
	}
	return pod
}

func TestRestrictedProfileBuildsAdmissiblePods(t *testing.T) {
	shapes := map[string]func() *runnerv1.StartWorkloadRequest{
		"main-only": func() *runnerv1.StartWorkloadRequest {
			return &runnerv1.StartWorkloadRequest{WorkloadId: fixtureWorkloadID, Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"}}
		},
		"main-with-sidecars": func() *runnerv1.StartWorkloadRequest {
			return &runnerv1.StartWorkloadRequest{WorkloadId: fixtureWorkloadID, Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
				Sidecars: []*runnerv1.ContainerSpec{{Name: "mcp-a", Image: "busybox"}, {Name: "mcp-b", Image: "busybox", RequiredCapabilities: []string{"net_bind_service"}}}}
		},
		"explicit-proxy-task": explicitProxyTaskRequest,
	}
	for name, build := range shapes {
		t.Run(name, func(t *testing.T) {
			client := newIdentityClientset()
			pod := startFixturePod(t, podSecurityServer(client, restrictedPolicy(config.CapabilityNetBindService)), client, build())
			assertRestrictedPod(t, pod, config.CapabilityNetBindService)
			checkPodSecurityFixture(t, "allowed-"+name, pod)
		})
	}
}

func TestRestrictedProfileExplicitProxyShape(t *testing.T) {
	client := newIdentityClientset()
	pod := startFixturePod(t, podSecurityServer(client, restrictedPolicy()), client, explicitProxyTaskRequest())
	readOnly := map[string]bool{}
	for _, c := range append(slices.Clone(pod.Spec.InitContainers), pod.Spec.Containers...) {
		readOnly[c.Name] = c.SecurityContext.ReadOnlyRootFilesystem != nil && *c.SecurityContext.ReadOnlyRootFilesystem
	}
	for name, want := range map[string]bool{"ziti-enroll": true, "ziti-sidecar": true, "agynd-cli-init": true, "agent-runtime": true, "ziti-wait": true, "agent-main": false, "mcp-00000001": false} {
		if readOnly[name] != want {
			t.Fatalf("%s readOnlyRootFilesystem = %v, want %v", name, readOnly[name], want)
		}
	}
	sidecar := pod.Spec.InitContainers[1]
	if sidecar.Name != "ziti-sidecar" || sidecar.RestartPolicy == nil || *sidecar.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Fatal("overlay sidecar must stay a restartable init container")
	}
	if pod.Spec.DNSPolicy != "" || pod.Spec.DNSConfig != nil {
		t.Fatal("no DNS override was requested")
	}
}

// A caller cannot loosen the server-owned shape: "false" is not a request for
// a writable root, merely the absence of a tightening.
func TestReadOnlyRootFilesystemIsTightenOnly(t *testing.T) {
	client := newIdentityClientset()
	s := podSecurityServer(client, restrictedPolicy())
	req := &runnerv1.StartWorkloadRequest{Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox", AdditionalProperties: map[string]string{readOnlyRootFilesystemKey: "false"}}}
	pod := startFixturePod(t, s, client, req)
	sc := pod.Spec.Containers[0].SecurityContext
	if sc.ReadOnlyRootFilesystem != nil || *sc.AllowPrivilegeEscalation || sc.Capabilities.Drop[0] != "ALL" {
		t.Fatalf("false loosened or replaced the profile: %#v", sc)
	}
	req = &runnerv1.StartWorkloadRequest{Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox", AdditionalProperties: map[string]string{readOnlyRootFilesystemKey: "yes"}}}
	if _, err := s.StartWorkload(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ambiguous read-only value accepted: %v", err)
	}
}

func TestDisallowedCapabilitiesAreRejectedBeforeAnyWrite(t *testing.T) {
	for _, capability := range []string{"NET_ADMIN", "sys_admin", "CAP_NET_RAW", "ALL"} {
		for _, place := range []string{"main", "sidecar", "init"} {
			t.Run(capability+"/"+place, func(t *testing.T) {
				client := newIdentityClientset()
				s := podSecurityServer(client, restrictedPolicy(config.CapabilityNetBindService))
				req := explicitProxyTaskRequest()
				req.ImagePullCredentials = []*runnerv1.ImagePullCredential{{Registry: "registry.invalid", Username: "u", Password: "p"}}
				spec := map[string]*runnerv1.ContainerSpec{"main": req.Main, "sidecar": req.Sidecars[0], "init": req.InitContainers[1]}[place]
				spec.RequiredCapabilities = []string{capability}
				_, err := s.StartWorkload(context.Background(), req)
				want := "required_capability_not_allowed: " + config.NormalizeCapability(capability)
				if status.Code(err) != codes.InvalidArgument || status.Convert(err).Message() != want {
					t.Fatalf("StartWorkload error = %v, want InvalidArgument %q", err, want)
				}
				assertNoWrites(t, client)
			})
		}
	}
}

// The legacy profile leaves the Pod shape alone but not the allowlist: the
// tproxy sidecar only gets NET_ADMIN where the operator listed it.
func TestLegacyProfileStillEnforcesTheAllowlist(t *testing.T) {
	req := func() *runnerv1.StartWorkloadRequest {
		return &runnerv1.StartWorkloadRequest{WorkloadId: fixtureWorkloadID, Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
			InitContainers: []*runnerv1.ContainerSpec{{Name: "ziti-sidecar", Image: "tunnel", RequiredCapabilities: []string{"NET_ADMIN"}, AdditionalProperties: map[string]string{"restart_policy": "Always"}}}}
	}
	client := newIdentityClientset()
	if _, err := podSecurityServer(client, config.PodSecurity{Profile: config.PodSecurityNone}).StartWorkload(context.Background(), req()); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unlisted NET_ADMIN accepted: %v", err)
	}
	assertNoWrites(t, client)

	client = newIdentityClientset()
	pod := startFixturePod(t, podSecurityServer(client, config.PodSecurity{Profile: config.PodSecurityNone, AllowedCapabilities: []string{"NET_ADMIN"}}), client, req())
	if pod.Spec.SecurityContext != nil || pod.Annotations[podSecurityAnnotation] != "" {
		t.Fatal("legacy profile must keep the upstream Pod shape")
	}
	if add := pod.Spec.InitContainers[0].SecurityContext.Capabilities.Add; len(add) != 1 || add[0] != "NET_ADMIN" {
		t.Fatalf("allowlisted capability not added: %v", add)
	}
	// Kept so the v1.35 evaluator proves the old shape is what restricted
	// admission refuses, not merely what this test asserts.
	checkPodSecurityFixture(t, "denied-legacy-tproxy", pod)
}

func TestDockerCapabilityRefusedUnderRestrictedProfile(t *testing.T) {
	client := newIdentityClientset()
	s := podSecurityServer(client, restrictedPolicy())
	req := &runnerv1.StartWorkloadRequest{Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"}, Capabilities: []string{"docker"},
		InlineFiles: map[string][]byte{"/etc/x": []byte("x")}}
	req.Main.InlineFileMounts = []*runnerv1.InlineFileMount{{Path: "/etc/x"}}
	_, err := s.StartWorkload(context.Background(), req)
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "capability_forbidden_by_pod_security") {
		t.Fatalf("docker under restricted: %v", err)
	}
	assertNoWrites(t, client)
}

func TestPreparedRestrictedPodIsAdmissible(t *testing.T) {
	client := preparedTestClient()
	s := preparedTestServer(client)
	s.podSecurity = restrictedPolicy()
	req := preparedTestRequest()
	req.Workload.WorkloadId = fixtureWorkloadID
	response, err := s.PrepareWorkload(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	pod := preparedTestPod(t, client, response.Binding)
	assertRestrictedPod(t, pod)
	checkPodSecurityFixture(t, "allowed-prepared", pod)
	if _, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: response.Binding}); err != nil {
		t.Fatalf("restricted prepared pod refused under the same profile: %v", err)
	}
}

// A runner restarted with the restricted profile must not start compute that a
// looser runner prepared, and must still be able to clean it up.
func TestPreparedActivationRechecksPodSecurity(t *testing.T) {
	client := preparedTestClient()
	before := preparedTestServer(client)
	before.podSecurity = config.PodSecurity{Profile: config.PodSecurityNone}
	binding := prepareAnchoredTest(t, before, anchoredTestRequest(t, before, false, false))
	after := preparedTestServer(client)
	after.podSecurity = restrictedPolicy()
	client.ClearActions()
	_, err := after.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: binding})
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "prepared_workload_security_mismatch" {
		t.Fatalf("looser prepared pod activated: %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" {
			t.Fatalf("security rejection mutated %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
	if !hasPreparedGate(preparedTestPod(t, client, binding)) {
		t.Fatal("rejected pod lost its gate")
	}
	request := &runnerv1.RemovePreparedWorkloadRequest{Expected: binding}
	if _, err := after.RemovePreparedWorkload(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	result, err := after.RemovePreparedWorkload(context.Background(), request)
	if err != nil || result.GetState() != runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT {
		t.Fatalf("cleanup blocked: %v", err)
	}
}

func assertNoWrites(t *testing.T, client *fake.Clientset) {
	t.Helper()
	for _, action := range client.Actions() {
		if verb := action.GetVerb(); verb != "get" && verb != "list" {
			t.Fatalf("rejected request wrote: %s %s", verb, action.GetResource().Resource)
		}
	}
}

func assertRestrictedPod(t *testing.T, pod *corev1.Pod, allowed ...string) {
	t.Helper()
	sc := pod.Spec.SecurityContext
	if sc == nil || sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot || sc.RunAsUser == nil || *sc.RunAsUser != 10001 ||
		sc.RunAsGroup == nil || *sc.RunAsGroup != 10001 || sc.FSGroup == nil || *sc.FSGroup != 10001 ||
		sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("pod security context not restricted: %#v", sc)
	}
	if pod.Spec.EnableServiceLinks == nil || *pod.Spec.EnableServiceLinks || pod.Annotations[podSecurityAnnotation] != podSecurityRestrictedV1 {
		t.Fatal("restricted pod lacks service-link opt-out or profile annotation")
	}
	if pod.Spec.HostNetwork || pod.Spec.HostPID || pod.Spec.HostIPC || pod.Spec.HostUsers != nil {
		t.Fatal("restricted pod shares a host namespace")
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.HostPath != nil {
			t.Fatalf("restricted pod mounts hostPath %s", volume.Name)
		}
	}
	for _, c := range append(slices.Clone(pod.Spec.InitContainers), pod.Spec.Containers...) {
		csc := c.SecurityContext
		if csc == nil || csc.AllowPrivilegeEscalation == nil || *csc.AllowPrivilegeEscalation || csc.Privileged != nil ||
			csc.Capabilities == nil || !slices.Equal(csc.Capabilities.Drop, []corev1.Capability{"ALL"}) {
			t.Fatalf("container %s not restricted: %#v", c.Name, csc)
		}
		for _, add := range csc.Capabilities.Add {
			if !slices.Contains(allowed, string(add)) {
				t.Fatalf("container %s adds %s", c.Name, add)
			}
		}
	}
}

// checkPodSecurityFixture keeps the Pod Security-relevant part of a built Pod
// in testdata for the v1.35 evaluator: the spec and the annotations admission
// reads. Values that vary per run (UIDs, startup attempts) are not part of it.
func checkPodSecurityFixture(t *testing.T, name string, pod *corev1.Pod) {
	t.Helper()
	annotations := map[string]string{}
	for key, value := range pod.Annotations {
		if key == podSecurityAnnotation || strings.Contains(key, "seccomp") || strings.Contains(key, "apparmor") {
			annotations[key] = value
		}
	}
	fixture := corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "agyn-workloads", Annotations: annotations},
		Spec:       *pod.Spec.DeepCopy(),
	}
	for i := range fixture.Spec.Volumes {
		if claim := fixture.Spec.Volumes[i].PersistentVolumeClaim; claim != nil {
			claim.ClaimName = "fixture-claim"
		}
	}
	encoded, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(podSecurityFixtureDir, name+".json")
	if os.Getenv("UPDATE_POD_SECURITY_FIXTURES") == "1" {
		if err := os.MkdirAll(podSecurityFixtureDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	committed, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("missing Pod Security fixture %s (UPDATE_POD_SECURITY_FIXTURES=1 regenerates it): %v", path, err)
	}
	if !bytes.Equal(committed, encoded) {
		t.Fatalf("Pod Security fixture %s is stale; review the Pod change and regenerate with UPDATE_POD_SECURITY_FIXTURES=1", path)
	}
}
