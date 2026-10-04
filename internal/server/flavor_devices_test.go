package server

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

const kvmResource corev1.ResourceName = "squat.ai/kvm"

// deviceCatalog is testCatalog plus the flavor the Android QA agent runs on:
// /dev/kvm through the KVM device plugin's extended resource, opened through
// the host kvm group.
func deviceCatalog() config.Catalog {
	catalog := testCatalog()
	catalog.Flavors = append(catalog.Flavors, config.FlavorEntry{
		Name:               "qa-android",
		Resources:          config.ComputeResources{RequestsCPU: "1", RequestsMemory: "4Gi", LimitsCPU: "2", LimitsMemory: "6Gi"},
		SidecarResources:   config.ComputeResources{RequestsCPU: "100m", RequestsMemory: "128Mi", LimitsCPU: "500m", LimitsMemory: "256Mi"},
		Devices:            []config.DeviceRequest{{Resource: string(kvmResource), Count: 1}},
		SupplementalGroups: config.SupplementalGroups{994},
	})
	return catalog
}

func hasDevice(container corev1.Container) (requested, limited bool) {
	_, requested = container.Resources.Requests[kvmResource]
	_, limited = container.Resources.Limits[kvmResource]
	return requested, limited
}

func TestResolveFlavorCarriesDevicesAndGroups(t *testing.T) {
	s := &Server{catalog: deviceCatalog()}
	resolved, err := s.resolveFlavor("qa-android")
	if err != nil {
		t.Fatal(err)
	}
	assertQuantity(t, resolved.devices, kvmResource, "1")
	if !slices.Equal(resolved.supplementalGroups, []int64{994}) {
		t.Fatalf("supplementalGroups = %v", resolved.supplementalGroups)
	}
	if _, ok := resolved.main.Requests[kvmResource]; ok {
		t.Fatal("the device became part of the flavor's size")
	}

	plain, err := s.resolveFlavor("ram-2gb")
	if err != nil {
		t.Fatal(err)
	}
	if plain.devices != nil || plain.supplementalGroups != nil {
		t.Fatalf("a flavor without devices resolved some: %+v", plain)
	}
}

func TestBuildContainersAddsDevicesToMainOnly(t *testing.T) {
	s := &Server{catalog: deviceCatalog()}
	resolved, err := s.resolveFlavor("qa-android")
	if err != nil {
		t.Fatal(err)
	}
	req := &runnerv1.StartWorkloadRequest{
		Main:           &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Sidecars:       []*runnerv1.ContainerSpec{{Name: "mcp-a", Image: "mcp"}, {Name: "mcp-b", Image: "mcp"}},
		InitContainers: []*runnerv1.ContainerSpec{{Name: "init-bin", Image: "agyn-bin"}},
	}
	containers, initContainers, _, err := buildContainers(req, nil, nil, resolved)
	if err != nil {
		t.Fatal(err)
	}
	main := containers[0]
	assertQuantity(t, main.Resources.Requests, kvmResource, "1")
	assertQuantity(t, main.Resources.Limits, kvmResource, "1")
	assertQuantity(t, main.Resources.Requests, corev1.ResourceMemory, "4Gi")
	assertQuantity(t, main.Resources.Limits, corev1.ResourceMemory, "6Gi")
	for _, container := range append(slices.Clone(containers[1:]), initContainers...) {
		if requested, limited := hasDevice(container); requested || limited {
			t.Fatalf("%s got the main container's device", container.Name)
		}
	}
	// The resolved flavor is shared by every start; a device added to one
	// container must not leak into it.
	if _, ok := resolved.main.Requests[kvmResource]; ok {
		t.Fatal("applying devices mutated the resolved flavor")
	}
}

// Explicit bounds replace the flavor's size, not its devices: a QA workload
// started with compute-resources still gets /dev/kvm.
func TestDevicesSurviveExplicitMainBounds(t *testing.T) {
	client := fake.NewSimpleClientset()
	s := New(Options{Clientset: client, Namespace: "workloads", Logger: zap.NewNop(), Catalog: deviceCatalog(), SupportingContainerResources: supportingResources()})
	req := boundedRequest()
	req.Flavor = "qa-android"
	response, err := s.StartWorkload(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	pod, err := client.CoreV1().Pods("workloads").Get(context.Background(), podNameFromID(response.Id), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	main := pod.Spec.Containers[0]
	explicit, _ := containerResources(req.Main.Resources)
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		if main.Resources.Requests[name] != explicit.Requests[name] || main.Resources.Limits[name] != explicit.Limits[name] {
			t.Fatalf("main %s = %v, want the explicit bounds %v", name, main.Resources, explicit)
		}
	}
	assertQuantity(t, main.Resources.Requests, kvmResource, "1")
	assertQuantity(t, main.Resources.Limits, kvmResource, "1")
	for _, container := range append(slices.Clone(pod.Spec.Containers[1:]), pod.Spec.InitContainers...) {
		if requested, limited := hasDevice(container); requested || limited {
			t.Fatalf("%s got the main container's device", container.Name)
		}
	}
	if sc := pod.Spec.SecurityContext; sc == nil || !slices.Equal(sc.SupplementalGroups, []int64{994}) {
		t.Fatalf("pod security context = %#v, want supplemental group 994", sc)
	}
}

// The legacy profile leaves the security context unset, except for the groups
// a flavor declares; the restricted one keeps every field it sets.
func TestSupplementalGroupsUnderEitherProfile(t *testing.T) {
	for _, policy := range []config.PodSecurity{restrictedPolicy(), {Profile: config.PodSecurityNone}} {
		t.Run(string(policy.Profile), func(t *testing.T) {
			client := newIdentityClientset()
			s := podSecurityServer(client, policy)
			s.catalog = deviceCatalog()
			pod := startFixturePod(t, s, client, &runnerv1.StartWorkloadRequest{WorkloadId: fixtureWorkloadID, Flavor: "qa-android",
				Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"}})
			if policy.Restricted() {
				assertRestrictedPod(t, pod)
				if !slices.Equal(pod.Spec.SecurityContext.SupplementalGroups, []int64{994}) {
					t.Fatalf("supplementalGroups = %v", pod.Spec.SecurityContext.SupplementalGroups)
				}
				return
			}
			want, _ := json.Marshal(corev1.PodSecurityContext{SupplementalGroups: []int64{994}})
			got, _ := json.Marshal(pod.Spec.SecurityContext)
			if string(got) != string(want) || pod.Annotations[podSecurityAnnotation] != "" {
				t.Fatalf("legacy security context = %s, want %s", got, want)
			}
		})
	}
}

// Adding a device flavor to the catalog changes no Pod built from any other
// flavor, nor an unsized one.
func TestDeviceFlavorLeavesOtherFlavorsUnchanged(t *testing.T) {
	request := func(flavor string) *runnerv1.StartWorkloadRequest {
		return &runnerv1.StartWorkloadRequest{WorkloadId: fixtureWorkloadID, Flavor: flavor,
			Main:           &runnerv1.ContainerSpec{Name: "main", Image: "busybox", Mounts: []*runnerv1.VolumeMount{{Volume: "scratch", MountPath: "/scratch"}}},
			Sidecars:       []*runnerv1.ContainerSpec{{Name: "mcp", Image: "mcp"}},
			InitContainers: []*runnerv1.ContainerSpec{{Name: "init", Image: "init"}},
			Volumes:        []*runnerv1.VolumeSpec{{Name: "scratch", Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL}},
		}
	}
	build := func(t *testing.T, policy config.PodSecurity, catalog config.Catalog, flavor string) []byte {
		client := newIdentityClientset()
		s := podSecurityServer(client, policy)
		s.catalog = catalog
		pod := startFixturePod(t, s, client, request(flavor))
		encoded, err := json.Marshal(struct {
			Labels, Annotations map[string]string
			Spec                corev1.PodSpec
		}{pod.Labels, pod.Annotations, pod.Spec})
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	for _, policy := range []config.PodSecurity{restrictedPolicy(), {Profile: config.PodSecurityNone}} {
		for _, flavor := range []string{"", "ram-2gb", "main-only"} {
			t.Run(string(policy.Profile)+"/"+flavor, func(t *testing.T) {
				before := build(t, policy, testCatalog(), flavor)
				after := build(t, policy, deviceCatalog(), flavor)
				if string(before) != string(after) {
					t.Fatalf("a device flavor changed a %q Pod:\nbefore %s\nafter  %s", flavor, before, after)
				}
			})
		}
	}
}
