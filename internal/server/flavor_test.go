package server

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

func testCatalog() config.Catalog {
	return config.Catalog{
		Flavors: []config.FlavorEntry{
			{
				Name:    "ram-2gb",
				Default: true,
				Resources: config.ComputeResources{
					RequestsCPU: "500m", RequestsMemory: "2Gi",
					LimitsCPU: "2", LimitsMemory: "2Gi",
				},
				SidecarResources: config.ComputeResources{
					RequestsCPU: "100m", RequestsMemory: "128Mi",
					LimitsCPU: "500m", LimitsMemory: "256Mi",
				},
			},
			{
				Name: "main-only",
				Resources: config.ComputeResources{
					RequestsCPU: "1", RequestsMemory: "4Gi",
					LimitsCPU: "4", LimitsMemory: "4Gi",
				},
			},
		},
	}
}

func assertQuantity(t *testing.T, list corev1.ResourceList, name corev1.ResourceName, want string) {
	t.Helper()
	got, ok := list[name]
	if !ok {
		t.Fatalf("expected %s to be set", name)
	}
	expected := resource.MustParse(want)
	if got.Cmp(expected) != 0 {
		t.Fatalf("expected %s %s, got %s", name, want, got.String())
	}
}

func TestResolveFlavorSizesMainAndSidecars(t *testing.T) {
	s := &Server{catalog: testCatalog()}

	resolved, err := s.resolveFlavor("ram-2gb")
	if err != nil {
		t.Fatalf("resolveFlavor returned error: %v", err)
	}
	if resolved.main == nil {
		t.Fatal("expected main resources to be set")
	}
	assertQuantity(t, resolved.main.Requests, corev1.ResourceCPU, "500m")
	assertQuantity(t, resolved.main.Requests, corev1.ResourceMemory, "2Gi")
	assertQuantity(t, resolved.main.Limits, corev1.ResourceCPU, "2")
	assertQuantity(t, resolved.main.Limits, corev1.ResourceMemory, "2Gi")

	if resolved.sidecar == nil {
		t.Fatal("expected sidecar resources to be set")
	}
	assertQuantity(t, resolved.sidecar.Requests, corev1.ResourceCPU, "100m")
	assertQuantity(t, resolved.sidecar.Limits, corev1.ResourceMemory, "256Mi")
}

func TestResolveFlavorUnknownNameFails(t *testing.T) {
	s := &Server{catalog: testCatalog()}

	if _, err := s.resolveFlavor("nope"); err == nil {
		t.Fatal("expected unknown flavor to fail")
	}
}

func TestResolveFlavorEmptyNameLeavesWorkloadUnsized(t *testing.T) {
	s := &Server{catalog: testCatalog()}

	resolved, err := s.resolveFlavor("")
	if err != nil {
		t.Fatalf("resolveFlavor returned error: %v", err)
	}
	if resolved.main != nil || resolved.sidecar != nil {
		t.Fatal("expected an empty flavor to size nothing")
	}
}

func TestResolveFlavorWithoutSidecarResourcesLeavesSidecarsUnsized(t *testing.T) {
	s := &Server{catalog: testCatalog()}

	resolved, err := s.resolveFlavor("main-only")
	if err != nil {
		t.Fatalf("resolveFlavor returned error: %v", err)
	}
	if resolved.main == nil {
		t.Fatal("expected main resources to be set")
	}
	if resolved.sidecar != nil {
		t.Fatal("expected sidecars to stay unsized")
	}
}

func TestBuildContainersAppliesFlavorToMainAndSidecars(t *testing.T) {
	s := &Server{catalog: testCatalog()}
	resolved, err := s.resolveFlavor("ram-2gb")
	if err != nil {
		t.Fatalf("resolveFlavor returned error: %v", err)
	}

	req := &runnerv1.StartWorkloadRequest{
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Sidecars: []*runnerv1.ContainerSpec{
			{Name: "mcp-files", Image: "files-mcp"},
			{Name: "mcp-search", Image: "search-mcp"},
		},
		InitContainers: []*runnerv1.ContainerSpec{
			{Name: "init-bin", Image: "agyn-bin"},
		},
	}

	containers, initContainers, _, err := buildContainers(req, nil, nil, resolved)
	if err != nil {
		t.Fatalf("buildContainers returned error: %v", err)
	}
	if len(containers) != 3 {
		t.Fatalf("expected 3 containers, got %d", len(containers))
	}

	assertQuantity(t, containers[0].Resources.Requests, corev1.ResourceMemory, "2Gi")
	for _, sidecar := range containers[1:] {
		assertQuantity(t, sidecar.Resources.Requests, corev1.ResourceMemory, "128Mi")
		assertQuantity(t, sidecar.Resources.Limits, corev1.ResourceCPU, "500m")
	}

	// Init containers are deliberately left out of the flavor: they run before
	// the workload occupies its size, and sizing them would add their request
	// to the pod's.
	if len(initContainers[0].Resources.Requests) != 0 {
		t.Fatal("expected init containers to stay unsized")
	}
}

func TestBuildContainersWithUnsizedFlavorLeavesResourcesEmpty(t *testing.T) {
	req := &runnerv1.StartWorkloadRequest{
		Main:     &runnerv1.ContainerSpec{Name: "main", Image: "busybox"},
		Sidecars: []*runnerv1.ContainerSpec{{Name: "mcp", Image: "mcp"}},
	}

	containers, _, _, err := buildContainers(req, nil, nil, workloadResources{})
	if err != nil {
		t.Fatalf("buildContainers returned error: %v", err)
	}
	for _, container := range containers {
		if len(container.Resources.Requests) != 0 || len(container.Resources.Limits) != 0 {
			t.Fatalf("expected %s to stay unsized", container.Name)
		}
	}
}
