package config

import "testing"

// The chart renders the catalog through `toYaml`, which emits a different shape
// than the hand-written dev file: unindented list items, reordered keys, and
// quotes only where the scalar would otherwise parse as a number. This is that
// output verbatim, so a chart change that stops loading fails here rather than
// on a cluster.
const chartRenderedCatalog = `capabilities: []
flavors:
- default: true
  name: ram-2gb
  resources:
    limitsCpu: "2"
    limitsMemory: 2Gi
    requestsCpu: 500m
    requestsMemory: 512Mi
  sidecarResources:
    limitsCpu: 500m
    limitsMemory: 256Mi
    requestsCpu: 50m
    requestsMemory: 64Mi
- name: ram-4gb
  resources:
    limitsCpu: "4"
    limitsMemory: 4Gi
    requestsCpu: "1"
    requestsMemory: 1Gi
  sidecarResources:
    limitsCpu: 500m
    limitsMemory: 256Mi
    requestsCpu: 50m
    requestsMemory: 64Mi
storageClasses:
- default: true
  name: default
  storageClassName: ""
`

func TestLoadCatalogAcceptsChartRenderedCatalog(t *testing.T) {
	catalog, err := LoadCatalog(writeCatalog(t, chartRenderedCatalog))
	if err != nil {
		t.Fatalf("LoadCatalog on chart-rendered catalog: %v", err)
	}
	if len(catalog.Flavors) != 2 {
		t.Fatalf("expected 2 flavors, got %d", len(catalog.Flavors))
	}
	flavor, ok := catalog.FlavorFor("ram-2gb")
	if !ok {
		t.Fatal("expected ram-2gb to resolve")
	}
	if flavor.Resources.RequestsMemory != "512Mi" {
		t.Fatalf("unexpected main memory %q", flavor.Resources.RequestsMemory)
	}
	if flavor.SidecarResources.RequestsMemory != "64Mi" {
		t.Fatalf("unexpected sidecar memory %q", flavor.SidecarResources.RequestsMemory)
	}
}
