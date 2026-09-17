package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeCatalog(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write catalog: %v", err)
	}
	return path
}

func TestLoadCatalogReadsDeclaredEntries(t *testing.T) {
	path := writeCatalog(t, `
flavors:
  - name: ram-2gb
    default: true
    resources:
      requestsCpu: "500m"
      requestsMemory: "2Gi"
      limitsCpu: "2"
      limitsMemory: "2Gi"
  - name: ram-4gb
    resources:
      requestsCpu: "1"
      requestsMemory: "4Gi"
      limitsCpu: "4"
      limitsMemory: "4Gi"
storageClasses:
  - name: default
    default: true
    storageClassName: ""
  - name: fast-ssd
    storageClassName: premium-rwo
capabilities: [docker]
`)

	catalog, err := LoadCatalog(path)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if len(catalog.Flavors) != 2 {
		t.Fatalf("expected 2 flavors, got %d", len(catalog.Flavors))
	}
	if !catalog.Flavors[0].Default || catalog.Flavors[0].Name != "ram-2gb" {
		t.Fatalf("expected ram-2gb to be the default flavor, got %+v", catalog.Flavors[0])
	}
	if catalog.Flavors[1].Resources.RequestsMemory != "4Gi" {
		t.Fatalf("expected 4Gi request, got %q", catalog.Flavors[1].Resources.RequestsMemory)
	}
	if len(catalog.Capabilities) != 1 || catalog.Capabilities[0] != "docker" {
		t.Fatalf("expected docker capability, got %v", catalog.Capabilities)
	}
}

func TestLoadCatalogTreatsAbsentFileAsEmpty(t *testing.T) {
	// A runner may run without a declared catalog; that is not a failure.
	catalog, err := LoadCatalog(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("load missing catalog: %v", err)
	}
	if len(catalog.Flavors) != 0 {
		t.Fatalf("expected empty catalog, got %+v", catalog)
	}

	if _, err := LoadCatalog(""); err != nil {
		t.Fatalf("load unset catalog path: %v", err)
	}
}

func TestLoadCatalogRejectsSecondDefault(t *testing.T) {
	path := writeCatalog(t, `
flavors:
  - name: ram-2gb
    default: true
    resources: {requestsCpu: "1", requestsMemory: 2Gi, limitsCpu: "1", limitsMemory: 2Gi}
  - name: ram-4gb
    default: true
    resources: {requestsCpu: "1", requestsMemory: 4Gi, limitsCpu: "1", limitsMemory: 4Gi}
`)

	_, err := LoadCatalog(path)
	if err == nil || !strings.Contains(err.Error(), "at most one flavor") {
		t.Fatalf("expected single-default rejection, got %v", err)
	}
}

func TestLoadCatalogRejectsIncompleteResources(t *testing.T) {
	path := writeCatalog(t, `
flavors:
  - name: ram-2gb
    resources: {requestsCpu: "1", requestsMemory: 2Gi, limitsCpu: "1"}
`)

	_, err := LoadCatalog(path)
	if err == nil || !strings.Contains(err.Error(), "limitsMemory") {
		t.Fatalf("expected missing limitsMemory to be rejected, got %v", err)
	}
}

func TestLoadCatalogRejectsDuplicateNames(t *testing.T) {
	path := writeCatalog(t, `
flavors:
  - name: ram-2gb
    resources: {requestsCpu: "1", requestsMemory: 2Gi, limitsCpu: "1", limitsMemory: 2Gi}
  - name: ram-2gb
    resources: {requestsCpu: "2", requestsMemory: 2Gi, limitsCpu: "2", limitsMemory: 2Gi}
`)

	if _, err := LoadCatalog(path); err == nil {
		t.Fatal("expected duplicate flavor name to be rejected")
	}
}

func TestStorageClassNameForMapsEntries(t *testing.T) {
	catalog := Catalog{StorageClasses: []StorageClassEntry{
		{Name: "default", StorageClassName: ""},
		{Name: "fast-ssd", StorageClassName: "premium-rwo"},
	}}

	if name, ok := catalog.StorageClassNameFor("fast-ssd"); !ok || name != "premium-rwo" {
		t.Fatalf("expected premium-rwo, got %q (ok=%v)", name, ok)
	}
	// The default entry maps to the cluster default, which is an empty name.
	if name, ok := catalog.StorageClassNameFor("default"); !ok || name != "" {
		t.Fatalf("expected empty cluster-default mapping, got %q (ok=%v)", name, ok)
	}
	// An unknown name must not silently fall back to the cluster default.
	if _, ok := catalog.StorageClassNameFor("nope"); ok {
		t.Fatal("expected unknown class to be unresolved")
	}
}

func TestLoadCatalogAcceptsFlavorWithoutSidecarResources(t *testing.T) {
	path := writeCatalog(t, `
flavors:
  - name: ram-2gb
    resources: {requestsCpu: "1", requestsMemory: 2Gi, limitsCpu: "1", limitsMemory: 2Gi}
`)

	catalog, err := LoadCatalog(path)
	if err != nil {
		t.Fatalf("LoadCatalog returned error: %v", err)
	}
	if !catalog.Flavors[0].SidecarResources.IsZero() {
		t.Fatal("expected sidecar resources to be unset")
	}
}

func TestLoadCatalogRejectsIncompleteSidecarResources(t *testing.T) {
	path := writeCatalog(t, `
flavors:
  - name: ram-2gb
    resources: {requestsCpu: "1", requestsMemory: 2Gi, limitsCpu: "1", limitsMemory: 2Gi}
    sidecarResources: {requestsCpu: 100m, requestsMemory: 128Mi}
`)

	_, err := LoadCatalog(path)
	if err == nil || !strings.Contains(err.Error(), "sidecarResources.limits") {
		t.Fatalf("expected partial sidecarResources to be rejected, got %v", err)
	}
}

func TestFlavorForMapsEntries(t *testing.T) {
	catalog := Catalog{Flavors: []FlavorEntry{
		{Name: "ram-2gb", Resources: ComputeResources{RequestsMemory: "2Gi"}},
	}}

	if flavor, ok := catalog.FlavorFor("ram-2gb"); !ok || flavor.Resources.RequestsMemory != "2Gi" {
		t.Fatalf("expected ram-2gb to resolve, got %+v (ok=%v)", flavor, ok)
	}
	if _, ok := catalog.FlavorFor("nope"); ok {
		t.Fatal("expected unknown flavor to be unresolved")
	}
}
