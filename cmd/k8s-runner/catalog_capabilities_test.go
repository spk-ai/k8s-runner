package main

import (
	"reflect"
	"testing"

	"github.com/agynio/k8s-runner/internal/config"
)

// The catalog listed capabilities by hand while the runner decided what it
// could serve from its configured implementations, and the two disagreed:
// docker was configured and working, the catalog said nothing, and the
// Orchestrator refused to place any agent that asked for it -- "no eligible
// runners found (required capabilities: [docker])".
func TestCatalogCapabilitiesReportsConfiguredDocker(t *testing.T) {
	cfg := config.Config{CapabilityImplementations: config.CapabilityImplementations{
		Docker: config.DockerImplementationRootless,
	}}

	if got := catalogCapabilities(cfg); !reflect.DeepEqual(got, []string{"docker"}) {
		t.Fatalf("expected [docker], got %v", got)
	}
}

// Without an implementation the runner cannot serve it, so it must not claim it.
func TestCatalogCapabilitiesOmitsUnconfiguredDocker(t *testing.T) {
	if got := catalogCapabilities(config.Config{}); len(got) != 0 {
		t.Fatalf("expected no capabilities, got %v", got)
	}
}

// The catalog entry still stands on its own, so an operator can advertise
// something ahead of the implementation, and listing docker there as well does
// not report it twice.
func TestCatalogCapabilitiesMergesTheCatalogEntry(t *testing.T) {
	cfg := config.Config{
		Catalog: config.Catalog{Capabilities: []string{"docker", " gpu "}},
		CapabilityImplementations: config.CapabilityImplementations{
			Docker: config.DockerImplementationPrivileged,
		},
	}

	if got := catalogCapabilities(cfg); !reflect.DeepEqual(got, []string{"docker", "gpu"}) {
		t.Fatalf("expected [docker gpu], got %v", got)
	}
}

// The restricted workload profile refuses every docker implementation, so a
// runner under it must not attract docker workloads it would then reject --
// whether docker comes from the implementation map or the catalog.
func TestCatalogCapabilitiesHidesDockerUnderRestrictedProfile(t *testing.T) {
	cfg := config.Config{
		CapabilityImplementations: config.CapabilityImplementations{Docker: config.DockerImplementationRootless},
		Catalog:                   config.Catalog{Capabilities: []string{"docker", "gpu"}},
		PodSecurity:               config.PodSecurity{Profile: config.PodSecurityRestricted},
	}
	if got := catalogCapabilities(cfg); !reflect.DeepEqual(got, []string{"gpu"}) {
		t.Fatalf("expected [gpu], got %v", got)
	}
}
