package main

import (
	"slices"
	"testing"

	"github.com/agynio/k8s-runner/internal/config"
)

func TestComputeCapabilityRequiresValidSupportingDefaults(t *testing.T) {
	cfg := config.Config{Catalog: config.Catalog{Capabilities: []string{config.CapabilityComputeResources}}}
	if slices.Contains(catalogCapabilities(cfg), config.CapabilityComputeResources) {
		t.Fatal("advertised unconfigured capability")
	}
	cfg.SupportingContainerResources = &config.ComputeResources{}
	if slices.Contains(catalogCapabilities(cfg), config.CapabilityComputeResources) {
		t.Fatal("advertised invalid defaults")
	}
	cfg.SupportingContainerResources = &config.ComputeResources{RequestsCPU: "100m", RequestsMemory: "64Mi", LimitsCPU: "500m", LimitsMemory: "256Mi"}
	cfg.Catalog.Capabilities = nil
	if !slices.Contains(catalogCapabilities(cfg), config.CapabilityComputeResources) {
		t.Fatal("configured capability was not advertised")
	}
}
