package main

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/agynio/k8s-runner/internal/config"
)

// runners.v1.FlavorEntry has no field for devices, so a device flavor is
// reported by its name and size alone; its devices are logged at startup and
// set on its Pods. The report a platform stores is the same as without them.
func TestCatalogReportCarriesDeviceFlavorBySize(t *testing.T) {
	sized := config.FlavorEntry{Name: "qa-android", Resources: config.ComputeResources{
		RequestsCPU: "1", RequestsMemory: "4Gi", LimitsCPU: "2", LimitsMemory: "6Gi"}}
	withDevices := sized
	withDevices.Devices = []config.DeviceRequest{{Resource: "squat.ai/kvm", Count: 1}}
	withDevices.SupplementalGroups = config.SupplementalGroups{994}

	plain := catalogReport(config.Config{Catalog: config.Catalog{Flavors: []config.FlavorEntry{sized}}})
	device := catalogReport(config.Config{Catalog: config.Catalog{Flavors: []config.FlavorEntry{withDevices}}})
	if !proto.Equal(plain, device) {
		t.Fatalf("devices changed the catalog report:\n%v\n%v", plain, device)
	}
	if got := device.GetFlavors()[0]; got.GetName() != "qa-android" || got.GetResources().GetLimitsMemory() != "6Gi" {
		t.Fatalf("device flavor reported as %v", got)
	}
}
