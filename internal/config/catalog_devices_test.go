package config

import (
	"reflect"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

// The shape `toYaml` renders from chart values: keys sorted, list items
// unindented. qa-android is the flavor the Android QA agent runs on.
const deviceCatalog = `flavors:
- name: ram-2gb
  resources:
    limitsCpu: "2"
    limitsMemory: 2Gi
    requestsCpu: 500m
    requestsMemory: 512Mi
- devices:
  - count: 1
    resource: squat.ai/kvm
  name: qa-android
  resources:
    limitsCpu: "2"
    limitsMemory: 6Gi
    requestsCpu: "1"
    requestsMemory: 4Gi
  supplementalGroups:
  - 994
`

func TestLoadCatalogReadsDevicesAndSupplementalGroups(t *testing.T) {
	catalog, err := LoadCatalog(writeCatalog(t, deviceCatalog))
	if err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	flavor, ok := catalog.FlavorFor("qa-android")
	if !ok {
		t.Fatal("qa-android did not resolve")
	}
	if want := []DeviceRequest{{Resource: "squat.ai/kvm", Count: 1}}; !reflect.DeepEqual(flavor.Devices, want) {
		t.Fatalf("devices = %+v, want %+v", flavor.Devices, want)
	}
	if want := (SupplementalGroups{994}); !reflect.DeepEqual(flavor.SupplementalGroups, want) {
		t.Fatalf("supplementalGroups = %v, want %v", flavor.SupplementalGroups, want)
	}
	devices, err := flavor.DeviceResources()
	if err != nil {
		t.Fatal(err)
	}
	if kvm, ok := devices["squat.ai/kvm"]; len(devices) != 1 || !ok || kvm.Cmp(resource.MustParse("1")) != 0 {
		t.Fatalf("device resources = %v, want squat.ai/kvm=1", devices)
	}
	if got := flavor.DeviceSummary(); !reflect.DeepEqual(got, []string{"squat.ai/kvm=1", "group=994"}) {
		t.Fatalf("summary = %v", got)
	}

	// A flavor that declares neither stays exactly as it was.
	plain, _ := catalog.FlavorFor("ram-2gb")
	if plain.Devices != nil || plain.SupplementalGroups != nil || len(plain.DeviceSummary()) != 0 {
		t.Fatalf("plain flavor gained devices: %+v", plain)
	}
	if devices, err := plain.DeviceResources(); devices != nil || err != nil {
		t.Fatalf("plain flavor device resources = %v, %v", devices, err)
	}
}

func TestLoadCatalogRejectsInvalidDevices(t *testing.T) {
	for name, devices := range map[string]string{
		"cpu":                  `[{resource: cpu, count: 1}]`,
		"memory":               `[{resource: memory, count: 1}]`,
		"ephemeral-storage":    `[{resource: ephemeral-storage, count: 1}]`,
		"hugepages":            `[{resource: hugepages-2Mi, count: 1}]`,
		"no domain":            `[{resource: kvm, count: 1}]`,
		"kubernetes.io":        `[{resource: kubernetes.io/kvm, count: 1}]`,
		"kubernetes.io suffix": `[{resource: devices.kubernetes.io/kvm, count: 1}]`,
		"quota prefix":         `[{resource: requests.squat.ai/kvm, count: 1}]`,
		"upper-case domain":    `[{resource: Squat.ai/kvm, count: 1}]`,
		"empty name":           `[{resource: squat.ai/, count: 1}]`,
		"two slashes":          `[{resource: squat.ai/kvm/0, count: 1}]`,
		"empty resource":       `[{resource: "", count: 1}]`,
		"numeric resource":     `[{resource: 1, count: 1}]`,
		"zero count":           `[{resource: squat.ai/kvm, count: 0}]`,
		"negative count":       `[{resource: squat.ai/kvm, count: -1}]`,
		"large count":          `[{resource: squat.ai/kvm, count: 9}]`,
		"fractional count":     `[{resource: squat.ai/kvm, count: 1.5}]`,
		"quoted count":         `[{resource: squat.ai/kvm, count: "1"}]`,
		"hex count":            `[{resource: squat.ai/kvm, count: 0x1}]`,
		"missing count":        `[{resource: squat.ai/kvm}]`,
		"missing resource":     `[{count: 1}]`,
		"unknown field":        `[{name: squat.ai/kvm, count: 1}]`,
		"repeated field":       `[{resource: squat.ai/kvm, resource: squat.ai/tun, count: 1}]`,
		"scalar entry":         `[squat.ai/kvm]`,
		"repeated resource":    `[{resource: squat.ai/kvm, count: 1}, {resource: squat.ai/kvm, count: 1}]`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := LoadCatalog(writeCatalog(t, `
flavors:
  - name: qa-android
    resources: {requestsCpu: "1", requestsMemory: 4Gi, limitsCpu: "2", limitsMemory: 6Gi}
    devices: `+devices+`
`))
			if err == nil {
				t.Fatalf("devices %s accepted", devices)
			}
			if !strings.Contains(err.Error(), "device") {
				t.Fatalf("error does not name the device: %v", err)
			}
		})
	}
}

func TestLoadCatalogRejectsInvalidSupplementalGroups(t *testing.T) {
	for name, groups := range map[string]string{
		"root":       `[0]`,
		"negative":   `[-994]`,
		"too large":  `[2147483648]`,
		"fractional": `[994.5]`,
		"quoted":     `["994"]`,
		"octal":      `[0o1742]`,
		"duplicate":  `[994, 994]`,
		"scalar":     `994`,
		"mapping":    `{kvm: 994}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := LoadCatalog(writeCatalog(t, `
flavors:
  - name: qa-android
    resources: {requestsCpu: "1", requestsMemory: 4Gi, limitsCpu: "2", limitsMemory: 6Gi}
    supplementalGroups: `+groups+`
`))
			if err == nil {
				t.Fatalf("supplementalGroups %s accepted", groups)
			}
			if !strings.Contains(err.Error(), "group") {
				t.Fatalf("error does not name the group: %v", err)
			}
		})
	}
}

// Entries built in code skip LoadCatalog, so the same checks run where the
// server resolves a flavor.
func TestDeviceChecksApplyOutsideLoadCatalog(t *testing.T) {
	flavor := FlavorEntry{Name: "qa-android", Devices: []DeviceRequest{{Resource: "memory", Count: 1}}}
	if _, err := flavor.DeviceResources(); err == nil {
		t.Fatal("native resource accepted as a device")
	}
	flavor = FlavorEntry{Name: "qa-android", SupplementalGroups: SupplementalGroups{0}}
	if err := flavor.ValidGroups(); err == nil {
		t.Fatal("group 0 accepted")
	}
}
