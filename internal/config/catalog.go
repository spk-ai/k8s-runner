package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Catalog is what this runner offers: the compute sizes, storage tiers and
// capabilities it can honour. It is declared here rather than in the platform
// because every entry needs a runner-side implementation anyway — a flavor is
// only real if this runner can allocate those resources, and a storage class is
// only real if it maps to a StorageClass in this cluster.
type Catalog struct {
	Flavors        []FlavorEntry       `yaml:"flavors"`
	StorageClasses []StorageClassEntry `yaml:"storageClasses"`
	Capabilities   []string            `yaml:"capabilities"`
}

type FlavorEntry struct {
	Name       string `yaml:"name"`
	Default    bool   `yaml:"default"`
	Deprecated bool   `yaml:"deprecated"`
	// Resources size the workload's main container, SidecarResources each of
	// its sidecars. One name covers both so a workload asks for a size, not a
	// per-container budget. Unset SidecarResources leaves sidecars unsized.
	Resources        ComputeResources `yaml:"resources"`
	SidecarResources ComputeResources `yaml:"sidecarResources"`
	// Devices are extended resources a device plugin advertises on the node
	// (squat.ai/kvm), requested by the main container alone with equal
	// requests and limits. A device is not a size: explicit container bounds
	// replace the flavor's cpu and memory, never its devices.
	Devices []DeviceRequest `yaml:"devices"`
	// SupplementalGroups are added to the Pod security context under every
	// profile. They are how a non-root workload opens a device node the plugin
	// exposes with its host group (the kvm GID), so the workload needs no
	// capability, privilege or root user for it.
	SupplementalGroups SupplementalGroups `yaml:"supplementalGroups"`
}

// MaxDeviceCount bounds one device request. A flavor that asks for many
// instances strands node capacity rather than sizing a workload.
const MaxDeviceCount = 8

// maxGroupID is the largest Linux group ID Kubernetes accepts.
const maxGroupID = 1<<31 - 1

// DeviceRequest is count instances of one extended resource.
type DeviceRequest struct {
	Resource string `yaml:"resource"`
	Count    int64  `yaml:"count"`
}

// UnmarshalYAML accepts exactly {resource, count}. A misspelt key would
// otherwise leave the request half-declared and silently unsatisfied.
func (d *DeviceRequest) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: a device is a mapping of resource and count", node.Line)
	}
	seen := map[string]bool{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		if seen[key.Value] {
			return fmt.Errorf("line %d: device field %q is declared more than once", key.Line, key.Value)
		}
		seen[key.Value] = true
		switch key.Value {
		case "resource":
			if value.Kind != yaml.ScalarNode || value.ShortTag() != "!!str" {
				return fmt.Errorf("line %d: device resource must be a string", value.Line)
			}
			d.Resource = value.Value
		case "count":
			count, err := plainInteger(value, "device count")
			if err != nil {
				return err
			}
			d.Count = count
		default:
			return fmt.Errorf("line %d: unknown device field %q (want resource and count)", key.Line, key.Value)
		}
	}
	if !seen["resource"] || !seen["count"] {
		return fmt.Errorf("line %d: a device needs both resource and count", node.Line)
	}
	return nil
}

// SupplementalGroups is a flavor's list of numeric group IDs.
type SupplementalGroups []int64

func (g *SupplementalGroups) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.SequenceNode {
		return fmt.Errorf("line %d: supplementalGroups is a list of group IDs", node.Line)
	}
	groups := make(SupplementalGroups, 0, len(node.Content))
	for _, item := range node.Content {
		gid, err := plainInteger(item, "supplemental group")
		if err != nil {
			return err
		}
		groups = append(groups, gid)
	}
	*g = groups
	return nil
}

var plainDecimal = regexp.MustCompile(`^-?(0|[1-9][0-9]{0,18})$`)

// plainInteger reads only a plain decimal integer. yaml.v3 would otherwise
// truncate 994.5 to 994 and read 0o17 or 0x10 as numbers nobody wrote, which
// for a group ID grants a different group than the one reviewed.
func plainInteger(node *yaml.Node, what string) (int64, error) {
	if node.Kind != yaml.ScalarNode || node.ShortTag() != "!!int" || !plainDecimal.MatchString(node.Value) {
		return 0, fmt.Errorf("line %d: %s must be a plain decimal integer, got %q", node.Line, what, node.Value)
	}
	value, err := strconv.ParseInt(node.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("line %d: %s %q: %w", node.Line, what, node.Value, err)
	}
	return value, nil
}

// DeviceResources validates the flavor's devices and returns them as one
// list, used as both the requests and the limits of the main container. No
// devices yields nil.
func (f FlavorEntry) DeviceResources() (corev1.ResourceList, error) {
	if len(f.Devices) == 0 {
		return nil, nil
	}
	devices := corev1.ResourceList{}
	for _, device := range f.Devices {
		if err := validateDeviceResource(device.Resource); err != nil {
			return nil, err
		}
		name := corev1.ResourceName(device.Resource)
		if _, duplicate := devices[name]; duplicate {
			return nil, fmt.Errorf("device %s is declared more than once", device.Resource)
		}
		if device.Count < 1 || device.Count > MaxDeviceCount {
			return nil, fmt.Errorf("device %s count must be from 1 to %d, got %d", device.Resource, MaxDeviceCount, device.Count)
		}
		devices[name] = *resource.NewQuantity(device.Count, resource.DecimalSI)
	}
	return devices, nil
}

// ValidGroups checks the flavor's supplemental groups: real, non-root,
// distinct group IDs.
func (f FlavorEntry) ValidGroups() error {
	seen := map[int64]bool{}
	for _, gid := range f.SupplementalGroups {
		if gid < 1 || gid > maxGroupID {
			return fmt.Errorf("supplemental group %d must be from 1 to %d", gid, maxGroupID)
		}
		if seen[gid] {
			return fmt.Errorf("supplemental group %d is declared more than once", gid)
		}
		seen[gid] = true
	}
	return nil
}

// DeviceSummary renders the devices and groups for a log line, for example
// ["squat.ai/kvm=1", "group=994"].
func (f FlavorEntry) DeviceSummary() []string {
	summary := make([]string, 0, len(f.Devices)+len(f.SupplementalGroups))
	for _, device := range f.Devices {
		summary = append(summary, fmt.Sprintf("%s=%d", device.Resource, device.Count))
	}
	for _, gid := range f.SupplementalGroups {
		summary = append(summary, fmt.Sprintf("group=%d", gid))
	}
	return summary
}

// validateDeviceResource accepts what Kubernetes calls an extended resource,
// the only kind a device plugin can advertise: a domain-qualified name outside
// kubernetes.io whose quota form (requests.<name>) is also a qualified name.
// Native resources are refused by name first, for a clearer message: cpu and
// memory are the flavor's size, and ephemeral-storage and hugepages are not
// devices.
func validateDeviceResource(name string) error {
	switch {
	case name == string(corev1.ResourceCPU), name == string(corev1.ResourceMemory),
		name == string(corev1.ResourceEphemeralStorage), strings.HasPrefix(name, corev1.ResourceHugePagesPrefix):
		return fmt.Errorf("device %q is a native resource, not a device", name)
	case !strings.Contains(name, "/"):
		return fmt.Errorf("device %q must be domain/name, like squat.ai/kvm", name)
	case strings.Contains(name, corev1.ResourceDefaultNamespacePrefix):
		return fmt.Errorf("device %q is in the reserved kubernetes.io domain", name)
	case strings.HasPrefix(name, corev1.DefaultResourceRequestsPrefix):
		return fmt.Errorf("device %q must not carry the quota prefix %s", name, corev1.DefaultResourceRequestsPrefix)
	}
	if errs := validation.IsQualifiedName(corev1.DefaultResourceRequestsPrefix + name); len(errs) > 0 {
		return fmt.Errorf("device %q is not a valid extended resource name: %s", name, strings.Join(errs, "; "))
	}
	return nil
}

type ComputeResources struct {
	RequestsCPU    string `yaml:"requestsCpu" json:"requestsCpu"`
	RequestsMemory string `yaml:"requestsMemory" json:"requestsMemory"`
	LimitsCPU      string `yaml:"limitsCpu" json:"limitsCpu"`
	LimitsMemory   string `yaml:"limitsMemory" json:"limitsMemory"`
}

// IsZero reports whether nothing at all was declared.
func (r ComputeResources) IsZero() bool {
	for _, value := range r.fields() {
		if strings.TrimSpace(value) != "" {
			return false
		}
	}
	return true
}

func (r ComputeResources) fields() map[string]string {
	return map[string]string{
		"requestsCpu":    r.RequestsCPU,
		"requestsMemory": r.RequestsMemory,
		"limitsCpu":      r.LimitsCPU,
		"limitsMemory":   r.LimitsMemory,
	}
}

type StorageClassEntry struct {
	Name string `yaml:"name"`
	// StorageClassName is the Kubernetes StorageClass this entry maps to. Empty
	// means the cluster default, which is what an unset class has always used.
	StorageClassName string `yaml:"storageClassName"`
	Default          bool   `yaml:"default"`
	Deprecated       bool   `yaml:"deprecated"`
}

// LoadCatalog reads the catalog from path. A missing path is not an error: a
// runner with no declared catalog reports nothing and simply offers no named
// entries.
func LoadCatalog(path string) (Catalog, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return Catalog{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Catalog{}, nil
		}
		return Catalog{}, fmt.Errorf("read catalog %s: %w", path, err)
	}
	var catalog Catalog
	if err := yaml.Unmarshal(data, &catalog); err != nil {
		return Catalog{}, fmt.Errorf("parse catalog %s: %w", path, err)
	}
	if err := catalog.validate(); err != nil {
		return Catalog{}, fmt.Errorf("catalog %s: %w", path, err)
	}
	return catalog, nil
}

// validate catches locally what the platform would reject anyway, so a bad
// configuration fails at startup with a clear message instead of as a rejected
// report.
func (c Catalog) validate() error {
	flavorNames := map[string]struct{}{}
	defaults := 0
	for _, flavor := range c.Flavors {
		name := strings.TrimSpace(flavor.Name)
		if name == "" {
			return fmt.Errorf("flavor name is empty")
		}
		if _, exists := flavorNames[name]; exists {
			return fmt.Errorf("flavor %q is declared more than once", name)
		}
		flavorNames[name] = struct{}{}
		if flavor.Default {
			defaults++
		}
		for field, value := range flavor.Resources.fields() {
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("flavor %q: %s is empty", name, field)
			}
		}
		// Sidecar sizing is opt-in, but half of it is a typo rather than a
		// choice: a flavor that sets two of the four would silently size
		// sidecars by a budget nobody wrote.
		if !flavor.SidecarResources.IsZero() {
			for field, value := range flavor.SidecarResources.fields() {
				if strings.TrimSpace(value) == "" {
					return fmt.Errorf("flavor %q: sidecarResources.%s is empty", name, field)
				}
			}
		}
		if _, err := flavor.DeviceResources(); err != nil {
			return fmt.Errorf("flavor %q: %w", name, err)
		}
		if err := flavor.ValidGroups(); err != nil {
			return fmt.Errorf("flavor %q: %w", name, err)
		}
	}
	if defaults > 1 {
		return fmt.Errorf("at most one flavor may be default, got %d", defaults)
	}

	classNames := map[string]struct{}{}
	classDefaults := 0
	for _, class := range c.StorageClasses {
		name := strings.TrimSpace(class.Name)
		if name == "" {
			return fmt.Errorf("storage class name is empty")
		}
		if _, exists := classNames[name]; exists {
			return fmt.Errorf("storage class %q is declared more than once", name)
		}
		classNames[name] = struct{}{}
		if class.Default {
			classDefaults++
		}
	}
	if classDefaults > 1 {
		return fmt.Errorf("at most one storage class may be default, got %d", classDefaults)
	}
	return nil
}

// FlavorFor maps a catalog entry name to the sizes backing it. An unknown name
// resolves to nothing, which is what makes an unresolvable reference fail the
// start rather than silently land unsized.
func (c Catalog) FlavorFor(name string) (FlavorEntry, bool) {
	for _, flavor := range c.Flavors {
		if flavor.Name == name {
			return flavor, true
		}
	}
	return FlavorEntry{}, false
}

// StorageClassNameFor maps a catalog entry name to the Kubernetes
// StorageClass backing it. An unknown name resolves to nothing, which is what
// makes an unresolvable reference fail scheduling rather than silently land on
// the cluster default.
func (c Catalog) StorageClassNameFor(name string) (string, bool) {
	for _, class := range c.StorageClasses {
		if class.Name == name {
			return class.StorageClassName, true
		}
	}
	return "", false
}
