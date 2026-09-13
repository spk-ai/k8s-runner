package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
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
