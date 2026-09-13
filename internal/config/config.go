package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	defaultGRPCAddr              = ":50051"
	defaultGatewayAddress        = "gateway:8080"
	defaultZitiEnrollmentTimeout = 2 * time.Minute
	defaultStorageSize           = "10Gi"
	defaultLogLevel              = "info"
)

// Config captures runtime configuration derived from the environment.
type Config struct {
	GRPCAddr                     string
	Namespace                    string
	ZitiEnabled                  bool
	ServiceToken                 string
	GatewayAddress               string
	ZitiEnrollmentTimeout        time.Duration
	StorageClass                 *string
	StorageSize                  string
	LogLevel                     string
	CapabilityImplementations    CapabilityImplementations
	SupportingContainerResources *ComputeResources
	// Catalog is what this runner reports it offers. Declared in the runner's
	// own configuration, since every entry needs an implementation here.
	Catalog Catalog
}

// CapabilityDocker is the capability name a workload asks for and the runner
// reports. Named here rather than in the server package so the startup report
// and the request handling cannot drift apart.
const CapabilityDocker = "docker"

type DockerImplementation string

const (
	DockerImplementationRootless   DockerImplementation = "rootless"
	DockerImplementationPrivileged DockerImplementation = "privileged"
	DockerImplementationKataQemu   DockerImplementation = "kata-qemu"
	DockerImplementationKataFc     DockerImplementation = "kata-fc"
)

type CapabilityImplementations struct {
	Docker DockerImplementation
}

// Load reads configuration from environment variables, applying defaults when
// values are not provided. Returns an error when supplied values are invalid.
func Load() (Config, error) {
	var cfg Config

	cfg.GRPCAddr = readEnv("GRPC_ADDR", defaultGRPCAddr)
	cfg.Namespace = strings.TrimSpace(os.Getenv("KUBE_NAMESPACE"))
	if cfg.Namespace == "" {
		return Config{}, fmt.Errorf("KUBE_NAMESPACE is required")
	}

	var err error
	cfg.ZitiEnabled, err = readBool("ZITI_ENABLED", false)
	if err != nil {
		return Config{}, err
	}

	if cfg.ZitiEnabled {
		cfg.GatewayAddress = readEnv("GATEWAY_ADDRESS", defaultGatewayAddress)

		serviceToken := strings.TrimSpace(os.Getenv("SERVICE_TOKEN"))
		if serviceToken == "" {
			return Config{}, fmt.Errorf("SERVICE_TOKEN is required when ZITI_ENABLED is true")
		}
		cfg.ServiceToken = serviceToken

		enrollmentTimeout, err := readDuration("ZITI_ENROLLMENT_TIMEOUT", defaultZitiEnrollmentTimeout)
		if err != nil {
			return Config{}, err
		}
		if enrollmentTimeout <= 0 {
			return Config{}, fmt.Errorf("ZITI_ENROLLMENT_TIMEOUT must be greater than 0")
		}
		cfg.ZitiEnrollmentTimeout = enrollmentTimeout
	} else {
		cfg.ZitiEnrollmentTimeout = defaultZitiEnrollmentTimeout
	}

	catalog, err := LoadCatalog(os.Getenv("CATALOG_PATH"))
	if err != nil {
		return Config{}, err
	}
	cfg.Catalog = catalog
	if value := strings.TrimSpace(os.Getenv("SUPPORTING_CONTAINER_RESOURCES")); value != "" {
		cfg.SupportingContainerResources, err = parseSupportingResources(value)
		if err != nil {
			return Config{}, err
		}
	}

	storageClass := strings.TrimSpace(os.Getenv("PVC_STORAGE_CLASS"))
	if storageClass != "" {
		cfg.StorageClass = &storageClass
	}

	cfg.StorageSize = readEnv("PVC_STORAGE_SIZE", defaultStorageSize)
	if _, err := resource.ParseQuantity(cfg.StorageSize); err != nil {
		return Config{}, fmt.Errorf("invalid PVC_STORAGE_SIZE: %w", err)
	}

	cfg.LogLevel = normalizeLogLevel(readEnv("LOG_LEVEL", defaultLogLevel))

	capabilityConfig := strings.TrimSpace(os.Getenv("CAPABILITY_IMPLEMENTATIONS"))
	if capabilityConfig != "" {
		implementations, err := parseCapabilityImplementations(capabilityConfig)
		if err != nil {
			return Config{}, err
		}
		cfg.CapabilityImplementations = implementations
	}

	return cfg, nil
}

func readEnv(key, def string) string {
	if value, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(value)
	}
	return def
}

func readBool(key string, def bool) (bool, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false, fmt.Errorf("%s must be true or false", key)
	}
	parsed, err := strconv.ParseBool(trimmed)
	if err != nil {
		return false, fmt.Errorf("invalid %s: %w", key, err)
	}
	return parsed, nil
}

func readDuration(key string, def time.Duration) (time.Duration, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("%s must be a duration", key)
	}
	parsed, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return parsed, nil
}

func normalizeLogLevel(level string) string {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "", "info":
		return "info"
	case "debug":
		return "debug"
	case "warn", "warning":
		return "warn"
	case "error":
		return "error"
	default:
		return "info"
	}
}

func parseCapabilityImplementations(raw string) (CapabilityImplementations, error) {
	if raw == "" {
		return CapabilityImplementations{}, nil
	}
	var parsed map[string]string
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return CapabilityImplementations{}, fmt.Errorf("invalid CAPABILITY_IMPLEMENTATIONS: %w", err)
	}
	if len(parsed) == 0 {
		return CapabilityImplementations{}, nil
	}
	var implementations CapabilityImplementations
	for key, value := range parsed {
		capability := strings.TrimSpace(key)
		if capability == "" {
			return CapabilityImplementations{}, fmt.Errorf("invalid CAPABILITY_IMPLEMENTATIONS key")
		}
		implementation := strings.ToLower(strings.TrimSpace(value))
		switch capability {
		case "docker":
			switch DockerImplementation(implementation) {
			case DockerImplementationRootless:
				implementations.Docker = DockerImplementationRootless
			case DockerImplementationPrivileged:
				implementations.Docker = DockerImplementationPrivileged
			case DockerImplementationKataQemu:
				implementations.Docker = DockerImplementationKataQemu
			case DockerImplementationKataFc:
				implementations.Docker = DockerImplementationKataFc
			default:
				return CapabilityImplementations{}, fmt.Errorf("invalid docker capability implementation %q", value)
			}
		default:
			return CapabilityImplementations{}, fmt.Errorf("unknown capability implementation %q", capability)
		}
	}
	return implementations, nil
}
