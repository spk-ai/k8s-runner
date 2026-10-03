package config

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// PodSecurityProfile names the server-owned security shape of every workload
// Pod. Callers cannot choose or loosen it: the RunnerService API carries no
// security context, only container capability requests, which are checked
// against WORKLOAD_ALLOWED_CAPABILITIES.
type PodSecurityProfile string

const (
	// PodSecurityRestricted builds Pods that the Kubernetes "restricted" Pod
	// Security Standard admits: non-root, every capability dropped, no
	// privilege escalation and the runtime's default seccomp profile.
	PodSecurityRestricted PodSecurityProfile = "restricted"
	// PodSecurityNone keeps the upstream Pod shape (no security fields set).
	// It exists for installations still running the NET_ADMIN tproxy sidecar,
	// which must then list NET_ADMIN in WORKLOAD_ALLOWED_CAPABILITIES.
	PodSecurityNone PodSecurityProfile = "none"

	// DefaultWorkloadUID is the non-root user, group and fsGroup of a
	// restricted workload. It matches the QA tool image's own user.
	DefaultWorkloadUID int64 = 10001
	maxWorkloadID      int64 = 1<<31 - 1

	// CapabilityNetBindService is the only capability the restricted standard
	// lets a container add.
	CapabilityNetBindService = "NET_BIND_SERVICE"
)

// PodSecurity is the operator policy applied to every workload Pod.
type PodSecurity struct {
	Profile    PodSecurityProfile
	RunAsUser  int64
	RunAsGroup int64
	FSGroup    int64
	// AllowedCapabilities is the complete set of Linux capabilities a
	// ContainerSpec may request, normalized (upper case, no CAP_ prefix).
	// Empty by default, so any request is rejected.
	AllowedCapabilities []string
}

// Restricted reports whether the restricted profile is in force. The zero
// value is the legacy shape, so code constructing a Server directly keeps the
// upstream behaviour unless it opts in.
func (p PodSecurity) Restricted() bool { return p.Profile == PodSecurityRestricted }

// AllowsCapability reports whether a normalized capability is allowlisted.
func (p PodSecurity) AllowsCapability(capability string) bool {
	for _, allowed := range p.AllowedCapabilities {
		if allowed == capability {
			return true
		}
	}
	return false
}

var capabilityName = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

// NormalizeCapability maps "cap_net_admin", "NET_ADMIN" and " net_admin " to
// the Kubernetes spelling "NET_ADMIN".
func NormalizeCapability(raw string) string {
	name := strings.ToUpper(strings.TrimSpace(raw))
	return strings.TrimPrefix(name, "CAP_")
}

func loadPodSecurity() (PodSecurity, error) {
	policy := PodSecurity{Profile: PodSecurityRestricted}
	if raw, ok := os.LookupEnv("WORKLOAD_POD_SECURITY"); ok {
		switch PodSecurityProfile(strings.ToLower(strings.TrimSpace(raw))) {
		case PodSecurityRestricted:
		case PodSecurityNone:
			policy.Profile = PodSecurityNone
		default:
			return PodSecurity{}, fmt.Errorf("WORKLOAD_POD_SECURITY must be restricted or none")
		}
	}
	var err error
	if policy.RunAsUser, err = readWorkloadID("WORKLOAD_RUN_AS_USER"); err != nil {
		return PodSecurity{}, err
	}
	if policy.RunAsGroup, err = readWorkloadID("WORKLOAD_RUN_AS_GROUP"); err != nil {
		return PodSecurity{}, err
	}
	if policy.FSGroup, err = readWorkloadID("WORKLOAD_FS_GROUP"); err != nil {
		return PodSecurity{}, err
	}
	policy.AllowedCapabilities, err = parseAllowedCapabilities(os.Getenv("WORKLOAD_ALLOWED_CAPABILITIES"), policy.Profile)
	if err != nil {
		return PodSecurity{}, err
	}
	return policy, nil
}

// readWorkloadID never accepts 0: root is not a configurable workload user.
func readWorkloadID(key string) (int64, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return DefaultWorkloadUID, nil
	}
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value < 1 || value > maxWorkloadID {
		return 0, fmt.Errorf("%s must be an integer from 1 to %d", key, maxWorkloadID)
	}
	return value, nil
}

func parseAllowedCapabilities(raw string, profile PodSecurityProfile) ([]string, error) {
	seen := map[string]struct{}{}
	var allowed []string
	for _, part := range strings.Split(raw, ",") {
		if strings.TrimSpace(part) == "" {
			continue
		}
		name := NormalizeCapability(part)
		if !capabilityName.MatchString(name) || name == "ALL" {
			return nil, fmt.Errorf("WORKLOAD_ALLOWED_CAPABILITIES contains invalid capability %q", strings.TrimSpace(part))
		}
		if profile == PodSecurityRestricted && name != CapabilityNetBindService {
			return nil, fmt.Errorf("WORKLOAD_ALLOWED_CAPABILITIES may only contain %s under WORKLOAD_POD_SECURITY=restricted, got %s", CapabilityNetBindService, name)
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		allowed = append(allowed, name)
	}
	sort.Strings(allowed)
	return allowed, nil
}
