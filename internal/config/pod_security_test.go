package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestLoadPodSecurityDefaultsToRestrictedWithEmptyAllowlist(t *testing.T) {
	setBaseEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := PodSecurity{Profile: PodSecurityRestricted, RunAsUser: 10001, RunAsGroup: 10001, FSGroup: 10001}
	if !reflect.DeepEqual(cfg.PodSecurity, want) {
		t.Fatalf("default pod security = %#v, want %#v", cfg.PodSecurity, want)
	}
	if !cfg.PodSecurity.Restricted() || cfg.PodSecurity.AllowsCapability("NET_ADMIN") {
		t.Fatal("default profile must be restricted with no allowed capability")
	}
}

func TestLoadPodSecurityExplicitValues(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("WORKLOAD_POD_SECURITY", " Restricted ")
	t.Setenv("WORKLOAD_RUN_AS_USER", "20001")
	t.Setenv("WORKLOAD_RUN_AS_GROUP", "20002")
	t.Setenv("WORKLOAD_FS_GROUP", "20003")
	t.Setenv("WORKLOAD_ALLOWED_CAPABILITIES", " net_bind_service, CAP_NET_BIND_SERVICE ")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := PodSecurity{Profile: PodSecurityRestricted, RunAsUser: 20001, RunAsGroup: 20002, FSGroup: 20003, AllowedCapabilities: []string{"NET_BIND_SERVICE"}}
	if !reflect.DeepEqual(cfg.PodSecurity, want) {
		t.Fatalf("pod security = %#v, want %#v", cfg.PodSecurity, want)
	}
}

func TestLoadPodSecurityLegacyProfileNeedsExplicitCapabilities(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("WORKLOAD_POD_SECURITY", "none")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PodSecurity.Restricted() || cfg.PodSecurity.AllowsCapability("NET_ADMIN") {
		t.Fatal("legacy profile must not allow NET_ADMIN unless listed")
	}
	t.Setenv("WORKLOAD_ALLOWED_CAPABILITIES", "NET_ADMIN")
	if cfg, err = Load(); err != nil || !cfg.PodSecurity.AllowsCapability("NET_ADMIN") {
		t.Fatalf("legacy allowlist not honoured: %v %#v", err, cfg.PodSecurity)
	}
}

func TestLoadPodSecurityRejectsUnsafeValues(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"unknown profile", map[string]string{"WORKLOAD_POD_SECURITY": "baseline"}, "WORKLOAD_POD_SECURITY"},
		{"root user", map[string]string{"WORKLOAD_RUN_AS_USER": "0"}, "WORKLOAD_RUN_AS_USER"},
		{"root group", map[string]string{"WORKLOAD_RUN_AS_GROUP": "0"}, "WORKLOAD_RUN_AS_GROUP"},
		{"root fs group", map[string]string{"WORKLOAD_FS_GROUP": "0"}, "WORKLOAD_FS_GROUP"},
		{"negative user", map[string]string{"WORKLOAD_RUN_AS_USER": "-1"}, "WORKLOAD_RUN_AS_USER"},
		{"too large user", map[string]string{"WORKLOAD_RUN_AS_USER": "2147483648"}, "WORKLOAD_RUN_AS_USER"},
		{"net admin restricted", map[string]string{"WORKLOAD_ALLOWED_CAPABILITIES": "NET_ADMIN"}, "NET_ADMIN"},
		{"sys admin restricted", map[string]string{"WORKLOAD_ALLOWED_CAPABILITIES": "NET_BIND_SERVICE,SYS_ADMIN"}, "SYS_ADMIN"},
		{"all restricted", map[string]string{"WORKLOAD_ALLOWED_CAPABILITIES": "ALL"}, "ALL"},
		{"all legacy", map[string]string{"WORKLOAD_POD_SECURITY": "none", "WORKLOAD_ALLOWED_CAPABILITIES": "ALL"}, "ALL"},
		{"malformed", map[string]string{"WORKLOAD_POD_SECURITY": "none", "WORKLOAD_ALLOWED_CAPABILITIES": "NET ADMIN"}, "NET ADMIN"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setBaseEnv(t)
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error = %v, want mention of %s", err, tc.want)
			}
		})
	}
}

func TestDockerUnavailableUnderRestrictedProfile(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("CAPABILITY_IMPLEMENTATIONS", `{"docker":"rootless"}`)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("a configured docker implementation must not stop the runner: %v", err)
	}
	if cfg.DockerAvailable() {
		t.Fatal("restricted profile must not offer docker")
	}
	t.Setenv("WORKLOAD_POD_SECURITY", "none")
	if cfg, err = Load(); err != nil || !cfg.DockerAvailable() {
		t.Fatalf("legacy profile keeps docker: %v", err)
	}
}
