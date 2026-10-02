// SPDX-License-Identifier: AGPL-3.0-only
package chart

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
)

func renderRBAC(t *testing.T, values map[string]any) ([]byte, error) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Fatal("Helm is required for chart tests")
	}
	data, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "values.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "helm", "template", "rbac-test", "../../charts/k8s-runner", "--namespace", "runner-platform", "-f", path).CombinedOutput()
}

func TestWorkloadRBACScope(t *testing.T) {
	cases := []struct {
		name, namespace, account string
		values                   map[string]any
		clusterWide, disabled    bool
	}{
		{name: "default", namespace: "agyn-workloads", account: "rbac-test-k8s-runner"},
		{name: "separate-workloads", namespace: "isolated-tasks", account: "custom-runner", values: map[string]any{
			"workloadNamespace": "isolated-tasks", "serviceAccount": map[string]any{"name": "custom-runner"}}},
		{name: "same-namespace", namespace: "runner-platform", account: "rbac-test-k8s-runner", values: map[string]any{"workloadNamespace": "runner-platform"}},
		{name: "external-account", namespace: "agyn-workloads", account: "existing-runner", values: map[string]any{
			"serviceAccount": map[string]any{"create": false, "name": "existing-runner"}}},
		{name: "default-account", namespace: "agyn-workloads", account: "default", values: map[string]any{"serviceAccount": map[string]any{"create": false}}},
		{name: "cluster-opt-in", account: "rbac-test-k8s-runner", clusterWide: true, values: map[string]any{"rbac": map[string]any{"clusterWide": true}}},
		{name: "disabled", disabled: true, values: map[string]any{"rbac": map[string]any{"create": false}}},
		{name: "disabled-cluster", disabled: true, values: map[string]any{"rbac": map[string]any{"create": false, "clusterWide": true}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := renderRBAC(t, tc.values)
			if err != nil {
				t.Fatalf("render: %v\n%s", err, output)
			}
			decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(output), 4096)
			roles, bindings, deployments := 0, 0, 0
			for {
				var raw json.RawMessage
				if err := decoder.Decode(&raw); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				var meta struct {
					metav1.TypeMeta `json:",inline"`
					Metadata        metav1.ObjectMeta `json:"metadata"`
				}
				if err := json.Unmarshal(raw, &meta); err != nil {
					t.Fatal(err)
				}
				// Other, separately scoped chart grants are not workload RBAC.
				if meta.Metadata.Name != "rbac-test-k8s-runner" {
					continue
				}
				switch meta.Kind {
				case "Role", "ClusterRole":
					roles++
					var role rbacv1.Role
					if err := json.Unmarshal(raw, &role); err != nil {
						t.Fatal(err)
					}
					if (meta.Kind == "ClusterRole") != tc.clusterWide || role.Namespace != tc.namespace || len(role.Rules) != 7 {
						t.Fatalf("unexpected workload role scope: %+v", role)
					}
				case "RoleBinding", "ClusterRoleBinding":
					bindings++
					var binding rbacv1.RoleBinding
					if err := json.Unmarshal(raw, &binding); err != nil {
						t.Fatal(err)
					}
					kind := "Role"
					if tc.clusterWide {
						kind = "ClusterRole"
					}
					if (meta.Kind == "ClusterRoleBinding") != tc.clusterWide || binding.Namespace != tc.namespace ||
						binding.RoleRef != (rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: kind, Name: "rbac-test-k8s-runner"}) ||
						!reflect.DeepEqual(binding.Subjects, []rbacv1.Subject{{Kind: "ServiceAccount", Name: tc.account, Namespace: "runner-platform"}}) {
						t.Fatalf("unexpected workload binding scope: %+v", binding)
					}
				case "ServiceAccount":
					var account corev1.ServiceAccount
					if err := json.Unmarshal(raw, &account); err != nil {
						t.Fatal(err)
					}
					if account.Namespace != "" && account.Namespace != "runner-platform" {
						t.Fatal("service account moved out of the release namespace")
					}
				case "Deployment":
					deployments++
					var deployment appsv1.Deployment
					if err := json.Unmarshal(raw, &deployment); err != nil {
						t.Fatal(err)
					}
					if deployment.Namespace != "" && deployment.Namespace != "runner-platform" {
						t.Fatal("runner moved out of the release namespace")
					}
					if !tc.disabled && deployment.Spec.Template.Spec.ServiceAccountName != tc.account {
						t.Fatal("deployment and workload binding select different service accounts")
					}
				}
			}
			want := 1
			if tc.disabled {
				want = 0
			}
			if roles != want || bindings != want || deployments != 1 {
				t.Fatalf("roles=%d bindings=%d deployments=%d", roles, bindings, deployments)
			}
		})
	}
}

func TestWorkloadRBACRejectsEmptyNamespace(t *testing.T) {
	output, err := renderRBAC(t, map[string]any{"workloadNamespace": ""})
	if err == nil || !strings.Contains(string(output), "workloadNamespace must be explicit") {
		t.Fatalf("empty workload namespace accepted: %v\n%s", err, output)
	}
}

// The volume backend identity is the workload Namespace UID. Its grant must stay
// a single named get: no list/watch/write, other namespaces or other resources.
func TestVolumeBackendRBACScope(t *testing.T) {
	cases := []struct {
		name, workloads, account string
		values                   map[string]any
		disabled                 bool
	}{
		{name: "default", workloads: "agyn-workloads", account: "rbac-test-k8s-runner"},
		{name: "separate-workloads", workloads: "isolated-tasks", account: "custom-runner", values: map[string]any{
			"workloadNamespace": "isolated-tasks", "serviceAccount": map[string]any{"name": "custom-runner"}}},
		{name: "external-account", workloads: "agyn-workloads", account: "existing-runner", values: map[string]any{
			"serviceAccount": map[string]any{"create": false, "name": "existing-runner"}}},
		{name: "cluster-opt-in", workloads: "agyn-workloads", account: "rbac-test-k8s-runner", values: map[string]any{
			"rbac": map[string]any{"clusterWide": true}}},
		{name: "disabled", disabled: true, values: map[string]any{"rbac": map[string]any{"create": false}}},
	}
	const name = "runner-platform-rbac-test-k8s-runner-volume-backend"
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := renderRBAC(t, tc.values)
			if err != nil {
				t.Fatalf("render: %v\n%s", err, output)
			}
			decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(output), 4096)
			roles, bindings := 0, 0
			for {
				var raw json.RawMessage
				if err := decoder.Decode(&raw); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				var meta struct {
					metav1.TypeMeta `json:",inline"`
					Metadata        metav1.ObjectMeta `json:"metadata"`
				}
				if err := json.Unmarshal(raw, &meta); err != nil {
					t.Fatal(err)
				}
				switch meta.Kind {
				case "ClusterRole":
					var role rbacv1.ClusterRole
					if err := json.Unmarshal(raw, &role); err != nil {
						t.Fatal(err)
					}
					for _, rule := range role.Rules {
						for _, resource := range rule.Resources {
							if resource == "namespaces" && meta.Metadata.Name != name {
								t.Fatalf("namespace access granted outside the volume backend role: %+v", role)
							}
						}
					}
					if meta.Metadata.Name != name {
						continue
					}
					roles++
					want := []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"namespaces"},
						ResourceNames: []string{tc.workloads}, Verbs: []string{"get"}}}
					if !reflect.DeepEqual(role.Rules, want) || role.AggregationRule != nil {
						t.Fatalf("unexpected volume backend role: %+v", role)
					}
				case "ClusterRoleBinding":
					if meta.Metadata.Name != name {
						continue
					}
					bindings++
					var binding rbacv1.ClusterRoleBinding
					if err := json.Unmarshal(raw, &binding); err != nil {
						t.Fatal(err)
					}
					if binding.RoleRef != (rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}) ||
						!reflect.DeepEqual(binding.Subjects, []rbacv1.Subject{{Kind: "ServiceAccount", Name: tc.account, Namespace: "runner-platform"}}) {
						t.Fatalf("unexpected volume backend binding: %+v", binding)
					}
				case "Role", "RoleBinding":
					if strings.HasSuffix(meta.Metadata.Name, "-volume-backend") {
						t.Fatal("volume backend grant must be cluster-scoped by resource name, not namespaced")
					}
				}
			}
			want := 1
			if tc.disabled {
				want = 0
			}
			if roles != want || bindings != want {
				t.Fatalf("volume backend roles=%d bindings=%d", roles, bindings)
			}
		})
	}
}

// Secret list is the one grant the orphan sweep needs and the default runner
// deliberately lacks. It must appear only with the sweep, as a list-only rule
// in the workload grant, together with the configuration that enables it.
func TestWorkloadSecretSweepRBAC(t *testing.T) {
	cases := []struct {
		name    string
		values  map[string]any
		enabled bool
	}{
		{name: "default"},
		{name: "default-cluster", values: map[string]any{"rbac": map[string]any{"clusterWide": true}}},
		{name: "enabled", enabled: true, values: map[string]any{"workloadSecretSweep": map[string]any{"enabled": true, "interval": "15s", "grace": "1m"}}},
		{name: "enabled-cluster", enabled: true, values: map[string]any{"rbac": map[string]any{"clusterWide": true},
			"workloadSecretSweep": map[string]any{"enabled": true, "interval": "15s", "grace": "1m"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := renderRBAC(t, tc.values)
			if err != nil {
				t.Fatalf("render: %v\n%s", err, output)
			}
			decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(output), 4096)
			lists, roles := 0, 0
			var env []corev1.EnvVar
			for {
				var raw json.RawMessage
				if err := decoder.Decode(&raw); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				var meta metav1.TypeMeta
				if err := json.Unmarshal(raw, &meta); err != nil {
					t.Fatal(err)
				}
				switch meta.Kind {
				case "Role", "ClusterRole":
					var role rbacv1.Role
					if err := json.Unmarshal(raw, &role); err != nil {
						t.Fatal(err)
					}
					if role.Name == "rbac-test-k8s-runner" {
						roles++
					}
					for _, rule := range role.Rules {
						for _, verb := range rule.Verbs {
							if verb == "*" || verb == "watch" && slices.Contains(rule.Resources, "secrets") {
								t.Fatalf("wildcard or Secret watch granted: %+v", rule)
							}
						}
						if !slices.Contains(rule.Verbs, "list") || !slices.Contains(rule.Resources, "secrets") {
							continue
						}
						lists++
						want := rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"list"}}
						if role.Name != "rbac-test-k8s-runner" || !reflect.DeepEqual(rule, want) {
							t.Fatalf("Secret list must be a separate list-only workload rule: %s %+v", role.Name, rule)
						}
					}
				case "Deployment":
					var deployment appsv1.Deployment
					if err := json.Unmarshal(raw, &deployment); err != nil {
						t.Fatal(err)
					}
					for _, variable := range deployment.Spec.Template.Spec.Containers[0].Env {
						if strings.HasPrefix(variable.Name, "WORKLOAD_SECRET_SWEEP_") {
							env = append(env, variable)
						}
					}
				}
			}
			want, wantEnv := 0, []corev1.EnvVar(nil)
			if tc.enabled {
				want = 1
				wantEnv = []corev1.EnvVar{{Name: "WORKLOAD_SECRET_SWEEP_INTERVAL", Value: "15s"}, {Name: "WORKLOAD_SECRET_SWEEP_GRACE", Value: "1m"}}
			}
			if roles != 1 || lists != want || !reflect.DeepEqual(env, wantEnv) {
				t.Fatalf("roles=%d Secret list rules=%d env=%+v", roles, lists, env)
			}
		})
	}
}
