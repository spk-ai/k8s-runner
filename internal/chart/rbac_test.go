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
