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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
)

func render(t *testing.T, values map[string]any) ([]byte, error) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Fatal("Helm is required for chart tests; install Helm and run helm dependency build charts/k8s-runner")
	}
	path := filepath.Join(t.TempDir(), "values.json")
	data, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "helm", "template", "quota-test", "../../charts/k8s-runner", "--namespace", "runner-platform", "-f", path).CombinedOutput()
}

func budget() map[string]any {
	return map[string]any{"enabled": true, "name": "task-budget", "hard": map[string]any{
		"requests.cpu": "1", "requests.memory": "256Mi", "limits.cpu": "2", "limits.memory": "512Mi", "count/pods": "4",
	}}
}

func decode(t *testing.T, data []byte) ([]corev1.ResourceQuota, []appsv1.Deployment) {
	t.Helper()
	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var quotas []corev1.ResourceQuota
	var deployments []appsv1.Deployment
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err == io.EOF {
			return quotas, deployments
		} else if err != nil {
			t.Fatal(err)
		}
		var meta metav1.TypeMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			t.Fatal(err)
		}
		switch meta.Kind {
		case "ResourceQuota":
			var quota corev1.ResourceQuota
			if err := json.Unmarshal(raw, &quota); err != nil {
				t.Fatal(err)
			}
			quotas = append(quotas, quota)
		case "Deployment":
			var deployment appsv1.Deployment
			if err := json.Unmarshal(raw, &deployment); err != nil {
				t.Fatal(err)
			}
			deployments = append(deployments, deployment)
		}
	}
}

func TestWorkloadQuotaOptIn(t *testing.T) {
	baseline, err := render(t, map[string]any{})
	if err != nil {
		t.Fatalf("default chart: %v\n%s", err, baseline)
	}
	quotas, original := decode(t, baseline)
	if len(quotas) != 0 || len(original) != 1 {
		t.Fatal("default chart must not create a workload quota")
	}
	values := map[string]any{"workloadResourceQuota": budget()}
	output, err := render(t, values)
	if err != nil {
		t.Fatalf("enabled chart: %v\n%s", err, output)
	}
	quotas, deployments := decode(t, output)
	if len(quotas) != 1 || len(deployments) != 1 {
		t.Fatalf("want one quota and unchanged runner deployment, got %d / %d", len(quotas), len(deployments))
	}
	quota := quotas[0]
	if quota.Namespace != "agyn-workloads" || quota.Name != "task-budget" || len(quota.Spec.Scopes) != 0 || quota.Spec.ScopeSelector != nil {
		t.Fatalf("quota must cover the complete workload namespace: %+v", quota)
	}
	if len(quota.Spec.Hard) != 5 {
		t.Fatalf("unexpected quota hard map: %v", quota.Spec.Hard)
	}
	for key, expected := range budget()["hard"].(map[string]any) {
		actual, found := quota.Spec.Hard[corev1.ResourceName(key)]
		if !found || actual.Cmp(resource.MustParse(expected.(string))) != 0 {
			t.Fatalf("quota %s mismatch: %v", key, actual.String())
		}
	}
	if !reflect.DeepEqual(original, deployments) {
		t.Fatal("opting into workload quota must not rewrite the runner's deployment")
	}
}

func TestWorkloadQuotaCustomNamespace(t *testing.T) {
	quota := budget()
	quota["hard"].(map[string]any)["count/persistentvolumeclaims"] = 8
	output, err := render(t, map[string]any{"workloadResourceQuota": quota, "workloadNamespace": "isolated-tasks",
		"env":     []any{map[string]any{"name": "KUBE_NAMESPACE", "value": "isolated-tasks"}},
		"envFrom": []any{map[string]any{"configMapRef": map[string]any{"name": "operator-settings"}}},
	})
	if err != nil {
		t.Fatalf("custom namespace: %v\n%s", err, output)
	}
	quotas, deployments := decode(t, output)
	if len(quotas) != 1 || quotas[0].Namespace != "isolated-tasks" || len(deployments) != 1 {
		t.Fatal("quota namespace must match the literal runner environment, not the release namespace")
	}
	actual := quotas[0].Spec.Hard["count/persistentvolumeclaims"]
	if actual.Cmp(resource.MustParse("8")) != 0 {
		t.Fatal("additional native quota fields must survive rendering")
	}
	if env := deployments[0].Spec.Template.Spec.Containers[0].Env; len(env) != 1 || env[0].Value != quotas[0].Namespace {
		t.Fatalf("rendered namespace binding mismatch: %v", env)
	}
}

func TestWorkloadQuotaRejectsIncompleteOrAmbiguousConfiguration(t *testing.T) {
	cases := []struct {
		name   string
		change func(map[string]any, map[string]any)
		want   string
	}{
		{"empty-budget", func(_, q map[string]any) { q["hard"] = map[string]any{} }, "hard requires requests.cpu"},
		{"empty-name", func(_, q map[string]any) { q["name"] = "" }, "name is required"},
		{"wrong-enabled-type", func(_, q map[string]any) { q["enabled"] = "false" }, "enabled must be a boolean"},
		{"numeric-enabled", func(_, q map[string]any) { q["enabled"] = 0 }, "enabled must be a boolean"},
		{"empty-enabled", func(_, q map[string]any) { q["enabled"] = "" }, "enabled must be a boolean"},
		{"null-enabled", func(_, q map[string]any) { q["enabled"] = nil }, "enabled must be a boolean"},
		{"wrong-budget-type", func(_, q map[string]any) { q["hard"] = []any{"1"} }, "hard must be a map"},
		{"empty-quantity", func(_, q map[string]any) { q["hard"].(map[string]any)["requests.cpu"] = " " }, "hard values must be scalar quantities"},
		{"nested-quantity", func(_, q map[string]any) { q["hard"].(map[string]any)["requests.cpu"] = map[string]any{"value": "1"} }, "hard values must be scalar quantities"},
		{"empty-namespace", func(v, _ map[string]any) { v["workloadNamespace"] = "" }, "workloadNamespace must be explicit"},
		{"wrong-namespace", func(v, _ map[string]any) { v["workloadNamespace"] = "another-namespace" }, "KUBE_NAMESPACE must match workloadNamespace"},
		{"missing-env", func(v, _ map[string]any) { v["env"] = []any{} }, "exactly one literal KUBE_NAMESPACE"},
		{"env-from-only", func(v, _ map[string]any) {
			v["env"] = []any{}
			v["extraEnvVarsCM"] = "namespace-source"
		}, "exactly one literal KUBE_NAMESPACE"},
		{"duplicate-env", func(v, _ map[string]any) {
			v["extraEnvVars"] = []any{map[string]any{"name": "KUBE_NAMESPACE", "value": "agyn-workloads"}}
		}, "exactly one literal KUBE_NAMESPACE"},
		{"dynamic-env", func(v, _ map[string]any) {
			v["env"] = []any{map[string]any{"name": "KUBE_NAMESPACE", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "metadata.namespace"}}}}
		}, "exactly one literal KUBE_NAMESPACE"},
		{"scope-not-supported", func(_, q map[string]any) { q["scopes"] = []any{"NotBestEffort"} }, "unsupported workloadResourceQuota option"},
	}
	for _, field := range []string{"requests.memory", "limits.cpu", "limits.memory", "count/pods"} {
		cases = append(cases, struct {
			name   string
			change func(map[string]any, map[string]any)
			want   string
		}{"missing-" + field, func(_, q map[string]any) { delete(q["hard"].(map[string]any), field) }, "hard requires " + field})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			quota := budget()
			values := map[string]any{"workloadResourceQuota": quota}
			tc.change(values, quota)
			output, err := render(t, values)
			if err == nil || !strings.Contains(string(output), tc.want) {
				t.Fatalf("want render rejection containing %q, got %v\n%s", tc.want, err, output)
			}
		})
	}
}
