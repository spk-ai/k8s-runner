package server

import (
	"bytes"
	"encoding/json"
	"io"
	"os/exec"
	"reflect"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestVolumeBackendChartGrantsOnlyNamedNamespaceRead(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is required for chart rendering acceptance")
	}
	for _, enabled := range []bool{false, true} {
		args := []string{"template", "backend-fixture", "../../charts/k8s-runner", "--namespace", "runner-home", "--set", "workloadNamespace=task-workloads", "--set", "serviceAccount.name=custom-runner"}
		if !enabled {
			args = append(args, "--set", "rbac.create=false")
		}
		output, err := exec.Command("helm", args...).Output()
		if err != nil {
			t.Fatalf("chart rendering: %v", err)
		}
		decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(output), 4096)
		roles, bindings := 0, 0
		name := "runner-home-backend-fixture-k8s-runner-volume-backend"
		for {
			var raw runtime.RawExtension
			if err := decoder.Decode(&raw); err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			if len(raw.Raw) == 0 {
				continue
			}
			var kind metav1.TypeMeta
			if err := json.Unmarshal(raw.Raw, &kind); err != nil {
				t.Fatal(err)
			}
			switch kind.Kind {
			case "ClusterRole":
				var role rbacv1.ClusterRole
				if err := json.Unmarshal(raw.Raw, &role); err != nil {
					t.Fatal(err)
				}
				roles++
				want := []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"namespaces"}, ResourceNames: []string{"task-workloads"}, Verbs: []string{"get"}}}
				if role.Name != name || !reflect.DeepEqual(role.Rules, want) || role.AggregationRule != nil {
					t.Fatalf("chart widened namespace access: %+v", role)
				}
			case "ClusterRoleBinding":
				var binding rbacv1.ClusterRoleBinding
				if err := json.Unmarshal(raw.Raw, &binding); err != nil {
					t.Fatal(err)
				}
				bindings++
				if binding.Name != name || binding.RoleRef != (rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}) ||
					!reflect.DeepEqual(binding.Subjects, []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: "runner-home", Name: "custom-runner"}}) {
					t.Fatalf("chart bound namespace access to the wrong identity: %+v", binding)
				}
			}
		}
		want := 0
		if enabled {
			want = 1
		}
		if roles != want || bindings != want {
			t.Fatalf("rbac.create=%t: roles=%d bindings=%d", enabled, roles, bindings)
		}
	}
}
