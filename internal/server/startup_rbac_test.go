package server

import (
	"os"
	"reflect"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

func startupChartRules(t *testing.T) []rbacv1.PolicyRule {
	t.Helper()
	data, err := os.ReadFile("../../charts/k8s-runner/values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		RBAC struct {
			Rules []rbacv1.PolicyRule `json:"rules"`
		} `json:"rbac"`
	}
	if err := yaml.Unmarshal(data, &values); err != nil {
		t.Fatal(err)
	}
	if len(values.RBAC.Rules) == 0 {
		t.Fatal("chart has no runner rules")
	}
	return values.RBAC.Rules
}

func TestStartupSecretChartPermissions(t *testing.T) {
	found := 0
	for _, rule := range startupChartRules(t) {
		if !slices.Contains(rule.Resources, "secrets") {
			continue
		}
		found++
		verbs := slices.Clone(rule.Verbs)
		slices.Sort(verbs)
		if !reflect.DeepEqual(verbs, []string{"create", "delete", "get", "patch"}) ||
			!reflect.DeepEqual(rule.APIGroups, []string{""}) || !reflect.DeepEqual(rule.Resources, []string{"secrets"}) {
			t.Fatalf("startup cleanup and prepared ownership require named Secret get/create/delete/patch, without list/watch or wildcard grants: %+v", rule)
		}
	}
	if found != 1 {
		t.Fatalf("want one Secret rule, got %d", found)
	}
}
