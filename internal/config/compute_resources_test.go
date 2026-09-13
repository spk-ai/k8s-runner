package config

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestComputeResourcesValidation(t *testing.T) {
	valid := ComputeResources{RequestsCPU: "250m", RequestsMemory: "128Mi", LimitsCPU: "1", LimitsMemory: "1Gi"}
	resources, err := valid.ResourceRequirements()
	if err != nil || resources.Requests.Cpu().MilliValue() != 250 || resources.Limits.Memory().Value() != 1<<30 {
		t.Fatalf("valid resource conversion: %+v, %v", resources, err)
	}
	for _, field := range []string{"requestsCpu", "requestsMemory", "limitsCpu", "limitsMemory"} {
		for _, value := range []string{"", "-1", "0", "invalid", "1e100", "0.00001"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				encoded, _ := json.Marshal(valid)
				var values map[string]string
				_ = json.Unmarshal(encoded, &values)
				values[field] = value
				encoded, _ = json.Marshal(values)
				var candidate ComputeResources
				_ = json.Unmarshal(encoded, &candidate)
				if _, err := candidate.ResourceRequirements(); err == nil {
					t.Fatal("invalid resource accepted")
				}
			})
		}
	}
	for _, candidate := range []ComputeResources{
		{RequestsCPU: "2", RequestsMemory: "128Mi", LimitsCPU: "1", LimitsMemory: "1Gi"},
		{RequestsCPU: "250m", RequestsMemory: "2Gi", LimitsCPU: "1", LimitsMemory: "1Gi"},
	} {
		if _, err := candidate.ResourceRequirements(); err == nil {
			t.Fatal("request exceeding limit accepted")
		}
	}
	if _, ok := resources.Requests[corev1.ResourceMemory]; !ok {
		t.Fatal("memory request missing")
	}
}

func TestSupportingResourcesConfiguration(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("ZITI_ENABLED", "false")
	t.Setenv("CATALOG_PATH", "")
	t.Setenv("SUPPORTING_CONTAINER_RESOURCES", "")
	cfg, err := Load()
	if err != nil || cfg.SupportingContainerResources != nil {
		t.Fatalf("legacy defaults changed: %+v, %v", cfg.SupportingContainerResources, err)
	}
	valid := `{"requestsCpu":"100m","requestsMemory":"64Mi","limitsCpu":"500m","limitsMemory":"256Mi"}`
	for _, value := range []string{"{}", "null", valid + " {}", valid[:len(valid)-1] + `,"unexpected":true}`} {
		t.Setenv("SUPPORTING_CONTAINER_RESOURCES", value)
		if _, err := Load(); err == nil {
			t.Fatalf("invalid configuration accepted: %s", value)
		}
	}
	t.Setenv("SUPPORTING_CONTAINER_RESOURCES", valid)
	cfg, err = Load()
	if err != nil || cfg.SupportingContainerResources == nil || cfg.SupportingContainerResources.LimitsMemory != "256Mi" {
		t.Fatalf("valid configuration rejected: %+v, %v", cfg.SupportingContainerResources, err)
	}
}
