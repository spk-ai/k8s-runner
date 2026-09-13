package config

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const CapabilityComputeResources = "compute-resources"

// ResourceRequirements validates complete bounds using Kubernetes quantities.
// A non-nil empty/partial specification is never treated as a default request.
func (r ComputeResources) ResourceRequirements() (corev1.ResourceRequirements, error) {
	result := corev1.ResourceRequirements{Requests: corev1.ResourceList{}, Limits: corev1.ResourceList{}}
	for _, field := range []struct {
		name   string
		value  string
		key    corev1.ResourceName
		target corev1.ResourceList
	}{
		{"requestsCpu", r.RequestsCPU, corev1.ResourceCPU, result.Requests},
		{"requestsMemory", r.RequestsMemory, corev1.ResourceMemory, result.Requests},
		{"limitsCpu", r.LimitsCPU, corev1.ResourceCPU, result.Limits},
		{"limitsMemory", r.LimitsMemory, corev1.ResourceMemory, result.Limits},
	} {
		quantity, err := resource.ParseQuantity(strings.TrimSpace(field.value))
		if err != nil || quantity.Sign() <= 0 {
			return corev1.ResourceRequirements{}, fmt.Errorf("%s must be a positive Kubernetes quantity", field.name)
		}
		if field.key == corev1.ResourceCPU && (quantity.MilliValue() <= 0 || quantity.Cmp(*resource.NewMilliQuantity(quantity.MilliValue(), resource.DecimalSI)) != 0) {
			return corev1.ResourceRequirements{}, fmt.Errorf("%s must use whole millicores", field.name)
		}
		if field.key == corev1.ResourceMemory && (quantity.Value() <= 0 || quantity.Cmp(*resource.NewQuantity(quantity.Value(), resource.BinarySI)) != 0) {
			return corev1.ResourceRequirements{}, fmt.Errorf("%s must use whole bytes", field.name)
		}
		field.target[field.key] = quantity
	}
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		request, limit := result.Requests[name], result.Limits[name]
		if request.Cmp(limit) > 0 {
			return corev1.ResourceRequirements{}, fmt.Errorf("%s request exceeds limit", name)
		}
	}
	return result, nil
}

func parseSupportingResources(value string) (*ComputeResources, error) {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	var resources ComputeResources
	if err := decoder.Decode(&resources); err != nil {
		return nil, fmt.Errorf("invalid SUPPORTING_CONTAINER_RESOURCES: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("SUPPORTING_CONTAINER_RESOURCES must contain one JSON object")
	}
	if _, err := resources.ResourceRequirements(); err != nil {
		return nil, fmt.Errorf("invalid SUPPORTING_CONTAINER_RESOURCES: %w", err)
	}
	return &resources, nil
}
