// Package podcheck evaluates the committed Pod Security fixtures with the
// Kubernetes v1.35 "restricted" evaluator. It is a separate module so the
// runner's own Kubernetes libraries are not upgraded for a test; it reads
// fixture files only and never contacts a cluster.
package podcheck

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/pod-security-admission/api"
	"k8s.io/pod-security-admission/policy"
)

// fixtureDir is written by internal/server/pod_security_test.go.
const fixtureDir = "../../internal/server/testdata/pod-security"

func TestFixturesAgainstRestrictedV135(t *testing.T) {
	evaluator, err := policy.NewEvaluator(policy.DefaultChecks(), nil)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(fixtureDir, "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no Pod Security fixtures under %s: %v", fixtureDir, err)
	}
	var allowed, denied int
	for _, path := range paths {
		name := strings.TrimSuffix(filepath.Base(path), ".json")
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.DisallowUnknownFields()
			var pod corev1.Pod
			if err := decoder.Decode(&pod); err != nil || pod.Kind != "Pod" || len(pod.Spec.Containers) == 0 {
				t.Fatalf("fixture is not one v1 Pod: %v", err)
			}
			result := policy.AggregateCheckResults(evaluator.EvaluatePod(api.LevelVersion{
				Level: api.LevelRestricted, Version: api.MajorMinorVersion(1, 35),
			}, &pod.ObjectMeta, &pod.Spec))
			switch {
			case strings.HasPrefix(name, "allowed-"):
				allowed++
				if !result.Allowed {
					t.Fatalf("restricted:v1.35 refuses %s: %v", name, result.ForbiddenReasons)
				}
			case strings.HasPrefix(name, "denied-"):
				denied++
				if result.Allowed {
					t.Fatalf("restricted:v1.35 admits %s, which must be refused", name)
				}
				t.Logf("refused as expected: %v", result.ForbiddenReasons)
			default:
				t.Fatalf("fixture %s must be named allowed-* or denied-*", name)
			}
		})
	}
	if allowed == 0 || denied == 0 {
		t.Fatalf("need both admitted and refused fixtures, got %d and %d", allowed, denied)
	}
}
