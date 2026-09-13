package server

import (
	"context"
	"reflect"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

func boundedRequest() *runnerv1.StartWorkloadRequest {
	return &runnerv1.StartWorkloadRequest{Capabilities: []string{config.CapabilityComputeResources},
		Main: &runnerv1.ContainerSpec{Image: "main:1", Resources: &runnerv1.ComputeResources{
			RequestsCpu: "500m", RequestsMemory: "1Gi", LimitsCpu: "2", LimitsMemory: "2Gi",
		}},
		Sidecars: []*runnerv1.ContainerSpec{{Image: "tool:1", Name: "tool", Resources: &runnerv1.ComputeResources{
			RequestsCpu: "50m", RequestsMemory: "32Mi", LimitsCpu: "100m", LimitsMemory: "64Mi",
		}}, {Image: "helper:1", Name: "helper"}},
		InitContainers: []*runnerv1.ContainerSpec{{Image: "init:1", Name: "init"},
			{Image: "sidecar:1", Name: "restartable", AdditionalProperties: map[string]string{"restart_policy": "Always"}}},
	}
}

func supportingResources() *config.ComputeResources {
	return &config.ComputeResources{RequestsCPU: "100m", RequestsMemory: "64Mi", LimitsCPU: "500m", LimitsMemory: "256Mi"}
}

func TestComputeResourcesAppliedToAllContainerRoles(t *testing.T) {
	for _, docker := range []config.DockerImplementation{"", config.DockerImplementationRootless, config.DockerImplementationPrivileged} {
		t.Run(string(docker), func(t *testing.T) {
			client := fake.NewSimpleClientset()
			server := New(Options{Clientset: client, Namespace: "workloads", Logger: zap.NewNop(),
				SupportingContainerResources: supportingResources(), CapabilityImplementations: config.CapabilityImplementations{Docker: docker}})
			request := boundedRequest()
			if docker != "" {
				request.Capabilities = append(request.Capabilities, config.CapabilityDocker)
			}
			response, err := server.StartWorkload(context.Background(), request)
			if err != nil {
				t.Fatalf("start: %v", err)
			}
			pod, err := client.CoreV1().Pods("workloads").Get(context.Background(), podNameFromID(response.Id), metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defaults, _ := supportingResources().ResourceRequirements()
			main, _ := containerResources(request.Main.Resources)
			tool, _ := containerResources(request.Sidecars[0].Resources)
			for _, group := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
				for _, container := range group {
					expected := defaults
					if container.Name == "main" {
						expected = main
					}
					if container.Name == "tool" {
						expected = tool
					}
					if !reflect.DeepEqual(container.Resources, expected) {
						t.Errorf("container %s resources: %+v, want %+v", container.Name, container.Resources, expected)
					}
				}
			}
			if request.InitContainers[0].Resources != nil || request.Sidecars[1].Resources != nil {
				t.Fatal("request was mutated by defaulting")
			}
		})
	}
}

func TestComputeResourcesFailBeforeAnyKubernetesAccess(t *testing.T) {
	for _, mode := range []string{"unconfigured", "bad-defaults", "missing-main", "empty-main", "missing-capability", "partial-sidecar", "bad-init", "unknown-capability"} {
		t.Run(mode, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			defaults := supportingResources()
			request := boundedRequest()
			request.ImagePullCredentials = []*runnerv1.ImagePullCredential{{Registry: "registry.example", Username: "user", Password: "fixture"}}
			request.Volumes = []*runnerv1.VolumeSpec{{Name: "workspace", Kind: runnerv1.VolumeKind_VOLUME_KIND_NAMED, PersistentName: "keep-me"}}
			want := codes.InvalidArgument
			switch mode {
			case "unconfigured":
				defaults = nil
				want = codes.FailedPrecondition
			case "bad-defaults":
				defaults.LimitsCPU = "0"
				want = codes.FailedPrecondition
			case "missing-main":
				request.Main.Resources = nil
			case "empty-main":
				request.Main.Resources = &runnerv1.ComputeResources{}
			case "missing-capability":
				request.Capabilities = nil
			case "partial-sidecar":
				request.Sidecars[0].Resources.LimitsMemory = ""
			case "bad-init":
				request.InitContainers[0].Resources = &runnerv1.ComputeResources{RequestsCpu: "-1"}
			case "unknown-capability":
				request.Capabilities = append(request.Capabilities, "future-unsupported")
			}
			server := New(Options{Clientset: client, Namespace: "workloads", Logger: zap.NewNop(), SupportingContainerResources: defaults})
			_, err := server.StartWorkload(context.Background(), request)
			if status.Code(err) != want {
				t.Fatalf("error: %v, want %v", err, want)
			}
			if len(client.Actions()) != 0 {
				t.Fatalf("invalid request touched Kubernetes: %+v", client.Actions())
			}
		})
	}
}

func TestComputeResourceDefaultsDoNotChangeLegacyWorkloadsOrAlias(t *testing.T) {
	client := fake.NewSimpleClientset()
	server := New(Options{Clientset: client, Namespace: "workloads", Logger: zap.NewNop(), SupportingContainerResources: supportingResources()})
	response, err := server.StartWorkload(context.Background(), &runnerv1.StartWorkloadRequest{Main: &runnerv1.ContainerSpec{Image: "main:1"}})
	if err != nil {
		t.Fatal(err)
	}
	pod, _ := client.CoreV1().Pods("workloads").Get(context.Background(), podNameFromID(response.Id), metav1.GetOptions{})
	if len(pod.Spec.Containers[0].Resources.Limits) != 0 {
		t.Fatal("legacy workload silently opted in")
	}
	defaults, _ := supportingResources().ResourceRequirements()
	containers := []corev1.Container{{}, {}}
	applySupportingResources(containers, nil, defaults)
	containers[0].Resources.Limits[corev1.ResourceMemory] = resource.MustParse("512Mi")
	if containers[1].Resources.Limits.Memory().Value() != 256<<20 || defaults.Limits.Memory().Value() != 256<<20 {
		t.Fatal("default resource maps alias each other")
	}
}
