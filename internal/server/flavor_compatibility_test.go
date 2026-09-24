package server

import (
	"context"
	"reflect"
	"testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestFlavorPreservesExplicitAndSupportingBounds(t *testing.T) {
	for _, flavor := range []string{"ram-2gb", "main-only"} {
		for _, docker := range []config.DockerImplementation{"", config.DockerImplementationRootless, config.DockerImplementationPrivileged} {
			t.Run(flavor+"/"+string(docker), func(t *testing.T) {
				client := fake.NewSimpleClientset()
				s := New(Options{Clientset: client, Namespace: "workloads", Logger: zap.NewNop(), Catalog: testCatalog(),
					SupportingContainerResources: supportingResources(), CapabilityImplementations: config.CapabilityImplementations{Docker: docker}})
				req := boundedRequest()
				req.Flavor = " " + flavor + " "
				if docker != "" {
					req.Capabilities = append(req.Capabilities, config.CapabilityDocker)
				}
				original := proto.Clone(req)
				response, err := s.StartWorkload(context.Background(), req)
				if err != nil {
					t.Fatal(err)
				}
				pod, err := client.CoreV1().Pods("workloads").Get(context.Background(), podNameFromID(response.Id), metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defaults, _ := supportingResources().ResourceRequirements()
				main, _ := containerResources(req.Main.Resources)
				tool, _ := containerResources(req.Sidecars[0].Resources)
				resolved, _ := s.resolveFlavor(flavor)
				for _, group := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
					for _, container := range group {
						want := defaults
						switch container.Name {
						case "main":
							want = main
						case "tool":
							want = tool
						case "helper":
							if resolved.sidecar != nil {
								want = *resolved.sidecar
							}
						}
						if !reflect.DeepEqual(container.Resources, want) {
							t.Errorf("%s resources = %+v, want %+v", container.Name, container.Resources, want)
						}
					}
				}
				if !proto.Equal(req, original) {
					t.Fatal("resource defaulting mutated the request")
				}
			})
		}
	}
}

func TestInvalidFlavorFailsBeforeKubernetesAccess(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		for _, change := range []string{"unknown", "partial-main", "negative", "zero", "fractional-cpu", "request-over-limit", "partial-sidecar"} {
			t.Run(map[bool]string{false: "legacy", true: "bounded"}[bounded]+"/"+change, func(t *testing.T) {
				client := fake.NewSimpleClientset()
				catalog := testCatalog()
				req := boundedRequest()
				if !bounded {
					req = &runnerv1.StartWorkloadRequest{Main: &runnerv1.ContainerSpec{Image: "main:1"}}
				}
				req.Flavor = "ram-2gb"
				req.ImagePullCredentials = []*runnerv1.ImagePullCredential{{Registry: "registry.example", Username: "user", Password: "fixture"}}
				req.Volumes = []*runnerv1.VolumeSpec{{Name: "workspace", Kind: runnerv1.VolumeKind_VOLUME_KIND_NAMED, PersistentName: "keep-me"}}
				switch change {
				case "unknown":
					req.Flavor = "missing"
				case "partial-main":
					catalog.Flavors[0].Resources.LimitsCPU = ""
				case "negative":
					catalog.Flavors[0].Resources.RequestsMemory = "-1Gi"
				case "zero":
					catalog.Flavors[0].Resources.LimitsMemory = "0"
				case "fractional-cpu":
					catalog.Flavors[0].Resources.RequestsCPU = "0.0001"
				case "request-over-limit":
					catalog.Flavors[0].Resources.RequestsCPU = "3"
				case "partial-sidecar":
					catalog.Flavors[0].SidecarResources.LimitsMemory = ""
				}
				s := New(Options{Clientset: client, Namespace: "workloads", Logger: zap.NewNop(), Catalog: catalog, SupportingContainerResources: supportingResources()})
				if _, err := s.StartWorkload(context.Background(), req); status.Code(err) != codes.InvalidArgument {
					t.Fatalf("invalid flavor accepted: %v", err)
				}
				if len(client.Actions()) != 0 {
					t.Fatalf("invalid flavor touched Kubernetes: %+v", client.Actions())
				}
			})
		}
	}
}

func TestPreparedFlavorKeepsGatesAndRejectsInvalidSizing(t *testing.T) {
	for _, mode := range []string{"valid", "unknown", "invalid-sidecar"} {
		t.Run(mode, func(t *testing.T) {
			client := preparedTestClient()
			s := preparedTestServer(client)
			s.catalog, s.supportingContainerResources = testCatalog(), supportingResources()
			req := preparedTestRequest()
			workload := boundedRequest()
			workload.WorkloadId, workload.Labels, workload.Volumes = req.Workload.WorkloadId, req.Workload.Labels, req.Workload.Volumes
			workload.Main.Mounts = req.Workload.Main.Mounts
			workload.Flavor = "ram-2gb"
			req.Workload = workload
			if mode == "unknown" {
				workload.Flavor = "missing"
			} else if mode == "invalid-sidecar" {
				s.catalog.Flavors[0].SidecarResources.RequestsMemory = "-1"
			}
			response, err := s.PrepareWorkload(context.Background(), req)
			if mode != "valid" {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("invalid prepared sizing accepted: %v", err)
				}
				for _, action := range client.Actions() {
					if action.GetVerb() != "get" && action.GetVerb() != "list" {
						t.Fatalf("invalid flavor mutated Kubernetes: %s %s", action.GetVerb(), action.GetResource())
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			pod := preparedTestPod(t, client, response.Binding)
			if !hasPreparedGate(pod) || pod.Annotations[preparedStateAnnotation] != "prepared" || pod.Spec.NodeName != "" {
				t.Fatal("flavor sizing bypassed prepared execution gating")
			}
			assertQuantity(t, pod.Spec.Containers[0].Resources.Requests, corev1.ResourceMemory, "1Gi")
			assertQuantity(t, pod.Spec.Containers[2].Resources.Requests, corev1.ResourceMemory, "128Mi")
			assertQuantity(t, pod.Spec.InitContainers[0].Resources.Limits, corev1.ResourceCPU, "500m")
		})
	}
}
