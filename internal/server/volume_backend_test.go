package server

import (
	"context"
	"errors"
	"testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

const testVolumeBackend = "kubernetes-namespace/v1/default/namespace-original"

func volumeTestNamespace() *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: "namespace-original", ResourceVersion: "1"}}
}

func backendTestServer(client *fake.Clientset, namespace string) *Server {
	return New(Options{Clientset: client, Namespace: namespace, Logger: zap.NewNop()})
}

func TestVolumeBackendInventorySurvivesRunnerRestart(t *testing.T) {
	for _, populated := range []bool{false, true} {
		objects := []runtime.Object{volumeTestNamespace()}
		if populated {
			objects = append(objects, checkedVolumePVC())
		}
		client := fake.NewSimpleClientset(objects...)
		for i := 0; i < 2; i++ {
			server := backendTestServer(client, "default")
			resp, err := server.ListVolumes(context.Background(), &runnerv1.ListVolumesRequest{})
			if err != nil || resp.GetBackendId() != testVolumeBackend {
				t.Fatalf("inventory must identify even an empty backend across restart: %v, %v", resp, err)
			}
			if populated && (len(resp.Volumes) != 1 || resp.Volumes[0].BackendId != testVolumeBackend) {
				t.Fatalf("inventory item lacks matching backend: %v", resp)
			}
			// Ordinary metadata updates are not a new namespace incarnation.
			ns := volumeTestNamespace()
			ns.ResourceVersion = "2"
			ns.Labels = map[string]string{"diagnostic": "changed"}
			if _, err := client.CoreV1().Namespaces().Update(context.Background(), ns, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestVolumeBackendRejectsWrongOrUnavailableScope(t *testing.T) {
	for _, name := range []string{"wrong-name", "replacement", "missing", "terminating", "empty-uid", "forbidden", "unavailable"} {
		t.Run(name, func(t *testing.T) {
			ns := volumeTestNamespace()
			objects := []runtime.Object{ns, checkedVolumePVC()}
			namespace := "default"
			switch name {
			case "wrong-name":
				namespace, ns.Name = "other", "other"
			case "replacement":
				ns.UID = "namespace-replacement"
			case "missing":
				objects = objects[1:]
			case "terminating":
				now := metav1.Now()
				ns.DeletionTimestamp = &now
			case "empty-uid":
				ns.UID = ""
			}
			client := fake.NewSimpleClientset(objects...)
			if name == "forbidden" || name == "unavailable" {
				client.PrependReactor("get", "namespaces", func(kubetesting.Action) (bool, runtime.Object, error) {
					if name == "forbidden" {
						return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, namespace, errors.New("fixture"))
					}
					return true, nil, apierrors.NewServiceUnavailable("fixture")
				})
			}
			req := checkedVolumeRequest(checkedVolumePVC())
			req.Expected.BackendId = testVolumeBackend
			resp, err := backendTestServer(client, namespace).RemoveVolumeBound(context.Background(), req)
			if err == nil || resp != nil {
				t.Fatalf("wrong/unavailable namespace authorized deletion or absence: %v, %v", resp, err)
			}
			for _, action := range client.Actions() {
				if action.GetResource().Resource != "namespaces" || action.GetVerb() != "get" {
					t.Fatalf("must reject before contacting any PVC: %v", action)
				}
			}
		})
	}
}

func TestVolumeBackendRechecksInventoryAndAbsence(t *testing.T) {
	for _, operation := range []string{"inventory", "absence"} {
		for _, failure := range []string{"replacement", "missing", "forbidden", "terminating"} {
			t.Run(operation+"/"+failure, func(t *testing.T) {
				client := fake.NewSimpleClientset(volumeTestNamespace())
				reads := 0
				client.PrependReactor("get", "namespaces", func(kubetesting.Action) (bool, runtime.Object, error) {
					reads++
					if reads == 1 {
						return false, nil, nil
					}
					ns := volumeTestNamespace()
					switch failure {
					case "replacement":
						ns.UID = "new-namespace"
					case "missing":
						return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, ns.Name)
					case "forbidden":
						return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, ns.Name, errors.New("fixture"))
					case "terminating":
						now := metav1.Now()
						ns.DeletionTimestamp = &now
					}
					return true, ns, nil
				})
				server := backendTestServer(client, "default")
				if operation == "inventory" {
					if resp, err := server.ListVolumes(context.Background(), &runnerv1.ListVolumesRequest{}); err == nil || resp != nil {
						t.Fatalf("changed/unavailable scope yielded inventory: %v, %v", resp, err)
					}
				} else {
					req := checkedVolumeRequest(checkedVolumePVC())
					req.Expected.BackendId = testVolumeBackend
					if resp, err := server.RemoveVolumeBound(context.Background(), req); err == nil || resp != nil {
						t.Fatalf("changed/unavailable scope yielded absence: %v, %v", resp, err)
					}
				}
				if reads != 2 {
					t.Fatalf("expected a fresh namespace check on both sides of the lookup, got %d", reads)
				}
				assertNoVolumeMutation(t, client)
			})
		}
	}
}

func TestVolumeBackendRequiresTargetAndReturnsObservedIdentity(t *testing.T) {
	client := fake.NewSimpleClientset(volumeTestNamespace())
	server := backendTestServer(client, "default")
	req := checkedVolumeRequest(checkedVolumePVC())
	req.Expected.BackendId = ""
	if resp, err := server.RemoveVolumeBound(context.Background(), req); resp != nil || status.Code(err) != codes.InvalidArgument || len(client.Actions()) != 0 {
		t.Fatalf("legacy target reached backend: %v, %v", resp, err)
	}
	req.Expected.BackendId = testVolumeBackend
	resp, err := server.RemoveVolumeBound(context.Background(), req)
	if err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT || resp.GetBackendId() != testVolumeBackend {
		t.Fatalf("verified absence must identify its backend: %v, %v", resp, err)
	}
}

func TestVolumeBackendRejectsLegacyCheckedRPC(t *testing.T) {
	for _, backend := range []string{"", testVolumeBackend} {
		client := fake.NewSimpleClientset(volumeTestNamespace(), checkedVolumePVC())
		expected := checkedVolumeRequest(checkedVolumePVC()).Expected
		expected.BackendId = backend
		resp, err := backendTestServer(client, "default").RemoveVolumeChecked(context.Background(), &runnerv1.RemoveVolumeCheckedRequest{Expected: expected})
		if resp != nil || status.Code(err) != codes.FailedPrecondition || len(client.Actions()) != 0 {
			t.Fatalf("legacy checked RPC reached backend: %v, %v", resp, err)
		}
	}
}
