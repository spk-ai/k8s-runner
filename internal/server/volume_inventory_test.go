package server

import (
	"context"
	"testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestListVolumesRejectsIncompleteInventory(t *testing.T) {
	for _, name := range []string{"missing_key", "empty_key", "blank_key", "padded_key", "duplicate_key"} {
		for _, badName := range []string{"a-invalid", "z-invalid"} {
			t.Run(name+"/"+badName, func(t *testing.T) {
				good := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
					Name: "m-valid", Namespace: "default",
					Labels: map[string]string{managedByLabelKey: managedByLabelValue, volumeKeyLabelKey: "volume-1"},
				}}
				bad := good.DeepCopy()
				bad.Name = badName
				switch name {
				case "missing_key":
					delete(bad.Labels, volumeKeyLabelKey)
				case "empty_key":
					bad.Labels[volumeKeyLabelKey] = ""
				case "blank_key":
					bad.Labels[volumeKeyLabelKey] = " \t"
				case "padded_key":
					bad.Labels[volumeKeyLabelKey] = "volume-1 "
				}
				clientset := fake.NewSimpleClientset(good, bad)
				server := New(Options{Clientset: clientset, Namespace: "default", Logger: zap.NewNop()})
				resp, err := server.ListVolumes(context.Background(), &runnerv1.ListVolumesRequest{})
				if status.Code(err) != codes.FailedPrecondition || resp != nil {
					t.Fatalf("incomplete inventory must fail without a partial response: response %v, error %v", resp, err)
				}
				if actions := clientset.Actions(); len(actions) != 1 || actions[0].GetVerb() != "list" || actions[0].GetResource().Resource != "persistentvolumeclaims" {
					t.Fatalf("inventory validation must not modify Kubernetes state: %v", actions)
				}
			})
		}
	}
}

func TestListVolumesAcceptsCompleteInventory(t *testing.T) {
	for _, count := range []int{0, 2} {
		clientset := fake.NewSimpleClientset()
		for _, name := range []string{"volume-1", "volume-2"}[:count] {
			_, err := clientset.CoreV1().PersistentVolumeClaims("default").Create(context.Background(), &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{managedByLabelKey: managedByLabelValue, volumeKeyLabelKey: name}}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
		}
		server := New(Options{Clientset: clientset, Namespace: "default", Logger: zap.NewNop()})
		resp, err := server.ListVolumes(context.Background(), &runnerv1.ListVolumesRequest{})
		if err != nil || len(resp.GetVolumes()) != count {
			t.Fatalf("complete inventory must be usable: count %d, response %v, error %v", count, resp, err)
		}
	}
}
