package server

import (
	"context"
	"testing"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

func TestListWorkloadsReturnsLabeledPods(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-1",
				Namespace: "default",
				Labels: map[string]string{
					managedByLabelKey:   managedByLabelValue,
					workloadKeyLabelKey: "workload-1",
				},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-2",
				Namespace: "default",
				Labels: map[string]string{
					managedByLabelKey: managedByLabelValue,
					"ignored":         "value",
				},
			},
		},
	)

	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	resp, err := server.ListWorkloads(context.Background(), &runnerv1.ListWorkloadsRequest{})
	if err != nil {
		t.Fatalf("ListWorkloads returned error: %v", err)
	}
	if len(resp.Workloads) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(resp.Workloads))
	}
	item := resp.Workloads[0]
	if item.InstanceId != "pod-1" {
		t.Fatalf("expected instance id pod-1, got %q", item.InstanceId)
	}
	if item.WorkloadKey != "workload-1" {
		t.Fatalf("expected workload key workload-1, got %q", item.WorkloadKey)
	}
}

func TestListVolumesReturnsLabeledPVCs(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		volumeTestNamespace(),
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pvc-1",
				Namespace: "default",
				Labels: map[string]string{
					managedByLabelKey: managedByLabelValue,
					volumeKeyLabelKey: "volume-1",
				},
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pvc-2",
				Namespace: "default",
				Labels: map[string]string{
					managedByLabelKey: "another-controller",
					"ignored":         "value",
				},
			},
		},
	)

	server := New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	})

	resp, err := server.ListVolumes(context.Background(), &runnerv1.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes returned error: %v", err)
	}
	if len(resp.Volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.Volumes))
	}
	item := resp.Volumes[0]
	if item.InstanceId != "pvc-1" {
		t.Fatalf("expected instance id pvc-1, got %q", item.InstanceId)
	}
	if item.VolumeKey != "volume-1" {
		t.Fatalf("expected volume key volume-1, got %q", item.VolumeKey)
	}
}
