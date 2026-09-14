package server

import (
	"context"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

func storageServer(t *testing.T, objects ...runtime.Object) (*Server, *fake.Clientset) {
	t.Helper()
	clientset := fake.NewSimpleClientset(append(objects, volumeTestNamespace())...)
	return New(Options{
		Clientset:   clientset,
		Namespace:   "default",
		StorageSize: "1Gi",
		Logger:      zap.NewNop(),
	}), clientset
}

func TestRemoveVolumeCheckedDeletesTheClaim(t *testing.T) {
	pvc := checkedVolumePVC()
	server, clientset := storageServer(t, pvc)

	resp, err := server.RemoveVolumeChecked(context.Background(), checkedVolumeRequest(pvc))
	if err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING {
		t.Fatalf("remove acknowledgement: %v, %v", resp, err)
	}
	_, err = clientset.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), pvc.Name, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("claim still present: %v", err)
	}
}

func TestRemoveVolumeCheckedIsIdempotent(t *testing.T) {
	server, _ := storageServer(t)

	resp, err := server.RemoveVolumeChecked(context.Background(), checkedVolumeRequest(checkedVolumePVC()))
	if err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT {
		t.Fatalf("confirmed absence: %v, %v", resp, err)
	}
}

func TestRemoveVolumeCheckedKeepsATerminatingClaimPending(t *testing.T) {
	deleting := metav1.Now()
	pvc := checkedVolumePVC()
	pvc.DeletionTimestamp = &deleting
	pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}
	server, clientset := storageServer(t, pvc)

	resp, err := server.RemoveVolumeChecked(context.Background(), checkedVolumeRequest(pvc))
	if err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING {
		t.Fatalf("terminating claim: %v, %v", resp, err)
	}
	assertNoVolumeMutation(t, clientset)
}

func TestRemoveVolumeRequiresAName(t *testing.T) {
	server, _ := storageServer(t)

	_, err := server.RemoveVolume(context.Background(), &runnerv1.RemoveVolumeRequest{VolumeName: "  "})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}
