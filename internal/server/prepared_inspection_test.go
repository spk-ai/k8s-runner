package server

import (
	"context"
	"testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

func assertPreparedInspectionReadOnly(t *testing.T, client *fake.Clientset) {
	t.Helper()
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" || action.GetResource().Resource != "pods" && action.GetResource().Resource != "persistentvolumeclaims" && action.GetResource().Resource != "namespaces" {
			t.Fatalf("inspection used unexpected API: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func TestPreparedInspectionReadOnlyLifecycle(t *testing.T) {
	ctx := context.Background()
	s, client, prepared := prepareTestWorkload(t)
	for _, active := range []bool{false, true} {
		if active {
			if _, err := s.ActivateWorkload(ctx, &runnerv1.ActivateWorkloadRequest{Expected: prepared.Binding}); err != nil {
				t.Fatal(err)
			}
			object, err := client.Tracker().Get(preparedPodResource, "default", podNameFromID(prepared.Binding.WorkloadId))
			if err != nil {
				t.Fatal(err)
			}
			pod := object.(*corev1.Pod).DeepCopy()
			pod.Status.Phase = corev1.PodRunning
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: pod.Spec.Containers[0].Name, Image: "reported-image", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
			if err := client.Tracker().Update(preparedPodResource, pod, "default"); err != nil {
				t.Fatal(err)
			}
		}
		client.ClearActions()
		response, err := preparedTestServer(client).InspectPreparedWorkload(ctx, &runnerv1.InspectPreparedWorkloadRequest{Expected: prepared.Binding})
		if err != nil || !proto.Equal(response.GetBinding(), prepared.Binding) || response.GetActivated() != active || response.GetRemovalPending() || response.GetResourceVersion() != "1" || response.GetWorkload().GetId() != prepared.Binding.WorkloadId || response.GetWorkload().GetStateRunning() != active {
			t.Fatalf("inspection active=%t: response=%v err=%v", active, response, err)
		}
		if active && response.Workload.Image != "reported-image" {
			t.Fatal("inspection lost observed container data")
		}
		assertPreparedInspectionReadOnly(t, client)
	}
	object, err := client.Tracker().Get(preparedPodResource, "default", podNameFromID(prepared.Binding.WorkloadId))
	if err != nil {
		t.Fatal(err)
	}
	pod := object.(*corev1.Pod).DeepCopy()
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	if err := client.Tracker().Update(preparedPodResource, pod, "default"); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	response, err := s.InspectPreparedWorkload(ctx, &runnerv1.InspectPreparedWorkloadRequest{Expected: prepared.Binding})
	if err != nil || !response.GetRemovalPending() {
		t.Fatalf("deletion was hidden: %v", err)
	}
	assertPreparedInspectionReadOnly(t, client)
	if err := client.Tracker().Delete(preparedPodResource, "default", pod.Name); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	if _, err := s.InspectPreparedWorkload(ctx, &runnerv1.InspectPreparedWorkloadRequest{Expected: prepared.Binding}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing Pod: %v", err)
	}
	assertPreparedInspectionReadOnly(t, client)
}

func TestPreparedInspectionRejectsChangedIdentities(t *testing.T) {
	for _, which := range []string{"pod-uid", "pod-binding", "pod-volumes", "pod-state", "pod-gate", "claim-uid", "claim-owner", "claim-hold", "claim-deleting", "claim-lost", "claim-missing", "backend"} {
		t.Run(which, func(t *testing.T) {
			s, client, prepared := prepareTestWorkload(t)
			if _, err := s.ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: prepared.Binding}); err != nil {
				t.Fatal(err)
			}
			object, _ := client.Tracker().Get(preparedPodResource, "default", podNameFromID(prepared.Binding.WorkloadId))
			pod := object.(*corev1.Pod).DeepCopy()
			object, _ = client.Tracker().Get(preparedClaimResource, "default", prepared.Binding.Volumes[0].InstanceId)
			claim := object.(*corev1.PersistentVolumeClaim).DeepCopy()
			switch which {
			case "pod-uid":
				pod.UID = types.UID(uuid.NewString())
			case "pod-binding":
				delete(pod.Annotations, preparedBindingAnnotation)
			case "pod-volumes":
				pod.Spec.Volumes = nil
			case "pod-state":
				pod.Annotations[preparedStateAnnotation] = "unknown"
			case "pod-gate":
				pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: preparedGate}}
			case "claim-uid":
				claim.UID = types.UID(uuid.NewString())
			case "claim-owner":
				claim.Labels["agent-instance-id"] = "another-owner"
			case "claim-hold":
				claim.Finalizers = nil
			case "claim-deleting":
				now := metav1.Now()
				claim.DeletionTimestamp = &now
			case "claim-lost":
				claim.Status.Phase = corev1.ClaimLost
			case "backend":
				prepared.Binding.BackendId = "another-backend"
			}
			if err := client.Tracker().Update(preparedPodResource, pod, "default"); err != nil {
				t.Fatal(err)
			}
			if err := client.Tracker().Update(preparedClaimResource, claim, "default"); err != nil {
				t.Fatal(err)
			}
			if which == "claim-missing" {
				if err := client.Tracker().Delete(preparedClaimResource, "default", claim.Name); err != nil {
					t.Fatal(err)
				}
			}
			client.ClearActions()
			response, err := s.InspectPreparedWorkload(context.Background(), &runnerv1.InspectPreparedWorkloadRequest{Expected: prepared.Binding})
			if response != nil || err == nil || status.Code(err) == codes.NotFound {
				t.Fatalf("mismatch accepted or misreported as Pod absence: %v", err)
			}
			assertPreparedInspectionReadOnly(t, client)
		})
	}
}

func TestPreparedInspectionRejectsChangedSnapshot(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(map[bool]string{false: "resource-version", true: "same-name-new-uid"}[replacement], func(t *testing.T) {
			s, client, prepared := prepareTestWorkload(t)
			reads := 0
			client.PrependReactor("get", "pods", func(action kubetesting.Action) (bool, runtime.Object, error) {
				reads++
				if reads != 2 {
					return false, nil, nil
				}
				object, err := client.Tracker().Get(preparedPodResource, "default", podNameFromID(prepared.Binding.WorkloadId))
				if err != nil {
					return true, nil, err
				}
				pod := object.(*corev1.Pod).DeepCopy()
				pod.ResourceVersion = "2"
				if replacement {
					pod.UID = types.UID(uuid.NewString())
				}
				return true, pod, nil
			})
			response, err := s.InspectPreparedWorkload(context.Background(), &runnerv1.InspectPreparedWorkloadRequest{Expected: prepared.Binding})
			want := codes.Aborted
			if replacement {
				want = codes.FailedPrecondition
			}
			if response != nil || status.Code(err) != want {
				t.Fatalf("changed snapshot accepted: %v", err)
			}
			assertPreparedInspectionReadOnly(t, client)
		})
	}
}

func TestPreparedInspectionCannotAuthorizeOrRepair(t *testing.T) {
	for _, mutation := range []string{"scheduled", "running", "terminated", "previously-terminated", "ungated"} {
		t.Run(mutation, func(t *testing.T) {
			s, client, prepared := prepareTestWorkload(t)
			object, _ := client.Tracker().Get(preparedPodResource, "default", podNameFromID(prepared.Binding.WorkloadId))
			pod := object.(*corev1.Pod).DeepCopy()
			switch mutation {
			case "scheduled":
				pod.Spec.NodeName = "node"
			case "running":
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
			case "terminated":
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
			case "previously-terminated":
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
			case "ungated":
				pod.Spec.SchedulingGates = nil
			}
			if err := client.Tracker().Update(preparedPodResource, pod, "default"); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			if response, err := s.InspectPreparedWorkload(context.Background(), &runnerv1.InspectPreparedWorkloadRequest{Expected: prepared.Binding}); response != nil || status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("invalid unactivated Pod: %v", err)
			}
			assertPreparedInspectionReadOnly(t, client)
		})
	}
}
