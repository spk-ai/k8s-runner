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
	kubetesting "k8s.io/client-go/testing"
)

func TestObservePreparationIsReadOnlyAndNeverAuthorizesExecution(t *testing.T) {
	for _, state := range []string{"preparing", "prepared", "deleting", "zero-volumes"} {
		t.Run(state, func(t *testing.T) {
			client := preparedTestClient()
			req := preparedTestRequest()
			req.Workload.Labels["thread-id"] = uuid.NewString()
			req.Workload.Labels["private-fixture-label"] = "must-not-be-returned"
			if state == "zero-volumes" {
				req.Workload.Volumes, req.Workload.Main.Mounts = nil, nil
			}
			prepared, err := preparedTestServer(client).PrepareWorkload(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			pod := preparedTestPod(t, client, prepared.Binding)
			if state == "preparing" {
				pod.Annotations[preparedStateAnnotation] = "preparing"
			} else if state == "deleting" {
				now := metav1.Now()
				pod.DeletionTimestamp = &now
			}
			if err := client.Tracker().Update(preparedPodResource, pod, "default"); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			response, err := preparedTestServer(client).ObserveWorkloadPreparation(context.Background(), &runnerv1.ObserveWorkloadPreparationRequest{WorkloadId: req.Workload.WorkloadId, BackendId: req.BackendId})
			if err != nil || !proto.Equal(response.GetBinding(), prepared.Binding) || response.GetResourceVersion() != pod.ResourceVersion || response.GetSetupComplete() != (state != "preparing") || response.GetRemovalPending() != (state == "deleting") {
				t.Fatalf("read-only preparation observation failed: %v", err)
			}
			if response.IdentityLabels["thread-id"] != req.Workload.Labels["thread-id"] || response.IdentityLabels["agent-instance-id"] != req.Workload.Labels["agent-instance-id"] || response.IdentityLabels["private-fixture-label"] != "" {
				t.Fatal("owner identity projection omitted identity or disclosed an unrelated label")
			}
			assertPreparedInspectionReadOnly(t, client)
		})
	}
}

func TestObservePreparationRejectsUnsafeNativeState(t *testing.T) {
	for _, change := range []string{"old-writer", "unknown-version", "active", "unknown-state", "missing-gate", "node", "phase", "running-main", "running-ephemeral", "terminated-init", "prior-execution", "bad-binding", "wrong-backend", "claim-uid", "claim-owner", "claim-missing", "claim-deleting", "claim-lost"} {
		t.Run(change, func(t *testing.T) {
			s, client, prepared := prepareTestWorkload(t)
			pod, claim := preparedTestPod(t, client, prepared.Binding), preparedTestClaim(t, client)
			req := &runnerv1.ObserveWorkloadPreparationRequest{WorkloadId: prepared.Binding.WorkloadId, BackendId: prepared.Binding.BackendId}
			switch change {
			case "old-writer":
				delete(pod.Annotations, "agyn.io/preparation-recovery")
			case "unknown-version":
				pod.Annotations["agyn.io/preparation-recovery"] = "unknown"
			case "active":
				pod.Annotations[preparedStateAnnotation] = "active"
			case "unknown-state":
				pod.Annotations[preparedStateAnnotation] = "unknown"
			case "missing-gate":
				pod.Spec.SchedulingGates = nil
			case "node":
				pod.Spec.NodeName = "node"
			case "running-main":
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
			case "running-ephemeral":
				pod.Status.EphemeralContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
			case "phase":
				pod.Status.Phase = corev1.PodSucceeded
			case "terminated-init":
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
			case "prior-execution":
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
			case "bad-binding":
				pod.Annotations[preparedBindingAnnotation] = "{}"
			case "wrong-backend":
				req.BackendId = "other-backend"
			case "claim-uid":
				claim.UID = types.UID(uuid.NewString())
			case "claim-owner":
				claim.Labels["agent-instance-id"] = "different-owner"
			case "claim-deleting":
				now := metav1.Now()
				claim.DeletionTimestamp = &now
			case "claim-lost":
				claim.Status.Phase = corev1.ClaimLost
			}
			if err := client.Tracker().Update(preparedPodResource, pod, "default"); err != nil {
				t.Fatal(err)
			}
			if err := client.Tracker().Update(preparedClaimResource, claim, "default"); err != nil {
				t.Fatal(err)
			}
			if change == "claim-missing" {
				if err := client.Tracker().Delete(preparedClaimResource, "default", claim.Name); err != nil {
					t.Fatal(err)
				}
			}
			client.ClearActions()
			response, err := s.ObserveWorkloadPreparation(context.Background(), req)
			if response != nil || status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("unsafe observation not rejected: %v", err)
			}
			assertPreparedInspectionReadOnly(t, client)
		})
	}
}

func TestObservePreparationMissingIsNotRemovalEvidence(t *testing.T) {
	s, client, prepared := prepareTestWorkload(t)
	if err := client.Tracker().Delete(preparedPodResource, "default", podNameFromID(prepared.Binding.WorkloadId)); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	response, err := s.ObserveWorkloadPreparation(context.Background(), &runnerv1.ObserveWorkloadPreparationRequest{WorkloadId: prepared.Binding.WorkloadId, BackendId: prepared.Binding.BackendId})
	if response != nil || status.Code(err) != codes.NotFound {
		t.Fatalf("missing Pod produced a usable receipt: %v", err)
	}
	assertPreparedInspectionReadOnly(t, client)
}

func TestObservePreparationRejectsConcurrentSnapshotChanges(t *testing.T) {
	for _, change := range []string{"uid", "revision"} {
		t.Run(change, func(t *testing.T) {
			s, client, prepared := prepareTestWorkload(t)
			reads := 0
			client.PrependReactor("get", "pods", func(action kubetesting.Action) (bool, runtime.Object, error) {
				reads++
				if reads == 2 {
					obj, err := client.Tracker().Get(preparedPodResource, "default", action.(kubetesting.GetAction).GetName())
					if err != nil {
						t.Fatal(err)
					}
					pod := obj.(*corev1.Pod)
					pod.ResourceVersion = "2"
					if change == "uid" {
						pod.UID = types.UID(uuid.NewString())
					}
					if err := client.Tracker().Update(preparedPodResource, pod, "default"); err != nil {
						t.Fatal(err)
					}
				}
				return false, nil, nil
			})
			client.ClearActions()
			response, err := s.ObserveWorkloadPreparation(context.Background(), &runnerv1.ObserveWorkloadPreparationRequest{WorkloadId: prepared.Binding.WorkloadId, BackendId: prepared.Binding.BackendId})
			if response != nil || err == nil || reads != 2 {
				t.Fatalf("concurrent snapshot change accepted: %v", err)
			}
			assertPreparedInspectionReadOnly(t, client)
		})
	}
}
