package server

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

func preparedSecretsRequest() *runnerv1.PrepareWorkloadRequest {
	req := preparedTestRequest()
	req.Workload.ImagePullCredentials = []*runnerv1.ImagePullCredential{{Registry: "fixture.invalid", Username: "fixture", Password: "synthetic-fixture-only"}}
	req.Workload.InlineFiles = map[string][]byte{"/fixture-marker": []byte("synthetic-fixture-only")}
	req.Workload.Main.InlineFileMounts = []*runnerv1.InlineFileMount{{Path: "/fixture-marker"}}
	return req
}

func preparedBindingFromPod(t *testing.T, pod *corev1.Pod) *runnerv1.WorkloadBinding {
	t.Helper()
	binding := &runnerv1.WorkloadBinding{}
	if err := protojson.Unmarshal([]byte(pod.Annotations[preparedBindingAnnotation]), binding); err != nil {
		t.Fatal(err)
	}
	binding.InstanceUid = string(pod.UID)
	return binding
}

func assertPreparedSecretsOwned(t *testing.T, client *fake.Clientset, pod *corev1.Pod, count int) {
	t.Helper()
	secrets, err := client.CoreV1().Secrets("default").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(secrets.Items) != count {
		t.Fatalf("expected %d temporary Secrets: %v", count, err)
	}
	owner := []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID}}
	for _, secret := range secrets.Items {
		if !reflect.DeepEqual(secret.OwnerReferences, owner) {
			t.Fatal("temporary Secret lacks the exact Pod owner")
		}
	}
}

func TestPreparedSecretsHaveAtomicPodOwner(t *testing.T) {
	client := preparedTestClient()
	req := preparedSecretsRequest()
	creates := 0
	client.PrependReactor("create", "secrets", func(action kubetesting.Action) (bool, runtime.Object, error) {
		secret := action.(kubetesting.CreateAction).GetObject().(*corev1.Secret)
		obj, err := client.Tracker().Get(preparedPodResource, "default", podNameFromID(req.Workload.WorkloadId))
		if err != nil {
			t.Fatal("credential was written before its Pod owner existed")
		}
		pod := obj.(*corev1.Pod)
		owner := []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID}}
		if !reflect.DeepEqual(secret.OwnerReferences, owner) || pod.Annotations[preparedStateAnnotation] != "preparing" || !hasPreparedGate(pod) {
			t.Fatal("Secret create lacked atomic ownership or incomplete setup was activatable")
		}
		creates++
		return false, nil, nil
	})
	response, err := preparedTestServer(client).PrepareWorkload(context.Background(), req)
	if err != nil || creates != 2 {
		t.Fatalf("expected both credential writes: creates=%d err=%v", creates, err)
	}
	pod := preparedTestPod(t, client, response.Binding)
	if pod.Annotations[preparedStateAnnotation] != "prepared" || !hasPreparedGate(pod) {
		t.Fatal("successful setup did not commit readiness while retaining its scheduling gate")
	}
	assertPreparedSecretsOwned(t, client, pod, 2)
	for _, action := range client.Actions() {
		if action.Matches("patch", "secrets") || action.Matches("update", "secrets") {
			t.Fatal("Secret ownership still requires a second write")
		}
	}
}

func TestPreparedPodCreateFailureWritesNoSecrets(t *testing.T) {
	for _, cause := range []string{"rejected", "unknown", "lost-reply"} {
		t.Run(cause, func(t *testing.T) {
			client := preparedTestClient()
			client.PrependReactor("create", "pods", func(action kubetesting.Action) (bool, runtime.Object, error) {
				if cause == "rejected" {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "fixture", errors.New("fixture"))
				}
				if cause == "lost-reply" {
					pod := action.(kubetesting.CreateAction).GetObject().(*corev1.Pod)
					if err := client.Tracker().Create(preparedPodResource, pod, "default"); err != nil {
						t.Fatal(err)
					}
				}
				return true, nil, apierrors.NewTimeoutError("fixture", 1)
			})
			response, err := preparedTestServer(client).PrepareWorkload(context.Background(), preparedSecretsRequest())
			if err == nil || response != nil {
				t.Fatal("failed Pod creation reported success")
			}
			for _, action := range client.Actions() {
				if action.Matches("create", "secrets") || action.Matches("delete", "secrets") {
					t.Fatal("failed/uncertain Pod create wrote temporary credentials")
				}
			}
		})
	}
}

func TestPreparedPartialSecretFailureStaysGated(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			client := preparedTestClient()
			req := preparedSecretsRequest()
			creates := 0
			client.PrependReactor("create", "secrets", func(action kubetesting.Action) (bool, runtime.Object, error) {
				creates++
				if creates == failAt {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "fixture", errors.New("fixture"))
				}
				return false, nil, nil
			})
			response, err := preparedTestServer(client).PrepareWorkload(context.Background(), req)
			if err == nil || response != nil {
				t.Fatal("partial credential setup reported success")
			}
			pod, err := client.CoreV1().Pods("default").Get(context.Background(), podNameFromID(req.Workload.WorkloadId), metav1.GetOptions{})
			if err != nil || !hasPreparedGate(pod) || pod.Annotations[preparedStateAnnotation] != "preparing" {
				t.Fatalf("failed setup lost its non-executable Pod: %v", err)
			}
			assertPreparedSecretsOwned(t, client, pod, failAt-1)
			if _, err := preparedTestServer(client).ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: preparedBindingFromPod(t, pod)}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("partial setup activated after runner restart: %v", err)
			}
		})
	}
}

func TestPreparedSecretLateCreateKeepsDeletedPodOwner(t *testing.T) {
	client := preparedTestClient()
	req := preparedSecretsRequest()
	var original *corev1.Pod
	client.PrependReactor("create", "secrets", func(action kubetesting.Action) (bool, runtime.Object, error) {
		obj, err := client.Tracker().Get(preparedPodResource, "default", podNameFromID(req.Workload.WorkloadId))
		if err != nil {
			t.Fatal(err)
		}
		original = obj.(*corev1.Pod).DeepCopy()
		if err := client.Tracker().Delete(preparedPodResource, "default", original.Name); err != nil {
			t.Fatal(err)
		}
		replacement := original.DeepCopy()
		replacement.UID, replacement.ResourceVersion = types.UID(uuid.NewString()), "2"
		if err := client.Tracker().Create(preparedPodResource, replacement, "default"); err != nil {
			t.Fatal(err)
		}
		secret := action.(kubetesting.CreateAction).GetObject().(*corev1.Secret)
		secret.UID, secret.ResourceVersion = types.UID(uuid.NewString()), "1"
		if handled, _, err := kubetesting.ObjectReaction(client.Tracker())(action); !handled || err != nil {
			t.Fatalf("delayed Secret create failed: %v", err)
		}
		return true, nil, apierrors.NewTimeoutError("lost create response", 1)
	})
	if response, err := preparedTestServer(client).PrepareWorkload(context.Background(), req); err == nil || response != nil {
		t.Fatal("late Secret create reported successful preparation")
	}
	assertPreparedSecretsOwned(t, client, original, 1)
	if _, err := preparedTestServer(client).ActivateWorkload(context.Background(), &runnerv1.ActivateWorkloadRequest{Expected: preparedBindingFromPod(t, original)}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("late reply authorized replacement: %v", err)
	}
	for _, action := range client.Actions() {
		if action.Matches("delete", "secrets") || action.Matches("patch", "secrets") {
			t.Fatal("late error attempted unsafe credential cleanup or adoption")
		}
	}
}

func TestPreparedReadinessCommitFencesChangedPod(t *testing.T) {
	for _, change := range []string{"uid", "resource-version", "deleting", "gate", "state", "missing", "backend"} {
		t.Run(change, func(t *testing.T) {
			client := preparedTestClient()
			req := preparedSecretsRequest()
			client.PrependReactor("patch", "pods", func(action kubetesting.Action) (bool, runtime.Object, error) {
				obj, err := client.Tracker().Get(preparedPodResource, "default", podNameFromID(req.Workload.WorkloadId))
				if err != nil {
					t.Fatal(err)
				}
				pod := obj.(*corev1.Pod)
				switch change {
				case "uid":
					pod.UID = types.UID(uuid.NewString())
				case "missing":
					if err := client.Tracker().Delete(preparedPodResource, "default", pod.Name); err != nil {
						t.Fatal(err)
					}
					return false, nil, nil
				case "deleting":
					now := metav1.Now()
					pod.DeletionTimestamp = &now
				case "gate":
					pod.Spec.SchedulingGates = nil
				case "state":
					pod.Annotations[preparedStateAnnotation] = "retired"
				case "backend":
					pod.Annotations[preparedBindingAnnotation] = "{}"
				}
				pod.ResourceVersion = "2"
				if err := client.Tracker().Update(preparedPodResource, pod, "default"); err != nil {
					t.Fatal(err)
				}
				return false, nil, nil
			})
			if response, err := preparedTestServer(client).PrepareWorkload(context.Background(), req); err == nil || response != nil {
				t.Fatal("stale readiness commit accepted a changed/replaced Pod")
			}
		})
	}
}

func TestPreparedReadinessLostReplyRetainsCommittedState(t *testing.T) {
	client := preparedTestClient()
	req := preparedSecretsRequest()
	commits := 0
	client.PrependReactor("patch", "pods", func(action kubetesting.Action) (bool, runtime.Object, error) {
		commits++
		if handled, _, err := kubetesting.ObjectReaction(client.Tracker())(action); !handled || err != nil {
			t.Fatalf("readiness commit failed: %v", err)
		}
		return true, nil, apierrors.NewTimeoutError("lost readiness response", 1)
	})
	if response, err := preparedTestServer(client).PrepareWorkload(context.Background(), req); err == nil || response != nil || commits != 1 {
		t.Fatal("lost reply was retried or presented as confirmed success")
	}
	pod, err := client.CoreV1().Pods("default").Get(context.Background(), podNameFromID(req.Workload.WorkloadId), metav1.GetOptions{})
	if err != nil || !hasPreparedGate(pod) || pod.Annotations[preparedStateAnnotation] != "prepared" {
		t.Fatalf("lost reply changed committed preparation or removed its gate: %v", err)
	}
	assertPreparedSecretsOwned(t, client, pod, 2)
	if _, err := preparedTestServer(client).InspectPreparedWorkload(context.Background(), &runnerv1.InspectPreparedWorkloadRequest{Expected: preparedBindingFromPod(t, pod)}); err != nil {
		t.Fatalf("committed readiness not inspectable after restart: %v", err)
	}
}
