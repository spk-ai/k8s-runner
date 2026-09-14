package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

func volumeRemovalRPC(t *testing.T, server *Server) runnerv1.RunnerServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rpc := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		switch info.FullMethod {
		case runnerv1.RunnerService_ListVolumes_FullMethodName, runnerv1.RunnerService_RemoveVolumeChecked_FullMethodName,
			runnerv1.RunnerService_RemoveVolume_FullMethodName, runnerv1.RunnerService_RemoveWorkload_FullMethodName:
			return handler(ctx, req)
		default:
			return nil, status.Error(codes.PermissionDenied, "fixture_rpc_not_allowed")
		}
	}))
	runnerv1.RegisterRunnerServiceServer(rpc, server)
	done := make(chan error, 1)
	go func() { done <- rpc.Serve(listener) }()
	t.Cleanup(func() {
		rpc.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return runnerv1.NewRunnerServiceClient(connection)
}

type removalRaceTransport struct {
	next     http.RoundTripper
	path     string
	uid      types.UID
	before   func(context.Context) error
	calls    atomic.Int32
	response atomic.Int32
}

func (r *removalRaceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodDelete && req.URL.Path == r.path && r.calls.CompareAndSwap(0, 1) {
		data, err := io.ReadAll(io.LimitReader(req.Body, 32769))
		_ = req.Body.Close()
		if err != nil || len(data) > 32768 {
			return nil, fmt.Errorf("invalid fixture delete body")
		}
		req.Body = io.NopCloser(bytes.NewReader(data))
		var options metav1.DeleteOptions
		if err := json.Unmarshal(data, &options); err != nil {
			return nil, err
		}
		if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != r.uid ||
			options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion == "" {
			return nil, fmt.Errorf("runner omitted the atomic delete preconditions")
		}
		if err := r.before(req.Context()); err != nil {
			return nil, err
		}
		resp, err := r.next.RoundTrip(req)
		if resp != nil {
			r.response.Store(int32(resp.StatusCode))
		}
		return resp, err
	}
	return r.next.RoundTrip(req)
}

func testLiveCheckedVolumeRemoval(t *testing.T, ctx context.Context, admin kubernetes.Interface, runnerConfig *rest.Config, server *Server, ownerLabel, run string, owned map[string]types.UID) {
	claims := admin.CoreV1().PersistentVolumeClaims(server.namespace)
	client := volumeRemovalRPC(t, server)
	newClaim := func(name string, identity map[string]string, finalizers []string) (*corev1.PersistentVolumeClaim, error) {
		labels := maps.Clone(identity)
		labels[ownerLabel] = run
		return claims.Create(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, Finalizers: finalizers},
			Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: server.storageClass, AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Mi")}}}}, metav1.CreateOptions{})
	}
	create := func(t *testing.T, name string, finalizers []string) *corev1.PersistentVolumeClaim {
		t.Helper()
		owned[name] = ""
		pvc, err := newClaim(name, map[string]string{
			managedByLabelKey: managedByLabelValue, workloadManagedByLabelKey: workloadManagedByLabelValue,
			volumeKeyLabelKey: uuid.NewString(), "managed-by": "agents-orchestrator", "agent-instance-id": "task-owner", "agent-id": "agent-class",
		}, finalizers)
		if err != nil {
			t.Fatal(err)
		}
		owned[name] = pvc.UID
		return pvc
	}
	expected := func(t *testing.T, pvc *corev1.PersistentVolumeClaim) *runnerv1.RemoveVolumeCheckedRequest {
		t.Helper()
		inventory, err := client.ListVolumes(ctx, &runnerv1.ListVolumesRequest{})
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range inventory.Volumes {
			if item.InstanceId == pvc.Name && item.InstanceUid == string(pvc.UID) {
				return &runnerv1.RemoveVolumeCheckedRequest{Expected: item}
			}
		}
		t.Fatal("native inventory did not return the created incarnation")
		return nil
	}
	ownedEmpty := func(pvc *corev1.PersistentVolumeClaim, uid types.UID) error {
		if pvc.UID != uid || pvc.Labels[ownerLabel] != run || pvc.Spec.VolumeName != "" || pvc.Status.Phase == corev1.ClaimBound ||
			pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != *server.storageClass {
			return fmt.Errorf("fixture claim is replaced, foreign or backed by storage")
		}
		return nil
	}
	waitAbsent := func(name string, uid types.UID) error {
		return wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
			pvc, err := claims.Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			if err != nil {
				return false, err
			}
			return false, ownedEmpty(pvc, uid)
		})
	}

	t.Run("pending-finalizer-and-replacement", func(t *testing.T) {
		const finalizer = "agyn.io/checked-removal-test"
		pvc := create(t, "checked-finalizer", []string{finalizer})
		releaseFinalizer := func() error {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
				current, err := claims.Get(cleanup, pvc.Name, metav1.GetOptions{})
				if apierrors.IsNotFound(err) {
					return nil
				}
				if err != nil {
					return err
				}
				if !slices.Contains(current.Finalizers, finalizer) {
					return nil
				}
				if err := ownedEmpty(current, pvc.UID); err != nil {
					return err
				}
				current.Finalizers = slices.DeleteFunc(current.Finalizers, func(value string) bool { return value == finalizer })
				_, err = claims.Update(cleanup, current, metav1.UpdateOptions{})
				return err
			})
		}
		t.Cleanup(func() {
			if err := releaseFinalizer(); err != nil {
				t.Error(err)
			}
		})
		req := expected(t, pvc)
		if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
			resp, err := client.RemoveVolumeChecked(ctx, req)
			if status.Code(err) == codes.Aborted {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if resp.State != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING {
				return false, fmt.Errorf("DELETE acknowledgement finalized a held claim")
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		held, err := claims.Get(ctx, pvc.Name, metav1.GetOptions{})
		if err != nil || held.DeletionTimestamp == nil || !slices.Contains(held.Finalizers, finalizer) {
			t.Fatalf("finalizer did not hold deletion: %v", err)
		}
		resp, err := client.RemoveVolumeChecked(ctx, req)
		if err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING {
			t.Fatalf("terminating claim reported absent: %v %v", resp, err)
		}
		if _, err := client.RemoveVolume(ctx, &runnerv1.RemoveVolumeRequest{VolumeName: pvc.Name, Force: true}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("legacy removal bypass: %v", err)
		}
		if _, err := client.RemoveWorkload(ctx, &runnerv1.RemoveWorkloadRequest{WorkloadId: "absent-test-workload", RemoveVolumes: true, Force: true}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("workload cleanup bypass: %v", err)
		}
		// Act as the owner of our synthetic test finalizer only. Kubernetes'
		// protection finalizer and every unrelated finalizer remain untouched.
		if err := releaseFinalizer(); err != nil {
			t.Fatal(err)
		}
		if err := waitAbsent(pvc.Name, pvc.UID); err != nil {
			t.Fatal(err)
		}
		resp, err = client.RemoveVolumeChecked(ctx, req)
		if err != nil || resp.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT {
			t.Fatalf("physical absence not confirmed: %v %v", resp, err)
		}
		replacement, err := newClaim(pvc.Name, pvc.Labels, nil)
		if err != nil {
			t.Fatal(err)
		}
		owned[pvc.Name] = replacement.UID
		resp, err = client.RemoveVolumeChecked(ctx, req)
		if status.Code(err) != codes.FailedPrecondition || resp != nil {
			t.Fatalf("stale completed deletion retargeted replacement: %v %v", resp, err)
		}
		kept, err := claims.Get(ctx, pvc.Name, metav1.GetOptions{})
		if err != nil || kept.UID != replacement.UID || kept.DeletionTimestamp != nil {
			t.Fatalf("replacement was changed: %v", err)
		}
		t.Logf("held native UID=%s; confirmed absence, retained replacement UID=%s", pvc.UID, replacement.UID)
	})

	for _, changeUID := range []bool{true, false} {
		t.Run(fmt.Sprintf("atomic-precondition/replace-uid=%t", changeUID), func(t *testing.T) {
			pvc := create(t, fmt.Sprintf("checked-race-%t", changeUID), nil)
			req := expected(t, pvc)
			var changed atomic.Pointer[corev1.PersistentVolumeClaim]
			race := &removalRaceTransport{path: "/api/v1/namespaces/" + server.namespace + "/persistentvolumeclaims/" + pvc.Name, uid: pvc.UID}
			race.before = func(ctx context.Context) error {
				var current *corev1.PersistentVolumeClaim
				err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
					var err error
					current, err = claims.Get(ctx, pvc.Name, metav1.GetOptions{})
					if err != nil {
						return err
					}
					if err := ownedEmpty(current, pvc.UID); err != nil {
						return err
					}
					if changeUID {
						return claims.Delete(ctx, pvc.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &current.UID, ResourceVersion: &current.ResourceVersion}})
					}
					current.Labels["agent-instance-id"] = "different-owner"
					current, err = claims.Update(ctx, current, metav1.UpdateOptions{})
					return err
				})
				if err != nil {
					return err
				}
				if changeUID {
					if err := waitAbsent(pvc.Name, pvc.UID); err != nil {
						return err
					}
					current, err = newClaim(pvc.Name, pvc.Labels, nil)
					if err != nil {
						return err
					}
				}
				changed.Store(current)
				return nil
			}
			cfg := rest.CopyConfig(runnerConfig)
			cfg.Wrap(func(next http.RoundTripper) http.RoundTripper { race.next = next; return race })
			kube, err := kubernetes.NewForConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			rpc := volumeRemovalRPC(t, New(Options{Clientset: kube, Namespace: server.namespace, Logger: zap.NewNop()}))
			resp, err := rpc.RemoveVolumeChecked(ctx, req)
			if changed.Load() != nil {
				owned[pvc.Name] = changed.Load().UID
			}
			if status.Code(err) != codes.Aborted || resp != nil || race.response.Load() != http.StatusConflict || changed.Load() == nil {
				t.Fatalf("Kubernetes did not reject stale atomic deletion: resp=%v err=%v HTTP=%d", resp, err, race.response.Load())
			}
			kept, err := claims.Get(ctx, pvc.Name, metav1.GetOptions{})
			if err != nil || kept.UID != changed.Load().UID || kept.DeletionTimestamp != nil {
				t.Fatalf("raced claim was deleted: %v", err)
			}
			resp, err = rpc.RemoveVolumeChecked(ctx, req)
			if status.Code(err) != codes.FailedPrecondition || resp != nil {
				t.Fatalf("retry adopted raced ownership: %v %v", resp, err)
			}
			t.Logf("Kubernetes HTTP 409 rejected stale UID/resourceVersion; old UID=%s retained UID=%s", pvc.UID, kept.UID)
		})
	}
}
