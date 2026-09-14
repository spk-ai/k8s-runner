package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

// Only synthetic credentials and unbound PVCs in a new owned namespace. A zero
// Pod quota stays installed throughout; no image or native agent should run.
func TestLiveStartupSecretCleanup(t *testing.T) {
	if os.Getenv("RUNNER_LIVE_STARTUP_TEST") != "trusted-local" {
		t.Skip("requires explicit trusted-local Kubernetes startup acceptance")
	}
	kubeconfig := os.Getenv("RUNNER_LIVE_KUBECONFIG")
	if !filepath.IsAbs(kubeconfig) {
		t.Fatal("RUNNER_LIVE_KUBECONFIG must explicitly select an absolute kubeconfig")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 10 * time.Second
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runID := uuid.NewString()
	const ownerLabel = "agyn.io/startup-test"
	namespace := "runner-startup-" + runID[:12]
	storageClass := "unprovisioned-" + runID
	if _, err := kube.StorageV1().StorageClasses().Get(ctx, storageClass, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("fixture requires its unique storage class to be absent")
	}
	var nsUID, quotaUID types.UID
	ownedClaims := map[string]types.UID{}
	ownedWorkloads := map[string]bool{}
	namespaceAttempted := false
	t.Cleanup(func() {
		if !namespaceAttempted {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		ns, err := kube.CoreV1().Namespaces().Get(cleanup, namespace, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil || ns.Labels[ownerLabel] != runID || nsUID != "" && ns.UID != nsUID {
			t.Errorf("namespace ownership unconfirmed; cleanup refused: %v", err)
			return
		}
		pods, pErr := kube.CoreV1().Pods(namespace).List(cleanup, metav1.ListOptions{})
		services, sErr := kube.CoreV1().Services(namespace).List(cleanup, metav1.ListOptions{})
		if pErr != nil || sErr != nil || len(pods.Items) != 0 || len(services.Items) != 0 {
			t.Error("unexpected Pods/Services; retaining fixture namespace")
			return
		}
		claims, err := kube.CoreV1().PersistentVolumeClaims(namespace).List(cleanup, metav1.ListOptions{})
		if err != nil {
			t.Error(err)
			return
		}
		for _, claim := range claims.Items {
			uid, planned := ownedClaims[claim.Name]
			if !planned || uid != "" && uid != claim.UID || claim.Labels[ownerLabel] != runID || claim.Spec.VolumeName != "" ||
				claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != storageClass {
				t.Errorf("foreign/replaced/bound PVC %s; retaining namespace", claim.Name)
				return
			}
		}
		secrets, err := kube.CoreV1().Secrets(namespace).List(cleanup, metav1.ListOptions{})
		if err != nil {
			t.Error(err)
			return
		}
		for _, secret := range secrets.Items {
			id := secret.Labels[workloadIDLabelKey]
			if !ownedWorkloads[id] || secret.Labels[managedByLabelKey] != managedByLabelValue || !strings.HasPrefix(secret.Name, "workload-"+id+"-") {
				t.Errorf("foreign Secret %s; retaining namespace", secret.Name)
				return
			}
		}
		quotas, err := kube.CoreV1().ResourceQuotas(namespace).List(cleanup, metav1.ListOptions{})
		if err != nil {
			t.Error(err)
			return
		}
		for _, quota := range quotas.Items {
			if quota.Name != "startup-proof" || quota.Labels[ownerLabel] != runID || quotaUID != "" && quota.UID != quotaUID {
				t.Error("foreign/replaced quota; retaining namespace")
				return
			}
		}
		if err := kube.CoreV1().Namespaces().Delete(cleanup, namespace, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &ns.UID, ResourceVersion: &ns.ResourceVersion},
		}); err != nil && !apierrors.IsNotFound(err) {
			t.Error(err)
			return
		}
		if err := wait.PollUntilContextCancel(cleanup, 250*time.Millisecond, true, func(ctx context.Context) (bool, error) {
			_, err := kube.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}); err != nil {
			t.Errorf("namespace deletion unconfirmed: %v", err)
		} else {
			t.Logf("cleanup confirmed namespace=%s uid=%s absent; no agent or existing PVC was touched", namespace, ns.UID)
		}
	})
	namespaceAttempted = true
	ns, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: namespace, Labels: map[string]string{ownerLabel: runID},
	}}, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			namespaceAttempted = false
		}
		t.Fatal(err)
	}
	nsUID = ns.UID
	t.Logf("startup run=%s namespace=%s uid=%s", runID, namespace, nsUID)
	budget := func(secrets, claims, storageMi int) corev1.ResourceList {
		return corev1.ResourceList{"count/pods": resource.MustParse("0"), "count/secrets": resource.MustParse(fmt.Sprint(secrets)),
			"persistentvolumeclaims": resource.MustParse(fmt.Sprint(claims)), "requests.storage": resource.MustParse(fmt.Sprintf("%dMi", storageMi))}
	}
	hard := budget(4, 2, 4)
	quota, err := kube.CoreV1().ResourceQuotas(namespace).Create(ctx, &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{
		Name: "startup-proof", Labels: map[string]string{ownerLabel: runID}}, Spec: corev1.ResourceQuotaSpec{Hard: hard}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	quotaUID = quota.UID
	equal := func(a, b corev1.ResourceList) bool {
		if len(a) != len(b) {
			return false
		}
		for name, amount := range a {
			other, ok := b[name]
			if !ok || amount.Cmp(other) != 0 {
				return false
			}
		}
		return true
	}
	waitUsage := func(t *testing.T, claims int) {
		t.Helper()
		err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
			current, err := kube.CoreV1().ResourceQuotas(namespace).Get(ctx, quota.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if current.UID != quotaUID || current.Labels[ownerLabel] != runID || !equal(current.Spec.Hard, hard) {
				return false, fmt.Errorf("quota changed outside this fixture")
			}
			used := budget(0, claims, claims)
			return equal(current.Status.Hard, hard) && equal(current.Status.Used, used), nil
		})
		if err != nil {
			t.Fatalf("quota hard/usage did not converge: %v", err)
		}
	}
	setBudget := func(t *testing.T, next corev1.ResourceList, claims int) {
		t.Helper()
		current, err := kube.CoreV1().ResourceQuotas(namespace).Get(ctx, quota.Name, metav1.GetOptions{})
		if err != nil || current.UID != quotaUID || current.Labels[ownerLabel] != runID || !equal(current.Spec.Hard, hard) {
			t.Fatalf("quota ownership/budget changed: %v", err)
		}
		current.Spec.Hard = next
		if _, err := kube.CoreV1().ResourceQuotas(namespace).Update(ctx, current, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		hard = next
		waitUsage(t, claims)
	}
	waitUsage(t, 0)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(grpc.WaitForHandlers(true))
	runnerv1.RegisterRunnerServiceServer(grpcServer, New(Options{Clientset: kube, Namespace: namespace,
		StorageSize: "1Mi", StorageClass: &storageClass, Logger: zap.NewNop()}))
	served := make(chan error, 1)
	go func() { served <- grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); <-served })
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	runner := runnerv1.NewRunnerServiceClient(conn)
	for _, stage := range []struct {
		name                         string
		secrets, claims, storageMi   int
		volumes, credentials, retain int
		reason                       string
	}{
		{"pull-first", 0, 2, 4, 1, 1, 0, "count/secrets"},
		{"pull-second", 1, 2, 4, 1, 2, 0, "count/secrets"},
		{"pvc-first", 4, 0, 4, 1, 1, 0, "persistentvolumeclaims"},
		{"storage", 4, 2, 0, 1, 1, 0, "requests.storage"},
		{"pvc-second", 4, 1, 4, 2, 1, 1, "persistentvolumeclaims"},
		{"inline", 1, 2, 4, 1, 1, 1, "count/secrets"},
		{"pod", 4, 2, 4, 2, 1, 2, "count/pods"},
	} {
		if !t.Run(stage.name, func(t *testing.T) {
			setBudget(t, budget(stage.secrets, stage.claims, stage.storageMi), 0)
			req := startupRequest()
			req.Main.Image = "registry.invalid/never-run:fixture"
			req.Labels = map[string]string{ownerLabel: runID}
			req.Volumes = nil
			for i := range stage.volumes {
				name := fmt.Sprintf("%s-%d", stage.name, i)
				ownedClaims[name] = ""
				req.Volumes = append(req.Volumes, &runnerv1.VolumeSpec{Name: name, PersistentName: name, Kind: runnerv1.VolumeKind_VOLUME_KIND_NAMED,
					Labels: map[string]string{volumeKeyLabelKey: uuid.NewString()}})
			}
			if stage.credentials == 2 {
				req.ImagePullCredentials = append(req.ImagePullCredentials, &runnerv1.ImagePullCredential{Registry: "second.invalid", Username: "fixture", Password: "second-fixture"})
			}
			startRejected := func(req *runnerv1.StartWorkloadRequest, reason string, expectedClaims int) []corev1.PersistentVolumeClaim {
				t.Helper()
				ownedWorkloads[req.WorkloadId] = true
				_, err := runner.StartWorkload(ctx, req)
				if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "exceeded quota: startup-proof") ||
					!strings.Contains(err.Error(), reason) || strings.Contains(err.Error(), "cleanup_unconfirmed") {
					t.Fatalf("expected native %s rejection and confirmed cleanup: %v", reason, err)
				}
				pods, pErr := kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
				secrets, sErr := kube.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{})
				claims, cErr := kube.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
				if pErr != nil || sErr != nil || cErr != nil || len(pods.Items) != 0 || len(secrets.Items) != 0 || len(claims.Items) != expectedClaims {
					t.Fatal("unexpected Pod, leaked startup Secret or missing durable PVC")
				}
				for _, claim := range claims.Items {
					if _, planned := ownedClaims[claim.Name]; !planned || claim.Labels[ownerLabel] != runID || claim.Spec.VolumeName != "" ||
						claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != storageClass {
						t.Fatal("PVC ownership/binding differs from the unprovisioned fixture")
					}
					ownedClaims[claim.Name] = claim.UID
				}
				waitUsage(t, expectedClaims)
				evidence, _ := json.Marshal(map[string]any{"workloadId": req.WorkloadId, "stage": stage.name, "rejection": reason,
					"pods": len(pods.Items), "secrets": len(secrets.Items), "retainedClaims": expectedClaims, "claims": ownedClaims})
				t.Logf("startup rejection verified %s", evidence)
				return claims.Items
			}
			claims := startRejected(req, stage.reason, stage.retain)
			if stage.name == "pvc-second" {
				first := claims[0].DeepCopy()
				setBudget(t, budget(4, 2, 4), 1)
				continuation := proto.Clone(req).(*runnerv1.StartWorkloadRequest)
				continuation.WorkloadId = uuid.NewString()
				claims = startRejected(continuation, "count/pods", 2)
				current, err := kube.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, first.Name, metav1.GetOptions{})
				if err != nil || current.UID != first.UID || !reflect.DeepEqual(current.Spec, first.Spec) {
					t.Fatalf("explicit continuation replaced/changed the partial workspace: %v", err)
				}
				t.Logf("explicit native continuation retained PVC name=%s uid=%s", first.Name, first.UID)
			}
			for _, claim := range claims {
				if err := kube.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, claim.Name, metav1.DeleteOptions{
					Preconditions: &metav1.Preconditions{UID: &claim.UID, ResourceVersion: &claim.ResourceVersion},
				}); err != nil {
					t.Fatal(err)
				}
			}
			waitUsage(t, 0)
		}) {
			return
		}
	}
}
