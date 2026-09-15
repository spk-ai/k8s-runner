package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

type preparedRaceTransport struct {
	next   http.RoundTripper
	path   string
	before func(context.Context) error
	called atomic.Bool
}

func (r *preparedRaceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPatch && req.URL.Path == r.path && r.called.CompareAndSwap(false, true) {
		if err := r.before(req.Context()); err != nil {
			return nil, err
		}
	}
	return r.next.RoundTrip(req)
}

// This creates and deletes only a new fixture namespace, its bounded model-free
// Pods/workspaces and a GET-only namespace grant. No platform workload is used.
func TestLivePreparedWorkloads(t *testing.T) {
	if os.Getenv("RUNNER_LIVE_PREPARED_TEST") != "trusted-local" {
		t.Skip("requires explicit trusted-local prepared-workload acceptance")
	}
	kubeconfig, image := os.Getenv("RUNNER_LIVE_KUBECONFIG"), os.Getenv("RUNNER_LIVE_NODE_IMAGE")
	if !filepath.IsAbs(kubeconfig) || !regexp.MustCompile(`^\S+@sha256:[a-f0-9]{64}$`).MatchString(image) {
		t.Fatal("require an absolute kubeconfig and digest-pinned model-free Node image")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 15 * time.Second
	admin, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	run := uuid.NewString()
	const ownerLabel = "agyn.io/prepared-workload-test"
	labels := map[string]string{ownerLabel: run}
	ns, err := admin.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "runner-prepared-", Labels: labels}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	backend := "kubernetes-namespace/v1/" + ns.Name + "/" + string(ns.UID)
	ownedPods, ownedClaims := map[string]types.UID{}, map[string]types.UID{}
	var bindings []*runnerv1.WorkloadBinding
	var role *rbacv1.ClusterRole
	var roleBinding *rbacv1.ClusterRoleBinding
	var server *Server
	serverFor := func(client kubernetes.Interface) *Server {
		return New(Options{Clientset: client, Namespace: ns.Name, StorageSize: "1Mi", Logger: zap.NewNop(),
			SupportingContainerResources: &config.ComputeResources{RequestsCPU: "50m", RequestsMemory: "64Mi", LimitsCPU: "250m", LimitsMemory: "128Mi"}})
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 100*time.Second)
		defer stop()
		current, err := admin.CoreV1().Namespaces().Get(cleanup, ns.Name, metav1.GetOptions{})
		if err != nil || current.UID != ns.UID || current.Labels[ownerLabel] != run {
			t.Errorf("fixture namespace identity changed; retaining resources: %v", err)
			return
		}
		pods, err := admin.CoreV1().Pods(ns.Name).List(cleanup, metav1.ListOptions{})
		if err != nil {
			t.Error(err)
			return
		}
		for _, pod := range pods.Items {
			if ownedPods[pod.Name] != pod.UID || pod.Labels[ownerLabel] != run {
				t.Error("foreign/replaced fixture Pod; cleanup refused")
				return
			}
		}
		// Only recorded bindings can release holds; never strip arbitrary finalizers.
		if server != nil {
			for _, binding := range bindings {
				err := wait.PollUntilContextTimeout(cleanup, 200*time.Millisecond, 40*time.Second, true, func(ctx context.Context) (bool, error) {
					resp, err := server.RemovePreparedWorkload(ctx, &runnerv1.RemovePreparedWorkloadRequest{Expected: binding})
					if status.Code(err) == codes.Aborted {
						return false, nil
					}
					return err == nil && resp.State == runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT, err
				})
				if err != nil {
					t.Errorf("bound cleanup unconfirmed: %v", err)
					return
				}
			}
		}
		claims, err := admin.CoreV1().PersistentVolumeClaims(ns.Name).List(cleanup, metav1.ListOptions{})
		if err != nil {
			t.Error(err)
			return
		}
		for _, pvc := range claims.Items {
			if ownedClaims[pvc.Name] != pvc.UID || pvc.Labels[ownerLabel] != run || slices.ContainsFunc(pvc.Finalizers, func(v string) bool { return strings.HasPrefix(v, preparedHoldPrefix) }) {
				t.Error("unowned claim or unreleased hold; cleanup refused")
				return
			}
		}
		if err := wait.PollUntilContextTimeout(cleanup, 200*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
			secrets, err := admin.CoreV1().Secrets(ns.Name).List(ctx, metav1.ListOptions{})
			return err == nil && len(secrets.Items) == 0, err
		}); err != nil {
			t.Errorf("Secret garbage collection unconfirmed; cleanup refused: %v", err)
			return
		}
		services, err := admin.CoreV1().Services(ns.Name).List(cleanup, metav1.ListOptions{})
		if err != nil || len(services.Items) != 0 {
			t.Errorf("unexpected Service; cleanup refused: %v", err)
			return
		}
		if err := admin.CoreV1().Namespaces().Delete(cleanup, ns.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &ns.UID, ResourceVersion: &current.ResourceVersion}}); err != nil {
			t.Error(err)
			return
		}
		if err := wait.PollUntilContextTimeout(cleanup, time.Second, 50*time.Second, true, func(ctx context.Context) (bool, error) {
			value, err := admin.CoreV1().Namespaces().Get(ctx, ns.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			if err == nil && value.UID != ns.UID {
				return false, fmt.Errorf("namespace replaced")
			}
			return false, err
		}); err != nil {
			t.Error(err)
			return
		}
		if roleBinding != nil {
			if err := admin.RbacV1().ClusterRoleBindings().Delete(cleanup, roleBinding.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &roleBinding.UID, ResourceVersion: &roleBinding.ResourceVersion}}); err != nil {
				t.Error(err)
				return
			}
			if _, err := admin.RbacV1().ClusterRoleBindings().Get(cleanup, roleBinding.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Error("namespace binding removal unconfirmed")
				return
			}
		}
		if role != nil {
			if err := admin.RbacV1().ClusterRoles().Delete(cleanup, role.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &role.UID, ResourceVersion: &role.ResourceVersion}}); err != nil {
				t.Error(err)
				return
			}
			if _, err := admin.RbacV1().ClusterRoles().Get(cleanup, role.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Error("namespace role removal unconfirmed")
				return
			}
		}
		t.Logf("fixture namespace %s uid=%s and GET-only RBAC confirmed absent", ns.Name, ns.UID)
	})
	t.Logf("prepared workload fixture run=%s namespace=%s uid=%s", run, ns.Name, ns.UID)
	account, err := admin.CoreV1().ServiceAccounts(ns.Name).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "runner", Labels: labels}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = admin.RbacV1().Roles(ns.Name).Create(ctx, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "runner", Labels: labels}, Rules: startupChartRules(t)}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	subject := rbacv1.Subject{Kind: "ServiceAccount", Name: account.Name, Namespace: ns.Name}
	_, err = admin.RbacV1().RoleBindings(ns.Name).Create(ctx, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "runner", Labels: labels}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "runner"}, Subjects: []rbacv1.Subject{subject}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	role, err = admin.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: ns.Name + "-backend", Labels: labels}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"namespaces"}, ResourceNames: []string{ns.Name}, Verbs: []string{"get"}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	roleBinding, err = admin.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: role.Name, Labels: labels}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name}, Subjects: []rbacv1.Subject{subject}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = admin.NetworkingV1().NetworkPolicies(ns.Name).Create(ctx, &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "deny-network", Labels: labels}, Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{}, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	runnerConfig := rest.CopyConfig(cfg)
	runnerConfig.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:" + ns.Name + ":" + account.Name, Groups: []string{"system:serviceaccounts", "system:serviceaccounts:" + ns.Name, "system:authenticated"}}
	client, err := kubernetes.NewForConfig(runnerConfig)
	if err != nil {
		t.Fatal(err)
	}
	server = serverFor(client)
	if _, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		t.Fatal("runner can list namespaces")
	}
	if _, err := client.CoreV1().Namespaces().Get(ctx, "default", metav1.GetOptions{}); !apierrors.IsForbidden(err) {
		t.Fatal("runner can read another namespace")
	}
	if _, err := client.CoreV1().Secrets(ns.Name).List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		t.Fatal("runner can list Secrets")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rpc := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		switch info.FullMethod {
		case runnerv1.RunnerService_PrepareWorkload_FullMethodName, runnerv1.RunnerService_ActivateWorkload_FullMethodName, runnerv1.RunnerService_InspectPreparedWorkload_FullMethodName, runnerv1.RunnerService_RemovePreparedWorkload_FullMethodName, runnerv1.RunnerService_RemoveVolumeBound_FullMethodName:
			return handler(ctx, req)
		default:
			return nil, status.Error(codes.PermissionDenied, "fixture_rpc_denied")
		}
	}))
	runnerv1.RegisterRunnerServiceServer(rpc, server)
	done := make(chan error, 1)
	go func() { done <- rpc.Serve(listener) }()
	t.Cleanup(func() { rpc.Stop(); <-done })
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	runner := runnerv1.NewRunnerServiceClient(connection)
	request := func(name, program string, expected []*runnerv1.VolumeListItem) *runnerv1.PrepareWorkloadRequest {
		req := preparedTestRequest()
		req.BackendId, req.ExpectedVolumes = backend, expected
		req.Workload.Labels[ownerLabel] = run
		req.Workload.Main.Image, req.Workload.Main.Entrypoint, req.Workload.Main.Cmd = image, "node", []string{"-e", program}
		req.Workload.Main.Resources = &runnerv1.ComputeResources{RequestsCpu: "50m", RequestsMemory: "64Mi", LimitsCpu: "250m", LimitsMemory: "128Mi"}
		req.Workload.Capabilities = []string{config.CapabilityComputeResources}
		req.Workload.InlineFiles = map[string][]byte{"/fixture-marker": []byte("prepared-fixture")}
		req.Workload.Main.InlineFileMounts = []*runnerv1.InlineFileMount{{Path: "/fixture-marker"}}
		req.Workload.Volumes[0].PersistentName = name
		req.Workload.Volumes[0].Labels[volumeKeyLabelKey] = name
		return req
	}
	prepare := func(t *testing.T, req *runnerv1.PrepareWorkloadRequest) *runnerv1.WorkloadBinding {
		t.Helper()
		response, err := runner.PrepareWorkload(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		binding := response.Binding
		bindings = append(bindings, binding)
		ownedPods[podNameFromID(binding.WorkloadId)] = types.UID(binding.InstanceUid)
		for _, claim := range binding.Volumes {
			ownedClaims[claim.InstanceId] = types.UID(claim.InstanceUid)
		}
		pod, err := admin.CoreV1().Pods(ns.Name).Get(ctx, podNameFromID(binding.WorkloadId), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range parseSecretAnnotation(pod.Annotations) {
			secret, err := admin.CoreV1().Secrets(ns.Name).Get(ctx, name, metav1.GetOptions{})
			if err != nil || len(secret.OwnerReferences) != 1 || string(secret.OwnerReferences[0].UID) != binding.InstanceUid || secret.OwnerReferences[0].Name != pod.Name {
				t.Fatalf("temporary Secret not bound to prepared Pod: %v", err)
			}
		}
		return binding
	}
	activate := func(t *testing.T, binding *runnerv1.WorkloadBinding) {
		t.Helper()
		if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
			resp, err := runner.ActivateWorkload(ctx, &runnerv1.ActivateWorkloadRequest{Expected: binding})
			// JSON-Patch test failures may use Invalid rather than Conflict. Retry
			// only this immutable activation, never preparation or a task message.
			if status.Code(err) == codes.Aborted || status.Code(err) == codes.InvalidArgument {
				return false, nil
			}
			return err == nil && proto.Equal(resp.GetBinding(), binding), err
		}); err != nil {
			t.Fatal(err)
		}
	}
	inspect := func(t *testing.T, binding *runnerv1.WorkloadBinding, activated bool) {
		t.Helper()
		if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
			response, err := runner.InspectPreparedWorkload(ctx, &runnerv1.InspectPreparedWorkloadRequest{Expected: binding})
			if status.Code(err) == codes.Aborted {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if !proto.Equal(response.GetBinding(), binding) || response.GetWorkload().GetId() != binding.WorkloadId || response.GetResourceVersion() == "" || response.GetActivated() != activated || response.GetRemovalPending() {
				return false, fmt.Errorf("inspection did not preserve exact binding/state")
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	remove := func(t *testing.T, binding *runnerv1.WorkloadBinding) {
		t.Helper()
		if err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 40*time.Second, true, func(ctx context.Context) (bool, error) {
			resp, err := runner.RemovePreparedWorkload(ctx, &runnerv1.RemovePreparedWorkloadRequest{Expected: binding})
			if status.Code(err) == codes.Aborted {
				return false, nil
			}
			return err == nil && resp.GetState() == runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT, err
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.CoreV1().Pods(ns.Name).Get(ctx, podNameFromID(binding.WorkloadId), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("independent Pod absence not confirmed: %v", err)
		}
	}
	recordInterrupted := func(t *testing.T, req *runnerv1.PrepareWorkloadRequest) (*corev1.Pod, *runnerv1.WorkloadBinding) {
		t.Helper()
		pod, err := admin.CoreV1().Pods(ns.Name).Get(ctx, podNameFromID(req.Workload.WorkloadId), metav1.GetOptions{})
		if err != nil || pod.Labels[ownerLabel] != run || !hasPreparedGate(pod) || pod.Spec.NodeName != "" || len(pod.Status.ContainerStatuses) != 0 || len(pod.Status.InitContainerStatuses) != 0 {
			t.Fatalf("interrupted fixture lost identity or executed: %v", err)
		}
		binding := preparedBindingFromPod(t, pod)
		if binding.WorkloadId != req.Workload.WorkloadId || binding.BackendId != backend || len(binding.Volumes) != 1 || binding.Volumes[0].InstanceId != req.Workload.Volumes[0].PersistentName {
			t.Fatal("interrupted fixture binding mismatch")
		}
		if err := server.matchPreparedPod(pod, binding); err != nil {
			t.Fatal(err)
		}
		bindings = append(bindings, binding)
		ownedPods[pod.Name] = pod.UID
		for _, target := range binding.Volumes {
			claim, err := admin.CoreV1().PersistentVolumeClaims(ns.Name).Get(ctx, target.InstanceId, metav1.GetOptions{})
			if err != nil || claim.Labels[ownerLabel] != run {
				t.Fatalf("interrupted claim ownership unconfirmed: %v", err)
			}
			if err := matchPreparedPVC(claim, target, ns.Name); err != nil {
				t.Fatal(err)
			}
			ownedClaims[claim.Name] = claim.UID
		}
		return pod, binding
	}
	waitSecretsAbsent := func(t *testing.T, names []string) {
		t.Helper()
		if err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
			for _, name := range names {
				_, err := admin.CoreV1().Secrets(ns.Name).Get(ctx, name, metav1.GetOptions{})
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, err
			}
			return true, nil
		}); err != nil {
			t.Fatalf("exact fixture Secret absence unconfirmed: %v", err)
		}
	}
	waitSucceeded := func(t *testing.T, binding *runnerv1.WorkloadBinding) {
		t.Helper()
		if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 75*time.Second, true, func(ctx context.Context) (bool, error) {
			pod, err := admin.CoreV1().Pods(ns.Name).Get(ctx, podNameFromID(binding.WorkloadId), metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if string(pod.UID) != binding.InstanceUid || pod.Status.Phase == corev1.PodFailed {
				return false, fmt.Errorf("probe failed/replaced: phase=%s", pod.Status.Phase)
			}
			return pod.Status.Phase == corev1.PodSucceeded, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("real-execution-only-after-activation-and-durable-resume", func(t *testing.T) {
		first := prepare(t, request("durable", `require('fs').appendFileSync('/workspace/turns', 'first\n')`, nil))
		inspect(t, first, false)
		for i := 0; i < 12; i++ {
			pod, err := admin.CoreV1().Pods(ns.Name).Get(ctx, podNameFromID(first.WorkloadId), metav1.GetOptions{})
			if err != nil || string(pod.UID) != first.InstanceUid || !hasPreparedGate(pod) || pod.Spec.NodeName != "" || len(pod.Status.ContainerStatuses) != 0 || len(pod.Status.InitContainerStatuses) != 0 {
				t.Fatalf("prepared Pod executed before activation: %v", err)
			}
			time.Sleep(250 * time.Millisecond)
		}
		activate(t, first)
		waitSucceeded(t, first)
		inspect(t, first, true)
		remove(t, first)
		if _, err := runner.InspectPreparedWorkload(ctx, &runnerv1.InspectPreparedWorkloadRequest{Expected: first}); status.Code(err) != codes.NotFound {
			t.Fatalf("removed Pod inspection: %v", err)
		}
		second := prepare(t, request("durable", `const fs=require('fs'); if(fs.readFileSync('/workspace/turns','utf8')!=='first\n') process.exit(7); fs.appendFileSync('/workspace/turns','second\n'); if(fs.readFileSync('/workspace/turns','utf8')!=='first\nsecond\n') process.exit(8)`, first.Volumes))
		if first.InstanceUid == second.InstanceUid || !proto.Equal(first.Volumes[0], second.Volumes[0]) {
			t.Fatal("Pod replacement changed the durable workspace")
		}
		activate(t, second)
		waitSucceeded(t, second)
		inspect(t, second, true)
		remove(t, second)
		t.Logf("12 gated observations; two real successful turns; distinct Pod UIDs; same PVC uid=%s; both removals confirmed", first.Volumes[0].InstanceUid)
	})
	t.Run("PVC-delete-between-hold-and-activation", func(t *testing.T) {
		binding := prepare(t, request("delete-race", `process.exit(9)`, nil))
		var race *preparedRaceTransport
		raceConfig := rest.CopyConfig(runnerConfig)
		raceConfig.Wrap(func(next http.RoundTripper) http.RoundTripper {
			race = &preparedRaceTransport{next: next, path: "/api/v1/namespaces/" + ns.Name + "/pods/" + podNameFromID(binding.WorkloadId), before: func(ctx context.Context) error {
				pvc, err := admin.CoreV1().PersistentVolumeClaims(ns.Name).Get(ctx, "delete-race", metav1.GetOptions{})
				if err != nil {
					return err
				}
				if string(pvc.UID) != binding.Volumes[0].InstanceUid || !slices.Contains(pvc.Finalizers, preparedHoldPrefix+binding.InstanceUid) {
					return fmt.Errorf("activation omitted PVC hold")
				}
				if err := admin.CoreV1().PersistentVolumeClaims(ns.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pvc.UID, ResourceVersion: &pvc.ResourceVersion}}); err != nil {
					return err
				}
				current, err := admin.CoreV1().PersistentVolumeClaims(ns.Name).Get(ctx, pvc.Name, metav1.GetOptions{})
				if err != nil || current.UID != pvc.UID || current.DeletionTimestamp == nil {
					return fmt.Errorf("hold did not preserve terminating PVC: %v", err)
				}
				pvc.UID, pvc.ResourceVersion, pvc.Finalizers = "", "", nil
				pvc.Spec.VolumeName, pvc.Status = "", corev1.PersistentVolumeClaimStatus{}
				if _, err := admin.CoreV1().PersistentVolumeClaims(ns.Name).Create(ctx, pvc, metav1.CreateOptions{}); !apierrors.IsAlreadyExists(err) {
					return fmt.Errorf("replacement claim not excluded: %v", err)
				}
				return nil
			}}
			return race
		})
		raceClient, err := kubernetes.NewForConfig(raceConfig)
		if err != nil {
			t.Fatal(err)
		}
		_, err = serverFor(raceClient).ActivateWorkload(ctx, &runnerv1.ActivateWorkloadRequest{Expected: binding})
		if err != nil && status.Code(err) != codes.InvalidArgument && status.Code(err) != codes.Aborted {
			t.Fatal(err)
		}
		if race == nil || !race.called.Load() {
			t.Fatal("real activation PATCH was not intercepted")
		}
		pod, err := admin.CoreV1().Pods(ns.Name).Get(ctx, podNameFromID(binding.WorkloadId), metav1.GetOptions{})
		if err != nil || pod.Spec.NodeName != "" || len(pod.Status.ContainerStatuses) != 0 {
			t.Fatalf("terminating claim executed: %v", err)
		}
		remove(t, binding)
		t.Log("real DELETE stayed pending under the hold; same-name PVC replacement rejected; workload removed without execution")
	})
	t.Run("stale-UID-patch-does-not-activate-replacement", func(t *testing.T) {
		binding := prepare(t, request("stale-pod", `process.exit(9)`, nil))
		var race *preparedRaceTransport
		raceConfig := rest.CopyConfig(runnerConfig)
		raceConfig.Wrap(func(next http.RoundTripper) http.RoundTripper {
			race = &preparedRaceTransport{next: next, path: "/api/v1/namespaces/" + ns.Name + "/pods/" + podNameFromID(binding.WorkloadId), before: func(ctx context.Context) error {
				pod, err := admin.CoreV1().Pods(ns.Name).Get(ctx, podNameFromID(binding.WorkloadId), metav1.GetOptions{})
				if err != nil || string(pod.UID) != binding.InstanceUid {
					return fmt.Errorf("fixture Pod missing: %v", err)
				}
				if err := admin.CoreV1().Pods(ns.Name).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID, ResourceVersion: &pod.ResourceVersion}}); err != nil {
					return err
				}
				if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
					_, err := admin.CoreV1().Pods(ns.Name).Get(ctx, pod.Name, metav1.GetOptions{})
					return apierrors.IsNotFound(err), nil
				}); err != nil {
					return err
				}
				pod.UID, pod.ResourceVersion, pod.Status = "", "", corev1.PodStatus{}
				replacement, err := admin.CoreV1().Pods(ns.Name).Create(ctx, pod, metav1.CreateOptions{})
				if err != nil {
					return err
				}
				ownedPods[pod.Name] = replacement.UID
				nextBinding := proto.Clone(binding).(*runnerv1.WorkloadBinding)
				nextBinding.InstanceUid = string(replacement.UID)
				bindings = append([]*runnerv1.WorkloadBinding{nextBinding}, bindings...)
				return nil
			}}
			return race
		})
		raceClient, err := kubernetes.NewForConfig(raceConfig)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := serverFor(raceClient).ActivateWorkload(ctx, &runnerv1.ActivateWorkloadRequest{Expected: binding}); err == nil {
			t.Fatal("stale activation succeeded")
		}
		if race == nil || !race.called.Load() {
			t.Fatal("activation PATCH was not intercepted")
		}
		pod, err := admin.CoreV1().Pods(ns.Name).Get(ctx, podNameFromID(binding.WorkloadId), metav1.GetOptions{})
		if err != nil || string(pod.UID) == binding.InstanceUid || !hasPreparedGate(pod) || pod.Spec.NodeName != "" {
			t.Fatalf("replacement Pod was activated: %v", err)
		}
		if response, err := runner.InspectPreparedWorkload(ctx, &runnerv1.InspectPreparedWorkloadRequest{Expected: binding}); response != nil || status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("same-name replacement inspected as original: %v", err)
		}
		t.Log("real UID/RV PATCH rejected after same-name Pod replacement; replacement remained gated")
	})
	for _, stage := range []string{"pod-reply", "first-secret-reply", "last-secret-reply", "readiness-reply"} {
		t.Run("SIGKILL-"+stage, func(t *testing.T) {
			req := request("crash-"+stage, `process.exit(9)`, nil)
			req.Workload.ImagePullCredentials = preparedSecretsRequest().Workload.ImagePullCredentials
			data, err := protojson.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			killPreparedSecretProcess(t, preparedSecretCrashSpec{Kubeconfig: kubeconfig, Namespace: ns.Name, Account: account.Name, Stage: stage, Request: data})
			pod, binding := recordInterrupted(t, req)
			expectedState, expectedSecrets := "preparing", 2
			if stage == "pod-reply" {
				expectedSecrets = 0
			} else if stage == "first-secret-reply" {
				expectedSecrets = 1
			} else if stage == "readiness-reply" {
				expectedState = "prepared"
			}
			if pod.Annotations[preparedStateAnnotation] != expectedState {
				t.Fatal("crash lost the preparation checkpoint")
			}
			count := 0
			for _, name := range parseSecretAnnotation(pod.Annotations) {
				secret, err := admin.CoreV1().Secrets(ns.Name).Get(ctx, name, metav1.GetOptions{})
				if apierrors.IsNotFound(err) {
					continue
				}
				if err != nil || len(secret.OwnerReferences) != 1 || secret.OwnerReferences[0].APIVersion != "v1" || secret.OwnerReferences[0].Kind != "Pod" || secret.OwnerReferences[0].UID != pod.UID || secret.OwnerReferences[0].Name != pod.Name {
					t.Fatalf("crash left an unowned credential: %v", err)
				}
				count++
			}
			if count != expectedSecrets {
				t.Fatalf("wrong committed Secret count: got %d want %d", count, expectedSecrets)
			}
			if expectedState == "preparing" {
				if _, err := runner.ActivateWorkload(ctx, &runnerv1.ActivateWorkloadRequest{Expected: binding}); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("interrupted setup could activate: %v", err)
				}
			}
			remove(t, binding)
			waitSecretsAbsent(t, parseSecretAnnotation(pod.Annotations))
			t.Logf("SIGKILL after %s; state=%s; %d atomically owned Secrets; exact Pod removal and Secret GC confirmed", stage, expectedState, count)
		})
	}
	t.Run("delayed-secret-create-after-owner-removal", func(t *testing.T) {
		req := request("late-secret", `process.exit(9)`, nil)
		req.Workload.ImagePullCredentials = preparedSecretsRequest().Workload.ImagePullCredentials
		var original *corev1.Pod
		var intercepted atomic.Bool
		var secretCommits atomic.Int32
		raceConfig := rest.CopyConfig(runnerConfig)
		raceConfig.Wrap(func(next http.RoundTripper) http.RoundTripper {
			return preparedSecretTransport(func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodPost && request.URL.Path == "/api/v1/namespaces/"+ns.Name+"/secrets" && intercepted.CompareAndSwap(false, true) {
					var binding *runnerv1.WorkloadBinding
					original, binding = recordInterrupted(t, req)
					remove(t, binding)
				}
				response, err := next.RoundTrip(request)
				if err == nil && request.Method == http.MethodPost && request.URL.Path == "/api/v1/namespaces/"+ns.Name+"/secrets" && response.StatusCode >= 200 && response.StatusCode < 300 {
					secretCommits.Add(1)
				}
				return response, err
			})
		})
		raceClient, err := kubernetes.NewForConfig(raceConfig)
		if err != nil {
			t.Fatal(err)
		}
		if response, err := serverFor(raceClient).PrepareWorkload(ctx, req); err == nil || response != nil || !intercepted.Load() {
			t.Fatalf("late creation unexpectedly completed preparation: %v", err)
		}
		if secretCommits.Load() != 2 {
			t.Fatalf("late Secret writes were not both committed: %d", secretCommits.Load())
		}
		waitSecretsAbsent(t, parseSecretAnnotation(original.Annotations))
		if _, err := admin.CoreV1().Pods(ns.Name).Get(ctx, original.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("late Secret operation recreated the Pod: %v", err)
		}
		t.Log("held Secret CREATE completed after exact owner deletion; no Pod recreated; late credentials garbage-collected")
	})
}
