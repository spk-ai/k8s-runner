// SPDX-License-Identifier: AGPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

// Lab-only integration of the independently reviewable quota and resource patches.
// No agent, provider credentials, PVCs, host mounts, stress loops or default kubecontext.
func TestLiveWorkloadQuota(t *testing.T) {
	if os.Getenv("RUNNER_LIVE_QUOTA_TEST") != "trusted-local" {
		t.Skip("requires an explicit trusted-local Kubernetes quota test")
	}
	kubeconfig, image := os.Getenv("RUNNER_LIVE_KUBECONFIG"), os.Getenv("RUNNER_LIVE_NODE_IMAGE")
	if !filepath.IsAbs(kubeconfig) || !regexp.MustCompile(`^\S+@sha256:[a-f0-9]{64}$`).MatchString(image) {
		t.Fatal("require an absolute RUNNER_LIVE_KUBECONFIG and digest-pinned, UID-1000 RUNNER_LIVE_NODE_IMAGE")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 15 * time.Second
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	runID := uuid.NewString()
	const ownerLabel = "agyn.io/quota-test"
	ns, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		GenerateName: "runner-quota-", Labels: map[string]string{ownerLabel: runID},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("quota run=%s namespace=%s uid=%s image=%s", runID, ns.Name, ns.UID, image)
	owned := map[string]types.UID{}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		current, err := kube.CoreV1().Namespaces().Get(cleanup, ns.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil || current.UID != ns.UID || current.Labels[ownerLabel] != runID {
			t.Errorf("namespace ownership unknown or changed; cleanup refused: %v", err)
			return
		}
		pods, err := kube.CoreV1().Pods(ns.Name).List(cleanup, metav1.ListOptions{})
		if err != nil {
			t.Error(err)
			return
		}
		for _, pod := range pods.Items {
			uid, known := owned[pod.Name]
			if !known || pod.Labels[ownerLabel] != runID || uid != "" && uid != pod.UID {
				t.Errorf("foreign/replaced Pod %s; retaining namespace", pod.Name)
				return
			}
		}
		claims, claimErr := kube.CoreV1().PersistentVolumeClaims(ns.Name).List(cleanup, metav1.ListOptions{})
		secrets, secretErr := kube.CoreV1().Secrets(ns.Name).List(cleanup, metav1.ListOptions{})
		services, serviceErr := kube.CoreV1().Services(ns.Name).List(cleanup, metav1.ListOptions{})
		if claimErr != nil || secretErr != nil || serviceErr != nil || len(claims.Items) != 0 || len(secrets.Items) != 0 || len(services.Items) != 0 {
			t.Error("unexpected/unreadable PVCs, secrets or Services; retaining namespace")
			return
		}
		for _, pod := range pods.Items {
			if err := kube.CoreV1().Pods(ns.Name).Delete(cleanup, pod.Name, metav1.DeleteOptions{
				GracePeriodSeconds: ptr.To(int64(1)), Preconditions: &metav1.Preconditions{UID: &pod.UID},
			}); err != nil && !apierrors.IsNotFound(err) {
				t.Error(err)
				return
			}
		}
		if err := kube.CoreV1().Namespaces().Delete(cleanup, ns.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &ns.UID},
		}); err != nil {
			t.Error(err)
			return
		}
		if err := wait.PollUntilContextCancel(cleanup, time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := kube.CoreV1().Namespaces().Get(ctx, ns.Name, metav1.GetOptions{})
			return apierrors.IsNotFound(err), nil
		}); err != nil {
			t.Errorf("namespace removal unconfirmed: %v", err)
		} else {
			t.Logf("cleanup confirmed namespace=%s uid=%s absent; no PVC was created or deleted", ns.Name, ns.UID)
		}
	})
	if _, err := kube.NetworkingV1().NetworkPolicies(ns.Name).Create(ctx, &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "deny-network", Labels: map[string]string{ownerLabel: runID}},
		Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	hard := corev1.ResourceList{
		"requests.cpu": resource.MustParse("1"), "requests.memory": resource.MustParse("512Mi"),
		"limits.cpu": resource.MustParse("3"), "limits.memory": resource.MustParse("768Mi"), "count/pods": resource.MustParse("6"),
	}
	values, err := json.Marshal(map[string]any{"workloadNamespace": ns.Name,
		"env":                   []corev1.EnvVar{{Name: "KUBE_NAMESPACE", Value: ns.Name}},
		"workloadResourceQuota": map[string]any{"enabled": true, "name": "quota-proof", "hard": hard},
	})
	if err != nil {
		t.Fatal(err)
	}
	valuesFile := filepath.Join(t.TempDir(), "values.json")
	if err := os.WriteFile(valuesFile, values, 0600); err != nil {
		t.Fatal(err)
	}
	rendered, err := exec.CommandContext(ctx, "helm", "template", "quota-proof", "../../charts/k8s-runner",
		"--namespace", "runner-platform", "-f", valuesFile, "--show-only", "templates/workload-resourcequota.yaml").CombinedOutput()
	if err != nil {
		t.Fatalf("render quota: %v\n%s", err, rendered)
	}
	var quota corev1.ResourceQuota
	if err := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(rendered), 4096).Decode(&quota); err != nil {
		t.Fatal(err)
	}
	if quota.Namespace != ns.Name || !quotaResourcesEqual(quota.Spec.Hard, hard) || len(quota.Spec.Scopes) != 0 || quota.Spec.ScopeSelector != nil {
		t.Fatal("rendered quota does not match the unscoped fixture budget")
	}
	quota.Labels[ownerLabel] = runID
	createdQuota, err := kube.CoreV1().ResourceQuotas(ns.Name).Create(ctx, &quota, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitUsage := func(expected corev1.ResourceList) {
		t.Helper()
		var latest *corev1.ResourceQuota
		err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			var err error
			latest, err = kube.CoreV1().ResourceQuotas(ns.Name).Get(ctx, quota.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if latest.UID != createdQuota.UID || latest.Labels[ownerLabel] != runID {
				return false, fmt.Errorf("quota ownership changed")
			}
			return quotaResourcesEqual(latest.Status.Hard, quota.Spec.Hard) && quotaResourcesEqual(latest.Status.Used, expected), nil
		})
		if err != nil {
			t.Fatalf("quota usage did not converge: %v; quota=%+v", err, latest)
		}
		data, _ := json.Marshal(latest.Status)
		t.Logf("quota uid=%s status=%s", latest.UID, data)
	}
	zero := corev1.ResourceList{}
	for name := range hard {
		zero[name] = resource.MustParse("0")
	}
	waitUsage(zero)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	defaults := &config.ComputeResources{RequestsCPU: "50m", RequestsMemory: "32Mi", LimitsCPU: "500m", LimitsMemory: "64Mi"}
	runnerv1.RegisterRunnerServiceServer(grpcServer, New(Options{Clientset: kube, RestConfig: cfg, Namespace: ns.Name,
		Logger: zap.NewNop(), SupportingContainerResources: defaults}))
	served := make(chan error, 1)
	go func() { served <- grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); <-served })
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	runner := runnerv1.NewRunnerServiceClient(conn)
	container := func(name string, bounds *runnerv1.ComputeResources, done bool) *runnerv1.ContainerSpec {
		effective := bounds
		if effective == nil {
			effective = &runnerv1.ComputeResources{LimitsCpu: defaults.LimitsCPU, LimitsMemory: defaults.LimitsMemory}
		}
		cpu, memory := resource.MustParse(effective.LimitsCpu), resource.MustParse(effective.LimitsMemory)
		program := fmt.Sprintf(quotaProbeProgram, cpu.MilliValue(), memory.Value(), runID, done)
		return &runnerv1.ContainerSpec{Name: name, Image: image, Entrypoint: "node", Cmd: []string{"-e", program}, Resources: bounds}
	}
	request := func(heavy, done bool) *runnerv1.StartWorkloadRequest {
		id := uuid.NewString()
		owned[podNameFromID(id)] = ""
		main := container("main", &runnerv1.ComputeResources{
			RequestsCpu: "100m", RequestsMemory: "64Mi", LimitsCpu: "250m", LimitsMemory: "128Mi",
		}, done)
		req := &runnerv1.StartWorkloadRequest{WorkloadId: id, Main: main,
			Capabilities: []string{config.CapabilityComputeResources}, Labels: map[string]string{ownerLabel: runID}}
		if heavy {
			req.Sidecars = []*runnerv1.ContainerSpec{container("helper", nil, false)}
			restartable := container("restartable", nil, false)
			restartable.AdditionalProperties = map[string]string{"restart_policy": "Always"}
			req.InitContainers = []*runnerv1.ContainerSpec{restartable, container("init", &runnerv1.ComputeResources{
				RequestsCpu: "400m", RequestsMemory: "192Mi", LimitsCpu: "1", LimitsMemory: "256Mi",
			}, true)}
		}
		return req
	}
	readProbe := func(pod *corev1.Pod, name string) (quotaProbeSnapshot, error) {
		data, err := kube.CoreV1().Pods(ns.Name).GetLogs(pod.Name, &corev1.PodLogOptions{
			Container: name, TailLines: ptr.To(int64(1)), LimitBytes: ptr.To(int64(4096)),
		}).DoRaw(ctx)
		var snapshot quotaProbeSnapshot
		if err == nil {
			err = json.Unmarshal(bytes.TrimSpace(data), &snapshot)
		}
		return snapshot, err
	}
	started := func(req *runnerv1.StartWorkloadRequest) *corev1.Pod {
		t.Helper()
		var pod *corev1.Pod
		err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 60*time.Second, true, func(ctx context.Context) (bool, error) {
			var err error
			pod, err = kube.CoreV1().Pods(ns.Name).Get(ctx, podNameFromID(req.WorkloadId), metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if pod.Labels[ownerLabel] != runID || pod.Spec.Containers[0].Image != image || pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken || len(pod.Spec.Volumes) != 0 {
				return false, fmt.Errorf("unexpected Pod binding, credentials or volumes")
			}
			if uid := owned[pod.Name]; uid != "" && uid != pod.UID {
				return false, fmt.Errorf("pod identity changed")
			}
			owned[pod.Name] = pod.UID
			if pod.Status.Phase == corev1.PodFailed {
				return false, fmt.Errorf("probe pod failed: %+v", pod.Status)
			}
			probe, err := readProbe(pod, "main")
			if err != nil {
				return false, nil
			}
			return probe.UID == 1000 && probe.Nonce == runID && probe.Beat > 0, nil
		})
		if err != nil {
			t.Fatalf("probe did not start: %v; pod=%+v", err, pod)
		}
		t.Logf("admitted workload=%s pod=%s uid=%s", req.WorkloadId, pod.Name, pod.UID)
		return pod
	}
	start := func(req *runnerv1.StartWorkloadRequest) *corev1.Pod {
		t.Helper()
		if _, err := runner.StartWorkload(ctx, req); err != nil {
			t.Fatal(err)
		}
		return started(req)
	}
	stop := func(pod *corev1.Pod) {
		t.Helper()
		if _, err := runner.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{WorkloadId: pod.Labels[workloadIDLabelKey], TimeoutSec: 1}); err != nil {
			t.Fatal(err)
		}
		if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			current, err := kube.CoreV1().Pods(ns.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			if err == nil && current.UID != pod.UID {
				return false, fmt.Errorf("pod identity changed during stop")
			}
			return false, err
		}); err != nil {
			t.Fatal(err)
		}
		t.Logf("physical removal confirmed pod=%s uid=%s", pod.Name, pod.UID)
	}
	setBudget := func(limits corev1.ResourceList) {
		t.Helper()
		current, err := kube.CoreV1().ResourceQuotas(ns.Name).Get(ctx, quota.Name, metav1.GetOptions{})
		if err != nil || current.UID != createdQuota.UID || current.Labels[ownerLabel] != runID {
			t.Fatalf("quota identity unavailable: %v", err)
		}
		current.Spec.Hard = limits.DeepCopy()
		updated, err := kube.CoreV1().ResourceQuotas(ns.Name).Update(ctx, current, metav1.UpdateOptions{})
		if err != nil || updated.UID != createdQuota.UID {
			t.Fatalf("quota update unconfirmed: %v", err)
		}
		quota.Spec.Hard = limits.DeepCopy()
	}
	reject := func(req *runnerv1.StartWorkloadRequest, field string) {
		t.Helper()
		_, err := runner.StartWorkload(ctx, req)
		if status.Code(err) != codes.PermissionDenied || !strings.Contains(status.Convert(err).Message(), "exceeded quota: quota-proof") || !strings.Contains(status.Convert(err).Message(), field) {
			t.Fatalf("want native quota rejection for %s, got %v", field, err)
		}
		if _, err := kube.CoreV1().Pods(ns.Name).Get(ctx, podNameFromID(req.WorkloadId), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("rejected Pod absence unconfirmed: %v", err)
		}
		t.Logf("rejected before execution workload=%s field=%s", req.WorkloadId, field)
	}
	heavy, neighbor := start(request(true, false)), start(request(false, false))
	used := corev1.ResourceList{
		"requests.cpu": resource.MustParse("550m"), "requests.memory": resource.MustParse("288Mi"),
		"limits.cpu": resource.MustParse("1750m"), "limits.memory": resource.MustParse("448Mi"), "count/pods": resource.MustParse("2"),
	}
	waitUsage(used)
	for _, name := range []string{"main", "helper", "restartable", "init"} {
		probe, err := readProbe(heavy, name)
		if err != nil || probe.UID != 1000 || probe.Nonce != runID || probe.Beat < 1 {
			t.Fatalf("missing real supporting-container cgroup proof for %s: %+v %v", name, probe, err)
		}
		data, _ := json.Marshal(probe)
		t.Logf("container=%s kernel=%s", name, data)
	}
	before, err := readProbe(neighbor, "main")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []corev1.ResourceName{"requests.cpu", "requests.memory", "limits.cpu", "limits.memory"} {
		limits := hard.DeepCopy()
		limits[field] = used[field].DeepCopy()
		setBudget(limits)
		waitUsage(used)
		reject(request(false, false), string(field))
	}
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 10*time.Second, true, func(context.Context) (bool, error) {
		probe, err := readProbe(neighbor, "main")
		return probe.Beat > before.Beat, err
	}); err != nil {
		t.Fatal("admitted neighbor stopped progressing during quota rejections: ", err)
	}
	limits := hard.DeepCopy()
	limits["count/pods"] = resource.MustParse("2")
	setBudget(limits)
	waitUsage(used)
	reject(request(false, false), "count/pods")
	stop(heavy)
	neighborUsage := corev1.ResourceList{
		"requests.cpu": resource.MustParse("100m"), "requests.memory": resource.MustParse("64Mi"),
		"limits.cpu": resource.MustParse("250m"), "limits.memory": resource.MustParse("128Mi"), "count/pods": resource.MustParse("1"),
	}
	waitUsage(neighborUsage)
	finished := start(request(false, true))
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		pod, err := kube.CoreV1().Pods(ns.Name).Get(ctx, finished.Name, metav1.GetOptions{})
		return err == nil && pod.UID == finished.UID && pod.Status.Phase == corev1.PodSucceeded, err
	}); err != nil {
		t.Fatal(err)
	}
	retainedUsage := neighborUsage.DeepCopy()
	retainedUsage["count/pods"] = resource.MustParse("2")
	waitUsage(retainedUsage)
	reject(request(false, false), "count/pods")
	stop(finished)
	waitUsage(neighborUsage)
	type result struct {
		req *runnerv1.StartWorkloadRequest
		err error
	}
	gate, results := make(chan struct{}), make(chan result, 6)
	for n := 0; n < cap(results); n++ {
		req := request(false, false)
		go func() {
			<-gate
			_, err := runner.StartWorkload(ctx, req)
			results <- result{req, err}
		}()
	}
	close(gate)
	var admitted []*runnerv1.StartWorkloadRequest
	for n := 0; n < cap(results); n++ {
		result := <-results
		if result.err == nil {
			admitted = append(admitted, result.req)
		} else if status.Code(result.err) != codes.PermissionDenied || !strings.Contains(status.Convert(result.err).Message(), "count/pods") {
			t.Errorf("unexpected competing admission error: %v", result.err)
		} else if _, err := kube.CoreV1().Pods(ns.Name).Get(ctx, podNameFromID(result.req.WorkloadId), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Errorf("competing rejected Pod absence unconfirmed: %v", err)
		}
	}
	if t.Failed() || len(admitted) != 1 {
		t.Fatalf("six concurrent requests with one slot must admit exactly one, got %d", len(admitted))
	}
	winner := started(admitted[0])
	t.Log("concurrent admission: one accepted, five quota rejections, zero rejected Pods")
	stop(winner)
	stop(neighbor)
	waitUsage(zero)
}

func quotaResourcesEqual(actual, expected corev1.ResourceList) bool {
	if len(actual) != len(expected) {
		return false
	}
	for name, expectedValue := range expected {
		value, found := actual[name]
		if !found || value.Cmp(expectedValue) != 0 {
			return false
		}
	}
	return true
}

type quotaProbeSnapshot struct {
	UID        int    `json:"uid"`
	Millicores int64  `json:"millicores"`
	Memory     int64  `json:"memory"`
	Nonce      string `json:"nonce"`
	Beat       int    `json:"beat"`
}

const quotaProbeProgram = `
const fs=require("node:fs");
const [quota,period]=fs.readFileSync("/sys/fs/cgroup/cpu.max","utf8").trim().split(/\s+/).map(Number);
const millicores=1000*quota/period,memory=Number(fs.readFileSync("/sys/fs/cgroup/memory.max","utf8"));
if(process.getuid()!==1000||millicores!==%d||memory!==%d)throw Error("kernel bounds or user mismatch");
const nonce=%q;let beat=0;
const report=()=>console.log(JSON.stringify({uid:process.getuid(),millicores,memory,nonce,beat:++beat}));
report();if(!%t){setInterval(report,500);setTimeout(()=>process.exit(0),240000);}
`
