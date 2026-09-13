package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

// This deliberately performs a cgroup-bounded OOM kill. It is never part of
// ordinary CI, never uses the current kubectl context, and carries no secrets.
func TestLiveComputeResources(t *testing.T) {
	if os.Getenv("RUNNER_LIVE_RESOURCE_TEST") != "trusted-local" {
		t.Skip("requires an explicit trusted-local Kubernetes resource test")
	}
	kubeconfig, image := os.Getenv("RUNNER_LIVE_KUBECONFIG"), os.Getenv("RUNNER_LIVE_NODE_IMAGE")
	if !filepath.IsAbs(kubeconfig) || !regexp.MustCompile(`^\S+@sha256:[a-f0-9]{64}$`).MatchString(image) {
		t.Fatal("require an absolute RUNNER_LIVE_KUBECONFIG and digest-pinned RUNNER_LIVE_NODE_IMAGE")
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
	const ownerLabel = "agyn.io/resource-test"
	ns, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		GenerateName: "runner-resources-", Labels: map[string]string{ownerLabel: runID},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("resource test run=%s namespace=%s", runID, ns.Name)
	ownedNames := map[string]bool{}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		current, err := kube.CoreV1().Namespaces().Get(cleanup, ns.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil || current.UID != ns.UID || current.Labels[ownerLabel] != runID {
			t.Errorf("namespace ownership changed; cleanup refused: %v", err)
			return
		}
		pods, err := kube.CoreV1().Pods(ns.Name).List(cleanup, metav1.ListOptions{})
		if err != nil {
			t.Error(err)
			return
		}
		foreign := false
		for _, pod := range pods.Items {
			if !ownedNames[pod.Name] || pod.Labels[ownerLabel] != runID {
				foreign = true
				continue
			}
			err := kube.CoreV1().Pods(ns.Name).Delete(cleanup, pod.Name, metav1.DeleteOptions{
				GracePeriodSeconds: ptr.To(int64(1)), Preconditions: &metav1.Preconditions{UID: &pod.UID},
			})
			if err != nil && !apierrors.IsNotFound(err) {
				t.Error(err)
				return
			}
		}
		claims, err := kube.CoreV1().PersistentVolumeClaims(ns.Name).List(cleanup, metav1.ListOptions{})
		if err != nil || len(claims.Items) != 0 || foreign {
			t.Errorf("foreign pods/PVCs present or unreadable; retaining namespace %s: %v", ns.Name, err)
			return
		}
		secrets, err := kube.CoreV1().Secrets(ns.Name).List(cleanup, metav1.ListOptions{})
		if err != nil || len(secrets.Items) != 0 {
			t.Errorf("unexpected secrets present or unreadable; retaining namespace %s: %v", ns.Name, err)
			return
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
		}
	})
	_, err = kube.NetworkingV1().NetworkPolicies(ns.Name).Create(ctx, &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "deny-network", Labels: map[string]string{ownerLabel: runID}},
		Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	runnerv1.RegisterRunnerServiceServer(grpcServer, New(Options{Clientset: kube, RestConfig: cfg, Namespace: ns.Name,
		Logger: zap.NewNop(), SupportingContainerResources: &config.ComputeResources{
			RequestsCPU: "50m", RequestsMemory: "64Mi", LimitsCPU: "500m", LimitsMemory: "128Mi",
		}}))
	served := make(chan error, 1)
	go func() { served <- grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); <-served })
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	runner := runnerv1.NewRunnerServiceClient(conn)
	container := func(name, program string) *runnerv1.ContainerSpec {
		return &runnerv1.ContainerSpec{Name: name, Image: image, Entrypoint: "node", Cmd: []string{"-e", program}}
	}
	start := func(program string, withSupporting bool) *corev1.Pod {
		t.Helper()
		id := uuid.NewString()
		ownedNames[podNameFromID(id)] = true
		main := container("main", liveResourceProbe+program)
		main.Resources = &runnerv1.ComputeResources{RequestsCpu: "100m", RequestsMemory: "64Mi", LimitsCpu: "250m", LimitsMemory: "128Mi"}
		req := &runnerv1.StartWorkloadRequest{WorkloadId: id, Main: main,
			Capabilities: []string{config.CapabilityComputeResources}, Labels: map[string]string{ownerLabel: runID}}
		if withSupporting {
			probe := liveResourceProbe + `check(500); console.log(JSON.stringify({bounds: snapshot()}));`
			req.Sidecars = []*runnerv1.ContainerSpec{container("helper", probe)}
			restartable := container("restartable", probe+`setInterval(() => {}, 1000); setTimeout(() => process.exit(0), 180000);`)
			restartable.AdditionalProperties = map[string]string{"restart_policy": "Always"}
			req.InitContainers = []*runnerv1.ContainerSpec{container("init", probe), restartable}
		}
		if _, err := runner.StartWorkload(ctx, req); err != nil {
			t.Fatal(err)
		}
		pod, err := kube.CoreV1().Pods(ns.Name).Get(ctx, podNameFromID(id), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return pod
	}
	waitPod := func(pod *corev1.Pod, predicate func(*corev1.Pod) bool) *corev1.Pod {
		t.Helper()
		var latest *corev1.Pod
		err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 75*time.Second, true, func(ctx context.Context) (bool, error) {
			var err error
			latest, err = kube.CoreV1().Pods(ns.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if latest.UID != pod.UID {
				return false, fmt.Errorf("pod identity changed")
			}
			return predicate(latest), nil
		})
		if err != nil {
			var status any
			if latest != nil {
				status = latest.Status
			}
			t.Fatalf("waiting for %s: %v; status=%+v", pod.Name, err, status)
		}
		return latest
	}
	logs := func(pod *corev1.Pod, name string) string {
		t.Helper()
		data, err := kube.CoreV1().Pods(ns.Name).GetLogs(pod.Name, &corev1.PodLogOptions{Container: name, LimitBytes: ptr.To(int64(65536))}).DoRaw(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	control := start(`check(250); let n=0; console.log("beat:"+(++n)); setInterval(() => console.log("beat:"+(++n)), 250); setTimeout(() => process.exit(0), 180000);`, false)
	control = waitPod(control, func(p *corev1.Pod) bool {
		return len(p.Status.ContainerStatuses) == 1 && p.Status.ContainerStatuses[0].Ready
	})
	before := logs(control, "main")
	cpu := start(`check(250); const before=snapshot(); const began=performance.now(); const used=process.cpuUsage(); while(performance.now()-began < 5000) {} const usage=process.cpuUsage(used); console.log(JSON.stringify({before, after:snapshot(), elapsedMs:performance.now()-began, cpuMs:(usage.user+usage.system)/1000}));`, true)
	cpu = waitPod(cpu, func(p *corev1.Pod) bool {
		return p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
	})
	if cpu.Status.Phase != corev1.PodSucceeded {
		t.Fatalf("CPU probe failed: %s", logs(cpu, "main"))
	}
	var result struct {
		Before, After    struct{ Throttled int64 }
		ElapsedMs, CPUMs float64
	}
	cpuLog := logs(cpu, "main")
	if err := json.Unmarshal([]byte(cpuLog), &result); err != nil {
		t.Fatalf("CPU evidence: %v: %s", err, cpuLog)
	}
	if result.After.Throttled <= result.Before.Throttled || result.CPUMs <= 0 || result.ElapsedMs < 5000 || result.CPUMs/result.ElapsedMs > 0.4 {
		t.Fatalf("CPU throttling not established: %s", cpuLog)
	}
	t.Logf("CPU evidence: %s", strings.TrimSpace(cpuLog))
	for _, name := range []string{"init", "restartable", "helper"} {
		value := logs(cpu, name)
		if !strings.Contains(value, `"memory":134217728`) || !strings.Contains(value, `"millicores":500`) {
			t.Fatalf("supporting cgroup not bounded: %s: %s", name, value)
		}
		t.Logf("%s cgroup: %s", name, strings.TrimSpace(value))
	}
	// The program refuses to allocate until it reads the expected cgroup caps.
	// Allocation is additionally finite, even if the runtime loses enforcement.
	memory := start(`check(250); console.log(JSON.stringify({bounds:snapshot()})); const retained=[]; for(let i=0;i<64;i++) retained.push(Buffer.alloc(4*1024*1024, 1)); console.error("OOM was not enforced"); process.exit(9);`, false)
	memory = waitPod(memory, func(p *corev1.Pod) bool {
		return p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded
	})
	if len(memory.Status.ContainerStatuses) != 1 || memory.Status.ContainerStatuses[0].State.Terminated == nil {
		t.Fatal("memory probe has no termination evidence")
	}
	termination := memory.Status.ContainerStatuses[0].State.Terminated
	if termination.Reason != "OOMKilled" || termination.ExitCode != 137 {
		t.Fatalf("memory cap not enforced: %+v; log=%s", termination, logs(memory, "main"))
	}
	t.Logf("memory evidence: reason=%s exit=%d bounds=%s", termination.Reason, termination.ExitCode, strings.TrimSpace(logs(memory, "main")))
	afterOOM := logs(control, "main")
	control = waitPod(control, func(p *corev1.Pod) bool {
		return p.Status.Phase == corev1.PodRunning && len(p.Status.ContainerStatuses) == 1 && p.Status.ContainerStatuses[0].Ready && logs(p, "main") != afterOOM
	})
	if control.Status.ContainerStatuses[0].RestartCount != 0 || len(strings.Split(logs(control, "main"), "\n")) <= len(strings.Split(before, "\n")) {
		t.Fatal("neighbor did not remain healthy and make progress")
	}
	t.Logf("neighbor evidence: uid=%s restarts=0 heartbeat advanced", control.UID)
	for _, pod := range []*corev1.Pod{cpu, memory, control} {
		if _, err := runner.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{WorkloadId: pod.Labels[workloadIDLabelKey], TimeoutSec: 1}); err != nil {
			t.Fatal(err)
		}
		if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := kube.CoreV1().Pods(ns.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}); err != nil {
			t.Fatalf("physical removal unconfirmed for %s: %v", pod.Name, err)
		}
		t.Logf("removed pod=%s uid=%s", pod.Name, pod.UID)
	}
}

const liveResourceProbe = `
const fs = require("node:fs");
function snapshot() {
  const read = name => fs.readFileSync("/sys/fs/cgroup/"+name, "utf8").trim();
  const [quota, period] = read("cpu.max").split(/\s+/).map(Number);
  const stat = Object.fromEntries(read("cpu.stat").split("\n").map(line => line.split(/\s+/)));
  return {millicores:1000*quota/period, memory:Number(read("memory.max")), throttled:Number(stat.nr_throttled)};
}
function check(millicores) {
  const s = snapshot();
  if(s.millicores !== millicores || s.memory !== 128*1024*1024 || !Number.isFinite(s.throttled)) {
    throw new Error("cgroup preflight failed: "+JSON.stringify(s));
  }
}
`
