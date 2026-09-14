package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	gatewayv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/gateway/v1"
	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runners/v1"
	"github.com/agynio/k8s-runner/internal/kube"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes/fake"
)

type blockedEnrollmentGateway struct {
	gatewayv1.UnimplementedRunnersGatewayServer
	called  chan struct{}
	release chan struct{}
}

func (s *blockedEnrollmentGateway) EnrollRunner(ctx context.Context, _ *runnersv1.EnrollRunnerRequest) (*runnersv1.EnrollRunnerResponse, error) {
	close(s.called)
	select {
	case <-s.release:
		return nil, status.Error(codes.PermissionDenied, "fixture_enrollment_denied")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestRunnerTransportProcessChild(t *testing.T) {
	if os.Getenv("RUNNER_TRANSPORT_PROCESS_CHILD") != "1" {
		t.Skip("child of the isolated startup transport test")
	}
	if os.Getenv("SERVICE_TOKEN") != "fixture-only" || os.Getenv("KUBE_NAMESPACE") != "fixture" || os.Getenv("ZITI_ENABLED") != "true" {
		t.Fatal("startup child requires fixture-only configuration")
	}
	for _, name := range []string{"GRPC_ADDR", "GATEWAY_ADDRESS"} {
		address, err := netip.ParseAddrPort(os.Getenv(name))
		if err != nil || address.Addr() != netip.MustParseAddr("127.0.0.1") || address.Port() == 0 {
			t.Fatal("startup child requires explicit loopback endpoints")
		}
	}
	client := fake.NewSimpleClientset()
	err := runWithKubeClient(func() (*kube.Client, error) { return &kube.Client{Clientset: client}, nil })
	if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "fixture_enrollment_denied") {
		t.Fatal("startup did not fail on the injected enrollment refusal")
	}
	if len(client.Actions()) != 0 {
		t.Fatal("the plaintext listener accessed Kubernetes during enrollment")
	}
}

func TestRunnerTCPStaysRestrictedBeforeEnrollment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	gateway := &blockedEnrollmentGateway{called: make(chan struct{}), release: make(chan struct{})}
	gatewayServer := grpc.NewServer()
	gatewayv1.RegisterRunnersGatewayServer(gatewayServer, gateway)
	gatewayConn := serveTransportProbe(t, gatewayServer)
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, binary, "-test.run=^TestRunnerTransportProcessChild$", "-test.v")
	// No inherited cluster, provider, overlay or host credentials. Only the
	// Kubernetes client constructor is replaced; startup and listeners are real.
	command.Env = []string{"RUNNER_TRANSPORT_PROCESS_CHILD=1", "KUBE_NAMESPACE=fixture", "ZITI_ENABLED=true",
		"SERVICE_TOKEN=fixture-only", "GRPC_ADDR=" + address, "GATEWAY_ADDRESS=" + gatewayConn.Target(),
		"ZITI_ENROLLMENT_TIMEOUT=10s", "HOME=" + t.TempDir()}
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	joined := false
	defer func() {
		if !joined {
			cancel()
			<-done
		}
	}()
	select {
	case <-gateway.called:
	case err := <-done:
		joined = true
		t.Fatalf("child exited before enrollment: %v; output=%s", err, output.String())
	case <-ctx.Done():
		t.Fatal("child never reached the fixture gateway")
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := runnerv1.NewRunnerServiceClient(conn)
	ready, err := client.Ready(ctx, &runnerv1.ReadyRequest{})
	if err != nil || ready.GetStatus() != "ok" {
		t.Fatal("TCP readiness unavailable while enrollment is pending")
	}
	if _, err := client.ListVolumes(ctx, &runnerv1.ListVolumesRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("startup exposed the volume API before enrollment: code=%s", status.Code(err))
	}
	if _, err := client.StartWorkload(ctx, &runnerv1.StartWorkloadRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("startup exposed workload admission before enrollment: code=%s", status.Code(err))
	}
	close(gateway.release)
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatalf("child startup/cleanup assertion failed: %v; output=%s", err, output.String())
		}
	case <-ctx.Done():
		t.Fatal("child did not exit after enrollment was refused")
	}
	probe, err := net.DialTimeout("tcp", address, time.Second)
	if err == nil {
		_ = probe.Close()
		t.Fatal("TCP listener remained after failed enrollment")
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("TCP closure was not observed: %v", err)
	}
}
