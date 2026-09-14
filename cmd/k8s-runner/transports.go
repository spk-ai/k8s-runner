package main

import (
	"context"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newRunnerRPCServers(zitiEnabled bool, runner runnerv1.RunnerServiceServer) (tcp, control *grpc.Server) {
	control = grpc.NewServer()
	runnerv1.RegisterRunnerServiceServer(control, runner)
	if !zitiEnabled {
		return control, control
	}
	// Plaintext health probes must not bypass the overlay's access policy. The
	// allowlist is independent of request metadata and denies future RPCs too.
	tcp = grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if info.FullMethod != runnerv1.RunnerService_Ready_FullMethodName {
				return nil, status.Error(codes.Unauthenticated, "runner_control_requires_ziti")
			}
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(any, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error {
			return status.Error(codes.Unauthenticated, "runner_control_requires_ziti")
		}),
	)
	runnerv1.RegisterRunnerServiceServer(tcp, runner)
	return tcp, control
}
