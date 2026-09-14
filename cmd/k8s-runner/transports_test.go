package main

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type transportProbe struct {
	runnerv1.UnimplementedRunnerServiceServer
	starts atomic.Int32
	execs  atomic.Int32
}

func (p *transportProbe) Ready(context.Context, *runnerv1.ReadyRequest) (*runnerv1.ReadyResponse, error) {
	return &runnerv1.ReadyResponse{Status: "ok"}, nil
}

func (p *transportProbe) StartWorkload(context.Context, *runnerv1.StartWorkloadRequest) (*runnerv1.StartWorkloadResponse, error) {
	p.starts.Add(1)
	return &runnerv1.StartWorkloadResponse{}, nil
}

func (p *transportProbe) Exec(stream runnerv1.RunnerService_ExecServer) error {
	p.execs.Add(1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return status.Error(codes.Aborted, "control_probe_reached")
}

func serveTransportProbe(t *testing.T, server *grpc.Server) *grpc.ClientConn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("transport server did not exit cleanly: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("transport server did not stop")
		}
	})
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestRunnerZitiTCPRejectsControlMethods(t *testing.T) {
	probe := &transportProbe{}
	tcp, control := newRunnerRPCServers(true, probe)
	t.Cleanup(control.Stop)
	conn := serveTransportProbe(t, tcp)
	service := (&runnerv1.ReadyRequest{}).ProtoReflect().Descriptor().ParentFile().Services().ByName("RunnerService")
	if service == nil {
		t.Fatal("generated runner service descriptor missing")
	}
	for _, headers := range []metadata.MD{nil, metadata.Pairs("x-identity-id", "forged-runner", "authorization", "Bearer forged", "x-ziti-identity", "forged-controller")} {
		label := "anonymous"
		if headers != nil {
			label = "forged-metadata"
		}
		for i := 0; i < service.Methods().Len(); i++ {
			method := service.Methods().Get(i)
			t.Run(label+"/"+string(method.Name()), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				ctx = metadata.NewOutgoingContext(ctx, headers)
				err := invokeTransportMethod(ctx, conn, method)
				if method.Name() == "Ready" {
					if err != nil {
						t.Fatalf("readiness is unavailable: %v", err)
					}
				} else if status.Code(err) != codes.Unauthenticated {
					t.Fatalf("plaintext listener reached a control RPC: code=%s", status.Code(err))
				}
			})
		}
	}
	if probe.starts.Load() != 0 || probe.execs.Load() != 0 {
		t.Fatalf("plaintext executed control handlers: starts=%d execs=%d", probe.starts.Load(), probe.execs.Load())
	}
}

func invokeTransportMethod(ctx context.Context, conn *grpc.ClientConn, method protoreflect.MethodDescriptor) error {
	path := "/" + string(method.Parent().FullName()) + "/" + string(method.Name())
	request, response := dynamicpb.NewMessage(method.Input()), dynamicpb.NewMessage(method.Output())
	if !method.IsStreamingClient() && !method.IsStreamingServer() {
		return conn.Invoke(ctx, path, request, response)
	}
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: method.IsStreamingClient(), ServerStreams: method.IsStreamingServer()}, path)
	if err != nil {
		return err
	}
	// The terminal receive reports the server status even if rejection won the
	// race against SendMsg. Do not mistake local EOF for authorization evidence.
	_ = stream.SendMsg(request)
	_ = stream.CloseSend()
	return stream.RecvMsg(response)
}

func TestRunnerControlAndStandaloneTransports(t *testing.T) {
	for _, ziti := range []bool{false, true} {
		name := "standalone"
		if ziti {
			name = "control-listener"
		}
		t.Run(name, func(t *testing.T) {
			probe := &transportProbe{}
			tcp, control := newRunnerRPCServers(ziti, probe)
			t.Cleanup(tcp.Stop)
			if !ziti && tcp != control {
				t.Fatal("standalone listener no longer serves the control API")
			}
			conn := serveTransportProbe(t, control)
			client := runnerv1.NewRunnerServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			ready, err := client.Ready(ctx, &runnerv1.ReadyRequest{})
			if err != nil || ready.GetStatus() != "ok" {
				t.Fatal("control readiness unavailable")
			}
			if _, err := client.StartWorkload(ctx, &runnerv1.StartWorkloadRequest{}); err != nil || probe.starts.Load() != 1 {
				t.Fatalf("control unary RPC failed: %v", err)
			}
			stream, err := client.Exec(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_ = stream.Send(&runnerv1.ExecRequest{})
			_, err = stream.Recv()
			if status.Code(err) != codes.Aborted || probe.execs.Load() != 1 {
				t.Fatalf("control stream did not reach its handler: %v", err)
			}
		})
	}
}

func TestRunnerZitiTCPRejectsFutureServices(t *testing.T) {
	tcp, control := newRunnerRPCServers(true, &transportProbe{})
	t.Cleanup(control.Stop)
	var calls atomic.Int32
	service := grpc.ServiceDesc{
		ServiceName: "future.RunnerControl", HandlerType: (*interface{})(nil),
		Methods: []grpc.MethodDesc{{MethodName: "Ready", Handler: func(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			request := &runnerv1.ReadyRequest{}
			if err := decode(request); err != nil {
				return nil, err
			}
			handler := func(context.Context, any) (any, error) {
				calls.Add(1)
				return &runnerv1.ReadyResponse{}, nil
			}
			if interceptor == nil {
				return handler(ctx, request)
			}
			return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/future.RunnerControl/Ready"}, handler)
		}}},
		Streams: []grpc.StreamDesc{{StreamName: "Control", ServerStreams: true, Handler: func(any, grpc.ServerStream) error {
			calls.Add(1)
			return status.Error(codes.Aborted, "future_handler_reached")
		}}},
	}
	tcp.RegisterService(&service, &struct{}{})
	conn := serveTransportProbe(t, tcp)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := conn.Invoke(ctx, "/future.RunnerControl/Ready", &runnerv1.ReadyRequest{}, &runnerv1.ReadyResponse{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("a similarly named method bypassed the readiness allowlist: %v", err)
	}
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/future.RunnerControl/Control")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = stream.Header()
	if err := stream.RecvMsg(&runnerv1.ReadyResponse{}); status.Code(err) != codes.Unauthenticated || calls.Load() != 0 {
		t.Fatalf("future control service was not denied: calls=%d code=%s", calls.Load(), status.Code(err))
	}
}
