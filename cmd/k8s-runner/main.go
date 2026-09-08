package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/openziti/sdk-golang/ziti"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	runnersgatewayv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/gateway/v1"
	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runners/v1"
	"github.com/agynio/k8s-runner/internal/config"
	"github.com/agynio/k8s-runner/internal/kube"
	"github.com/agynio/k8s-runner/internal/logging"
	"github.com/agynio/k8s-runner/internal/reporter"
	"github.com/agynio/k8s-runner/internal/server"
)

const (
	retryInitialBackoff = 1 * time.Second
	retryMaxBackoff     = 15 * time.Second
)

const (
	zitiIdentityCheckInterval    = 30 * time.Second
	zitiIdentityFailureThreshold = 5
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "k8s-runner failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger, err := logging.New(cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	kubeClient, err := kube.New()
	if err != nil {
		return fmt.Errorf("init kube client: %w", err)
	}

	grpcServer := grpc.NewServer()
	runnerv1.RegisterRunnerServiceServer(
		grpcServer,
		server.New(server.Options{
			Clientset:                 kubeClient.Clientset,
			RestConfig:                kubeClient.RestConfig,
			Namespace:                 cfg.Namespace,
			StorageClass:              cfg.StorageClass,
			StorageSize:               cfg.StorageSize,
			Catalog:                   cfg.Catalog,
			Logger:                    logger,
			CapabilityImplementations: cfg.CapabilityImplementations,
		}),
	)

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	startServe := func(listener net.Listener, label string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.Info("gRPC server starting", zap.String("listener", label), zap.String("addr", listener.Addr().String()))
			err := grpcServer.Serve(listener)
			if errors.Is(err, grpc.ErrServerStopped) {
				err = nil
			}
			if err != nil {
				errCh <- err
			}
		}()
	}

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}
	defer listener.Close()
	startServe(listener, "tcp")

	if cfg.ZitiEnabled {
		gatewayConn, err := grpc.DialContext(ctx, cfg.GatewayAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial gateway: %w", err)
		}
		defer gatewayConn.Close()

		gatewayClient := runnersgatewayv1.NewRunnersGatewayClient(gatewayConn)

		enrollmentCtx, cancel := context.WithTimeout(ctx, cfg.ZitiEnrollmentTimeout)
		defer cancel()

		var enrollResponse *runnersv1.EnrollRunnerResponse
		if err := retryWithBackoff(enrollmentCtx, logger, "gateway enrollment", func(attemptCtx context.Context) error {
			var requestErr error
			enrollResponse, requestErr = gatewayClient.EnrollRunner(attemptCtx, &runnersv1.EnrollRunnerRequest{
				ServiceToken: cfg.ServiceToken,
			})
			return requestErr
		}); err != nil {
			return fmt.Errorf("enroll runner via gateway: %w", err)
		}

		// Reported before the service is bound, so the platform knows what this
		// runner offers by the time anything can be scheduled onto it.
		//
		// On its own deadline rather than the enrollment one: enrollment has
		// already consumed an unknown share of that budget by this point, so
		// sharing it made a slow enrollment leave no time to report and killed
		// the runner for a delay it had already survived.
		catalogCtx, cancelCatalog := context.WithTimeout(ctx, cfg.ZitiEnrollmentTimeout)
		catalogErr := retryWithBackoff(catalogCtx, logger, "catalog report", func(attemptCtx context.Context) error {
			_, requestErr := gatewayClient.ReportRunnerCatalog(attemptCtx, catalogReport(cfg))
			return requestErr
		})
		cancelCatalog()
		if catalogErr != nil {
			// Not fatal. A platform whose Runners service predates
			// ReportRunnerCatalog answers Unimplemented, and refusing to start
			// would mean a runner cannot be upgraded before the platform is —
			// it would crash-loop rather than serve.
			//
			// Serving without a reported catalog is already a handled state: a
			// workload naming a flavor the platform cannot resolve fails to
			// schedule with the standard retry and unschedulable flagging, and
			// recovers as soon as a report lands.
			logger.Error("catalog report failed; serving without it",
				zap.Error(catalogErr))
		} else {
			logger.Info("catalog reported",
				zap.Int("flavors", len(cfg.Catalog.Flavors)),
				zap.Int("storageClasses", len(cfg.Catalog.StorageClasses)),
				zap.Int("capabilities", len(cfg.Catalog.Capabilities)))
		}

		zitiConfig := &ziti.Config{}
		if err := json.Unmarshal([]byte(enrollResponse.IdentityJson), zitiConfig); err != nil {
			return fmt.Errorf("parse ziti identity: %w", err)
		}

		zitiContext, err := ziti.NewContext(zitiConfig)
		if err != nil {
			return fmt.Errorf("create ziti context: %w", err)
		}
		defer zitiContext.Close()

		zitiListener, err := zitiContext.ListenWithOptions(enrollResponse.ServiceName, ziti.DefaultListenOptions())
		if err != nil {
			return fmt.Errorf("listen on ziti service %s: %w", enrollResponse.ServiceName, err)
		}
		defer zitiListener.Close()
		startServe(zitiListener, "ziti")

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := watchZitiIdentity(ctx, zitiContext.RefreshServices, zitiIdentityCheckInterval, zitiIdentityFailureThreshold, logger)
			if err != nil {
				errCh <- err
			}
		}()

		// The platform used to discover a workload was running by dialing this
		// runner on its reconcile interval and asking.
		wg.Add(1)
		go func() {
			defer wg.Done()
			workloadReporter := reporter.New(kubeClient.Clientset, cfg.Namespace, gatewayClient, cfg.ServiceToken, logger)
			if err := workloadReporter.Run(ctx); err != nil && ctx.Err() == nil {
				// Not fatal: reconciliation still converges without it, so a
				// runner that cannot report serves on and is late, not wrong.
				logger.Warn("workload state reporting stopped", zap.Error(err))
			}
		}()
	}

	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		logger.Info("shutting down")
		grpcServer.GracefulStop()
	}

	wg.Wait()
	return nil
}

func retryWithBackoff(ctx context.Context, logger *zap.Logger, operationName string, fn func(context.Context) error) error {
	backoff := retryInitialBackoff
	attempt := 1
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if !isRetryableGrpcError(err) {
			return err
		}

		delay := backoff
		if delay > retryMaxBackoff {
			delay = retryMaxBackoff
		}

		logger.Warn(
			"operation failed, retrying",
			zap.String("operation", operationName),
			zap.Int("attempt", attempt),
			zap.Duration("backoff", delay),
			zap.Error(err),
		)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > retryMaxBackoff {
			backoff = retryMaxBackoff
		}
		attempt++
	}
}

func isRetryableGrpcError(err error) bool {
	statusErr, ok := status.FromError(err)
	if !ok {
		return false
	}
	return statusErr.Code() == codes.Unavailable || statusErr.Code() == codes.Unknown
}

// catalogReport converts the runner's declared catalog into the report the
// platform stores. The runner-side mapping (which Kubernetes StorageClass
// backs an entry) stays here — the platform only needs the names.
func catalogReport(cfg config.Config) *runnersv1.ReportRunnerCatalogRequest {
	flavors := make([]*runnersv1.FlavorEntry, 0, len(cfg.Catalog.Flavors))
	for _, flavor := range cfg.Catalog.Flavors {
		flavors = append(flavors, &runnersv1.FlavorEntry{
			Name:       flavor.Name,
			Default:    flavor.Default,
			Deprecated: flavor.Deprecated,
			Resources: &runnersv1.ComputeResources{
				RequestsCpu:    flavor.Resources.RequestsCPU,
				RequestsMemory: flavor.Resources.RequestsMemory,
				LimitsCpu:      flavor.Resources.LimitsCPU,
				LimitsMemory:   flavor.Resources.LimitsMemory,
			},
		})
	}
	storageClasses := make([]*runnersv1.StorageClassEntry, 0, len(cfg.Catalog.StorageClasses))
	for _, class := range cfg.Catalog.StorageClasses {
		storageClasses = append(storageClasses, &runnersv1.StorageClassEntry{
			Name:       class.Name,
			Default:    class.Default,
			Deprecated: class.Deprecated,
		})
	}
	return &runnersv1.ReportRunnerCatalogRequest{
		ServiceToken:   cfg.ServiceToken,
		Flavors:        flavors,
		StorageClasses: storageClasses,
		Capabilities:   catalogCapabilities(cfg),
	}
}

// catalogCapabilities reports what this runner can actually do, which is what
// it has an implementation configured for. The catalog listed them separately
// and the two silently disagreed: docker was configured and working, the
// catalog said nothing, and the Orchestrator placed no agent that asked for it
// -- "no eligible runners found (required capabilities: [docker])" -- with the
// runner right there able to serve them.
//
// The catalog entry is still honoured, so a capability can be advertised ahead
// of the implementation landing, but it no longer has to be kept in step by
// hand for the ones the runner already implements.
func catalogCapabilities(cfg config.Config) []string {
	capabilities := make([]string, 0, len(cfg.Catalog.Capabilities)+1)
	seen := map[string]struct{}{}
	add := func(capability string) {
		if capability == "" {
			return
		}
		if _, ok := seen[capability]; ok {
			return
		}
		seen[capability] = struct{}{}
		capabilities = append(capabilities, capability)
	}
	for _, capability := range cfg.Catalog.Capabilities {
		add(strings.TrimSpace(capability))
	}
	if cfg.CapabilityImplementations.Docker != "" {
		add(config.CapabilityDocker)
	}
	sort.Strings(capabilities)
	return capabilities
}

// The runner enrols once, at startup. If the controller stops accepting that
// identity -- it was deleted, or the controller lost it -- the SDK retries the
// bind forever and the runner stays Running with no terminator, so the platform
// keeps scheduling onto a runner nothing can reach. Exiting hands the problem to
// Kubernetes, which restarts the pod into a fresh enrolment.
func watchZitiIdentity(ctx context.Context, refresh func() error, interval time.Duration, threshold int, logger *zap.Logger) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	failures := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		if err := refresh(); err != nil {
			failures++
			logger.Warn("ziti identity check failed",
				zap.Int("consecutiveFailures", failures), zap.Error(err))
			if failures >= threshold {
				return fmt.Errorf("ziti identity is no longer usable after %d checks: %w", failures, err)
			}
			continue
		}
		if failures > 0 {
			logger.Info("ziti identity recovered", zap.Int("afterFailures", failures))
		}
		failures = 0
	}
}
