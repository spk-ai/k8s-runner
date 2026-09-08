package server

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

const (
	managedByLabelKey           = "app.kubernetes.io/managed-by"
	managedByLabelValue         = "k8s-runner"
	workloadManagedByLabelKey   = "agyn.dev/managed-by"
	workloadManagedByLabelValue = "agents-orchestrator"
	workloadIDLabelKey          = "agyn.io/workload-id"
	workloadKeyLabelKey         = "workload_key"
	volumeKeyLabelKey           = "volume_key"
	pvcAnnotationKey            = "agyn.io/pvc-names"
	secretAnnotationKey         = "agyn.io/pull-secret-names"
	inlineFilesVolumeName       = "inline-files"
	touchedAtAnnotationKey      = "agyn.io/touched-at"
)

// Server implements the RunnerService gRPC API against the Kubernetes API.
type Server struct {
	runnerv1.UnimplementedRunnerServiceServer
	clientset                 kubernetes.Interface
	restConfig                *rest.Config
	namespace                 string
	storageClass              *string
	storageSize               string
	catalog                   config.Catalog
	logger                    *zap.Logger
	capabilityImplementations config.CapabilityImplementations

	execSessions   map[string]*execSession
	execSessionsMu sync.Mutex
}

// Options defines required inputs for constructing a Server.
type Options struct {
	Clientset                 kubernetes.Interface
	RestConfig                *rest.Config
	Namespace                 string
	StorageClass              *string
	StorageSize               string
	Catalog                   config.Catalog
	Logger                    *zap.Logger
	CapabilityImplementations config.CapabilityImplementations
}

// New constructs a RunnerService server.
func New(options Options) *Server {
	return &Server{
		clientset:                 options.Clientset,
		restConfig:                options.RestConfig,
		namespace:                 options.Namespace,
		storageClass:              options.StorageClass,
		storageSize:               options.StorageSize,
		catalog:                   options.Catalog,
		logger:                    options.Logger,
		capabilityImplementations: options.CapabilityImplementations,
		execSessions:              make(map[string]*execSession),
	}
}

func (s *Server) Ready(_ context.Context, _ *runnerv1.ReadyRequest) (*runnerv1.ReadyResponse, error) {
	return &runnerv1.ReadyResponse{Status: "ok"}, nil
}
