package server

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

func (s *Server) volumeBackendID(ctx context.Context) (string, error) {
	if len(validation.IsDNS1123Label(s.namespace)) != 0 {
		return "", status.Error(codes.FailedPrecondition, "valid_volume_namespace_required")
	}
	ns, err := s.clientset.CoreV1().Namespaces().Get(ctx, s.namespace, metav1.GetOptions{})
	if err != nil {
		return "", grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if ns.Name != s.namespace || ns.UID == "" || len(ns.UID) > 256 || strings.TrimSpace(string(ns.UID)) != string(ns.UID) || ns.DeletionTimestamp != nil {
		return "", status.Error(codes.FailedPrecondition, "live_volume_namespace_identity_required")
	}
	return "kubernetes-namespace/v1/" + s.namespace + "/" + string(ns.UID), nil
}

func (s *Server) checkVolumeBackend(ctx context.Context, expected string) (string, error) {
	observed, err := s.volumeBackendID(ctx)
	if err != nil {
		return "", err
	}
	if observed != expected {
		return "", status.Error(codes.FailedPrecondition, "volume_backend_identity_mismatch")
	}
	return observed, nil
}
