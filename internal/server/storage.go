package server

import (
	"bytes"
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

func (s *Server) PutArchive(ctx context.Context, req *runnerv1.PutArchiveRequest) (*runnerv1.PutArchiveResponse, error) {
	workloadID := strings.TrimSpace(req.GetWorkloadId())
	path := strings.TrimSpace(req.GetPath())
	if workloadID == "" || path == "" {
		return nil, status.Error(codes.InvalidArgument, "workload_id_and_path_required")
	}

	podName := podNameFromID(workloadID)

	pod, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	containerName, err := mainContainerName(pod)
	if err != nil {
		return nil, err
	}

	command := []string{"tar", "-xf", "-", "-C", path}
	options := &corev1.PodExecOptions{
		Container: containerName,
		Command:   command,
		Stdin:     true,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}

	reqURL := s.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(s.namespace).
		SubResource("exec").
		VersionedParams(options, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(s.restConfig, "POST", reqURL.URL())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create exec: %v", err)
	}

	stdin := bytes.NewReader(req.GetTarPayload())
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		Tty:    false,
	}); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = strings.TrimSpace(stdout.String())
		}
		if message != "" {
			return nil, status.Errorf(codes.Internal, "put archive failed: %s", message)
		}
		return nil, status.Errorf(codes.Internal, "put archive failed: %v", err)
	}

	return &runnerv1.PutArchiveResponse{}, nil
}

func (s *Server) RemoveVolume(_ context.Context, req *runnerv1.RemoveVolumeRequest) (*runnerv1.RemoveVolumeResponse, error) {
	volumeName := strings.TrimSpace(req.GetVolumeName())
	if volumeName == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_name_required")
	}

	return nil, status.Error(codes.FailedPrecondition, "checked_volume_removal_required")
}

func mainContainerName(pod *corev1.Pod) (string, error) {
	if pod == nil || len(pod.Spec.Containers) == 0 {
		return "", status.Error(codes.Internal, "pod_missing_containers")
	}
	return pod.Spec.Containers[0].Name, nil
}
