package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

func (s *Server) StreamWorkloadLogs(req *runnerv1.StreamWorkloadLogsRequest, stream runnerv1.RunnerService_StreamWorkloadLogsServer) error {
	workloadID := strings.TrimSpace(req.GetWorkloadId())
	if workloadID == "" {
		return status.Error(codes.InvalidArgument, "workload_id_required")
	}
	containerName := strings.TrimSpace(req.GetContainerName())
	if containerName == "" {
		return status.Error(codes.InvalidArgument, "container_name_required")
	}

	podName := podNameFromID(workloadID)
	ctx := stream.Context()

	pod, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if !podHasContainer(pod, containerName) {
		return status.Error(codes.NotFound, "container_not_found")
	}

	options := &corev1.PodLogOptions{
		Container:  containerName,
		Follow:     req.GetFollow(),
		Timestamps: req.GetTimestamps(),
	}
	if req.GetTailLines() > 0 {
		tail := int64(req.GetTailLines())
		options.TailLines = &tail
	} else if req.GetTail() > 0 {
		tail := int64(req.GetTail())
		options.TailLines = &tail
	}
	if req.GetSinceTime() != nil {
		sinceTime := metav1.NewTime(req.GetSinceTime().AsTime())
		options.SinceTime = &sinceTime
	} else if req.GetSince() > 0 {
		sinceTime := metav1.NewTime(time.Unix(req.GetSince(), 0))
		options.SinceTime = &sinceTime
	}

	logStream, err := s.clientset.CoreV1().Pods(s.namespace).GetLogs(podName, options).Stream(ctx)
	if err != nil {
		return grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	defer logStream.Close()

	const logReadBufferSize = 4096
	buf := make([]byte, logReadBufferSize)
	for {
		n, readErr := logStream.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			resp := &runnerv1.StreamWorkloadLogsResponse{
				Event: &runnerv1.StreamWorkloadLogsResponse_Chunk{
					Chunk: &runnerv1.LogChunk{
						Data: chunk,
						Ts:   timestamppb.New(time.Now().UTC()),
					},
				},
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
		if readErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			podAlive, err := podExists(ctx, s.clientset, s.namespace, podName)
			if err != nil {
				return grpcErrorFromKube(s.logger, err, codes.Internal)
			}
			return logReadError(readErr, podAlive)
		}
	}
}

const (
	// tailLogsMaxBytes is the runner's ceiling on TailWorkloadLogs.max_bytes.
	tailLogsMaxBytes = 256 * 1024
	// tailLogsDefaultLines and tailLogsMaxLines bound the kubelet's own tail.
	tailLogsDefaultLines = 2000
	tailLogsMaxLines     = 10000
	tailLogsTimeout      = 30 * time.Second
)

// tailLogsReadLimit stops reading a backend tail whose lines are huge. The
// kubelet's rotated log file already bounds it; this keeps the call bounded
// even when that setting is raised. A read that reaches it is retried with a
// tenth of the lines, so the bytes returned are still the newest. A variable
// only so tests can reach it without 16 MiB of output.
var tailLogsReadLimit = 16 * 1024 * 1024

// TailWorkloadLogs reads a bounded, non-following snapshot of a container's
// newest output. Only the newest max_bytes cross the wire, whatever the kubelet
// returns for tail_lines. A waiting container or an absent previous instance
// has no output to read and answers FailedPrecondition, not an empty success,
// so evidence can say why it holds none.
// @see api::proto/agynio/api/runner/v1/runner
func (s *Server) TailWorkloadLogs(ctx context.Context, req *runnerv1.TailWorkloadLogsRequest) (*runnerv1.TailWorkloadLogsResponse, error) {
	workloadID := strings.TrimSpace(req.GetWorkloadId())
	if workloadID == "" {
		return nil, status.Error(codes.InvalidArgument, "workload_id_required")
	}
	containerName := strings.TrimSpace(req.GetContainerName())
	if containerName == "" {
		return nil, status.Error(codes.InvalidArgument, "container_name_required")
	}
	maxBytes := req.GetMaxBytes()
	if maxBytes == 0 {
		return nil, status.Error(codes.InvalidArgument, "max_bytes_required")
	}
	if maxBytes > tailLogsMaxBytes {
		maxBytes = tailLogsMaxBytes
	}
	lines := int64(req.GetTailLines())
	if lines == 0 {
		lines = tailLogsDefaultLines
	}
	if lines > tailLogsMaxLines {
		lines = tailLogsMaxLines
	}

	ctx, cancel := context.WithTimeout(ctx, tailLogsTimeout)
	defer cancel()
	podName := podNameFromID(workloadID)
	pod, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if !podHasContainer(pod, containerName) {
		return nil, status.Error(codes.NotFound, "container_not_found")
	}
	for reduced := false; ; reduced = true {
		data, truncated, exceeded, err := s.readContainerLogTail(ctx, podName, containerName, lines, req.GetPrevious(), int(maxBytes))
		if err != nil {
			return nil, err
		}
		if !exceeded {
			return &runnerv1.TailWorkloadLogsResponse{Data: data, Truncated: truncated || reduced, MaxBytes: maxBytes}, nil
		}
		// Bytes read up to the limit end before the output does: never
		// present them as the newest output.
		if lines == 1 {
			return nil, status.Error(codes.ResourceExhausted, "log_line_exceeds_read_limit")
		}
		lines = max(lines/10, 1)
	}
}

// readContainerLogTail reads one bounded tail. exceeded reports that the read
// limit was reached before the end of the output.
func (s *Server) readContainerLogTail(ctx context.Context, podName, containerName string, lines int64, previous bool, maxBytes int) ([]byte, bool, bool, error) {
	logStream, err := s.clientset.CoreV1().Pods(s.namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: containerName,
		TailLines: &lines,
		Previous:  previous,
	}).Stream(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, false, status.Error(codes.DeadlineExceeded, "logs_open_timeout")
		}
		return nil, false, false, s.tailLogsOpenError(err)
	}
	defer logStream.Close()
	data, truncated, exceeded, err := readLogTail(logStream, maxBytes, tailLogsReadLimit)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, false, status.Error(codes.DeadlineExceeded, "logs_read_timeout")
		}
		return nil, false, false, status.Error(codes.Internal, "logs_stream_error")
	}
	return data, truncated, exceeded, nil
}

// tailLogsOpenError maps a refused log request. The kubelet answers BadRequest
// when a container has no output to read, e.g. "container ... is waiting to
// start" or no previous terminated instance; that is a state, not a fault.
func (s *Server) tailLogsOpenError(err error) error {
	if apierrors.IsBadRequest(err) {
		return status.Error(codes.FailedPrecondition, formatKubeError("logs_unavailable", kubeStatusMessage(err)))
	}
	return grpcErrorFromKube(s.logger, err, codes.Internal)
}

// readLogTail keeps the newest maxBytes of reader. Reading stops at readLimit;
// exceeded then reports that the kept bytes end where reading did, not where
// the output does.
func readLogTail(reader io.Reader, maxBytes, readLimit int) (tail []byte, truncated, exceeded bool, err error) {
	if maxBytes <= 0 {
		return nil, false, false, fmt.Errorf("max bytes must be positive")
	}
	tail = make([]byte, 0, maxBytes)
	buf := make([]byte, 32*1024)
	read := 0
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			read += n
			chunk := buf[:n]
			if len(chunk) >= maxBytes {
				truncated = truncated || len(tail) > 0 || len(chunk) > maxBytes
				tail = append(tail[:0], chunk[len(chunk)-maxBytes:]...)
			} else if overflow := len(tail) + len(chunk) - maxBytes; overflow > 0 {
				truncated = true
				tail = append(tail[:0], tail[overflow:]...)
				tail = append(tail, chunk...)
			} else {
				tail = append(tail, chunk...)
			}
		}
		if errors.Is(err, io.EOF) {
			return tail, truncated, false, nil
		}
		if err != nil {
			return nil, false, false, err
		}
		if read >= readLimit {
			return tail, true, true, nil
		}
	}
}

func podExists(ctx context.Context, clientset kubernetes.Interface, namespace, name string) (bool, error) {
	if _, err := clientset.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func podHasContainer(pod *corev1.Pod, name string) bool {
	if pod == nil {
		return false
	}
	for _, container := range pod.Spec.InitContainers {
		if container.Name == name {
			return true
		}
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == name {
			return true
		}
	}
	return false
}

func logReadError(readErr error, podAlive bool) error {
	if !podAlive {
		return status.Error(codes.Unavailable, "pod_deleted")
	}
	if errors.Is(readErr, io.EOF) {
		return nil
	}
	return status.Error(codes.Internal, "logs_stream_error")
}

func (s *Server) StreamEvents(req *runnerv1.StreamEventsRequest, stream runnerv1.RunnerService_StreamEventsServer) error {
	ctx := stream.Context()
	since := req.GetSince()
	var sinceTime time.Time
	if since > 0 {
		sinceTime = time.Unix(since, 0).UTC()
	}

	watcher, err := s.clientset.CoreV1().Events(s.namespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		return grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.ResultChan():
			if !ok {
				s.logger.Warn("event watcher closed", zap.String("namespace", s.namespace))
				return stream.Send(&runnerv1.StreamEventsResponse{
					Event: &runnerv1.StreamEventsResponse_Error{
						Error: streamError("events_watcher_closed", fmt.Errorf("event watch closed")),
					},
				})
			}
			if event.Type == watch.Error {
				return stream.Send(&runnerv1.StreamEventsResponse{
					Event: &runnerv1.StreamEventsResponse_Error{Error: streamError("events_stream_error", fmt.Errorf("event watch error"))},
				})
			}
			eventObj, ok := event.Object.(*corev1.Event)
			if !ok {
				continue
			}
			if !matchesEventFilters(eventObj, req.GetFilters()) {
				continue
			}
			eventTime := eventTimestamp(eventObj)
			if !sinceTime.IsZero() && eventTime.Before(sinceTime) {
				continue
			}
			payload, err := json.Marshal(eventObj)
			if err != nil {
				return stream.Send(&runnerv1.StreamEventsResponse{
					Event: &runnerv1.StreamEventsResponse_Error{Error: streamError("events_marshal_error", err)},
				})
			}
			resp := &runnerv1.StreamEventsResponse{
				Event: &runnerv1.StreamEventsResponse_Data{
					Data: &runnerv1.RunnerEventData{
						Json: string(payload),
						Ts:   timestamppb.New(eventTime),
					},
				},
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

func streamError(code string, err error) *runnerv1.RunnerError {
	return &runnerv1.RunnerError{
		Code:      code,
		Message:   err.Error(),
		Retryable: false,
	}
}

func matchesEventFilters(event *corev1.Event, filters []*runnerv1.EventFilter) bool {
	for _, filter := range filters {
		if filter == nil || len(filter.Values) == 0 {
			continue
		}
		value := eventFieldValue(event, filter.Key)
		if value == "" {
			return false
		}
		matched := false
		for _, candidate := range filter.Values {
			if candidate == value {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func eventFieldValue(event *corev1.Event, key string) string {
	switch strings.TrimSpace(key) {
	case "involvedObject.name":
		return event.InvolvedObject.Name
	case "involvedObject.namespace":
		return event.InvolvedObject.Namespace
	case "involvedObject.kind":
		return event.InvolvedObject.Kind
	case "involvedObject.uid":
		return string(event.InvolvedObject.UID)
	case "reason":
		return event.Reason
	case "type":
		return event.Type
	case "namespace":
		return event.Namespace
	case "metadata.name":
		return event.Name
	default:
		return ""
	}
}

func eventTimestamp(event *corev1.Event) time.Time {
	if !event.EventTime.IsZero() {
		return event.EventTime.Time
	}
	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp.Time
	}
	if !event.FirstTimestamp.IsZero() {
		return event.FirstTimestamp.Time
	}
	return time.Now().UTC()
}
