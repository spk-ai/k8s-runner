package server

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestReadLogTailKeepsNewestBytes(t *testing.T) {
	for _, test := range []struct {
		name          string
		input         string
		maxBytes      int
		readLimit     int
		chunk         int
		want          string
		wantTruncated bool
	}{
		{name: "fits", input: "hello", maxBytes: 8, readLimit: 1 << 20, chunk: 2, want: "hello"},
		{name: "exact", input: "hello", maxBytes: 5, readLimit: 1 << 20, chunk: 5, want: "hello"},
		{name: "small_chunks_overflow", input: "0123456789", maxBytes: 4, readLimit: 1 << 20, chunk: 3, want: "6789", wantTruncated: true},
		{name: "one_large_chunk", input: "0123456789", maxBytes: 4, readLimit: 1 << 20, chunk: 10, want: "6789", wantTruncated: true},
		{name: "chunk_equal_after_data", input: "abcdefgh", maxBytes: 4, readLimit: 1 << 20, chunk: 4, want: "efgh", wantTruncated: true},
		{name: "read_limit", input: strings.Repeat("x", 64) + "TAIL", maxBytes: 8, readLimit: 32, chunk: 16, want: strings.Repeat("x", 8), wantTruncated: true},
		{name: "empty", input: "", maxBytes: 4, readLimit: 1 << 20, chunk: 1, want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := &chunkReader{data: []byte(test.input), chunk: test.chunk}
			got, truncated, exceeded, err := readLogTail(reader, test.maxBytes, test.readLimit)
			if err != nil {
				t.Fatalf("readLogTail: %v", err)
			}
			if string(got) != test.want || truncated != test.wantTruncated {
				t.Fatalf("got %q truncated=%v, want %q truncated=%v", got, truncated, test.want, test.wantTruncated)
			}
			// Only a read stopped by its limit ends before the output does.
			if exceeded != (test.name == "read_limit") {
				t.Fatalf("exceeded=%v", exceeded)
			}
			if len(got) > test.maxBytes {
				t.Fatalf("returned %d bytes, bound is %d", len(got), test.maxBytes)
			}
		})
	}
}

func TestReadLogTailRejectsBadBoundsAndErrors(t *testing.T) {
	if _, _, _, err := readLogTail(strings.NewReader("x"), 0, 10); err == nil {
		t.Fatal("expected error for zero bound")
	}
	boom := errors.New("boom")
	if _, _, _, err := readLogTail(iotest.ErrReader(boom), 4, 10); !errors.Is(err, boom) {
		t.Fatalf("expected read error, got %v", err)
	}
}

func TestTailWorkloadLogsValidatesRequest(t *testing.T) {
	s := New(Options{Clientset: fake.NewSimpleClientset(), Namespace: "workloads", Logger: zap.NewNop(), Catalog: testCatalog()})
	for _, test := range []struct {
		name string
		req  *runnerv1.TailWorkloadLogsRequest
		want string
	}{
		{name: "workload", req: &runnerv1.TailWorkloadLogsRequest{ContainerName: "main", MaxBytes: 1}, want: "workload_id_required"},
		{name: "container", req: &runnerv1.TailWorkloadLogsRequest{WorkloadId: "w", MaxBytes: 1}, want: "container_name_required"},
		{name: "bound", req: &runnerv1.TailWorkloadLogsRequest{WorkloadId: "w", ContainerName: "main"}, want: "max_bytes_required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := s.TailWorkloadLogs(context.Background(), test.req)
			if status.Code(err) != codes.InvalidArgument || status.Convert(err).Message() != test.want {
				t.Fatalf("got %v, want InvalidArgument %s", err, test.want)
			}
		})
	}
}

func TestTailWorkloadLogsAddressesTheWorkloadPod(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podNameFromID("w1"), Namespace: "workloads"},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "enroll"}},
			Containers:     []corev1.Container{{Name: "main"}},
		},
	}
	client := fake.NewSimpleClientset(pod)
	s := New(Options{Clientset: client, Namespace: "workloads", Logger: zap.NewNop(), Catalog: testCatalog()})

	if _, err := s.TailWorkloadLogs(context.Background(), &runnerv1.TailWorkloadLogsRequest{WorkloadId: "missing", ContainerName: "main", MaxBytes: 16}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing Pod: got %v, want NotFound", err)
	}
	if _, err := s.TailWorkloadLogs(context.Background(), &runnerv1.TailWorkloadLogsRequest{WorkloadId: "w1", ContainerName: "other", MaxBytes: 16}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown container: got %v, want NotFound", err)
	}
	// The fake clientset answers every log read with "fake logs".
	for _, name := range []string{"main", "enroll"} {
		response, err := s.TailWorkloadLogs(context.Background(), &runnerv1.TailWorkloadLogsRequest{WorkloadId: "w1", ContainerName: name, MaxBytes: 64, Previous: true})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if string(response.Data) != "fake logs" || response.Truncated || response.MaxBytes != 64 {
			t.Fatalf("%s: unexpected response %+v", name, response)
		}
	}
	response, err := s.TailWorkloadLogs(context.Background(), &runnerv1.TailWorkloadLogsRequest{WorkloadId: "w1", ContainerName: "main", MaxBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Data) != "logs" || !response.Truncated {
		t.Fatalf("bounded read: got %q truncated=%v", response.Data, response.Truncated)
	}
	response, err = s.TailWorkloadLogs(context.Background(), &runnerv1.TailWorkloadLogsRequest{WorkloadId: "w1", ContainerName: "main", MaxBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if response.MaxBytes != tailLogsMaxBytes {
		t.Fatalf("ceiling not applied: %d", response.MaxBytes)
	}
}

// Output larger than the read limit even at one line is refused rather than
// returned as an earlier slice that looks like the newest output.
func TestTailWorkloadLogsRefusesAnOversizedSlice(t *testing.T) {
	limit := tailLogsReadLimit
	tailLogsReadLimit = 4
	t.Cleanup(func() { tailLogsReadLimit = limit })
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podNameFromID("w1"), Namespace: "workloads"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
	}
	s := New(Options{Clientset: fake.NewSimpleClientset(pod), Namespace: "workloads", Logger: zap.NewNop(), Catalog: testCatalog()})
	// The fake answers every read with "fake logs", over the 4-byte limit at
	// any line count, so every reduced retry also reaches the limit.
	_, err := s.TailWorkloadLogs(context.Background(), &runnerv1.TailWorkloadLogsRequest{WorkloadId: "w1", ContainerName: "main", MaxBytes: 64})
	if status.Code(err) != codes.ResourceExhausted || status.Convert(err).Message() != "log_line_exceeds_read_limit" {
		t.Fatalf("got %v, want ResourceExhausted", err)
	}
}

func TestTailLogsOpenErrorSeparatesStateFromFaults(t *testing.T) {
	s := &Server{logger: zap.NewNop()}
	waiting := apierrors.NewBadRequest(`container "main" in pod "workload-w1" is waiting to start: ContainerCreating`)
	err := s.tailLogsOpenError(waiting)
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), "waiting to start") {
		t.Fatalf("waiting container: got %v", err)
	}
	if err := s.tailLogsOpenError(apierrors.NewNotFound(corev1.Resource("pods"), "workload-w1")); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted Pod: got %v", err)
	}
	if err := s.tailLogsOpenError(io.ErrUnexpectedEOF); status.Code(err) != codes.Internal {
		t.Fatalf("transport fault: got %v", err)
	}
}

// chunkReader returns its data a fixed number of bytes at a time.
type chunkReader struct {
	data  []byte
	chunk int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := r.chunk
	if n > len(p) {
		n = len(p)
	}
	if n > len(r.data) {
		n = len(r.data)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}
