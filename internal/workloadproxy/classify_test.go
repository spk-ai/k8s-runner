package workloadproxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openziti/sdk-golang/ziti"
)

type fakeClassifier struct {
	mu        sync.Mutex
	known     map[string]bool
	refreshes int
	// learn is added to known on the first refresh, like a rule attached after
	// the sidecar's last scheduled refresh.
	learn string
}

func (c *fakeClassifier) Intercepted(host string, port uint16) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.known[net.JoinHostPort(host, fmt.Sprint(port))]
}

func (c *fakeClassifier) Refresh() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshes++
	if c.learn != "" {
		c.known[c.learn] = true
	}
}

func rawConnect(t *testing.T, server *httptest.Server, authority string) (*http.Response, *bufio.Reader, net.Conn) {
	t.Helper()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", authority, authority)
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	return response, reader, conn
}

// An unknown destination is refused before the overlay is asked, with a
// distinct Proxy-Status error type -- the property the readiness tripwire
// checks for every pod.
func TestUnknownDestinationIsRefusedWithoutDialing(t *testing.T) {
	var dials atomic.Int32
	classifier := &fakeClassifier{known: map[string]bool{}}
	p := NewWithOptions(dialFunc(func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, fmt.Errorf("must not dial")
	}), Options{Classifier: classifier})
	server := httptest.NewServer(p)
	defer server.Close()
	defer p.Close()
	for _, authority := range []string{"unrouted.example.org:443", "192.0.2.1:9", "agyn-tripwire.invalid:443", "127.0.0.1:18443", "[::1]:22", "localhost:80", "0.0.0.0:80"} {
		response, _, _ := rawConnect(t, server, authority)
		if response.StatusCode != http.StatusBadGateway || response.Header.Get("Proxy-Status") != "agyn-workload-proxy; error=destination_not_found" {
			t.Fatalf("CONNECT %s: %d %q", authority, response.StatusCode, response.Header.Get("Proxy-Status"))
		}
	}
	response, err := clientFor(t, p).Get("http://unrouted.example.org/")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadGateway || !strings.Contains(response.Header.Get("Proxy-Status"), "destination_not_found") {
		t.Fatalf("plain HTTP to unknown host: %d %q", response.StatusCode, response.Header.Get("Proxy-Status"))
	}
	if dials.Load() != 0 {
		t.Fatalf("unknown destinations reached the dialer %d times", dials.Load())
	}
	if classifier.refreshes == 0 {
		t.Fatal("a miss must refresh the service list once before refusing")
	}
}

func TestMissRefreshesBeforeRefusing(t *testing.T) {
	classifier := &fakeClassifier{known: map[string]bool{}, learn: "new-rule.example:443"}
	p := NewWithOptions(dialFunc(func(context.Context, string, string) (net.Conn, error) {
		a, b := net.Pipe()
		go b.Close()
		return a, nil
	}), Options{Classifier: classifier})
	server := httptest.NewServer(p)
	defer server.Close()
	defer p.Close()
	response, _, _ := rawConnect(t, server, "new-rule.example:443")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("freshly attached rule refused: %d", response.StatusCode)
	}
}

func TestDialFailureIsDestinationUnavailable(t *testing.T) {
	classifier := &fakeClassifier{known: map[string]bool{"known.example:443": true, "known.example:80": true}}
	p := NewWithOptions(dialFunc(func(context.Context, string, string) (net.Conn, error) {
		return nil, fmt.Errorf("no terminator")
	}), Options{Classifier: classifier})
	server := httptest.NewServer(p)
	defer server.Close()
	defer p.Close()
	response, _, _ := rawConnect(t, server, "known.example:443")
	if response.StatusCode != http.StatusBadGateway || response.Header.Get("Proxy-Status") != "agyn-workload-proxy; error=destination_unavailable" {
		t.Fatalf("dial failure: %d %q", response.StatusCode, response.Header.Get("Proxy-Status"))
	}
	plain, err := clientFor(t, p).Get("http://known.example/")
	if err != nil {
		t.Fatal(err)
	}
	plain.Body.Close()
	if plain.StatusCode != http.StatusBadGateway || !strings.Contains(plain.Header.Get("Proxy-Status"), "destination_unavailable") {
		t.Fatalf("plain HTTP dial failure: %d %q", plain.StatusCode, plain.Header.Get("Proxy-Status"))
	}
}

// A client that ignores NO_PROXY sends the fixed forward's own authority to
// the proxy; it still reaches the forward's overlay target, never loopback.
func TestForwardAuthorityThroughProxyReachesItsOverlayTarget(t *testing.T) {
	seen := make(chan string, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "llm") }))
	defer upstream.Close()
	p := NewWithOptions(dialFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		seen <- address
		return (&net.Dialer{}).DialContext(ctx, network, upstream.Listener.Addr().String())
	}), Options{Classifier: &fakeClassifier{known: map[string]bool{}}, Forwards: map[string]string{"127.0.0.1:18081": "llm-proxy.agyn:80"}})
	response, err := clientFor(t, p).Get("http://127.0.0.1:18081/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if string(data) != "llm" || <-seen != "llm-proxy.agyn:80" {
		t.Fatalf("forward authority not mapped: %q", data)
	}
}

func TestTunnelCapRefusesExcessConnections(t *testing.T) {
	release := make(chan struct{})
	p := NewWithOptions(dialFunc(func(context.Context, string, string) (net.Conn, error) {
		a, b := net.Pipe()
		go func() { <-release; b.Close() }()
		return a, nil
	}), Options{MaxTunnels: 1})
	server := httptest.NewServer(p)
	defer server.Close()
	defer p.Close()
	defer close(release)
	if first, _, _ := rawConnect(t, server, "a.example:443"); first.StatusCode != http.StatusOK {
		t.Fatalf("first tunnel: %d", first.StatusCode)
	}
	second, _, _ := rawConnect(t, server, "b.example:443")
	if second.StatusCode != http.StatusServiceUnavailable || !strings.Contains(second.Header.Get("Proxy-Status"), "connection_limit_reached") {
		t.Fatalf("over-limit tunnel: %d %q", second.StatusCode, second.Header.Get("Proxy-Status"))
	}
}

// A client that half-closes after its request still receives the response.
func TestCONNECTPropagatesHalfClose(t *testing.T) {
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	go func() {
		conn, err := upstream.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		request, _ := io.ReadAll(conn) // until the client's half-close
		conn.Write([]byte("echo:" + string(request)))
	}()
	p := New(dialFunc(func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, upstream.Addr().String())
	}))
	server := httptest.NewServer(p)
	defer server.Close()
	defer p.Close()
	response, reader, conn := rawConnect(t, server, "half.example:443")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT: %d", response.StatusCode)
	}
	conn.Write([]byte("ping"))
	conn.(*net.TCPConn).CloseWrite()
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "echo:ping" {
		t.Fatalf("half-close lost the response: %v %q", err, data)
	}
}

func TestValidateForwards(t *testing.T) {
	good := map[string]string{"127.0.0.1:18443": "gateway.agyn:443", "127.0.0.1:18081": "llm-proxy.agyn:80"}
	if err := ValidateForwards("127.0.0.1:18080", good); err != nil {
		t.Fatal(err)
	}
	for name, forwards := range map[string]map[string]string{
		"collides with proxy": {"127.0.0.1:18080": "gateway.agyn:443"},
		"wildcard":            {"0.0.0.0:18443": "gateway.agyn:443"},
		"hostname":            {"localhost:18443": "gateway.agyn:443"},
		"pod address":         {"10.0.0.5:18443": "gateway.agyn:443"},
		"bad target":          {"127.0.0.1:18443": "gateway.agyn"},
		"unnormalized":        {"127.0.0.1:018443": "gateway.agyn:443"},
	} {
		if err := ValidateForwards("127.0.0.1:18080", forwards); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

// Only the collection's NewDialer is acceptable: a fallback dialer would turn
// every unmatched destination into a direct host-network connection.
func TestRequireNoFallbackAcceptsOnlyTheOverlayOnlyDialer(t *testing.T) {
	collection := ziti.NewSdkCollection()
	if err := RequireNoFallback(collection.NewDialer()); err != nil {
		t.Fatalf("overlay-only dialer refused: %v", err)
	}
	if err := RequireNoFallback(collection.NewDialerWithFallback(context.Background(), nil)); err == nil {
		t.Fatal("fallback dialer accepted")
	}
	if err := RequireNoFallback(collection.NewDialerWithFallback(context.Background(), &net.Dialer{})); err == nil {
		t.Fatal("explicit net.Dialer fallback accepted")
	}
	if err := RequireNoFallback(dialFunc(nil)); err == nil {
		t.Fatal("unknown dialer shape accepted")
	}
}
