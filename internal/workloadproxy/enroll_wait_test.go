package workloadproxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const fixtureToken = "header.fixture-enrollment-secret.signature"

func TestEnrollIdentityWritesPrivateIdentityOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.json")
	calls := 0
	enrollFn := func(_ context.Context, token string) ([]byte, error) {
		calls++
		if token != fixtureToken {
			t.Fatalf("token not passed through: %q", token)
		}
		return []byte(`{"ztAPI":"https://controller.example:1280/edge/client/v1"}`), nil
	}
	if err := EnrollIdentity(context.Background(), path, " "+fixtureToken+"\n", enrollFn); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode %v: %v", info.Mode().Perm(), err)
	}
	// A restarted sandbox must not spend the single-use token again.
	if err := EnrollIdentity(context.Background(), path, "", enrollFn); err != nil || calls != 1 {
		t.Fatalf("second run enrolled again (%d calls): %v", calls, err)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("temporary identity left behind: %v", entries)
	}
}

func TestEnrollIdentityNeverExposesTheToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.json")
	err := EnrollIdentity(context.Background(), path, fixtureToken, func(context.Context, string) ([]byte, error) {
		return nil, fmt.Errorf("controller rejected %s", fixtureToken)
	})
	if err == nil || strings.Contains(err.Error(), "fixture-enrollment-secret") {
		t.Fatalf("token leaked or failure hidden: %v", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatal("failed enrollment left an identity")
	}
	if err := EnrollIdentity(context.Background(), path, "  ", nil); err == nil {
		t.Fatal("missing token accepted")
	}
}

// fakeOverlay stands in for a running serve: a proxy whose CONNECT answers
// are chosen per authority, and forwards that answer HTTP or only accept.
type fakeOverlay struct {
	proxy   *httptest.Server
	connect map[string]int
}

func newFakeOverlay(t *testing.T, connect map[string]int) *fakeOverlay {
	t.Helper()
	overlay := &fakeOverlay{connect: connect}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "gateway") }))
	t.Cleanup(backend.Close)
	overlay.proxy = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code, ok := overlay.connect[r.Host]
		if !ok || code != http.StatusOK {
			if !ok {
				code = http.StatusBadGateway
			}
			w.Header().Set("Proxy-Status", "agyn-workload-proxy; error=destination_not_found")
			w.WriteHeader(code)
			return
		}
		upstream, err := net.Dial("tcp", backend.Listener.Addr().String())
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
		conn, buffered, _ := w.(http.Hijacker).Hijack()
		go func() { io.Copy(upstream, buffered); upstream.Close() }()
		io.Copy(conn, upstream)
		conn.Close()
	}))
	t.Cleanup(overlay.proxy.Close)
	return overlay
}

func httpForward(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	t.Cleanup(server.Close)
	return server.Listener.Addr().String()
}

// silentForward accepts TCP and closes without a byte, which is what a
// forward does when its overlay dial fails.
func silentForward(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	return listener.Addr().String()
}

func quickWait(proxy string, http []string, connect []string, log io.Writer) WaitOptions {
	return WaitOptions{Proxy: proxy, HTTP: http, Connect: connect, Timeout: 600 * time.Millisecond, Interval: 20 * time.Millisecond, ProbeTimeout: 200 * time.Millisecond, Log: log}
}

func TestWaitSucceedsOnlyWithHTTPAnswersAndRefusedTripwires(t *testing.T) {
	overlay := newFakeOverlay(t, map[string]int{"gateway.agyn:443": http.StatusOK})
	var log bytes.Buffer
	err := Wait(context.Background(), quickWait(overlay.proxy.Listener.Addr().String(), []string{httpForward(t)}, []string{"gateway.agyn:443"}, &log))
	if err != nil || !strings.Contains(log.String(), "ready") {
		t.Fatalf("ready overlay not accepted: %v %s", err, log.String())
	}
}

func TestWaitRejectsForwardThatOnlyAcceptsTCP(t *testing.T) {
	overlay := newFakeOverlay(t, map[string]int{"gateway.agyn:443": http.StatusOK})
	err := Wait(context.Background(), quickWait(overlay.proxy.Listener.Addr().String(), []string{silentForward(t)}, nil, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "no HTTP response") {
		t.Fatalf("TCP accept treated as ready: %v", err)
	}
}

func TestWaitRejectsProxyGeneratedBadGateway(t *testing.T) {
	overlay := newFakeOverlay(t, map[string]int{})
	err := Wait(context.Background(), quickWait(overlay.proxy.Listener.Addr().String(), nil, []string{"gateway.agyn:443"}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "CONNECT returned 502") {
		t.Fatalf("502 from the proxy treated as ready: %v", err)
	}
}

func TestWaitFailsImmediatelyWhenTripwireIsAdmitted(t *testing.T) {
	overlay := newFakeOverlay(t, map[string]int{"192.0.2.1:9": http.StatusOK})
	start := time.Now()
	options := quickWait(overlay.proxy.Listener.Addr().String(), nil, nil, io.Discard)
	options.Timeout = 10 * time.Second
	err := Wait(context.Background(), options)
	if !errors.Is(err, ErrTripwire) || time.Since(start) > 2*time.Second {
		t.Fatalf("fail-open proxy not fatal at once: %v after %s", err, time.Since(start))
	}
}

func TestWaitRefusesNonLoopbackProbes(t *testing.T) {
	if err := Wait(context.Background(), WaitOptions{Proxy: "10.0.0.5:18080"}); err == nil {
		t.Fatal("non-loopback proxy probe accepted")
	}
	if err := Wait(context.Background(), WaitOptions{Proxy: "127.0.0.1:18080", HTTP: []string{"gateway.agyn:443"}}); err == nil {
		t.Fatal("non-loopback forward probe accepted")
	}
}

// The real proxy, end to end through the wait: a classifier that knows only
// the gateway, the gateway's forward, and the built-in tripwires.
func TestWaitAgainstRealProxy(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	defer gateway.Close()
	dial := dialFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "gateway.agyn:443" {
			t.Errorf("dialed %s", address)
			return nil, errors.New("unexpected")
		}
		return (&net.Dialer{}).DialContext(ctx, network, gateway.Listener.Addr().String())
	})
	p := NewWithOptions(dial, Options{Classifier: &fakeClassifier{known: map[string]bool{"gateway.agyn:443": true}}})
	defer p.Close()
	proxyListener, err := ListenLoopback("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: p}
	go server.Serve(proxyListener)
	defer server.Close()
	forwardListener, err := ListenLoopback("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Forward(ctx, forwardListener, "gateway.agyn:443")
	if err := Wait(ctx, quickWait(proxyListener.Addr().String(), []string{forwardListener.Addr().String()}, []string{"gateway.agyn:443"}, io.Discard)); err != nil {
		t.Fatalf("real proxy not ready: %v", err)
	}
}
