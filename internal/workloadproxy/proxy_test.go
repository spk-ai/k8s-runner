package workloadproxy

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type dialFunc func(context.Context, string, string) (net.Conn, error)

func (f dialFunc) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return f(ctx, network, address)
}
func clientFor(t *testing.T, p *Proxy) *http.Client {
	t.Helper()
	server := httptest.NewServer(p)
	t.Cleanup(server.Close)
	t.Cleanup(p.Close)
	u, _ := url.Parse(server.URL)
	tr := &http.Transport{Proxy: http.ProxyURL(u), TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} // synthetic loopback certificate only
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 3 * time.Second}
}
func TestHTTPPreservesDestinationAndStripsProxyCredentials(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "api.example.com" || r.Header.Get("Proxy-Authorization") != "" || r.Header.Get("X-Hop") != "" {
			t.Error("destination or proxy headers changed")
		}
		io.WriteString(w, "ok")
	}))
	defer upstream.Close()
	p := New(dialFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "api.example.com:80" {
			t.Errorf("wrong overlay destination %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(upstream.URL, "http://"))
	}))
	request, _ := http.NewRequest("GET", "http://api.example.com/path", nil)
	request.Header.Set("Proxy-Authorization", "fixture-only")
	request.Header.Set("Connection", "X-Hop")
	request.Header.Set("X-Hop", "fixture")
	response, err := clientFor(t, p).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if string(data) != "ok" {
		t.Fatalf("response: %s", data)
	}
}
func TestCONNECTPreservesTLSNameAndHTTPHost(t *testing.T) {
	names := make(chan string, 1)
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "api.anthropic.example" {
			t.Errorf("Host rewritten: %s", r.Host)
		}
		io.WriteString(w, "tls-ok")
	}))
	upstream.TLS = &tls.Config{GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) { names <- hello.ServerName; return nil, nil }}
	upstream.StartTLS()
	defer upstream.Close()
	p := New(dialFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "api.anthropic.example:443" {
			t.Errorf("wrong CONNECT target %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, upstream.Listener.Addr().String())
	}))
	response, err := clientFor(t, p).Get("https://api.anthropic.example/v1/messages")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if string(data) != "tls-ok" || <-names != "api.anthropic.example" {
		t.Fatal("TLS identity changed")
	}
}
func TestCONNECTRetainsPipelinedBytes(t *testing.T) {
	p := New(dialFunc(func(context.Context, string, string) (net.Conn, error) {
		a, b := net.Pipe()
		go func() { defer b.Close(); data := make([]byte, 3); io.ReadFull(b, data); b.Write(data) }()
		return a, nil
	}))
	defer p.Close()
	server := httptest.NewServer(p)
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	io.WriteString(conn, "CONNECT provider.example:443 HTTP/1.1\r\nHost: provider.example:443\r\n\r\nABC")
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: "CONNECT"})
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("CONNECT: %v", err)
	}
	data := make([]byte, 3)
	if _, err = io.ReadFull(reader, data); err != nil || string(data) != "ABC" {
		t.Fatalf("buffered bytes lost: %v %q", err, data)
	}
}
func TestRejectedDestinationsHaveNoFallback(t *testing.T) {
	calls := 0
	p := New(dialFunc(func(context.Context, string, string) (net.Conn, error) {
		calls++
		return nil, errors.New("overlay denied")
	}))
	client := clientFor(t, p)
	response, err := client.Get("http://unavailable.example/")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 502 || calls != 1 {
		t.Fatal("overlay denial not retained")
	}
	for _, authority := range []string{"host:0", "host:65536", "host:-1", "host:443/path", "host%zone:443", ":443"} {
		if _, err := Destination(authority); err == nil {
			t.Errorf("invalid destination accepted: %s", authority)
		}
	}
	for _, address := range []string{"0.0.0.0:0", "[::]:0", "localhost:0", "192.0.2.1:0"} {
		if listener, err := ListenLoopback(address); err == nil {
			listener.Close()
			t.Errorf("nonliteral loopback accepted: %s", address)
		}
	}
}

func TestFixedForwardUsesOriginalOverlayTargetAndCloses(t *testing.T) {
	seen := make(chan string, 1)
	p := New(dialFunc(func(_ context.Context, _ string, address string) (net.Conn, error) {
		seen <- address
		a, b := net.Pipe()
		go func() { defer b.Close(); io.Copy(b, b) }()
		return a, nil
	}))
	listener, err := ListenLoopback("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Forward(ctx, listener, "gateway.agyn:50051") }()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	io.WriteString(conn, "grpc")
	data := make([]byte, 4)
	if _, err = io.ReadFull(conn, data); err != nil || string(data) != "grpc" || <-seen != "gateway.agyn:50051" {
		t.Fatalf("forwarding failed: %v", err)
	}
	p.Close()
	if _, err = conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("shutdown retained tunnel")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("listener did not stop")
	}
}

func TestInvalidConnectRejectedBeforeOverlayDial(t *testing.T) {
	p := New(dialFunc(func(context.Context, string, string) (net.Conn, error) {
		t.Fatal("invalid request dialed")
		return nil, nil
	}))
	defer p.Close()
	for _, authority := range []string{"host:0", "host:65536", "host:-1", "host:443/path"} {
		request := httptest.NewRequest(http.MethodConnect, "http://fixture.invalid", nil)
		request.Host = authority
		response := httptest.NewRecorder()
		p.ServeHTTP(response, request)
		if response.Code != 400 {
			t.Errorf("invalid CONNECT accepted: %s", authority)
		}
	}
}
