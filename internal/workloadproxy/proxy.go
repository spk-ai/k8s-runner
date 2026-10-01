// Package workloadproxy adapts explicit HTTP clients to an overlay-only dialer.
// It never resolves or dials a destination through the host network itself.
package workloadproxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Dialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

type Proxy struct {
	dialer    Dialer
	transport *http.Transport
	mu        sync.Mutex
	tunnels   map[net.Conn]struct{}
	closed    bool
}

func New(dialer Dialer) *Proxy {
	if dialer == nil {
		panic("overlay dialer required")
	}
	return &Proxy{dialer: dialer, transport: &http.Transport{DialContext: dialer.DialContext, Proxy: nil, ResponseHeaderTimeout: 30 * time.Second, IdleConnTimeout: 30 * time.Second, MaxIdleConns: 32}, tunnels: map[net.Conn]struct{}{}}
}

func Destination(authority string) (string, error) {
	host, port, err := net.SplitHostPort(authority)
	if err != nil || host == "" || strings.ContainsAny(host, "/@?#% \t\r\n") {
		return "", errors.New("invalid destination")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", errors.New("invalid destination port")
	}
	return net.JoinHostPort(host, strconv.Itoa(n)), nil
}

// ListenLoopback deliberately accepts only literal loopback IPs. No DNS result,
// wildcard bind, low privileged port or namespace capability is required.
func ListenLoopback(address string) (net.Listener, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return nil, errors.New("literal loopback listener required")
	}
	return net.Listen("tcp", address)
}

func stripHopHeaders(header http.Header) {
	for _, v := range header.Values("Connection") {
		for _, name := range strings.Split(v, ",") {
			header.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range []string{"Connection", "Proxy-Connection", "Proxy-Authorization", "Proxy-Authenticate", "Keep-Alive", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		header.Del(name)
	}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.connect(w, r)
		return
	}
	if r.URL.Scheme != "http" || r.URL.Host == "" || r.URL.User != nil || r.Header.Get("Upgrade") != "" || r.Host != r.URL.Host {
		http.Error(w, "explicit HTTP URL or CONNECT required", http.StatusBadRequest)
		return
	}
	authority := r.URL.Host
	if r.URL.Port() == "" {
		authority = net.JoinHostPort(r.URL.Hostname(), "80")
	}
	if _, err := Destination(authority); err != nil {
		http.Error(w, "invalid destination", http.StatusBadRequest)
		return
	}
	request := r.Clone(r.Context())
	request.RequestURI = ""
	request.Header = r.Header.Clone()
	stripHopHeaders(request.Header)
	response, err := p.transport.RoundTrip(request)
	if err != nil {
		http.Error(w, "overlay destination unavailable", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	stripHopHeaders(response.Header)
	for name, values := range response.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(response.StatusCode)
	_, _ = io.Copy(w, response.Body)
}

func (p *Proxy) connect(w http.ResponseWriter, r *http.Request) {
	destination, err := Destination(r.Host)
	if err != nil {
		http.Error(w, "invalid destination", http.StatusBadRequest)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "tunneling unavailable", http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	upstream, err := p.dialer.DialContext(ctx, "tcp", destination)
	cancel()
	if err != nil {
		http.Error(w, "overlay destination unavailable", http.StatusBadGateway)
		return
	}
	downstream, buffered, err := hijacker.Hijack()
	if err != nil {
		upstream.Close()
		return
	}
	if !p.track(upstream, downstream) {
		upstream.Close()
		downstream.Close()
		return
	}
	defer p.finish(upstream, downstream)
	// Preserve bytes already buffered after CONNECT, including a pipelined TLS
	// ClientHello. Host/SNI stay with the original client; this is not TLS MITM.
	if _, err = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err = buffered.Flush(); err != nil {
		return
	}
	done := make(chan struct{})
	go func() { _, _ = io.Copy(upstream, buffered); upstream.Close(); downstream.Close(); close(done) }()
	_, _ = io.Copy(downstream, upstream)
	downstream.Close()
	upstream.Close()
	<-done
}

func (p *Proxy) track(connections ...net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	for _, conn := range connections {
		p.tunnels[conn] = struct{}{}
	}
	return true
}
func (p *Proxy) finish(connections ...net.Conn) {
	for _, conn := range connections {
		conn.Close()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range connections {
		delete(p.tunnels, conn)
	}
}
func (p *Proxy) Close() {
	p.mu.Lock()
	p.closed = true
	connections := make([]net.Conn, 0, len(p.tunnels))
	for conn := range p.tunnels {
		connections = append(connections, conn)
	}
	p.mu.Unlock()
	for _, conn := range connections {
		conn.Close()
	}
	p.transport.CloseIdleConnections()
}

// Forward serves a fixed native endpoint without changing its overlay address.
// The listener must already have been created through ListenLoopback.
func (p *Proxy) Forward(ctx context.Context, listener net.Listener, destination string) error {
	target, err := Destination(destination)
	if err != nil {
		return err
	}
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			listener.Close()
		case <-stopped:
		}
	}()
	for {
		client, err := listener.Accept()
		if err != nil {
			return err
		}
		go func() {
			dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			remote, err := p.dialer.DialContext(dialCtx, "tcp", target)
			cancel()
			if err != nil {
				client.Close()
				return
			}
			if !p.track(client, remote) {
				client.Close()
				remote.Close()
				return
			}
			defer p.finish(client, remote)
			done := make(chan struct{})
			go func() { io.Copy(remote, client); remote.Close(); client.Close(); close(done) }()
			io.Copy(client, remote)
			client.Close()
			remote.Close()
			<-done
		}()
	}
}
