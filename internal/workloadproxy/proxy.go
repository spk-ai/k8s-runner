// Package workloadproxy adapts explicit HTTP clients to an overlay-only dialer.
// It never resolves or dials a destination through the host network itself.
package workloadproxy

import (
	"context"
	"errors"
	"fmt"
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

// Classifier answers whether the overlay intercepts a destination, so an
// unknown one is refused before any dial and with a distinct reason. Refresh
// asks the overlay for its current service list once; implementations must
// bound how often that reaches the controller.
type Classifier interface {
	Intercepted(host string, port uint16) bool
	Refresh()
}

// Options are the serve-time settings of a Proxy. The zero value classifies
// nothing (every destination goes to the dialer, whose own refusal is the
// answer), has no fixed forwards and the default tunnel cap.
type Options struct {
	Classifier Classifier
	// Forwards maps a fixed loopback listen authority to its overlay target.
	// A proxied request for that authority is sent to the target, so a client
	// that ignores NO_PROXY still reaches the platform endpoint.
	Forwards map[string]string
	// MaxTunnels caps concurrent CONNECT tunnels and forwarded connections.
	MaxTunnels int
}

// DefaultMaxTunnels bounds what a runaway workload can hold open at once.
const DefaultMaxTunnels = 512

// ProxyStatusName is this proxy's name in RFC 9209 Proxy-Status responses.
const ProxyStatusName = "agyn-workload-proxy"

const (
	errDestinationNotFound    = "destination_not_found"
	errDestinationUnavailable = "destination_unavailable"
	errConnectionLimit        = "connection_limit_reached"
)

// proxyError is a refusal the client is told about with a Proxy-Status error
// type, so a workload (and the readiness tripwire) can tell "the overlay has no
// such destination" from "the destination did not answer".
type proxyError struct {
	kind   string
	status int
}

func (e *proxyError) Error() string { return e.kind }

var (
	notFound    = &proxyError{kind: errDestinationNotFound, status: http.StatusBadGateway}
	unavailable = &proxyError{kind: errDestinationUnavailable, status: http.StatusBadGateway}
	overLimit   = &proxyError{kind: errConnectionLimit, status: http.StatusServiceUnavailable}
)

type Proxy struct {
	dialer     Dialer
	classifier Classifier
	forwards   map[string]string
	slots      chan struct{}
	transport  *http.Transport
	mu         sync.Mutex
	tunnels    map[net.Conn]struct{}
	closed     bool
}

func New(dialer Dialer) *Proxy {
	return NewWithOptions(dialer, Options{})
}

func NewWithOptions(dialer Dialer, options Options) *Proxy {
	if dialer == nil {
		panic("overlay dialer required")
	}
	limit := options.MaxTunnels
	if limit <= 0 {
		limit = DefaultMaxTunnels
	}
	p := &Proxy{dialer: dialer, classifier: options.Classifier, forwards: map[string]string{}, slots: make(chan struct{}, limit), tunnels: map[net.Conn]struct{}{}}
	for listen, target := range options.Forwards {
		p.forwards[listen] = target
	}
	p.transport = &http.Transport{DialContext: p.dialAuthority, Proxy: nil, ResponseHeaderTimeout: 30 * time.Second, IdleConnTimeout: 30 * time.Second, MaxIdleConns: 32}
	return p
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

// target maps a validated authority to the overlay address to dial, or
// refuses it. A fixed forward's own listen address maps to its target; any
// other loopback or unspecified address is refused outright, so the proxy is
// never a way back into the pod. Everything else must be intercepted.
func (p *Proxy) target(authority string) (string, *proxyError) {
	destination, err := Destination(authority)
	if err != nil {
		return "", notFound
	}
	if forward, ok := p.forwards[destination]; ok {
		return forward, nil
	}
	host, portText, _ := net.SplitHostPort(destination)
	if ip := net.ParseIP(host); ip != nil && (ip.IsLoopback() || ip.IsUnspecified()) {
		return "", notFound
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return "", notFound
	}
	if p.classifier == nil {
		return destination, nil
	}
	port, _ := strconv.ParseUint(portText, 10, 16)
	if p.classifier.Intercepted(host, uint16(port)) {
		return destination, nil
	}
	// A rule attached moments ago may not have reached this identity's
	// service list yet; one bounded refresh rather than a stale refusal.
	p.classifier.Refresh()
	if p.classifier.Intercepted(host, uint16(port)) {
		return destination, nil
	}
	return "", notFound
}

// dialAuthority is the HTTP transport's only dialer: it maps and classifies
// like CONNECT does, so absolute-form requests cannot take a different path.
func (p *Proxy) dialAuthority(ctx context.Context, network, address string) (net.Conn, error) {
	target, refusal := p.target(address)
	if refusal != nil {
		return nil, refusal
	}
	conn, err := p.dialer.DialContext(ctx, network, target)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", unavailable, err)
	}
	return conn, nil
}

func refuse(w http.ResponseWriter, refusal *proxyError) {
	w.Header().Set("Proxy-Status", ProxyStatusName+"; error="+refusal.kind)
	w.Header().Set("Connection", "close")
	http.Error(w, "overlay "+strings.ReplaceAll(refusal.kind, "_", " "), refusal.status)
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
	if _, refusal := p.target(authority); refusal != nil {
		refuse(w, refusal)
		return
	}
	request := r.Clone(r.Context())
	request.RequestURI = ""
	request.Header = r.Header.Clone()
	stripHopHeaders(request.Header)
	response, err := p.transport.RoundTrip(request)
	if err != nil {
		var refusal *proxyError
		if !errors.As(err, &refusal) {
			refusal = unavailable
		}
		refuse(w, refusal)
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
	if _, err := Destination(r.Host); err != nil {
		http.Error(w, "invalid destination", http.StatusBadRequest)
		return
	}
	target, refusal := p.target(r.Host)
	if refusal != nil {
		refuse(w, refusal)
		return
	}
	if !p.acquire() {
		refuse(w, overLimit)
		return
	}
	defer p.release()
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "tunneling unavailable", http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	upstream, err := p.dialer.DialContext(ctx, "tcp", target)
	cancel()
	if err != nil {
		refuse(w, unavailable)
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
	splice(downstream, buffered, upstream)
}

// halfCloseGrace bounds how long one direction may stay open after the other
// finished, so a peer that never closes cannot pin a tunnel slot forever.
const halfCloseGrace = 30 * time.Second

// splice copies both directions and propagates a finished direction as a
// half-close where the connection supports it, so request/response protocols
// that signal the end of a request with EOF keep working.
func splice(client net.Conn, clientReader io.Reader, upstream net.Conn) {
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, clientReader); closeWrite(upstream); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, upstream); closeWrite(client); done <- struct{}{} }()
	<-done
	select {
	case <-done:
	case <-time.After(halfCloseGrace):
	}
	client.Close()
	upstream.Close()
}

func closeWrite(conn net.Conn) {
	if writer, ok := conn.(interface{ CloseWrite() error }); ok && writer.CloseWrite() == nil {
		return
	}
	conn.Close()
}

func (p *Proxy) acquire() bool {
	select {
	case p.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (p *Proxy) release() { <-p.slots }

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
		if !p.acquire() {
			client.Close()
			continue
		}
		go func() {
			defer p.release()
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
			splice(client, client, remote)
		}()
	}
}

// ValidateForwards checks a serve configuration before anything listens: every
// forward listens on a distinct literal loopback authority that is not the
// proxy's own, and targets a valid overlay destination.
func ValidateForwards(listen string, forwards map[string]string) error {
	own, err := Destination(listen)
	if err != nil {
		return fmt.Errorf("invalid proxy listener")
	}
	for from, to := range forwards {
		normalized, err := Destination(from)
		if err != nil || normalized != from {
			return fmt.Errorf("invalid forwarding listener %q", from)
		}
		host, _, _ := net.SplitHostPort(from)
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("forwarding listener %q is not a literal loopback address", from)
		}
		if from == own {
			return fmt.Errorf("forwarding listener %q collides with the proxy listener", from)
		}
		if _, err := Destination(to); err != nil {
			return fmt.Errorf("invalid forwarding destination for %q", from)
		}
	}
	return nil
}
