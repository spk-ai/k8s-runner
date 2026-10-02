package workloadproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// DefaultTripwires are destinations no overlay may intercept: a TEST-NET-1
// address (RFC 5737) and a reserved .invalid name (RFC 6761), which a CIDR
// private-resource intercept cannot cover. The proxy must refuse both as
// destination_not_found; anything else means a fallback path exists.
var DefaultTripwires = []string{"192.0.2.1:9", "agyn-tripwire.invalid:443"}

// WaitOptions describe the readiness the overlay sidecar must prove before the
// workload starts. Every probe is HTTP-level: a TCP accept alone proves only
// that a local listener exists, not that a terminator answered.
type WaitOptions struct {
	// Proxy is the sidecar's explicit proxy listener.
	Proxy string
	// HTTP are fixed loopback forwards that must return any HTTP response.
	HTTP []string
	// Connect are overlay authorities that must accept CONNECT through the
	// proxy and then answer a plaintext HTTP request inside the tunnel.
	Connect   []string
	Tripwires []string
	Timeout   time.Duration
	Interval  time.Duration
	// ProbeTimeout bounds one probe's dial, write and read.
	ProbeTimeout time.Duration
	Log          io.Writer
}

// ErrTripwire means the proxy admitted a destination no overlay intercepts.
// It is fatal immediately: retrying would only hide a fail-open proxy.
var ErrTripwire = errors.New("workload proxy admitted an unrouted destination")

func Wait(ctx context.Context, options WaitOptions) error {
	if options.Timeout <= 0 {
		options.Timeout = 180 * time.Second
	}
	if options.Interval <= 0 {
		options.Interval = 250 * time.Millisecond
	}
	if options.ProbeTimeout <= 0 {
		options.ProbeTimeout = 5 * time.Second
	}
	if options.Log == nil {
		options.Log = io.Discard
	}
	if options.Tripwires == nil {
		options.Tripwires = DefaultTripwires
	}
	for _, address := range append([]string{options.Proxy}, options.HTTP...) {
		host, _, err := net.SplitHostPort(address)
		if ip := net.ParseIP(host); err != nil || ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("readiness probe %q is not a literal loopback address", address)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()
	attempt := 0
	for {
		reason, err := waitOnce(ctx, options)
		if err != nil {
			return err
		}
		if reason == "" {
			fmt.Fprintf(options.Log, "workload proxy ready after %d attempts\n", attempt+1)
			return nil
		}
		// Every probe proves something distinct, so report which one is
		// pending, about once every few seconds.
		if attempt%int(max(1, 4*time.Second/options.Interval)) == 0 {
			fmt.Fprintf(options.Log, "waiting for workload proxy: %s\n", reason)
		}
		attempt++
		select {
		case <-ctx.Done():
			return fmt.Errorf("workload proxy not ready within %s: %s", options.Timeout, reason)
		case <-time.After(options.Interval):
		}
	}
}

// waitOnce returns the first pending reason, "" when everything is ready, or
// an error when readiness can never be reached.
func waitOnce(ctx context.Context, options WaitOptions) (string, error) {
	for _, address := range options.HTTP {
		if err := probeHTTP(ctx, address, address, options.ProbeTimeout, nil); err != nil {
			return fmt.Sprintf("forward %s: %v", address, err), nil
		}
	}
	for _, authority := range options.Connect {
		if err := probeHTTP(ctx, options.Proxy, authority, options.ProbeTimeout, &authority); err != nil {
			return fmt.Sprintf("CONNECT %s: %v", authority, err), nil
		}
	}
	for _, tripwire := range options.Tripwires {
		response, err := connectThrough(ctx, options.Proxy, tripwire, options.ProbeTimeout)
		if err != nil {
			return fmt.Sprintf("tripwire %s: %v", tripwire, err), nil
		}
		if response.StatusCode == http.StatusOK {
			return "", fmt.Errorf("%w: CONNECT %s returned 200", ErrTripwire, tripwire)
		}
		status := response.Header.Get("Proxy-Status")
		if response.StatusCode != http.StatusBadGateway || !strings.Contains(status, ProxyStatusName) || !strings.Contains(status, "error="+errDestinationNotFound) {
			return fmt.Sprintf("tripwire %s: got %d %q, want 502 %s", tripwire, response.StatusCode, status, errDestinationNotFound), nil
		}
	}
	return "", nil
}

// probeHTTP sends a plaintext GET to address and requires a parseable HTTP
// response. With tunnel set, address is the proxy and the GET goes inside a
// CONNECT tunnel to *tunnel.
func probeHTTP(ctx context.Context, address, host string, timeout time.Duration, tunnel *string) error {
	conn, reader, err := dialProbe(ctx, address, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	if tunnel != nil {
		fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", *tunnel, *tunnel)
		response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
		if err != nil {
			return fmt.Errorf("no CONNECT response: %v", err)
		}
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("CONNECT returned %d %s", response.StatusCode, response.Header.Get("Proxy-Status"))
		}
	}
	fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nUser-Agent: %s-wait\r\nConnection: close\r\n\r\n", host, ProxyStatusName)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		return fmt.Errorf("no HTTP response: %v", err)
	}
	response.Body.Close()
	if tunnel == nil && response.Header.Get("Proxy-Status") != "" {
		return fmt.Errorf("answered by a proxy, not the destination: %s", response.Header.Get("Proxy-Status"))
	}
	return nil
}

func connectThrough(ctx context.Context, proxy, authority string, timeout time.Duration) (*http.Response, error) {
	conn, reader, err := dialProbe(ctx, proxy, timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", authority, authority)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return nil, fmt.Errorf("no CONNECT response: %v", err)
	}
	response.Body.Close()
	return response, nil
}

func dialProbe(ctx context.Context, address string, timeout time.Duration) (net.Conn, *bufio.Reader, error) {
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, bufio.NewReader(conn), nil
}
