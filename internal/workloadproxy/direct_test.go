package workloadproxy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"testing"
)

type fakeResolver map[string][]string

func (r fakeResolver) LookupNetIP(_ context.Context, _ string, host string) ([]netip.Addr, error) {
	values, ok := r[host]
	if !ok {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	if len(values) == 1 && values[0] == "temporary" {
		return nil, &net.DNSError{Err: "server misbehaving", Name: host, IsTemporary: true}
	}
	addrs := make([]netip.Addr, 0, len(values))
	for _, value := range values {
		addrs = append(addrs, netip.MustParseAddr(value))
	}
	return addrs, nil
}

type recordingDialer struct {
	mu       sync.Mutex
	upstream string
	dialed   []string
}

func (d *recordingDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	d.dialed = append(d.dialed, address)
	d.mu.Unlock()
	return (&net.Dialer{}).DialContext(ctx, network, d.upstream)
}

func (d *recordingDialer) calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.dialed...)
}

func TestDirectEgressReachesOnlyPublicAddresses(t *testing.T) {
	// RFC 5737 ranges are already non-public, so the deny entries here are
	// public addresses next to TEST-NET-3.
	deny, err := ParseDeny([]string{"203.0.115.7", "203.0.114.0/24"})
	if err != nil {
		t.Fatal(err)
	}
	direct := &DirectEgress{Deny: deny}
	for address, want := range map[string]bool{
		"93.184.215.14": true, "2606:2800:21f:cb07:6820:80da:af6b:8b2c": true,
		"10.43.0.10": false, "10.42.0.5": false, "172.20.0.1": false, "192.168.1.1": false, "100.64.0.1": false,
		"169.254.169.254": false, "127.0.0.1": false, "0.0.0.0": false, "192.0.2.1": false, "198.18.0.1": false,
		"224.0.0.1": false, "255.255.255.255": false, "::1": false, "fd00::1": false, "fe80::1": false,
		"::ffff:10.0.0.1": false, "::ffff:93.184.215.14": true, "64:ff9b::a00:1": false,
		"203.0.115.7": false, "203.0.115.6": true, "203.0.114.7": false,
	} {
		if got := direct.Public(netip.MustParseAddr(address)); got != want {
			t.Errorf("Public(%s) = %t, want %t", address, got, want)
		}
	}
	if _, err := ParseDeny([]string{"not-an-address"}); err == nil {
		t.Error("invalid deny prefix accepted")
	}
}

func TestDirectEgressCarriesOnlyUninterceptedPublicDestinations(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "host="+r.Host)
	}))
	defer upstream.Close()
	upstreamAddress := strings.TrimPrefix(upstream.URL, "http://")
	overlay := &recordingDialer{upstream: upstreamAddress}
	directDialer := &recordingDialer{upstream: upstreamAddress}
	classifier := &fakeClassifier{known: map[string]bool{"gateway.agyn:443": true}}
	p := NewWithOptions(overlay, Options{Classifier: classifier, Direct: &DirectEgress{
		Resolver: fakeResolver{
			"public.example":   {"93.184.215.14"},
			"internal.example": {"10.43.0.10"},
			"mixed.example":    {"10.0.0.1", "2606:2800:21f:cb07:6820:80da:af6b:8b2c", "93.184.215.15"},
			"node.example":     {"203.0.115.7"},
			"flaky.example":    {"temporary"},
		},
		Dialer: directDialer,
		Deny:   []netip.Prefix{netip.MustParsePrefix("203.0.115.7/32")},
	}})
	server := httptest.NewServer(p)
	defer server.Close()
	defer p.Close()

	for authority, want := range map[string]string{
		"public.example:443": "93.184.215.14:443",
		"mixed.example:8443": "93.184.215.15:8443",
		"93.184.215.16:443":  "93.184.215.16:443",
	} {
		response, _, conn := rawConnect(t, server, authority)
		conn.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("CONNECT %s: %d %q", authority, response.StatusCode, response.Header.Get("Proxy-Status"))
		}
		if calls := directDialer.calls(); calls[len(calls)-1] != want {
			t.Fatalf("CONNECT %s dialed %v, want %s", authority, calls, want)
		}
	}

	response, _, conn := rawConnect(t, server, "gateway.agyn:443")
	conn.Close()
	if response.StatusCode != http.StatusOK || len(overlay.calls()) != 1 || overlay.calls()[0] != "gateway.agyn:443" {
		t.Fatalf("intercepted destination left the overlay: %d overlay=%v", response.StatusCode, overlay.calls())
	}

	before := len(directDialer.calls())
	for authority, kind := range map[string]string{
		"internal.example:443":      errDestinationNotFound,
		"node.example:443":          errDestinationNotFound,
		"192.0.2.1:9":               errDestinationNotFound,
		"agyn-tripwire.invalid:443": errDestinationNotFound,
		"10.43.0.1:443":             errDestinationNotFound,
		"169.254.169.254:80":        errDestinationNotFound,
		"127.0.0.1:18081":           errDestinationNotFound,
		"flaky.example:443":         errDestinationUnavailable,
	} {
		response, _, conn := rawConnect(t, server, authority)
		conn.Close()
		if response.StatusCode != http.StatusBadGateway || !strings.Contains(response.Header.Get("Proxy-Status"), "error="+kind) {
			t.Errorf("CONNECT %s: %d %q, want 502 %s", authority, response.StatusCode, response.Header.Get("Proxy-Status"), kind)
		}
	}
	if after := len(directDialer.calls()); after != before {
		t.Fatalf("refused destinations were dialed: %v", directDialer.calls()[before:])
	}

	u := server.URL
	client := &http.Client{Transport: &http.Transport{Proxy: func(*http.Request) (*url.URL, error) { return url.Parse(u) }}}
	got, err := client.Get("http://public.example/path")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(got.Body)
	got.Body.Close()
	if string(body) != "host=public.example" {
		t.Fatalf("absolute-form request: %s", body)
	}
	if calls := directDialer.calls(); calls[len(calls)-1] != "93.184.215.14:80" {
		t.Fatalf("absolute-form request dialed %v", calls)
	}
}

func TestWithoutDirectEgressUninterceptedIsRefused(t *testing.T) {
	overlay := &recordingDialer{upstream: "127.0.0.1:1"}
	p := NewWithOptions(overlay, Options{Classifier: &fakeClassifier{known: map[string]bool{}}})
	server := httptest.NewServer(p)
	defer server.Close()
	defer p.Close()
	response, _, conn := rawConnect(t, server, "public.example:443")
	conn.Close()
	if response.StatusCode != http.StatusBadGateway || len(overlay.calls()) != 0 {
		t.Fatalf("fail-closed default changed: %d %v", response.StatusCode, overlay.calls())
	}
}
