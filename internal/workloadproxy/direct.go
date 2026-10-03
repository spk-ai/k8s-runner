package workloadproxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"
)

// Resolver is the subset of *net.Resolver direct egress needs.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// DirectEgress lets a destination the overlay does not intercept leave the
// Pod directly, but only to a public unicast address. The host is resolved
// here and the chosen literal address is dialed, so a later DNS answer cannot
// steer the connection into the cluster, the node or a private network.
// Overlay-intercepted destinations and fixed forwards never take this path.
type DirectEgress struct {
	Resolver Resolver
	// Dialer dials the resolved literal address.
	Dialer Dialer
	// Deny adds operator prefixes (for example the node's public address) to
	// the built-in non-public ranges.
	Deny []netip.Prefix
}

// NewDirectEgress is the host-network direct path with the standard resolver.
func NewDirectEgress(deny []netip.Prefix) *DirectEgress {
	return &DirectEgress{
		Resolver: net.DefaultResolver,
		Dialer:   &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second},
		Deny:     deny,
	}
}

// nonPublic are special-purpose ranges (RFC 6890 and successors) a direct
// connection must never reach: the cluster, the node and private networks
// are all inside them, as are the readiness tripwires.
var nonPublic = func() []netip.Prefix {
	var prefixes []netip.Prefix
	for _, cidr := range []string{
		"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12",
		"192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15",
		"198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
		"::/128", "::1/128", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001::/23", "2001:db8::/32",
		"2002::/16", "fc00::/7", "fe80::/10", "ff00::/8",
	} {
		prefixes = append(prefixes, netip.MustParsePrefix(cidr))
	}
	return prefixes
}()

// ParseDeny parses operator deny prefixes; a bare address is a single host.
func ParseDeny(values []string) ([]netip.Prefix, error) {
	prefixes := make([]netip.Prefix, 0, len(values))
	for _, value := range values {
		if prefix, err := netip.ParsePrefix(value); err == nil {
			prefixes = append(prefixes, prefix.Masked())
			continue
		}
		addr, err := netip.ParseAddr(value)
		if err != nil {
			return nil, fmt.Errorf("invalid direct egress deny prefix %q", value)
		}
		prefixes = append(prefixes, netip.PrefixFrom(addr.Unmap(), addr.Unmap().BitLen()))
	}
	return prefixes, nil
}

// Public reports whether a direct connection may reach addr.
func (d *DirectEgress) Public(addr netip.Addr) bool {
	addr = addr.Unmap()
	if !addr.IsValid() || !addr.IsGlobalUnicast() || addr.IsPrivate() {
		return false
	}
	for _, prefixes := range [][]netip.Prefix{nonPublic, d.Deny} {
		for _, prefix := range prefixes {
			if prefix.Contains(addr) {
				return false
			}
		}
	}
	return true
}

// resolve maps host:port to one public literal address. An unknown name or a
// name with no public address is destination_not_found, like an overlay miss;
// a resolver failure is destination_unavailable.
func (d *DirectEgress) resolve(ctx context.Context, host string, port uint16) (string, *proxyError) {
	var candidates []netip.Addr
	if addr, err := netip.ParseAddr(host); err == nil {
		candidates = []netip.Addr{addr}
	} else {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		addrs, err := d.Resolver.LookupNetIP(ctx, "ip", host)
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
			return "", notFound
		}
		if err != nil {
			return "", unavailable
		}
		candidates = addrs
	}
	// IPv4 first: task Pods are not assumed to have IPv6 egress.
	for _, wantV4 := range []bool{true, false} {
		for _, addr := range candidates {
			if addr.Unmap().Is4() == wantV4 && d.Public(addr) {
				return net.JoinHostPort(addr.Unmap().String(), strconv.Itoa(int(port))), nil
			}
		}
	}
	return "", notFound
}
