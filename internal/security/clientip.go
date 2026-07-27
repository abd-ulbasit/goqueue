// =============================================================================
// CLIENT IP RESOLUTION - TRUSTED PROXY AWARE
// =============================================================================
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ WHY THIS EXISTS                                                             │
// │                                                                             │
// │ The audit log records a client IP on every auth failure and ACL denial.     │
// │ That field is only worth anything if the caller cannot choose it.           │
// │                                                                             │
// │ X-Forwarded-For is a request header. Anyone who can reach the API can send  │
// │ it and set it to whatever they like. It is trustworthy ONLY for the hops    │
// │ appended by proxies you actually run, and you can only tell those apart     │
// │ from forged entries if you know which peers are your proxies.               │
// │                                                                             │
// │ So: X-Forwarded-For is ignored unless the TCP peer is a configured trusted  │
// │ proxy. With no proxies configured (the default), the client IP is always    │
// │ the TCP peer address, which an attacker cannot spoof over a TCP handshake.  │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// CONFIGURATION:
//
//	GOQUEUE_TRUSTED_PROXIES=10.0.0.0/8,192.168.1.7,::1
//
// Comma-separated CIDR blocks or bare IPs. A bare IP is treated as a /32
// (IPv4) or /128 (IPv6). Set this to the addresses your load balancer or
// ingress controller connects from — nothing else.
//
// WHAT NOT TO DO:
//
//	Do not use chi's middleware.RealIP. It rewrites r.RemoteAddr from the
//	leftmost X-Forwarded-For value with no notion of which peers are
//	trusted, so it hands attacker-controlled data to everything downstream.
//	It is deprecated upstream for exactly this reason (GHSA-3fxj-6jh8-hvhx,
//	GHSA-rjr7-jggh-pgcp, GHSA-9g5q-2w5x-hmxf).
//
// =============================================================================

package security

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
)

// maxForwardedHops caps how many X-Forwarded-For entries we will parse.
// The header is caller-supplied and unbounded; a chain deeper than this is
// not a real deployment, it is someone probing. Beyond the cap we fall back
// to the TCP peer.
const maxForwardedHops = 32

// clientIPContextKey is the context key under which the resolved client IP
// is stored. Unexported so nothing outside this package can forge it.
type clientIPContextKeyType struct{}

var clientIPContextKey = clientIPContextKeyType{}

// TrustedProxies is the set of peer addresses whose X-Forwarded-For header
// is believed. The zero value trusts nothing, which is the safe default:
// with it, ClientIP always reports the TCP peer.
type TrustedProxies struct {
	nets []*net.IPNet
}

// ParseTrustedProxies builds a TrustedProxies from CIDR blocks or bare IPs.
//
// Entries may be CIDR ("10.0.0.0/8", "fd00::/8") or a bare address
// ("192.168.1.7", "::1"), which is widened to a single-host mask.
// Empty entries are skipped. An unparseable entry is an error rather than a
// silent skip — a typo in this list quietly turns the audit log back into
// caller-controlled data, so it should fail loudly.
func ParseTrustedProxies(entries []string) (*TrustedProxies, error) {
	tp := &TrustedProxies{}

	for _, raw := range entries {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}

		if _, network, err := net.ParseCIDR(entry); err == nil {
			tp.nets = append(tp.nets, network)
			continue
		}

		ip := net.ParseIP(entry)
		if ip == nil {
			return nil, &net.ParseError{Type: "trusted proxy CIDR or IP", Text: entry}
		}

		bits := 32
		if ip.To4() == nil {
			bits = 128
		}
		tp.nets = append(tp.nets, &net.IPNet{
			IP:   ip,
			Mask: net.CIDRMask(bits, bits),
		})
	}

	return tp, nil
}

// LoadTrustedProxiesFromEnv reads GOQUEUE_TRUSTED_PROXIES.
//
// Unset or empty yields a TrustedProxies that trusts nothing. A malformed
// value is logged and also yields a trust-nothing set: a bad config must
// degrade toward ignoring X-Forwarded-For, never toward believing it.
func LoadTrustedProxiesFromEnv() *TrustedProxies {
	raw := os.Getenv("GOQUEUE_TRUSTED_PROXIES")
	if strings.TrimSpace(raw) == "" {
		return &TrustedProxies{}
	}

	tp, err := ParseTrustedProxies(strings.Split(raw, ","))
	if err != nil {
		slog.Error("invalid GOQUEUE_TRUSTED_PROXIES, ignoring X-Forwarded-For entirely",
			"error", err)
		return &TrustedProxies{}
	}

	return tp
}

// Empty reports whether no proxies are trusted.
func (tp *TrustedProxies) Empty() bool {
	return tp == nil || len(tp.nets) == 0
}

// contains reports whether ip falls inside any trusted block.
func (tp *TrustedProxies) contains(ip net.IP) bool {
	if tp == nil || ip == nil {
		return false
	}
	for _, network := range tp.nets {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

// Resolve returns the client IP for a request.
//
// ALGORITHM:
//
//  1. peer := host part of r.RemoteAddr — this is the TCP source address,
//     set by the kernel, not by the caller.
//  2. If the peer is not a trusted proxy, return peer. X-Forwarded-For is
//     ignored: a direct caller has no business claiming to have forwarded
//     anything.
//  3. Otherwise walk X-Forwarded-For right-to-left. The rightmost entries
//     were appended by proxies nearest to us; the leftmost is the one the
//     original caller could have written. Return the first entry that is
//     NOT itself a trusted proxy — that is the last hop we cannot vouch for,
//     i.e. the real client.
//  4. If every entry is trusted, or the header is absent or unparseable,
//     return peer.
func (tp *TrustedProxies) Resolve(r *http.Request) string {
	peer := remoteHost(r.RemoteAddr)

	if tp.Empty() {
		return peer
	}

	peerIP := net.ParseIP(peer)
	if !tp.contains(peerIP) {
		return peer
	}

	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded == "" {
		return peer
	}

	hops := strings.Split(forwarded, ",")
	if len(hops) > maxForwardedHops {
		slog.Warn("X-Forwarded-For chain longer than the cap, using the TCP peer",
			"hops", len(hops), "cap", maxForwardedHops, "peer", peer)
		return peer
	}

	for i := len(hops) - 1; i >= 0; i-- {
		ip := net.ParseIP(normalizeHop(hops[i]))
		if ip == nil {
			// A hop we cannot parse breaks the chain of custody: everything
			// to its left could have been written by the caller.
			return peer
		}
		if !tp.contains(ip) {
			return ip.String()
		}
	}

	return peer
}

// ClientIPMiddleware resolves the client IP once per request and puts it in
// the request context for downstream handlers.
//
// It deliberately does NOT mutate r.RemoteAddr. r.RemoteAddr stays the real
// TCP peer for the lifetime of the request, so any code that reads it
// directly still gets an unspoofable value.
func ClientIPMiddleware(tp *TrustedProxies) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), clientIPContextKey, tp.Resolve(r))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ClientIP returns the client IP for a request.
//
// If ClientIPMiddleware ran, this is the resolved value. If it did not, this
// falls back to the TCP peer — never to a header. Callers therefore cannot
// end up with attacker-controlled data by forgetting to install the
// middleware; the worst case is losing the real client behind a proxy.
func ClientIP(r *http.Request) string {
	if ip, ok := r.Context().Value(clientIPContextKey).(string); ok && ip != "" {
		return ip
	}
	return remoteHost(r.RemoteAddr)
}

// remoteHost strips the port from a "host:port" RemoteAddr.
// net/http always sets a port, but a synthetic request in a test may not,
// so fall back to the raw value rather than returning "".
func remoteHost(remoteAddr string) string {
	if remoteAddr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return strings.Trim(remoteAddr, "[]")
	}
	return host
}

// normalizeHop cleans one X-Forwarded-For entry into something ParseIP takes.
// Entries are normally bare IPs, but bracketed IPv6 and stray host:port forms
// show up in the wild.
func normalizeHop(hop string) string {
	hop = strings.TrimSpace(hop)
	if hop == "" {
		return ""
	}
	if net.ParseIP(hop) != nil {
		return hop
	}
	if host, _, err := net.SplitHostPort(hop); err == nil {
		return host
	}
	return strings.Trim(hop, "[]")
}
