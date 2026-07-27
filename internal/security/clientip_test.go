package security

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// forgedIP is the address an attacker plants in X-Forwarded-For hoping it is
// what gets written to the audit log instead of their own.
const forgedIP = "1.2.3.4"

func mustTrust(t *testing.T, entries ...string) *TrustedProxies {
	t.Helper()
	tp, err := ParseTrustedProxies(entries)
	if err != nil {
		t.Fatalf("ParseTrustedProxies(%v): %v", entries, err)
	}
	return tp
}

func TestTrustedProxies_Resolve(t *testing.T) {
	tests := []struct {
		name       string
		trusted    []string
		remoteAddr string
		forwarded  string
		want       string
	}{
		{
			// The default deployment: no proxies configured. A direct caller
			// sends X-Forwarded-For and it must be ignored outright.
			name:       "no trusted proxies ignores a forged header",
			trusted:    nil,
			remoteAddr: "203.0.113.9:44321",
			forwarded:  forgedIP,
			want:       "203.0.113.9",
		},
		{
			// A proxy list is configured, but this caller is not one of them.
			name:       "untrusted peer ignores a forged header",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "203.0.113.9:44321",
			forwarded:  forgedIP,
			want:       "203.0.113.9",
		},
		{
			// The caller tries to look like the whole chain came through our
			// own proxies. The peer is still what decides.
			name:       "untrusted peer cannot forge a trusted-looking chain",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "203.0.113.9:44321",
			forwarded:  forgedIP + ", 10.0.0.1",
			want:       "203.0.113.9",
		},
		{
			name:       "trusted peer with no header falls back to the peer",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.1:44321",
			forwarded:  "",
			want:       "10.0.0.1",
		},
		{
			name:       "trusted peer reports the forwarded client",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.1:44321",
			forwarded:  "198.51.100.7",
			want:       "198.51.100.7",
		},
		{
			// Two of our own proxies in the path. Walking right-to-left skips
			// both and stops at the first address we cannot vouch for.
			name:       "trusted peer skips trusted hops right to left",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.1:44321",
			forwarded:  "198.51.100.7, 10.0.0.9, 10.0.0.5",
			want:       "198.51.100.7",
		},
		{
			// The client itself prepended a lie before hitting our edge. The
			// rightmost untrusted entry is the address our own proxy saw.
			name:       "client-prepended entries do not win",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.1:44321",
			forwarded:  forgedIP + ", 198.51.100.7, 10.0.0.5",
			want:       "198.51.100.7",
		},
		{
			name:       "all hops trusted falls back to the peer",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.1:44321",
			forwarded:  "10.0.0.9, 10.0.0.5",
			want:       "10.0.0.1",
		},
		{
			// A hop we cannot parse breaks the chain of custody, so nothing
			// to its left can be believed.
			name:       "unparseable hop falls back to the peer",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.1:44321",
			forwarded:  "198.51.100.7, not-an-ip",
			want:       "10.0.0.1",
		},
		{
			name:       "absurdly long chain falls back to the peer",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.1:44321",
			forwarded:  strings.Repeat("10.0.0.5, ", maxForwardedHops) + forgedIP,
			want:       "10.0.0.1",
		},
		{
			name:       "bare IP entry is treated as a single host",
			trusted:    []string{"192.168.1.7"},
			remoteAddr: "192.168.1.7:44321",
			forwarded:  "198.51.100.7",
			want:       "198.51.100.7",
		},
		{
			name:       "bare IP entry does not trust its neighbors",
			trusted:    []string{"192.168.1.7"},
			remoteAddr: "192.168.1.8:44321",
			forwarded:  forgedIP,
			want:       "192.168.1.8",
		},
		{
			name:       "ipv6 proxy and ipv6 client",
			trusted:    []string{"fd00::/8"},
			remoteAddr: "[fd00::1]:44321",
			forwarded:  "2001:db8::5",
			want:       "2001:db8::5",
		},
		{
			name:       "forwarded entry carrying a port is still parsed",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.1:44321",
			forwarded:  "198.51.100.7:9999",
			want:       "198.51.100.7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/v1/topics", nil)
			req.RemoteAddr = tt.remoteAddr
			if tt.forwarded != "" {
				req.Header.Set("X-Forwarded-For", tt.forwarded)
			}

			if got := mustTrust(t, tt.trusted...).Resolve(req); got != tt.want {
				t.Errorf("Resolve() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseTrustedProxies_RejectsGarbage(t *testing.T) {
	// A typo here silently turns the audit log back into caller-controlled
	// data, so it has to be an error rather than a skipped entry.
	if _, err := ParseTrustedProxies([]string{"10.0.0.0/8", "nonsense"}); err == nil {
		t.Fatal("ParseTrustedProxies should reject an unparseable entry")
	}
}

func TestParseTrustedProxies_SkipsEmptyEntries(t *testing.T) {
	tp, err := ParseTrustedProxies([]string{"", "  ", "10.0.0.0/8", ""})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tp.nets) != 1 {
		t.Errorf("expected 1 trusted block, got %d", len(tp.nets))
	}
}

func TestLoadTrustedProxiesFromEnv_BadValueTrustsNothing(t *testing.T) {
	t.Setenv("GOQUEUE_TRUSTED_PROXIES", "10.0.0.0/8,nonsense")

	// A malformed config must degrade toward ignoring X-Forwarded-For, never
	// toward believing it.
	if tp := LoadTrustedProxiesFromEnv(); !tp.Empty() {
		t.Error("a malformed GOQUEUE_TRUSTED_PROXIES should trust nothing")
	}
}

func TestLoadTrustedProxiesFromEnv_Unset(t *testing.T) {
	t.Setenv("GOQUEUE_TRUSTED_PROXIES", "")

	if tp := LoadTrustedProxiesFromEnv(); !tp.Empty() {
		t.Error("an unset GOQUEUE_TRUSTED_PROXIES should trust nothing")
	}
}

func TestClientIP_FallsBackToPeerWithoutMiddleware(t *testing.T) {
	// If someone wires up AuditMiddleware but forgets ClientIPMiddleware,
	// the fallback must be the TCP peer — not the header.
	req := httptest.NewRequest("GET", "/v1/topics", nil)
	req.RemoteAddr = "203.0.113.9:44321"
	req.Header.Set("X-Forwarded-For", forgedIP)

	if got := ClientIP(req); got != "203.0.113.9" {
		t.Errorf("ClientIP() = %q, want the TCP peer 203.0.113.9", got)
	}
}

func TestClientIPMiddleware_DoesNotMutateRemoteAddr(t *testing.T) {
	// chi's middleware.RealIP rewrote r.RemoteAddr in place, which is how
	// attacker-controlled data reached everything downstream. Ours must not.
	var seen string
	handler := ClientIPMiddleware(mustTrust(t, "10.0.0.0/8"))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen = r.RemoteAddr
		}),
	)

	req := httptest.NewRequest("GET", "/v1/topics", nil)
	req.RemoteAddr = "10.0.0.1:44321"
	req.Header.Set("X-Forwarded-For", forgedIP)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if seen != "10.0.0.1:44321" {
		t.Errorf("RemoteAddr = %q, want it left as the TCP peer 10.0.0.1:44321", seen)
	}
}

// =============================================================================
// THE REGRESSION TEST
// =============================================================================

// runAuditChain drives a request through the real middleware chain
// (ClientIPMiddleware then AuditMiddleware) against a handler that 401s, and
// returns the decoded audit entry.
func runAuditChain(t *testing.T, tp *TrustedProxies, remoteAddr, forwarded string) map[string]any {
	t.Helper()

	logFile := filepath.Join(t.TempDir(), "audit.log")
	auditLogger := NewAuditLogger(AuditConfig{Enabled: true, LogFile: logFile})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})

	// Same order as internal/api/server.go: client IP is resolved before the
	// audit middleware reads it.
	wrapped := ClientIPMiddleware(tp)(AuditMiddleware(auditLogger)(handler))

	req := httptest.NewRequest("POST", "/v1/topics/orders/messages", nil)
	req.RemoteAddr = remoteAddr
	if forwarded != "" {
		req.Header.Set("X-Forwarded-For", forwarded)
	}
	wrapped.ServeHTTP(httptest.NewRecorder(), req)

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read audit log: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("audit log should have captured the auth failure")
	}

	// Nothing anywhere in the record may carry the forged address — not the
	// client_ip field, not some other attribute that happens to echo it.
	if strings.Contains(string(data), forgedIP) {
		t.Errorf("forged address %s reached the audit log:\n%s", forgedIP, data)
	}

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(data), &entry); err != nil {
		t.Fatalf("invalid JSON in audit log: %v\n%s", err, data)
	}
	return entry
}

// TestAuditMiddleware_ForgedXForwardedForIsNotLogged is the regression test
// for the client-IP spoof.
//
// Before the fix, chi's middleware.RealIP rewrote r.RemoteAddr to the
// leftmost X-Forwarded-For value, and AuditMiddleware then read the raw
// header a second time on top of that. Either path put a caller-chosen
// string in the client_ip of every auth-failure and ACL-denied event, so the
// one field an investigator relies on to identify an attacker was written by
// the attacker.
//
// With no trusted proxies configured, X-Forwarded-For must not be consulted
// at all.
func TestAuditMiddleware_ForgedXForwardedForIsNotLogged(t *testing.T) {
	entry := runAuditChain(t, mustTrust(t), "203.0.113.9:44321", forgedIP)

	if entry["client_ip"] != "203.0.113.9" {
		t.Errorf("client_ip = %v, want the TCP peer 203.0.113.9", entry["client_ip"])
	}
	if entry["event"] != string(AuditAuthFailure) {
		t.Errorf("event = %v, want %s", entry["event"], AuditAuthFailure)
	}
}

// TestAuditMiddleware_ForgedXForwardedForFromUntrustedPeer covers the
// deployment that does run behind a proxy: a caller who reaches the API
// directly still cannot forge the field just because a proxy list exists.
func TestAuditMiddleware_ForgedXForwardedForFromUntrustedPeer(t *testing.T) {
	entry := runAuditChain(t,
		mustTrust(t, "10.0.0.0/8"),
		"203.0.113.9:44321",
		forgedIP+", 10.0.0.5",
	)

	if entry["client_ip"] != "203.0.113.9" {
		t.Errorf("client_ip = %v, want the TCP peer 203.0.113.9", entry["client_ip"])
	}
}

// TestAuditMiddleware_TrustedProxyClientIsLogged is the other half: the fix
// must not blind the audit log in a real load-balanced deployment. When the
// peer IS a configured proxy, the forwarded client is what gets recorded.
func TestAuditMiddleware_TrustedProxyClientIsLogged(t *testing.T) {
	entry := runAuditChain(t,
		mustTrust(t, "10.0.0.0/8"),
		"10.0.0.1:44321",
		forgedIP+", 198.51.100.7, 10.0.0.5",
	)

	if entry["client_ip"] != "198.51.100.7" {
		t.Errorf("client_ip = %v, want the address our own proxy saw, 198.51.100.7",
			entry["client_ip"])
	}
}
