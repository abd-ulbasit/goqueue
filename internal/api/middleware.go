// =============================================================================
// HTTP MIDDLEWARE - REQUEST SAFETY AND RATE LIMITING
// =============================================================================
//
// WHAT IS THIS?
// Production-hardening middleware for the HTTP API server:
//   - maxBodySizeMiddleware: Prevents OOM via request body size limits
//   - NewRateLimiterMiddleware: Token bucket rate limiting for single-tenant mode
//
// WHY MIDDLEWARE?
// Middleware intercepts every request BEFORE it reaches the handler.
// This is the correct layer for cross-cutting concerns like:
//   - Body size limits (prevents OOM attacks)
//   - Rate limiting (prevents API abuse)
//   - Authentication (already in security middleware)
//   - Logging (already in loggingMiddleware)
//
// MIDDLEWARE CHAIN:
//
//   Request ──► RequestID ──► RealIP ──► Logger ──► Recoverer
//           ──► MaxBodySize ──► RateLimit ──► Auth ──► Handler
//
// =============================================================================

package api

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

// =============================================================================
// REQUEST BODY SIZE LIMITER
// =============================================================================
//
// WHY: Without body size limits, a single malicious request with a multi-GB
// body can exhaust server memory (OOM). http.MaxBytesReader wraps the
// request body and enforces a hard limit. If the client sends more bytes
// than allowed, subsequent reads return an error.
//
// HOW http.MaxBytesReader WORKS:
//  1. Wraps r.Body with a counting reader
//  2. On each Read(), tracks bytes consumed
//  3. If cumulative bytes > limit → returns *http.MaxBytesError
//  4. Handler's json.Decode (or io.ReadAll) sees the error
//  5. Server responds with HTTP 413 Payload Too Large
//  6. Connection is automatically closed (RFC 7231 §6.5.11)
//
// COMPARISON:
//   - Kafka: message.max.bytes (1MB default per message)
//   - RabbitMQ: max_message_size (128MB, was unlimited before 3.8.x!)
//   - SQS: 256KB hard limit
//   - Nginx: client_max_body_size (1MB default)
//   - Express.js: body-parser limit option
//   - goqueue: 1MB default, publish endpoint gets 16MB
//
// WHY NOT JUST ReadTimeout?
//
//	ReadTimeout limits TIME, not SIZE. A fast connection can push 1GB
//	in well under 30 seconds. We need BOTH:
//	- ReadTimeout: Protects against slow clients (slowloris)
//	- MaxBytesReader: Protects against large payloads (OOM)
//
// FLOW:
//
//	┌──────────┐   POST /topics/orders/messages   ┌──────────────────┐
//	│  Client  │─────────────────────────────────►│ MaxBytesReader   │
//	│          │   Body: 50MB JSON                │ limit=1MB        │
//	└──────────┘                                  └────────┬─────────┘
//	                                                       │
//	                                                 reads 1MB OK
//	                                                 next read → error
//	                                                       │
//	                                                       ▼
//	                                              HTTP 413 + close
func maxBodySizeMiddleware(maxBytes int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Only apply to requests with bodies (POST, PUT, PATCH)
			// GET, HEAD, DELETE, OPTIONS typically have no body
			if r.ContentLength == 0 && r.Body == http.NoBody {
				next.ServeHTTP(w, r)
				return
			}

			// Wrap the body with a size limit
			// http.MaxBytesReader also sets a flag on the response writer
			// that tells the server to close the connection if limit is hit
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			next.ServeHTTP(w, r)
		})
	}
}

// =============================================================================
// TOKEN BUCKET RATE LIMITER
// =============================================================================
//
// WHY: In single-tenant mode, there's no per-tenant quota enforcement.
// Without any rate limiting, a single client (or a runaway script) can
// overwhelm the broker with requests, causing cascading failures.
//
// ALGORITHM: Token Bucket
// ─────────────────────────
//
// The token bucket is one of the most common rate limiting algorithms,
// used by Kafka, AWS, Google Cloud, and most production systems.
//
// HOW IT WORKS:
//
//   ┌──────────────────────────────────────────────────────────────┐
//   │              TOKEN BUCKET (capacity = RPS)                   │
//   │                                                              │
//   │  Tokens refill at RPS per second (e.g., 1000 tokens/sec)   │
//   │                                                              │
//   │  ┌──────────────────────────────┐                           │
//   │  │ [tok] [tok] [tok] ... [tok]  │ ◄── refill from top      │
//   │  │                              │                           │
//   │  │    current: 847 tokens       │                           │
//   │  └──────────┬───────────────────┘                           │
//   │             │                                                │
//   │             ▼ Each request takes 1 token                    │
//   │                                                              │
//   │  Request arrives:                                           │
//   │    tokens > 0 ? → Allow, tokens--                           │
//   │    tokens == 0 ? → Reject with HTTP 429                    │
//   │                                                              │
//   └──────────────────────────────────────────────────────────────┘
//
// WHY TOKEN BUCKET (vs alternatives)?
//
//   | Algorithm      | Burst-Friendly | Memory  | Used By          |
//   |----------------|----------------|---------|------------------|
//   | Token Bucket   | ✅ Yes         | O(1)    | Kafka, AWS, GCP  |
//   | Leaky Bucket   | ❌ No (smooth) | O(1)    | Nginx            |
//   | Fixed Window   | ⚠️  Edge burst  | O(1)    | Simple APIs      |
//   | Sliding Window | ✅ Accurate    | O(N)    | Redis-based      |
//
// Token bucket allows short bursts (good for queue workloads where
// publish patterns are bursty), while still enforcing long-term rate.
//
// COMPARISON:
//   - Kafka: Token bucket per client-id (bytes/sec + request-rate)
//   - RabbitMQ: Per-connection credit-based flow control
//   - SQS: Per-queue request rate (3000 TPS standard, 30000 FIFO)
//   - goqueue: Global token bucket (single-tenant mode only)
//
// RESPONSE ON LIMIT:
//   HTTP 429 Too Many Requests
//   Retry-After: 1 (seconds until tokens refill)
//
//   The Retry-After header tells well-behaved clients when to retry.
//   This is important for client-side backoff implementations.
//

// tokenBucket implements a thread-safe token bucket rate limiter.
//
// DESIGN NOTES:
//   - Uses mutex instead of atomic for simplicity (not hot enough to matter)
//   - Tokens are float64 to support fractional refill
//   - Capacity equals RPS (allows 1-second burst)
//   - Refill is calculated lazily (no background goroutine needed)
type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64   // Current available tokens
	capacity   float64   // Maximum tokens (= RPS)
	refillRate float64   // Tokens added per second (= RPS)
	lastRefill time.Time // Last time tokens were refilled
}

// newTokenBucket creates a token bucket with the given rate (requests per second).
// Starts full (allows initial burst up to capacity).
func newTokenBucket(rps int) *tokenBucket {
	return &tokenBucket{
		tokens:     float64(rps),
		capacity:   float64(rps),
		refillRate: float64(rps),
		lastRefill: time.Now(),
	}
}

// allow checks if a request should be allowed.
// Returns true if a token is available, false otherwise.
//
// LAZY REFILL:
//
//	Instead of a background goroutine refilling tokens every millisecond,
//	we calculate how many tokens should have been added since the last
//	call. This is more efficient and eliminates timer overhead.
//
//	Example: If RPS=1000 and 0.5s passed since last check:
//	  tokensToAdd = 1000 * 0.5 = 500 tokens
//	  newTokens = min(current + 500, capacity)
func (tb *tokenBucket) allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()

	// Refill tokens based on elapsed time
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	tb.lastRefill = now

	// Try to consume a token
	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}

	return false
}

// NewRateLimiterMiddleware creates an HTTP middleware that enforces rate limiting
// using a token bucket algorithm.
//
// PARAMETERS:
//   - rps: Maximum requests per second (also the burst capacity)
//
// BEHAVIOR:
//   - Each request consumes 1 token
//   - If no tokens available → HTTP 429 Too Many Requests
//   - Includes Retry-After header (RFC 6585)
//   - Health/metrics endpoints should be excluded from rate limiting
//     (currently excluded: /health, /healthz, /readyz, /livez, /metrics)
func NewRateLimiterMiddleware(rps int) func(http.Handler) http.Handler {
	bucket := newTokenBucket(rps)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip rate limiting for health and metrics endpoints
			// These MUST always be accessible for Kubernetes probes
			// and Prometheus scraping, even under load.
			path := r.URL.Path
			if path == "/health" || path == "/healthz" || path == "/readyz" ||
				path == "/livez" || path == "/metrics" || path == "/version" {
				next.ServeHTTP(w, r)
				return
			}

			if !bucket.allow() {
				w.Header().Set("Retry-After", strconv.Itoa(1))
				w.Header().Set("X-RateLimit-Limit", strconv.Itoa(rps))
				w.Header().Set("X-RateLimit-Remaining", "0")
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
