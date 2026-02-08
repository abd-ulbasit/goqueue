package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// MAX BODY SIZE MIDDLEWARE TESTS
// =============================================================================

func TestMaxBodySizeMiddleware_AllowsSmallBody(t *testing.T) {
	// WHAT: A request with a body smaller than the limit should pass through
	handler := maxBodySizeMiddleware(1024)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 1024)
		_, _ = r.Body.Read(buf)
		w.WriteHeader(http.StatusOK)
	}))

	body := strings.NewReader(`{"key": "value"}`)
	req := httptest.NewRequest(http.MethodPost, "/test", body)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestMaxBodySizeMiddleware_SkipsNoBody(t *testing.T) {
	// WHAT: GET requests (no body) should pass through without wrapping
	handler := maxBodySizeMiddleware(1024)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	req.ContentLength = 0
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

// =============================================================================
// TOKEN BUCKET TESTS
// =============================================================================

func TestTokenBucket_AllowsUpToCapacity(t *testing.T) {
	// WHAT: A fresh bucket should allow exactly 'capacity' requests
	bucket := newTokenBucket(10)

	allowed := 0
	for i := 0; i < 15; i++ {
		if bucket.allow() {
			allowed++
		}
	}

	if allowed != 10 {
		t.Errorf("expected 10 allowed, got %d", allowed)
	}
}

func TestTokenBucket_RefillsOverTime(t *testing.T) {
	// WHAT: After draining, waiting should refill tokens
	bucket := newTokenBucket(100)

	// Drain all tokens
	for i := 0; i < 100; i++ {
		bucket.allow()
	}

	// Should be empty
	if bucket.allow() {
		t.Error("expected no tokens available after draining")
	}

	// Wait for refill (100 tokens/sec, 50ms = ~5 tokens)
	time.Sleep(60 * time.Millisecond)

	// Should have some tokens now
	if !bucket.allow() {
		t.Error("expected tokens to refill after waiting")
	}
}

func TestTokenBucket_ConcurrentAccess(t *testing.T) {
	// WHAT: Token bucket must be safe under concurrent access
	bucket := newTokenBucket(1000)

	var wg sync.WaitGroup
	allowed := int64(0)
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localAllowed := int64(0)
			for j := 0; j < 200; j++ {
				if bucket.allow() {
					localAllowed++
				}
			}
			mu.Lock()
			allowed += localAllowed
			mu.Unlock()
		}()
	}

	wg.Wait()

	// Should have allowed at most ~1000 + some refill during test
	if allowed > 1500 {
		t.Errorf("expected ~1000 allowed, got %d (too many)", allowed)
	}
	if allowed < 500 {
		t.Errorf("expected ~1000 allowed, got %d (too few)", allowed)
	}
}

// =============================================================================
// RATE LIMITER MIDDLEWARE TESTS
// =============================================================================

func TestRateLimiterMiddleware_AllowsNormalTraffic(t *testing.T) {
	// WHAT: Normal requests should pass through
	handler := NewRateLimiterMiddleware(100)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestRateLimiterMiddleware_Returns429WhenExhausted(t *testing.T) {
	// WHAT: When rate limit exceeded, return HTTP 429
	handler := NewRateLimiterMiddleware(5)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Exhaust the bucket
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/topics", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
	}

	// This request should be rate limited
	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", rr.Code)
	}

	// Check Retry-After header
	if rr.Header().Get("Retry-After") != "1" {
		t.Errorf("expected Retry-After: 1, got %s", rr.Header().Get("Retry-After"))
	}
}

func TestRateLimiterMiddleware_SkipsHealthEndpoints(t *testing.T) {
	// WHAT: Health/metrics endpoints must ALWAYS pass even under rate limit
	handler := NewRateLimiterMiddleware(1)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Exhaust the single token
	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// Health endpoints should still pass
	healthPaths := []string{"/health", "/healthz", "/readyz", "/livez", "/metrics", "/version"}
	for _, path := range healthPaths {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("expected %s to pass (200), got %d", path, rr.Code)
		}
	}
}
