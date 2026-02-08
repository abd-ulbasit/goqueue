package broker

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

// =============================================================================
// RETENTION RUNNER TESTS
// =============================================================================

func TestRetentionRunner_DefaultConfig(t *testing.T) {
	// WHAT: Default config should have sensible values
	config := DefaultRetentionConfig()

	if config.CheckInterval != 60*time.Second {
		t.Errorf("expected 60s check interval, got %v", config.CheckInterval)
	}

	if config.DefaultRetentionHours != 0 {
		t.Errorf("expected 0 (keep forever) default retention, got %d", config.DefaultRetentionHours)
	}
}

func TestRetentionRunner_StartStop(t *testing.T) {
	// WHAT: Runner should start and stop cleanly without errors
	config := DefaultRetentionConfig()
	config.CheckInterval = 100 * time.Millisecond // Fast for testing

	// Need a minimal broker with topics map and logger so
	// enforceRetention() doesn't panic on nil pointer
	broker := &Broker{
		topics: make(map[string]*Topic),
		logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}

	rr := NewRetentionRunner(broker, config)
	rr.Start()

	// Start again should be no-op
	rr.Start()

	// Wait a bit for at least one tick
	time.Sleep(150 * time.Millisecond)

	rr.Stop()

	// Stop again should be no-op
	rr.Stop()
}
