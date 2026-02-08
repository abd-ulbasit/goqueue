package broker

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

// =============================================================================
// DISK MONITOR TESTS
// =============================================================================

func TestDiskMonitor_DefaultConfig(t *testing.T) {
	config := DefaultDiskMonitorConfig("/tmp/test-data")

	if config.ThresholdPercent != 90.0 {
		t.Errorf("expected 90%% threshold, got %.1f%%", config.ThresholdPercent)
	}

	if config.CheckInterval != 30*time.Second {
		t.Errorf("expected 30s check interval, got %v", config.CheckInterval)
	}

	if config.DataDir != "/tmp/test-data" {
		t.Errorf("expected /tmp/test-data, got %s", config.DataDir)
	}
}

func TestDiskMonitor_StartStop(t *testing.T) {
	// Use a real directory (temp) so Statfs works
	tmpDir := t.TempDir()

	config := DiskMonitorConfig{
		DataDir:          tmpDir,
		ThresholdPercent: 99.9, // Very high so it won't trigger
		CheckInterval:    100 * time.Millisecond,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}

	dm := NewDiskMonitor(config)

	if err := dm.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Should have stats after start
	stats := dm.GetStats()
	if stats.TotalBytes == 0 {
		t.Error("expected non-zero TotalBytes after start")
	}
	if stats.LastChecked.IsZero() {
		t.Error("expected non-zero LastChecked after start")
	}

	// Should NOT be full (threshold is 99.9%)
	if dm.IsDiskFull() {
		t.Error("disk should not be full with 99.9% threshold")
	}

	dm.Stop()

	// Stop again should be no-op
	dm.Stop()
}

func TestDiskMonitor_StatsFields(t *testing.T) {
	tmpDir := t.TempDir()

	config := DiskMonitorConfig{
		DataDir:          tmpDir,
		ThresholdPercent: 99.9,
		CheckInterval:    1 * time.Hour, // Long interval, we only check initial
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}

	dm := NewDiskMonitor(config)

	if err := dm.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer dm.Stop()

	stats := dm.GetStats()

	// Validate stats are reasonable
	if stats.TotalBytes == 0 {
		t.Error("TotalBytes should be > 0")
	}

	if stats.AvailableBytes == 0 {
		t.Error("AvailableBytes should be > 0 (unless disk is actually full)")
	}

	if stats.UsagePercent < 0 || stats.UsagePercent > 100 {
		t.Errorf("UsagePercent should be 0-100, got %.1f", stats.UsagePercent)
	}

	if stats.UsedBytes > stats.TotalBytes {
		t.Errorf("UsedBytes (%d) should not exceed TotalBytes (%d)",
			stats.UsedBytes, stats.TotalBytes)
	}

	t.Logf("Disk stats: total=%.2f GB, available=%.2f GB, usage=%.1f%%",
		float64(stats.TotalBytes)/(1024*1024*1024),
		float64(stats.AvailableBytes)/(1024*1024*1024),
		stats.UsagePercent,
	)
}

func TestDiskMonitor_IsDiskFull_AtomicRead(t *testing.T) {
	// WHAT: IsDiskFull should be safe for concurrent reads
	// This is the hot path called on every publish

	tmpDir := t.TempDir()
	config := DiskMonitorConfig{
		DataDir:          tmpDir,
		ThresholdPercent: 99.9,
		CheckInterval:    1 * time.Hour,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}

	dm := NewDiskMonitor(config)
	if err := dm.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer dm.Stop()

	// Concurrent reads should not panic
	done := make(chan struct{})
	for i := 0; i < 100; i++ {
		go func() {
			_ = dm.IsDiskFull()
			done <- struct{}{}
		}()
	}

	for i := 0; i < 100; i++ {
		<-done
	}
}
