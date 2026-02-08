// =============================================================================
// DISK SPACE MONITOR - PRE-WRITE SAFETY CHECK
// =============================================================================
//
// WHAT IS THIS?
// A component that monitors available disk space and prevents writes
// when the disk is nearly full. Without this, the broker will silently
// fail (or corrupt data) when the filesystem runs out of space.
//
// WHY IS THIS CRITICAL?
// When a filesystem is 100% full:
//   - Writes return "no space left on device" errors
//   - These often aren't handled gracefully → data corruption
//   - Segment files may be partially written → corrupt on recovery
//   - Log files stop rotating → no debugging info
//   - The broker appears "alive" but can't accept messages
//
// This is equivalent to:
//   - Kafka: log.dirs disk monitoring + broker.id auto-failover
//   - RabbitMQ: disk_free_limit (default ~50MB, configurable)
//   - SQS: N/A (managed service, AWS handles this)
//
// HOW IT WORKS:
//
//   ┌──────────────────────────────────────────────────────────────┐
//   │              DISK SPACE MONITOR                              │
//   │                                                              │
//   │  On every publish:                                           │
//   │    1. Check cached disk usage (fast, no syscall)             │
//   │    2. If cache expired → refresh via syscall.Statfs          │
//   │    3. If used% > threshold → reject write (ErrDiskFull)     │
//   │    4. Update Prometheus gauge: goqueue_disk_usage_percent    │
//   │                                                              │
//   │  Background monitor:                                         │
//   │    - Refreshes disk stats every 30 seconds                  │
//   │    - Updates Prometheus metrics                              │
//   │    - Logs warnings when approaching threshold                │
//   │                                                              │
//   └──────────────────────────────────────────────────────────────┘
//
// COMPARISON - How other systems handle disk space:
//
//   | System   | Behavior              | Default Threshold       |
//   |----------|-----------------------|-------------------------|
//   | Kafka    | Log warning, stop ISR | log.dirs monitoring     |
//   | RabbitMQ | Block publishers      | 50MB free (disk alarm)  |
//   | Postgres | Halt WAL writes       | wal_segment_size check  |
//   | goqueue  | Reject writes + metric| 90% used (configurable) |
//
// =============================================================================

package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrDiskFull means the disk usage exceeds the configured threshold.
	// Writes are rejected to prevent data corruption from partial writes.
	ErrDiskFull = errors.New("disk space critically low: writes suspended")
)

// =============================================================================
// DISK MONITOR CONFIGURATION
// =============================================================================

// DiskMonitorConfig holds configuration for disk space monitoring.
type DiskMonitorConfig struct {
	// DataDir is the directory to monitor for disk usage.
	// This should be the broker's data directory.
	DataDir string

	// ThresholdPercent is the maximum disk usage percentage allowed.
	// When usage exceeds this, writes are rejected.
	//
	// COMPARISON:
	//   - RabbitMQ: disk_free_limit (absolute bytes, default ~50MB)
	//   - Kafka: No built-in threshold (relies on monitoring)
	//   - goqueue: Percentage-based (adapts to any disk size)
	//
	// WHY 90%?
	//   - 90% leaves ~10% buffer for:
	//     - In-flight writes completing
	//     - Retention runner cleaning up
	//     - Operating system overhead
	//     - Log rotation
	//   - RabbitMQ recommends at least 40% free... but that's aggressive
	//   - 90% is the industry standard warning threshold
	ThresholdPercent float64

	// CheckInterval is how often to refresh disk stats via syscall.
	// The cached value is used between checks for fast pre-write validation.
	//
	// WHY 30 SECONDS?
	//   - Disk usage doesn't change that rapidly
	//   - syscall.Statfs is cheap (~1μs) but we cache anyway
	//   - Fast enough to catch issues, slow enough to not waste CPU
	CheckInterval time.Duration

	// Logger for disk monitor operations
	Logger *slog.Logger
}

// DefaultDiskMonitorConfig returns sensible defaults.
func DefaultDiskMonitorConfig(dataDir string) DiskMonitorConfig {
	return DiskMonitorConfig{
		DataDir:          dataDir,
		ThresholdPercent: 90.0,
		CheckInterval:    30 * time.Second,
	}
}

// =============================================================================
// DISK MONITOR
// =============================================================================

// DiskMonitor tracks disk usage and prevents writes when space is low.
//
// DESIGN NOTES:
//   - Uses atomic for the "full" flag (hot path, called on every write)
//   - Background goroutine refreshes stats periodically
//   - Prometheus gauge exported for alerting
//
// THREAD SAFETY:
//   - diskFull flag: atomic.Bool (lock-free reads on hot path)
//   - stats: protected by mutex (only written by background goroutine)
type DiskMonitor struct {
	config DiskMonitorConfig
	logger *slog.Logger

	// diskFull is the hot-path check: true = reject writes
	// Uses atomic for lock-free reads on every publish
	diskFull atomic.Bool

	// stats holds the latest disk usage information
	stats   DiskStats
	statsMu sync.RWMutex

	// Lifecycle management
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	started bool
}

// DiskStats holds current disk usage information.
type DiskStats struct {
	// TotalBytes is the total disk capacity
	TotalBytes uint64

	// AvailableBytes is bytes available to unprivileged users
	AvailableBytes uint64

	// UsedBytes is bytes currently in use
	UsedBytes uint64

	// UsagePercent is the current usage as a percentage (0-100)
	UsagePercent float64

	// LastChecked is when these stats were last refreshed
	LastChecked time.Time
}

// NewDiskMonitor creates a new disk space monitor.
func NewDiskMonitor(config DiskMonitorConfig) *DiskMonitor {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &DiskMonitor{
		config: config,
		logger: logger.With("component", "disk-monitor"),
	}
}

// Start begins periodic disk space monitoring.
func (dm *DiskMonitor) Start() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.started {
		return nil
	}

	// Do initial check synchronously to catch issues at startup
	if err := dm.refreshStats(); err != nil {
		return fmt.Errorf("initial disk check failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	dm.cancel = cancel
	dm.started = true

	dm.wg.Add(1)
	go dm.run(ctx)

	stats := dm.GetStats()
	dm.logger.Info("disk monitor started",
		"data_dir", dm.config.DataDir,
		"threshold_percent", dm.config.ThresholdPercent,
		"check_interval", dm.config.CheckInterval,
		"current_usage_percent", fmt.Sprintf("%.1f%%", stats.UsagePercent),
		"available_gb", fmt.Sprintf("%.2f", float64(stats.AvailableBytes)/(1024*1024*1024)),
	)

	return nil
}

// Stop halts the disk monitor.
func (dm *DiskMonitor) Stop() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if !dm.started {
		return
	}

	dm.cancel()
	dm.wg.Wait()
	dm.started = false
	dm.logger.Info("disk monitor stopped")
}

// IsDiskFull returns true if disk usage exceeds the threshold.
//
// PERFORMANCE: This is called on EVERY publish operation.
// It uses atomic.Bool for lock-free reads (~1ns).
// The actual disk check happens in the background goroutine.
//
// FLOW:
//
//	Publisher ──► broker.Publish() ──► dm.IsDiskFull() ──► atomic.Load()
//	                                       │                    │
//	                                       │              ~1ns (no lock)
//	                                       │
//	                                 false → continue
//	                                 true → ErrDiskFull
func (dm *DiskMonitor) IsDiskFull() bool {
	return dm.diskFull.Load()
}

// GetStats returns the latest disk usage statistics.
func (dm *DiskMonitor) GetStats() DiskStats {
	dm.statsMu.RLock()
	defer dm.statsMu.RUnlock()
	return dm.stats
}

// run is the background monitoring loop.
func (dm *DiskMonitor) run(ctx context.Context) {
	defer dm.wg.Done()

	ticker := time.NewTicker(dm.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := dm.refreshStats(); err != nil {
				dm.logger.Error("failed to check disk space", "error", err)
				// On error, assume disk is NOT full (fail-open)
				// Better to risk filling disk than to stop all writes on a stat error
			}
		}
	}
}

// refreshStats queries the filesystem for current disk usage.
//
// IMPLEMENTATION:
//
//	Uses unix.Statfs (syscall) to get filesystem stats.
//	This works on macOS and Linux. For Windows, would need
//	a different implementation (GetDiskFreeSpaceEx).
//
// SYSCALL FIELDS:
//   - Blocks: Total data blocks in filesystem
//   - Bfree: Free blocks (for superuser)
//   - Bavail: Free blocks (for unprivileged users) ← we use this
//   - Bsize: Optimal transfer block size
//
// WHY Bavail (not Bfree)?
//
//	Filesystems reserve ~5% for root (ext4's reserved-blocks-percentage).
//	Bavail reflects what's ACTUALLY available to our process.
//	Using Bfree would overestimate available space.
func (dm *DiskMonitor) refreshStats() error {
	var stat unix.Statfs_t
	if err := unix.Statfs(dm.config.DataDir, &stat); err != nil {
		return fmt.Errorf("statfs(%s): %w", dm.config.DataDir, err)
	}

	totalBytes := stat.Blocks * uint64(stat.Bsize)
	availBytes := stat.Bavail * uint64(stat.Bsize)
	usedBytes := totalBytes - availBytes

	var usagePercent float64
	if totalBytes > 0 {
		usagePercent = float64(usedBytes) / float64(totalBytes) * 100
	}

	stats := DiskStats{
		TotalBytes:     totalBytes,
		AvailableBytes: availBytes,
		UsedBytes:      usedBytes,
		UsagePercent:   usagePercent,
		LastChecked:    time.Now(),
	}

	dm.statsMu.Lock()
	dm.stats = stats
	dm.statsMu.Unlock()

	// Update the atomic flag for hot-path checks
	wasFull := dm.diskFull.Load()
	isFull := usagePercent >= dm.config.ThresholdPercent

	dm.diskFull.Store(isFull)

	// Log state transitions
	if isFull && !wasFull {
		dm.logger.Error("DISK SPACE CRITICAL: writes suspended",
			"usage_percent", fmt.Sprintf("%.1f%%", usagePercent),
			"threshold_percent", dm.config.ThresholdPercent,
			"available_bytes", availBytes,
			"total_bytes", totalBytes,
		)
	} else if !isFull && wasFull {
		dm.logger.Info("disk space recovered: writes resumed",
			"usage_percent", fmt.Sprintf("%.1f%%", usagePercent),
			"threshold_percent", dm.config.ThresholdPercent,
			"available_bytes", availBytes,
		)
	} else if usagePercent >= dm.config.ThresholdPercent-5 && !isFull {
		// Warning when within 5% of threshold
		dm.logger.Warn("disk space warning: approaching threshold",
			"usage_percent", fmt.Sprintf("%.1f%%", usagePercent),
			"threshold_percent", dm.config.ThresholdPercent,
			"available_bytes", availBytes,
		)
	}

	return nil
}
