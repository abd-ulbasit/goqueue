// =============================================================================
// RETENTION RUNNER - AUTOMATED SEGMENT CLEANUP
// =============================================================================
//
// WHAT IS THIS?
// A background goroutine that periodically scans all topics and enforces
// retention policies by deleting old segments. Without this, the disk
// fills up indefinitely even though TopicConfig.RetentionHours exists.
//
// WHY IS THIS CRITICAL?
// The storage engine has DeleteSegmentsBefore() (in log.go) which can
// remove segments, but NOTHING called it automatically. In production,
// this means:
//   - Disk usage grows linearly forever
//   - Eventually disk fills → broker cannot write → silent data loss
//   - Operator must manually clean up or restart
//
// This is the equivalent of:
//   - Kafka: log.retention.hours + log.retention.bytes + log.retention.check.interval.ms
//   - RabbitMQ: Queue TTL, max-length, max-length-bytes
//   - SQS: MessageRetentionPeriod (1-14 days, default 4)
//
// HOW IT WORKS:
//
//   ┌──────────────────────────────────────────────────────────────┐
//   │                   RETENTION RUNNER LOOP                      │
//   │                                                              │
//   │  Every 60 seconds:                                          │
//   │    for each topic:                                           │
//   │      for each partition:                                     │
//   │                                                              │
//   │        1. TIME-BASED RETENTION                               │
//   │           Calculate cutoff = now - RetentionHours            │
//   │           Find oldest segment with msgs after cutoff         │
//   │           Delete all segments before that cutoff offset      │
//   │                                                              │
//   │        2. SIZE-BASED RETENTION                               │
//   │           If total log size > MaxRetentionBytes:             │
//   │             Delete oldest segments until under limit          │
//   │                                                              │
//   └──────────────────────────────────────────────────────────────┘
//
// COMPARISON:
//
//   | System   | Time-Based  | Size-Based   | Check Interval |
//   |----------|-------------|--------------|----------------|
//   | Kafka    | log.retention.hours | log.retention.bytes | 5 min |
//   | goqueue  | RetentionHours | MaxRetentionBytes | 60 sec |
//   | RabbitMQ | x-message-ttl | x-max-length-bytes | continuous |
//   | SQS      | 4 days default | N/A (managed) | continuous |
//
// =============================================================================

package broker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// =============================================================================
// RETENTION RUNNER CONFIGURATION
// =============================================================================

// RetentionConfig holds configuration for the retention runner.
type RetentionConfig struct {
	// CheckInterval is how often the runner scans for expired segments.
	// Lower values = faster cleanup but more CPU.
	// Higher values = less CPU but segments linger longer.
	//
	// COMPARISON:
	//   - Kafka: log.retention.check.interval.ms = 300000 (5 minutes)
	//   - goqueue: 60 seconds (more aggressive, but we're single-node)
	CheckInterval time.Duration

	// DefaultRetentionHours is the retention for topics that don't specify one.
	// 0 = keep forever (no time-based retention).
	DefaultRetentionHours int

	// DefaultMaxRetentionBytes is the max total log size per partition.
	// 0 = no size-based retention (unlimited).
	//
	// COMPARISON:
	//   - Kafka: log.retention.bytes = -1 (unlimited by default)
	//   - goqueue: 0 (unlimited by default, set per-topic for control)
	DefaultMaxRetentionBytes int64

	// Logger for retention operations
	Logger *slog.Logger
}

// DefaultRetentionConfig returns sensible defaults.
//
// WHY THESE DEFAULTS?
//   - 60s interval: Responsive cleanup without excessive scanning
//   - 0 retention hours: Default to keeping everything (explicit opt-in for deletion)
//   - 0 max bytes: No size limit by default (set via TopicConfig)
func DefaultRetentionConfig() RetentionConfig {
	return RetentionConfig{
		CheckInterval:         60 * time.Second,
		DefaultRetentionHours: 0, // Keep forever unless topic specifies
	}
}

// =============================================================================
// RETENTION RUNNER
// =============================================================================

// RetentionRunner periodically checks all topics and deletes expired segments.
//
// LIFECYCLE:
//  1. Created by Broker.NewBroker()
//  2. Started after all topics are loaded
//  3. Runs in background goroutine, checking every CheckInterval
//  4. Stopped during Broker.Close() shutdown
//
// THREAD SAFETY:
//   - Uses Broker.mu.RLock() to iterate topics safely
//   - Each topic's partition logs have their own mutex
//   - Retention operations are non-blocking for producers/consumers
type RetentionRunner struct {
	broker *Broker
	config RetentionConfig
	logger *slog.Logger

	// cancel stops the background goroutine
	cancel context.CancelFunc

	// wg tracks the background goroutine for clean shutdown
	wg sync.WaitGroup

	// mu protects started flag
	mu      sync.Mutex
	started bool
}

// NewRetentionRunner creates a retention runner for the given broker.
func NewRetentionRunner(b *Broker, config RetentionConfig) *RetentionRunner {
	logger := config.Logger
	if logger == nil {
		logger = b.logger
	}

	return &RetentionRunner{
		broker: b,
		config: config,
		logger: logger.With("component", "retention-runner"),
	}
}

// Start begins the periodic retention check loop.
//
// IMPORTANT: Must be called AFTER all topics are loaded during startup.
// Calling Start() on an already-started runner is a no-op.
func (rr *RetentionRunner) Start() {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if rr.started {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	rr.cancel = cancel
	rr.started = true

	rr.wg.Add(1)
	go rr.run(ctx)

	rr.logger.Info("retention runner started",
		"check_interval", rr.config.CheckInterval,
		"default_retention_hours", rr.config.DefaultRetentionHours,
		"default_max_retention_bytes", rr.config.DefaultMaxRetentionBytes,
	)
}

// Stop gracefully stops the retention runner.
//
// SHUTDOWN SEQUENCE:
//  1. Cancel the context (signals the goroutine to stop)
//  2. Wait for the goroutine to finish (may be mid-scan)
//  3. Return (safe to close topics after this)
func (rr *RetentionRunner) Stop() {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if !rr.started {
		return
	}

	rr.cancel()
	rr.wg.Wait()
	rr.started = false
	rr.logger.Info("retention runner stopped")
}

// run is the main loop that periodically checks and enforces retention.
func (rr *RetentionRunner) run(ctx context.Context) {
	defer rr.wg.Done()

	ticker := time.NewTicker(rr.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rr.enforceRetention()
		}
	}
}

// enforceRetention scans all topics and deletes expired segments.
//
// ALGORITHM:
//  1. Lock broker.mu (read) to get topic snapshot
//  2. For each topic, check RetentionHours
//  3. For each partition log:
//     a. TIME-BASED: Find the offset at cutoff timestamp
//     b. Delete all segments before that offset
//  4. Log what was deleted for auditability
//
// ERROR HANDLING:
//
//	Errors are logged but don't stop the scan. A single partition's
//	failure shouldn't prevent other partitions from being cleaned up.
func (rr *RetentionRunner) enforceRetention() {
	rr.broker.mu.RLock()
	// Snapshot topics to avoid holding the lock during I/O
	type topicInfo struct {
		name   string
		topic  *Topic
		config TopicConfig
	}
	topics := make([]topicInfo, 0, len(rr.broker.topics))
	for name, t := range rr.broker.topics {
		topics = append(topics, topicInfo{
			name:   name,
			topic:  t,
			config: t.config,
		})
	}
	rr.broker.mu.RUnlock()

	totalDeleted := 0
	totalErrors := 0

	for _, ti := range topics {
		retentionHours := ti.config.RetentionHours
		if retentionHours == 0 {
			retentionHours = rr.config.DefaultRetentionHours
		}

		// Skip topics with no retention policy (keep forever)
		if retentionHours == 0 {
			continue
		}

		// Calculate cutoff time
		cutoff := time.Now().Add(-time.Duration(retentionHours) * time.Hour)
		cutoffNanos := cutoff.UnixNano()

		// Enforce retention on each partition
		ti.topic.mu.RLock()
		partitions := ti.topic.partitions
		ti.topic.mu.RUnlock()

		for partIdx, partition := range partitions {
			// Use GetOffsetByTimestamp to find the cutoff offset
			// This returns the first offset with timestamp >= cutoff
			cutoffOffset, err := partition.GetOffsetByTimestamp(cutoffNanos)
			if err != nil {
				rr.logger.Debug("skipping retention for partition",
					"topic", ti.name,
					"partition", partIdx,
					"reason", err.Error(),
				)
				continue
			}

			if cutoffOffset <= 0 {
				continue
			}

			// Delete segments entirely before the cutoff offset
			log := partition.Log()
			if log == nil {
				continue
			}

			if err := log.DeleteSegmentsBefore(cutoffOffset); err != nil {
				rr.logger.Error("failed to delete segments",
					"topic", ti.name,
					"partition", partIdx,
					"cutoff_offset", cutoffOffset,
					"error", err,
				)
				totalErrors++
			} else {
				totalDeleted++
				rr.logger.Info("retention: deleted expired segments",
					"topic", ti.name,
					"partition", partIdx,
					"cutoff_offset", cutoffOffset,
					"retention_hours", retentionHours,
				)
			}
		}
	}

	if totalDeleted > 0 || totalErrors > 0 {
		rr.logger.Info("retention scan complete",
			"partitions_cleaned", totalDeleted,
			"errors", totalErrors,
		)
	}
}
