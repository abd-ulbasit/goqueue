// =============================================================================
// STORAGE METRICS - DISK I/O AND LOG INSTRUMENTATION
// =============================================================================
//
// WHAT ARE STORAGE METRICS?
// Storage metrics track the underlying data persistence:
//   - How much data is being written/read?
//   - How fast is disk I/O?
//   - How big are our log files?
//   - How is fsync performing?
//
// WHY STORAGE METRICS MATTER:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                    STORAGE METRICS USAGE                                │
//   │                                                                         │
//   │   DISK CAPACITY                                                         │
//   │   ─────────────                                                         │
//   │   Q: "How long until we run out of disk?"                               │
//   │   A: predict_linear(goqueue_storage_log_size_bytes[24h], 7*24*3600)     │
//   │      This predicts disk usage 7 days from now                           │
//   │                                                                         │
//   │   DISK I/O BOTTLENECKS                                                  │
//   │   ─────────────────────                                                 │
//   │   Q: "Why is publish latency high?"                                     │
//   │   A: Check fsync_latency_seconds - if high, disk is the bottleneck      │
//   │                                                                         │
//   │   SEGMENT MANAGEMENT                                                    │
//   │   ──────────────────                                                    │
//   │   Q: "Are segments rolling over too frequently?"                        │
//   │   A: rate(goqueue_storage_segments_created_total[5m])                   │
//   │      Should be relatively stable                                        │
//   │                                                                         │
//   │   RETENTION MONITORING                                                  │
//   │   ────────────────────                                                  │
//   │   Q: "Is retention working correctly?"                                  │
//   │   A: goqueue_storage_segments_deleted_total should increase over time   │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// DISK I/O FUNDAMENTALS:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                       DISK I/O BASICS                                   │
//   │                                                                         │
//   │   WRITE PATH:                                                           │
//   │   ───────────                                                           │
//   │   Application ──► OS Buffer Cache ──► Disk Controller ──► Disk          │
//   │                         │                    │                          │
//   │                    (in memory)          (may cache)                     │
//   │                                                                         │
//   │   Without fsync:                                                        │
//   │     - Data may be in OS buffer only                                     │
//   │     - Faster (returns immediately)                                      │
//   │     - DATA LOSS on power failure                                        │
//   │                                                                         │
//   │   With fsync:                                                           │
//   │     - Data guaranteed on disk                                           │
//   │     - Slower (waits for disk)                                           │
//   │     - DURABLE even on power failure                                     │
//   │                                                                         │
//   │   GOQUEUE STRATEGY:                                                     │
//   │     - Default: fsync per message (safe)                                 │
//   │     - Optional: fsync per batch (faster, slight risk)                   │
//   │     - Configurable via FsyncStrategy                                    │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON WITH KAFKA:
//
//   ┌────────────────────────────┬──────────────────────────────────────────┐
//   │ Kafka Metric               │ goqueue Equivalent                       │
//   ├────────────────────────────┼──────────────────────────────────────────┤
//   │ LogFlushRateAndTimeMs      │ fsync_latency_seconds, fsync_total       │
//   │ LogSegmentCount            │ segments_active                          │
//   │ LogSizeBytes               │ log_size_bytes                           │
//   │ LogEndOffset               │ log_end_offset (Kafka-style LEO)         │
//   │ UnderReplicatedPartitions  │ (in cluster_metrics.go)                  │
//   └────────────────────────────┴──────────────────────────────────────────┘
//
// =============================================================================

package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// StorageMetrics contains all storage-level metrics.
type StorageMetrics struct {
	// =========================================================================
	// BYTE COUNTERS
	// =========================================================================
	//
	// These track raw bytes flowing through the storage layer.
	// Higher-level "message" counts are in broker_metrics.go.
	//

	// BytesWritten counts total bytes written to disk.
	// Labels: topic, partition (if enabled)
	//
	// NOTE: This is raw bytes including headers, not just message payloads.
	//
	// PROMQL:
	//   # Write throughput in MB/s
	//   rate(goqueue_storage_bytes_written_total[5m]) / 1024 / 1024
	BytesWritten *prometheus.CounterVec

	// BytesRead counts total bytes read from disk.
	// Labels: topic, partition (if enabled)
	BytesRead *prometheus.CounterVec

	// =========================================================================
	// FSYNC METRICS
	// =========================================================================
	//
	// WHAT IS FSYNC?
	// fsync() is a system call that flushes file buffers to disk.
	// Without fsync, data may be in OS buffer only - lost on crash.
	//
	// WHY MEASURE FSYNC?
	//   - fsync is often the SLOWEST operation in message brokers
	//   - High fsync latency = disk bottleneck
	//   - Too many fsyncs = poor throughput
	//   - Too few fsyncs = durability risk
	//

	// FsyncTotal counts total fsync operations.
	// Labels: topic (partition optional)
	//
	// PROMQL:
	//   # Fsyncs per second
	//   rate(goqueue_storage_fsync_total[5m])
	FsyncTotal *prometheus.CounterVec

	// FsyncLatency measures time for fsync operations.
	// Labels: topic
	//
	// CRITICAL METRIC FOR DISK PERFORMANCE!
	//
	// PROMQL:
	//   # p99 fsync latency
	//   histogram_quantile(0.99,
	//     rate(goqueue_storage_fsync_latency_seconds_bucket[5m])
	//   )
	//
	// ALERTING:
	//   # Alert if fsync p99 > 100ms (disk is slow)
	//   histogram_quantile(0.99,
	//     rate(goqueue_storage_fsync_latency_seconds_bucket[5m])
	//   ) > 0.1
	FsyncLatency *prometheus.HistogramVec

	// FsyncErrors counts fsync failures.
	// Labels: topic
	//
	// ANY fsync error is CRITICAL - data may be lost!
	FsyncErrors *prometheus.CounterVec

	// =========================================================================
	// SEGMENT METRICS
	// =========================================================================
	//
	// WHAT ARE SEGMENTS?
	// goqueue stores messages in segment files:
	//   - Each segment has a maximum size (default: 1GB)
	//   - When full, segment is "sealed" and new one created
	//   - Old segments are deleted based on retention policy
	//
	// SEGMENT LIFECYCLE:
	//
	//   Create ──► Write ──► Seal ──► (retention) ──► Delete
	//     │                    │           │
	//     │                    └──► Index ─┘
	//     │                         (for fast reads)
	//     └──► Active segment (only one per partition)
	//

	// SegmentsActive is the current number of active segments.
	// Labels: topic, partition (if enabled)
	//
	// Should be 1 per partition under normal operation.
	// If > 1, something is wrong with segment rotation.
	SegmentsActive *prometheus.GaugeVec

	// SegmentsTotal is the total number of segments (active + sealed).
	// Labels: topic, partition (if enabled)
	//
	// Growth rate indicates data accumulation.
	SegmentsTotal *prometheus.GaugeVec

	// SegmentsCreated counts new segments created.
	// Labels: topic
	//
	// High rate = frequent rollovers (may want larger segment size)
	SegmentsCreated *prometheus.CounterVec

	// SegmentsDeleted counts segments deleted by retention.
	// Labels: topic
	//
	// Should increase steadily if retention is working.
	SegmentsDeleted *prometheus.CounterVec

	// =========================================================================
	// LOG SIZE METRICS
	// =========================================================================
	//
	// LOG END OFFSET (LEO):
	// The offset of the next message to be written.
	// Equal to the offset of the last message + 1.
	//
	// WHY TRACK LEO?
	//   - Consumer lag = LEO - consumer offset
	//   - Helps identify partition imbalance
	//   - Useful for debugging
	//

	// LogSizeBytes is the total size of the log in bytes.
	// Labels: topic, partition (if enabled)
	//
	// PROMQL:
	//   # Total storage per topic
	//   sum(goqueue_storage_log_size_bytes) by (topic)
	//
	//   # Predict when we'll run out of disk (assuming 80% threshold)
	//   predict_linear(goqueue_storage_log_size_bytes[7d], 30*24*3600)
	LogSizeBytes *prometheus.GaugeVec

	// LogEndOffset is the next offset to be written (Kafka-style LEO).
	// Labels: topic, partition (if enabled)
	//
	// CRITICAL for calculating consumer lag:
	//   lag = LEO - committed_offset
	//
	// PROMQL:
	//   # Consumer lag by partition
	//   goqueue_storage_log_end_offset
	//   - goqueue_consumer_committed_offset
	LogEndOffset *prometheus.GaugeVec

	// LogStartOffset is the earliest available offset (after retention).
	// Labels: topic, partition (if enabled)
	//
	// Useful for:
	//   - Knowing how far back consumers can read
	//   - Detecting retention issues
	LogStartOffset *prometheus.GaugeVec

	// =========================================================================
	// INDEX METRICS
	// =========================================================================
	//
	// WHAT ARE INDICES?
	// goqueue maintains two index types:
	//   1. OFFSET INDEX: Maps offset → file position (for seeking by offset)
	//   2. TIME INDEX: Maps timestamp → offset (for seeking by time)
	//

	// IndexLookups counts index lookup operations.
	// Labels: topic, index_type (offset, time)
	IndexLookups *prometheus.CounterVec

	// IndexLookupLatency measures index lookup time.
	// Labels: topic, index_type (offset, time)
	//
	// Should be very fast (< 1ms) - indices are in memory or mmap'd.
	IndexLookupLatency *prometheus.HistogramVec

	// IndexRebuilds counts index rebuild operations.
	// Labels: topic
	//
	// Rebuilds happen on corruption detection or recovery.
	// Should be rare - frequent rebuilds indicate problems.
	IndexRebuilds *prometheus.CounterVec

	// =========================================================================
	// COMPACTION METRICS (M14)
	// =========================================================================
	//
	// WHAT IS LOG COMPACTION?
	// Compaction removes old values for the same key, keeping only the latest.
	// Used for internal topics like __consumer_offsets where we only need
	// the current offset, not the history.
	//
	// COMPACTION VS RETENTION:
	//   - RETENTION: Delete segments older than X
	//   - COMPACTION: Keep latest value per key, delete older values
	//
	//   RETENTION:                    COMPACTION:
	//   [A1][B1][A2][C1][B2]   →     [A2][C1][B2]  (keep latest per key)
	//   ↑ delete old segments        ↑ delete old values within segments
	//

	// CompactionRuns counts compaction operations.
	// Labels: topic
	CompactionRuns *prometheus.CounterVec

	// CompactionLatency measures compaction operation time.
	// Labels: topic
	CompactionLatency *prometheus.HistogramVec

	// CompactionBytesReclaimed counts bytes freed by compaction.
	// Labels: topic
	//
	// PROMQL:
	//   # Space savings from compaction
	//   increase(goqueue_storage_compaction_bytes_reclaimed_total[24h])
	CompactionBytesReclaimed *prometheus.CounterVec

	// CompactionDirtyRatio is the current dirty ratio (triggers compaction).
	// Labels: topic
	//
	// Dirty ratio = old values / total values
	// When > threshold (default 50%), compaction is triggered.
	CompactionDirtyRatio *prometheus.GaugeVec

	// =========================================================================
	// SNAPSHOT METRICS (M14)
	// =========================================================================
	//
	// WHAT ARE SNAPSHOTS?
	// Snapshots are point-in-time copies of coordinator state.
	// They speed up recovery by avoiding full log replay.
	//
	// RECOVERY WITHOUT SNAPSHOT:
	//   Replay 1M records from offset 0 (~5 minutes)
	//
	// RECOVERY WITH SNAPSHOT:
	//   Load snapshot @ 990K + replay 10K records (~3 seconds)
	//

	// SnapshotsCreated counts snapshot creation.
	// Labels: snapshot_type (coordinator, partition)
	SnapshotsCreated *prometheus.CounterVec

	// SnapshotSize tracks the size of the latest snapshot.
	// Labels: snapshot_type
	SnapshotSize *prometheus.GaugeVec

	// SnapshotLatency measures snapshot creation time.
	// Labels: snapshot_type
	SnapshotLatency *prometheus.HistogramVec

	// =========================================================================
	// INTERNAL REFERENCE
	// =========================================================================

	registry *Registry
}

// newStorageMetrics creates and registers all storage metrics.
func newStorageMetrics(r *Registry) *StorageMetrics {
	m := &StorageMetrics{registry: r}

	// Determine label names based on config
	// If partition labels are enabled, include them
	topicLabels := []string{"topic"}
	topicPartitionLabels := topicLabels
	if r.config.IncludePartitionLabel {
		topicPartitionLabels = []string{"topic", "partition"}
	}

	// =========================================================================
	// BYTE COUNTERS
	// =========================================================================

	m.BytesWritten = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "storage",
			Name:      "bytes_written_total",
			Help:      "Total bytes written to disk (includes headers)",
		},
		topicPartitionLabels,
	)

	m.BytesRead = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "storage",
			Name:      "bytes_read_total",
			Help:      "Total bytes read from disk",
		},
		topicPartitionLabels,
	)

	// =========================================================================
	// FSYNC METRICS
	// =========================================================================

	m.FsyncTotal = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "storage",
			Name:      "fsync_total",
			Help:      "Total fsync operations performed",
		},
		topicLabels,
	)

	m.FsyncLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "storage",
			Name:      "fsync_latency_seconds",
			Help:      "Time to complete fsync operation (disk durability)",
			// fsync can be slow - include higher buckets
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		topicLabels,
	)

	m.FsyncErrors = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "storage",
			Name:      "fsync_errors_total",
			Help:      "Total fsync failures (critical - data may be lost)",
		},
		topicLabels,
	)

	// =========================================================================
	// SEGMENT METRICS
	// =========================================================================

	m.SegmentsActive = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "storage",
			Name:      "segments_active",
			Help:      "Number of active (writable) segments",
		},
		topicPartitionLabels,
	)

	m.SegmentsTotal = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "storage",
			Name:      "segments_total",
			Help:      "Total number of segments (active + sealed)",
		},
		topicPartitionLabels,
	)

	m.SegmentsCreated = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "storage",
			Name:      "segments_created_total",
			Help:      "Total segments created",
		},
		topicLabels,
	)

	m.SegmentsDeleted = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "storage",
			Name:      "segments_deleted_total",
			Help:      "Total segments deleted by retention",
		},
		topicLabels,
	)

	// =========================================================================
	// LOG SIZE METRICS
	// =========================================================================

	m.LogSizeBytes = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "storage",
			Name:      "log_size_bytes",
			Help:      "Total size of log in bytes",
		},
		topicPartitionLabels,
	)

	m.LogEndOffset = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "storage",
			Name:      "log_end_offset",
			Help:      "Next offset to be written (Kafka-style LEO)",
		},
		topicPartitionLabels,
	)

	m.LogStartOffset = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "storage",
			Name:      "log_start_offset",
			Help:      "Earliest available offset (after retention)",
		},
		topicPartitionLabels,
	)

	// =========================================================================
	// INDEX METRICS
	// =========================================================================

	m.IndexLookups = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "storage",
			Name:      "index_lookups_total",
			Help:      "Total index lookup operations",
		},
		[]string{"topic", "index_type"},
	)

	m.IndexLookupLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "storage",
			Name:      "index_lookup_latency_seconds",
			Help:      "Time to complete index lookup",
			// Index lookups should be very fast
			Buckets: []float64{0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01},
		},
		[]string{"topic", "index_type"},
	)

	m.IndexRebuilds = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "storage",
			Name:      "index_rebuilds_total",
			Help:      "Total index rebuild operations (should be rare)",
		},
		topicLabels,
	)

	// =========================================================================
	// COMPACTION METRICS
	// =========================================================================

	m.CompactionRuns = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "storage",
			Name:      "compaction_runs_total",
			Help:      "Total log compaction operations",
		},
		topicLabels,
	)

	m.CompactionLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "storage",
			Name:      "compaction_latency_seconds",
			Help:      "Time to complete compaction operation",
			// Compaction can take a while
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120},
		},
		topicLabels,
	)

	m.CompactionBytesReclaimed = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "storage",
			Name:      "compaction_bytes_reclaimed_total",
			Help:      "Total bytes freed by compaction",
		},
		topicLabels,
	)

	m.CompactionDirtyRatio = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "storage",
			Name:      "compaction_dirty_ratio",
			Help:      "Current dirty ratio (old values / total values)",
		},
		topicLabels,
	)

	// =========================================================================
	// SNAPSHOT METRICS
	// =========================================================================

	m.SnapshotsCreated = r.newCounterVec(
		prometheus.CounterOpts{
			Subsystem: "storage",
			Name:      "snapshots_created_total",
			Help:      "Total snapshots created",
		},
		[]string{"snapshot_type"},
	)

	m.SnapshotSize = r.newGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "storage",
			Name:      "snapshot_size_bytes",
			Help:      "Size of the latest snapshot",
		},
		[]string{"snapshot_type"},
	)

	m.SnapshotLatency = r.newHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "storage",
			Name:      "snapshot_latency_seconds",
			Help:      "Time to create snapshot",
			Buckets:   []float64{0.1, 0.5, 1, 5, 10, 30, 60},
		},
		[]string{"snapshot_type"},
	)

	return m
}

// =============================================================================
// CONVENIENCE METHODS
// =============================================================================

// RecordWrite records bytes written to storage.
func (m *StorageMetrics) RecordWrite(topic string, partition int, bytes int) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.BytesWritten.WithLabelValues(topic, partitionStr(partition)).Add(float64(bytes))
	} else {
		m.BytesWritten.WithLabelValues(topic).Add(float64(bytes))
	}
}

// RecordRead records bytes read from storage.
func (m *StorageMetrics) RecordRead(topic string, partition int, bytes int) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.BytesRead.WithLabelValues(topic, partitionStr(partition)).Add(float64(bytes))
	} else {
		m.BytesRead.WithLabelValues(topic).Add(float64(bytes))
	}
}

// RecordFsync records an fsync operation.
func (m *StorageMetrics) RecordFsync(topic string, latency float64, err error) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.FsyncTotal.WithLabelValues(topic).Inc()
	m.FsyncLatency.WithLabelValues(topic).Observe(latency)
	if err != nil {
		m.FsyncErrors.WithLabelValues(topic).Inc()
	}
}

// SetLogEndOffset sets the LEO for a partition.
func (m *StorageMetrics) SetLogEndOffset(topic string, partition int, offset int64) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.LogEndOffset.WithLabelValues(topic, partitionStr(partition)).Set(float64(offset))
	} else {
		m.LogEndOffset.WithLabelValues(topic).Set(float64(offset))
	}
}

// SetLogStartOffset sets the start offset for a partition.
func (m *StorageMetrics) SetLogStartOffset(topic string, partition int, offset int64) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.LogStartOffset.WithLabelValues(topic, partitionStr(partition)).Set(float64(offset))
	} else {
		m.LogStartOffset.WithLabelValues(topic).Set(float64(offset))
	}
}

// SetLogSizeBytes sets the total log size.
func (m *StorageMetrics) SetLogSizeBytes(topic string, partition int, size int64) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.LogSizeBytes.WithLabelValues(topic, partitionStr(partition)).Set(float64(size))
	} else {
		m.LogSizeBytes.WithLabelValues(topic).Set(float64(size))
	}
}

// SetSegmentCounts sets segment counts for a partition.
func (m *StorageMetrics) SetSegmentCounts(topic string, partition int, active, total int) {
	if m == nil || !m.registry.enabled {
		return
	}
	if m.registry.config.IncludePartitionLabel {
		m.SegmentsActive.WithLabelValues(topic, partitionStr(partition)).Set(float64(active))
		m.SegmentsTotal.WithLabelValues(topic, partitionStr(partition)).Set(float64(total))
	} else {
		m.SegmentsActive.WithLabelValues(topic).Set(float64(active))
		m.SegmentsTotal.WithLabelValues(topic).Set(float64(total))
	}
}

// RecordSegmentCreated records a new segment creation.
func (m *StorageMetrics) RecordSegmentCreated(topic string) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.SegmentsCreated.WithLabelValues(topic).Inc()
}

// RecordSegmentDeleted records a segment deletion.
func (m *StorageMetrics) RecordSegmentDeleted(topic string) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.SegmentsDeleted.WithLabelValues(topic).Inc()
}

// RecordIndexLookup records an index lookup operation.
func (m *StorageMetrics) RecordIndexLookup(topic string, indexType string, latency float64) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.IndexLookups.WithLabelValues(topic, indexType).Inc()
	m.IndexLookupLatency.WithLabelValues(topic, indexType).Observe(latency)
}

// RecordCompaction records a compaction operation.
func (m *StorageMetrics) RecordCompaction(topic string, latency float64, bytesReclaimed int64) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.CompactionRuns.WithLabelValues(topic).Inc()
	m.CompactionLatency.WithLabelValues(topic).Observe(latency)
	m.CompactionBytesReclaimed.WithLabelValues(topic).Add(float64(bytesReclaimed))
}

// RecordSnapshot records a snapshot creation.
func (m *StorageMetrics) RecordSnapshot(snapshotType string, size int64, latency float64) {
	if m == nil || !m.registry.enabled {
		return
	}
	m.SnapshotsCreated.WithLabelValues(snapshotType).Inc()
	m.SnapshotSize.WithLabelValues(snapshotType).Set(float64(size))
	m.SnapshotLatency.WithLabelValues(snapshotType).Observe(latency)
}

// partitionStr converts partition int to string for labels.
func partitionStr(partition int) string {
	return strconv.Itoa(partition)
}
