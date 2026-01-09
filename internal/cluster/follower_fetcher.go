// =============================================================================
// FOLLOWER FETCHER - BACKGROUND LOG REPLICATION
// =============================================================================
//
// WHAT: A background goroutine that fetches messages from the leader.
//
// FLOW:
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │                         FOLLOWER FETCHER LOOP                          │
//   │                                                                        │
//   │   ┌───────────────────┐                                                │
//   │   │ 1. Check shutdown │                                                │
//   │   └─────────┬─────────┘                                                │
//   │             │                                                          │
//   │             ▼                                                          │
//   │   ┌───────────────────┐                                                │
//   │   │ 2. Build fetch    │  "Give me messages from offset X"              │
//   │   │    request        │                                                │
//   │   └─────────┬─────────┘                                                │
//   │             │                                                          │
//   │             ▼                                                          │
//   │   ┌───────────────────┐                                                │
//   │   │ 3. Send to leader │  HTTP POST /cluster/fetch                      │
//   │   └─────────┬─────────┘                                                │
//   │             │                                                          │
//   │             ▼                                                          │
//   │   ┌───────────────────┐   ┌───────────────────────────────────────┐    │
//   │   │ 4. Handle errors  │──►│ Error? Back off, retry                │    │
//   │   │                   │   │ Epoch mismatch? Stop fetcher          │    │
//   │   └─────────┬─────────┘   │ Far behind? Request snapshot          │    │
//   │             │             └───────────────────────────────────────┘    │
//   │             ▼                                                          │
//   │   ┌───────────────────┐                                                │
//   │   │ 5. Append messages│  Write to local log                            │
//   │   │    to local log   │                                                │
//   │   └─────────┬─────────┘                                                │
//   │             │                                                          │
//   │   ┌───────────────────┐                                                │
//   │   │ 6. Update replica │  LEO, HW updated                               │
//   │   │    state          │                                                │
//   │   └─────────┬─────────┘                                                │
//   │             │                                                          │
//   │             ▼                                                          │
//   │   ┌───────────────────┐                                                │
//   │   │ 7. Sleep interval │  500ms default                                 │
//   │   └─────────┬─────────┘                                                │
//   │             │                                                          │
//   │             └──────────────────────────►  Loop back to step 1          │
//   │                                                                        │
//   └────────────────────────────────────────────────────────────────────────┘
//
// ERROR HANDLING:
//   - Network error: Exponential backoff (100ms → 200ms → 400ms → max 5s)
//   - Not leader: Stop fetcher (leader changed)
//   - Epoch mismatch: Stop fetcher (stale fetcher)
//   - Offset out of range: Request snapshot or reset
//
// =============================================================================

package cluster

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// =============================================================================
// FOLLOWER FETCHER CONFIG
// =============================================================================

// FollowerFetcherConfig configures the fetcher.
type FollowerFetcherConfig struct {
	// Topic is the topic to fetch.
	Topic string

	// Partition is the partition to fetch.
	Partition int

	// LeaderID is the current leader's node ID.
	LeaderID NodeID

	// LeaderAddr is the leader's cluster address (host:port).
	LeaderAddr string

	// LeaderEpoch is the expected leader epoch.
	LeaderEpoch int64

	// ReplicaID is this follower's node ID.
	ReplicaID NodeID

	// FetchIntervalMs is how often to fetch.
	FetchIntervalMs int

	// FetchMaxBytes is max bytes per fetch.
	FetchMaxBytes int

	// SnapshotDir is directory for snapshot downloads.
	// If empty, snapshots are disabled (fallback to offset reset).
	SnapshotDir string
}

// =============================================================================
// FOLLOWER FETCHER
// =============================================================================

// FollowerFetcher fetches messages from leader in background.
type FollowerFetcher struct {
	// config is fetcher configuration.
	config FollowerFetcherConfig

	// replicaManager updates local replica state.
	replicaManager *ReplicaManager

	// client makes HTTP requests to leader.
	client *ClusterClient

	// logger for operations.
	logger *slog.Logger

	// fetchOffset is the next offset to fetch.
	fetchOffset int64

	// mu protects fetchOffset.
	mu sync.Mutex

	// ctx is the fetcher's context.
	ctx context.Context

	// cancel stops the fetcher.
	cancel context.CancelFunc

	// wg waits for fetcher goroutine.
	wg sync.WaitGroup

	// running indicates if fetcher is active.
	running bool

	// consecutiveErrors counts errors for backoff.
	consecutiveErrors int

	// lastFetchTime is when we last successfully fetched.
	lastFetchTime time.Time

	// stats tracks fetcher statistics.
	stats FetcherStats
}

// FetcherStats tracks fetcher metrics.
type FetcherStats struct {
	// TotalFetches is total fetch attempts.
	TotalFetches int64

	// SuccessfulFetches is successful fetches.
	SuccessfulFetches int64

	// FailedFetches is failed fetches.
	FailedFetches int64

	// MessagesFetched is total messages fetched.
	MessagesFetched int64

	// BytesFetched is total bytes fetched.
	BytesFetched int64

	// AvgFetchLatencyMs is average fetch latency.
	AvgFetchLatencyMs float64

	// SnapshotDownloads is number of snapshot downloads for catch-up.
	SnapshotDownloads int64

	// LastError is the most recent error.
	LastError string

	// LastErrorTime is when last error occurred.
	LastErrorTime time.Time
}

// =============================================================================
// CONSTRUCTOR
// =============================================================================

// NewFollowerFetcher creates a new follower fetcher.
func NewFollowerFetcher(config FollowerFetcherConfig, rm *ReplicaManager, client *ClusterClient, logger *slog.Logger) *FollowerFetcher {
	ctx, cancel := context.WithCancel(context.Background())

	return &FollowerFetcher{
		config:         config,
		replicaManager: rm,
		client:         client,
		logger: logger.With(
			"component", "follower-fetcher",
			"topic", config.Topic,
			"partition", config.Partition,
			"leader", config.LeaderID,
		),
		fetchOffset: 0, // Will be set on first fetch or from local log.
		ctx:         ctx,
		cancel:      cancel,
	}
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Start begins the fetch loop.
func (ff *FollowerFetcher) Start() {
	ff.mu.Lock()
	if ff.running {
		ff.mu.Unlock()
		return
	}
	ff.running = true
	ff.mu.Unlock()

	ff.logger.Info("starting follower fetcher",
		"leader_addr", ff.config.LeaderAddr,
		"epoch", ff.config.LeaderEpoch)

	// Initialize fetch offset from local replica state.
	state := ff.replicaManager.GetReplicaState(ff.config.Topic, ff.config.Partition)
	if state != nil {
		ff.fetchOffset = state.LogEndOffset
	}

	ff.wg.Add(1)
	go ff.fetchLoop()
}

// Stop stops the fetch loop.
func (ff *FollowerFetcher) Stop() {
	ff.mu.Lock()
	if !ff.running {
		ff.mu.Unlock()
		return
	}
	ff.running = false
	ff.mu.Unlock()

	ff.logger.Info("stopping follower fetcher")
	ff.cancel()
	ff.wg.Wait()
	ff.logger.Info("follower fetcher stopped")
}

// =============================================================================
// FETCH LOOP
// =============================================================================

// fetchLoop is the main fetch loop.
func (ff *FollowerFetcher) fetchLoop() {
	defer ff.wg.Done()

	interval := time.Duration(ff.config.FetchIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ff.ctx.Done():
			return
		case <-ticker.C:
			ff.doFetch()
		}
	}
}

// doFetch performs a single fetch operation.
func (ff *FollowerFetcher) doFetch() {
	ff.stats.TotalFetches++
	startTime := time.Now()

	// Build fetch request.
	req := FetchRequest{
		Topic:       ff.config.Topic,
		Partition:   ff.config.Partition,
		FromOffset:  ff.fetchOffset,
		MaxBytes:    ff.config.FetchMaxBytes,
		FollowerID:  ff.config.ReplicaID,
		LeaderEpoch: ff.config.LeaderEpoch,
	}

	// Send to leader.
	resp, err := ff.client.Fetch(ff.ctx, ff.config.LeaderAddr, &req)

	fetchLatency := time.Since(startTime)

	if err != nil {
		ff.handleFetchError(err)
		return
	}

	// Handle response errors.
	if resp.ErrorCode != FetchErrorNone {
		ff.handleResponseError(resp)
		return
	}

	// Success - reset error counter.
	ff.consecutiveErrors = 0
	ff.lastFetchTime = time.Now()
	ff.stats.SuccessfulFetches++

	// Update latency stats (simple moving average).
	// TODO: this seems incorrect, consider using a proper moving average calculation, or at least weight it properly. or exponential moving average.
	ff.stats.AvgFetchLatencyMs = (ff.stats.AvgFetchLatencyMs + float64(fetchLatency.Milliseconds())) / 2

	// Process messages.
	if len(resp.Messages) > 0 {
		ff.stats.MessagesFetched += int64(len(resp.Messages))

		// Calculate bytes fetched.
		for _, msg := range resp.Messages {
			ff.stats.BytesFetched += int64(len(msg.Key) + len(msg.Value))
		}

		// Update next fetch offset.
		ff.mu.Lock()
		ff.fetchOffset = resp.NextFetchOffset
		ff.mu.Unlock()

		ff.logger.Debug("fetched messages",
			"count", len(resp.Messages),
			"next_offset", resp.NextFetchOffset,
			"leader_hw", resp.HighWatermark,
			"leader_leo", resp.LogEndOffset)
	}

	// Update replica manager with fetched data.
	if err := ff.replicaManager.ApplyFetchedMessages(
		ff.config.Topic,
		ff.config.Partition,
		resp.Messages,
		resp.HighWatermark,
		resp.LogEndOffset,
	); err != nil {
		ff.logger.Error("failed to apply fetched messages", "error", err)
	}
}

// =============================================================================
// ERROR HANDLING
// =============================================================================

// handleFetchError handles network/transport errors.
func (ff *FollowerFetcher) handleFetchError(err error) {
	ff.consecutiveErrors++
	ff.stats.FailedFetches++
	ff.stats.LastError = err.Error()
	ff.stats.LastErrorTime = time.Now()

	// Exponential backoff.
	backoff := ff.calculateBackoff()
	ff.logger.Warn("fetch error, backing off",
		"error", err,
		"consecutive_errors", ff.consecutiveErrors,
		"backoff_ms", backoff.Milliseconds())

	select {
	case <-ff.ctx.Done():
	case <-time.After(backoff):
	}
}

// handleResponseError handles errors in fetch response.
func (ff *FollowerFetcher) handleResponseError(resp *FetchResponse) {
	ff.stats.FailedFetches++
	ff.stats.LastError = resp.ErrorMessage
	ff.stats.LastErrorTime = time.Now()

	switch resp.ErrorCode {
	case FetchErrorNotLeader:
		// Leader changed - stop fetcher (coordinator will restart with new leader).
		ff.logger.Warn("not leader error, stopping fetcher",
			"error", resp.ErrorMessage)
		ff.Stop()

	case FetchErrorEpochFenced:
		// Our epoch is stale - stop fetcher.
		ff.logger.Warn("epoch fenced, stopping fetcher",
			"our_epoch", ff.config.LeaderEpoch,
			"leader_epoch", resp.LeaderEpoch)
		ff.Stop()

	case FetchErrorOffsetOutOfRange:
		// Our offset is invalid - need to reset or get snapshot.
		ff.logger.Warn("offset out of range",
			"fetch_offset", ff.fetchOffset,
			"leader_leo", resp.LogEndOffset)
		ff.handleOffsetOutOfRange(resp)

	default:
		// Unknown error - back off.
		ff.consecutiveErrors++
		backoff := ff.calculateBackoff()
		ff.logger.Warn("fetch response error",
			"code", resp.ErrorCode,
			"message", resp.ErrorMessage,
			"backoff_ms", backoff.Milliseconds())

		select {
		case <-ff.ctx.Done():
		case <-time.After(backoff):
		}
	}
}

// handleOffsetOutOfRange handles invalid offset scenarios.
//
// WHAT HAPPENS:
//
//	When a follower's fetch offset is out of range, it means:
//	1. The log on the leader has been truncated/compacted, OR
//	2. The follower is too far behind
//
// SOLUTION:
//
//	Request and download a snapshot from the leader for fast catch-up.
//
// FLOW:
//
//	┌───────────────────────────────────────────────────────────────────────┐
//	│                                                                       │
//	│   1. Follower fetch fails: "offset 100 out of range, earliest: 5000"  │
//	│                                                                       │
//	│   2. Follower requests snapshot:                                      │
//	│      POST /cluster/snapshot/create → SnapshotResponse                 │
//	│                                                                       │
//	│   3. Follower downloads snapshot:                                     │
//	│      GET /cluster/snapshot/{topic}/{partition}/{offset}               │
//	│                                                                       │
//	│   4. Follower applies snapshot (handled by SnapshotManager)           │
//	│                                                                       │
//	│   5. Follower resumes fetching from snapshot's last offset + 1        │
//	│                                                                       │
//	└───────────────────────────────────────────────────────────────────────┘
func (ff *FollowerFetcher) handleOffsetOutOfRange(resp *FetchResponse) {
	ff.mu.Lock()
	defer ff.mu.Unlock()

	if ff.fetchOffset > resp.LogEndOffset {
		// We're ahead of leader (shouldn't happen normally).
		// Could be due to leader failover with data loss.
		// Reset to leader's LEO.
		ff.logger.Warn("fetch offset ahead of leader, resetting",
			"our_offset", ff.fetchOffset,
			"leader_leo", resp.LogEndOffset)
		ff.fetchOffset = resp.LogEndOffset
		return
	}

	// We're behind and our offset is no longer available.
	// This means log was truncated. We need snapshot.
	ff.logger.Warn("fetch offset no longer available, attempting snapshot catch-up",
		"our_offset", ff.fetchOffset,
		"leader_leo", resp.LogEndOffset)

	// Check if snapshots are enabled.
	if ff.config.SnapshotDir == "" {
		ff.logger.Warn("snapshots disabled, resetting to offset 0 (data loss!)")
		ff.fetchOffset = 0
		return
	}

	// Request snapshot from leader.
	ctx, cancel := context.WithTimeout(ff.ctx, 60*time.Second)
	defer cancel()

	snapResp, err := ff.client.RequestSnapshot(ctx, ff.config.LeaderAddr, ff.config.Topic, ff.config.Partition, ff.config.ReplicaID)
	if err != nil {
		ff.logger.Error("failed to request snapshot, resetting to 0",
			"error", err)
		ff.fetchOffset = 0
		return
	}

	ff.logger.Info("snapshot created on leader",
		"offset", snapResp.Snapshot.LastIncludedOffset,
		"size", snapResp.Snapshot.SizeBytes,
		"download_url", snapResp.DownloadURL)

	// Download snapshot.
	snapshotPath, err := ff.client.DownloadSnapshot(ctx, ff.config.LeaderAddr, snapResp.DownloadURL, ff.config.SnapshotDir)
	if err != nil {
		ff.logger.Error("failed to download snapshot, resetting to 0",
			"error", err)
		ff.fetchOffset = 0
		return
	}

	ff.logger.Info("snapshot downloaded",
		"path", snapshotPath,
		"offset", snapResp.Snapshot.LastIncludedOffset)

	// Apply the snapshot (extract and replace local log).
	// The snapshot manager will extract the tar.gz and update local storage.
	snapshotMgr := ff.replicaManager.GetSnapshotManager()
	if snapshotMgr != nil {
		logDir := ff.replicaManager.GetLogDir(ff.config.Topic, ff.config.Partition)
		err = snapshotMgr.LoadSnapshot(snapshotPath, logDir, snapResp.Snapshot.Checksum)
		if err != nil {
			ff.logger.Error("failed to apply snapshot, resetting to 0",
				"error", err)
			ff.fetchOffset = 0
			return
		}
	}

	// Update fetch offset to resume from after snapshot.
	ff.fetchOffset = snapResp.Snapshot.LastIncludedOffset + 1

	ff.logger.Info("snapshot applied, resuming fetch",
		"new_offset", ff.fetchOffset)

	// Update stats.
	ff.stats.SnapshotDownloads++
}

// calculateBackoff returns backoff duration based on consecutive errors.
func (ff *FollowerFetcher) calculateBackoff() time.Duration {
	// Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms, max 5000ms.
	baseMs := 100
	maxMs := 5000

	backoffMs := baseMs << ff.consecutiveErrors // 100 * 2^errors
	if backoffMs > maxMs {
		backoffMs = maxMs
	}

	return time.Duration(backoffMs) * time.Millisecond
}

// =============================================================================
// STATS
// =============================================================================

// Stats returns current fetcher statistics.
func (ff *FollowerFetcher) Stats() FetcherStats {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.stats
}

// GetFetchOffset returns the current fetch offset.
func (ff *FollowerFetcher) GetFetchOffset() int64 {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.fetchOffset
}

// SetFetchOffset sets the fetch offset (used after snapshot load).
func (ff *FollowerFetcher) SetFetchOffset(offset int64) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	ff.fetchOffset = offset
	ff.logger.Info("fetch offset set", "offset", offset)
}
