// =============================================================================
// ISR MANAGER - IN-SYNC REPLICA TRACKING
// =============================================================================
//
// WHAT: Tracks which replicas are "in-sync" with the leader.
//
// ISR DEFINITION:
//   A replica is "in-sync" if it meets BOTH criteria:
//   1. TIME: Has fetched within ISRLagTimeMaxMs (e.g., 10 seconds)
//   2. OFFSET: Is within ISRLagMaxMessages of leader's LEO (e.g., 1000 messages)
//
// WHY BOTH? (Combined approach as requested)
//   - Time-only: A slow network might cause false positives (fetching but far behind)
//   - Offset-only: High-throughput topics might remove slow-but-recent replicas unfairly
//   - Combined: Replica must be BOTH recent AND close to catch up
//
// ISR STATE MACHINE:
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │                                                                        │
//   │          ┌─────────────┐                                               │
//   │          │  IN_SYNC    │ ◄──────────────────────────────────┐          │
//   │          └──────┬──────┘                                    │          │
//   │                 │                                           │          │
//   │                 │ (lag_time > max_time) OR                  │          │
//   │                 │ (lag_messages > max_messages)             │          │
//   │                 │                                           │          │
//   │                 ▼                                           │          │
//   │          ┌─────────────┐                                    │          │
//   │          │ OUT_OF_SYNC │ ─────────────────────────────────►─┘          │
//   │          └─────────────┘   caught up to leader LEO                     │
//   │                                                                        │
//   └────────────────────────────────────────────────────────────────────────┘
//
// HIGH WATERMARK CALCULATION:
//   HW = min(LEO across all ISR members)
//
//   Example:
//     Leader LEO:     1000
//     Follower-A LEO: 995  (in ISR)
//     Follower-B LEO: 800  (not in ISR)
//
//     HW = min(1000, 995) = 995  (B is ignored, not in ISR)
//
// =============================================================================

package cluster

import (
	"log/slog"
	"sync"
	"time"
)

// =============================================================================
// ISR CONFIGURATION
// =============================================================================

// ISRConfig controls ISR behavior.
type ISRConfig struct {
	// LagTimeMaxMs is max time since last fetch before ISR removal.
	LagTimeMaxMs int

	// LagMaxMessages is max messages behind before ISR removal.
	LagMaxMessages int64

	// MinInSyncReplicas is minimum ISR size for writes.
	MinInSyncReplicas int
}

// =============================================================================
// ISR MANAGER
// =============================================================================

// ISRManager tracks in-sync replicas for a partition (leader only).
type ISRManager struct {
	// topic is the topic name.
	topic string

	// partition is the partition number.
	partition int

	// leaderID is the leader's node ID.
	leaderID NodeID

	// allReplicas is the full replica set (from assignment).
	allReplicas []NodeID

	// isr is the current in-sync replica set.
	isr map[NodeID]bool

	// followerProgress tracks each follower's state.
	followerProgress map[NodeID]*FollowerProgress

	// config is ISR configuration.
	config ISRConfig

	// mu protects state.
	mu sync.RWMutex

	// logger for operations.
	logger *slog.Logger
}

// NewISRManager creates a new ISR manager.
func NewISRManager(topic string, partition int, leaderID NodeID, replicas []NodeID, config ISRConfig, logger *slog.Logger) *ISRManager {
	// Initialize ISR with all replicas (optimistic start).
	isr := make(map[NodeID]bool)
	for _, r := range replicas {
		isr[r] = true
	}

	// Initialize follower progress.
	progress := make(map[NodeID]*FollowerProgress)
	for _, r := range replicas {
		if r != leaderID {
			progress[r] = &FollowerProgress{
				ReplicaID:     r,
				LastFetchTime: time.Now(), // Optimistic: assume caught up initially
				IsInSync:      true,
			}
		}
	}

	return &ISRManager{
		topic:            topic,
		partition:        partition,
		leaderID:         leaderID,
		allReplicas:      replicas,
		isr:              isr,
		followerProgress: progress,
		config:           config,
		logger: logger.With(
			"component", "isr-manager",
			"topic", topic,
			"partition", partition,
		),
	}
}

// =============================================================================
// ISR ACCESS
// =============================================================================

// GetISR returns the current ISR list.
func (im *ISRManager) GetISR() []NodeID {
	im.mu.RLock()
	defer im.mu.RUnlock()

	result := make([]NodeID, 0, len(im.isr))
	for nodeID := range im.isr {
		result = append(result, nodeID)
	}
	return result
}

// ISRSize returns the number of in-sync replicas.
func (im *ISRManager) ISRSize() int {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return len(im.isr)
}

// IsInSync returns true if the given replica is in ISR.
func (im *ISRManager) IsInSync(nodeID NodeID) bool {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return im.isr[nodeID]
}

// HasMinISR returns true if ISR meets minimum requirement.
func (im *ISRManager) HasMinISR() bool {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return len(im.isr) >= im.config.MinInSyncReplicas
}

// GetFollowerProgress returns progress for a specific follower.
func (im *ISRManager) GetFollowerProgress(nodeID NodeID) *FollowerProgress {
	im.mu.RLock()
	defer im.mu.RUnlock()

	progress, ok := im.followerProgress[nodeID]
	if !ok {
		return nil
	}

	// Return copy.
	progressCopy := *progress
	return &progressCopy
}

// =============================================================================
// FETCH TRACKING
// =============================================================================

// RecordFetch records that a follower fetched from the leader.
// This is called when leader receives a fetch request.
func (im *ISRManager) RecordFetch(followerID NodeID, fetchOffset int64, leaderLEO int64) {
	im.mu.Lock()
	defer im.mu.Unlock()

	progress, ok := im.followerProgress[followerID]
	if !ok {
		// Unknown follower - might be a new replica.
		progress = &FollowerProgress{
			ReplicaID: followerID,
		}
		im.followerProgress[followerID] = progress
	}

	now := time.Now()
	progress.LastFetchTime = now
	progress.LastFetchOffset = fetchOffset

	// Calculate lag.
	progress.LagMessages = max(leaderLEO-fetchOffset, 0)

	// The fetched offset is what follower has successfully appended.
	// So matchedOffset = fetchOffset - 1 (they've appended up to fetchOffset-1).
	// Actually, fetchOffset is "give me from this offset", meaning they have up to fetchOffset-1.
	// Let's use fetchOffset as what they're requesting, so they have fetchOffset-1.
	progress.MatchedOffset = fetchOffset - 1
	if progress.MatchedOffset < 0 {
		progress.MatchedOffset = 0
	}

	// Check ISR status using COMBINED criteria.
	wasInSync := progress.IsInSync
	progress.IsInSync = im.checkInSyncLocked(progress, leaderLEO)

	// Update ISR set if status changed.
	if wasInSync && !progress.IsInSync {
		// Fell out of ISR.
		delete(im.isr, followerID)
		im.logger.Info("follower removed from ISR",
			"follower", followerID,
			"lag_messages", progress.LagMessages,
			"last_fetch", progress.LastFetchTime)
	} else if !wasInSync && progress.IsInSync {
		// Rejoined ISR.
		im.isr[followerID] = true
		im.logger.Info("follower rejoined ISR",
			"follower", followerID,
			"matched_offset", progress.MatchedOffset)
	}
}

// checkInSyncLocked checks if a follower should be in ISR.
// Uses COMBINED criteria: must pass BOTH time AND offset checks.
// Must be called with im.mu held.
func (im *ISRManager) checkInSyncLocked(progress *FollowerProgress, leaderLEO int64) bool {
	now := time.Now()

	// TIME CHECK: Has the follower fetched recently?
	lagTimeMs := now.Sub(progress.LastFetchTime).Milliseconds()
	timeOK := lagTimeMs <= int64(im.config.LagTimeMaxMs)

	// OFFSET CHECK: Is the follower close enough to leader?
	lagMessages := leaderLEO - progress.MatchedOffset - 1 // -1 because LEO is next offset
	if lagMessages < 0 {
		lagMessages = 0
	}
	offsetOK := lagMessages <= im.config.LagMaxMessages

	// COMBINED: Must pass BOTH criteria.
	return timeOK && offsetOK
}

// =============================================================================
// ISR MAINTENANCE
// =============================================================================

// CheckAndShrinkISR removes followers that have fallen behind.
// Should be called periodically (e.g., every second).
func (im *ISRManager) CheckAndShrinkISR(leaderLEO int64) []NodeID {
	im.mu.Lock()
	defer im.mu.Unlock()

	var removed []NodeID

	for nodeID, progress := range im.followerProgress {
		if nodeID == im.leaderID {
			continue // Skip leader.
		}

		if !im.checkInSyncLocked(progress, leaderLEO) {
			if im.isr[nodeID] {
				delete(im.isr, nodeID)
				progress.IsInSync = false
				removed = append(removed, nodeID)
				im.logger.Info("shrinking ISR",
					"follower", nodeID,
					"lag_messages", progress.LagMessages,
					"last_fetch_ms", time.Since(progress.LastFetchTime).Milliseconds())
			}
		}
	}

	return removed
}

// TryExpandISR adds followers that have caught up.
// Called after recording a fetch to check if follower can rejoin.
func (im *ISRManager) TryExpandISR(followerID NodeID, leaderLEO int64) bool {
	im.mu.Lock()
	defer im.mu.Unlock()

	progress, ok := im.followerProgress[followerID]
	if !ok {
		return false
	}

	// Already in ISR.
	if im.isr[followerID] {
		return true
	}

	// Check if caught up (matched offset equals or exceeds leader LEO - 1).
	if progress.MatchedOffset >= leaderLEO-1 && im.checkInSyncLocked(progress, leaderLEO) {
		im.isr[followerID] = true
		progress.IsInSync = true
		im.logger.Info("expanding ISR",
			"follower", followerID,
			"matched_offset", progress.MatchedOffset,
			"leader_leo", leaderLEO)
		return true
	}

	return false
}

// =============================================================================
// HIGH WATERMARK CALCULATION
// =============================================================================

// CalculateHighWatermark computes the new HW based on ISR progress.
// HW = min(LEO of all ISR members).
func (im *ISRManager) CalculateHighWatermark(leaderLEO int64) int64 {
	im.mu.RLock()
	defer im.mu.RUnlock()

	// Start with leader's LEO.
	hw := leaderLEO

	// Find minimum across ISR.
	for nodeID := range im.isr {
		if nodeID == im.leaderID {
			continue // Leader's LEO is already accounted for.
		}

		progress, ok := im.followerProgress[nodeID]
		if !ok {
			continue
		}

		// Follower's effective LEO is matchedOffset + 1.
		followerLEO := progress.MatchedOffset + 1
		if followerLEO < hw {
			hw = followerLEO
		}
	}

	return hw
}

// =============================================================================
// STATISTICS
// =============================================================================

// Stats returns ISR statistics.
type ISRStats struct {
	Topic             string
	Partition         int
	ISRSize           int
	ISRMembers        []NodeID
	OutOfSyncReplicas []NodeID
	MinMatchedOffset  int64
	MaxLagMessages    int64
}

// GetStats returns current ISR statistics.
func (im *ISRManager) GetStats() ISRStats {
	im.mu.RLock()
	defer im.mu.RUnlock()

	stats := ISRStats{
		Topic:             im.topic,
		Partition:         im.partition,
		ISRSize:           len(im.isr),
		ISRMembers:        make([]NodeID, 0, len(im.isr)),
		OutOfSyncReplicas: make([]NodeID, 0),
		MinMatchedOffset:  -1,
	}

	for nodeID := range im.isr {
		stats.ISRMembers = append(stats.ISRMembers, nodeID)
	}

	for _, r := range im.allReplicas {
		if !im.isr[r] {
			stats.OutOfSyncReplicas = append(stats.OutOfSyncReplicas, r)
		}
	}

	// Calculate lag stats.
	for _, progress := range im.followerProgress {
		if stats.MinMatchedOffset < 0 || progress.MatchedOffset < stats.MinMatchedOffset {
			stats.MinMatchedOffset = progress.MatchedOffset
		}
		if progress.LagMessages > stats.MaxLagMessages {
			stats.MaxLagMessages = progress.LagMessages
		}
	}

	return stats
}
