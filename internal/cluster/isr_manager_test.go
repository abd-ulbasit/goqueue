package cluster

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestISRManager_ShrinkExpandAndHighWatermark(t *testing.T) {
	// =========================================================================
	// ISR MANAGER: SHRINK / EXPAND + HIGH WATERMARK
	// =========================================================================
	//
	// WHY THIS MATTERS:
	//   The ISR (In-Sync Replica set) is the *durability contract* for replication.
	//   The leader can only claim a message is "committed" once it is present on
	//   enough replicas (Kafka: ISR, Raft: majority).
	//
	// BUGS THIS TEST CATCHES:
	//   - Removing the wrong follower from ISR due to time/offset math
	//   - Never re-adding a follower after it catches up
	//   - High watermark (HW) computed from non-ISR replicas (too low)
	//
	// NOTE:
	//   We keep time thresholds tiny and manipulate timestamps directly to avoid
	//   sleeping and to make the test deterministic.
	// =========================================================================

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	replicas := []NodeID{"n1", "n2", "n3"}
	cfg := ISRConfig{
		LagTimeMaxMs: 5,
		// Keep the offset threshold generous so the test can isolate the
		// time-based eviction path (n2) without accidentally evicting other
		// followers due to initial MatchedOffset defaults.
		LagMaxMessages:    100,
		MinInSyncReplicas: 2,
	}

	im := NewISRManager("orders", 0, "n1", replicas, cfg, logger)

	// Start with everyone in ISR (optimistic initialization).
	if got := im.ISRSize(); got != 3 {
		t.Fatalf("ISRSize=%d want=3", got)
	}
	if !im.HasMinISR() {
		t.Fatalf("expected HasMinISR=true")
	}

	leaderLEO := int64(100)

	// Force n2 to fall out of sync.
	im.mu.Lock()
	im.followerProgress["n2"].LastFetchTime = time.Now().Add(-100 * time.Millisecond)
	im.followerProgress["n2"].MatchedOffset = 0
	im.followerProgress["n2"].LagMessages = 999
	im.mu.Unlock()

	removed := im.CheckAndShrinkISR(leaderLEO)
	if len(removed) != 1 || removed[0] != "n2" {
		t.Fatalf("removed=%v want=[n2]", removed)
	}
	if im.IsInSync("n2") {
		t.Fatalf("expected n2 to be out of ISR")
	}
	if !im.HasMinISR() {
		// With n1+n3 still in ISR and MinISR=2, writes should still be allowed.
		t.Fatalf("expected HasMinISR=true after shrink")
	}

	// Set follower matched offsets to validate HW computation.
	// - n3 is behind leader but still in ISR.
	im.mu.Lock()
	im.followerProgress["n3"].MatchedOffset = 50 // follower LEO = 51
	im.mu.Unlock()

	if hw := im.CalculateHighWatermark(leaderLEO); hw != 51 {
		t.Fatalf("CalculateHighWatermark=%d want=51 (min of leader=100 and n3=51)", hw)
	}

	// Now n2 catches up and should rejoin ISR.
	im.mu.Lock()
	im.followerProgress["n2"].LastFetchTime = time.Now()
	im.followerProgress["n2"].MatchedOffset = 99 // follower LEO = 100
	im.followerProgress["n2"].LagMessages = 0
	im.mu.Unlock()

	if ok := im.TryExpandISR("n2", leaderLEO); !ok {
		t.Fatalf("expected TryExpandISR to succeed")
	}
	if !im.IsInSync("n2") {
		t.Fatalf("expected n2 to be back in ISR")
	}

	stats := im.GetStats()
	if stats.Topic != "orders" || stats.Partition != 0 {
		t.Fatalf("GetStats topic/partition mismatch: %+v", stats)
	}
	if stats.ISRSize != 3 {
		t.Fatalf("GetStats ISRSize=%d want=3", stats.ISRSize)
	}
	if len(stats.OutOfSyncReplicas) != 0 {
		t.Fatalf("expected no out-of-sync replicas after expansion; got=%v", stats.OutOfSyncReplicas)
	}
}
