package broker

import (
	"errors"
	"testing"
	"time"

	"goqueue/internal/storage"
)

func TestPriorityIndex_AddAndQuery_SkipsConsumedAndRespectsFromOffset(t *testing.T) {
	pi := NewPriorityIndex()

	// Add 4 HIGH priority messages at offsets 0..3.
	for i := int64(0); i < 4; i++ {
		pi.AddMessage(&storage.Message{Offset: i, Timestamp: 100 + i, Priority: storage.PriorityHigh, Value: []byte("v")})
	}

	// Mark offsets 1 and 2 as consumed; queries should skip them.
	pi.MarkConsumed(1)
	pi.MarkConsumed(2)

	if off, ok := pi.GetNextOffset(storage.PriorityHigh, 0); !ok || off != 0 {
		t.Fatalf("GetNextOffset(from=0)=(%d,%v), want (0,true)", off, ok)
	}
	if off, ok := pi.GetNextOffset(storage.PriorityHigh, 1); !ok || off != 3 {
		t.Fatalf("GetNextOffset(from=1)=(%d,%v), want (3,true)", off, ok)
	}
	if off, ok := pi.GetNextOffset(storage.PriorityHigh, 4); ok || off != -1 {
		t.Fatalf("GetNextOffset(from=4)=(%d,%v), want (-1,false)", off, ok)
	}

	got := pi.GetNextN(storage.PriorityHigh, 0, 10)
	if len(got) != 2 || got[0] != 0 || got[1] != 3 {
		t.Fatalf("GetNextN=%v, want [0 3]", got)
	}
}

func TestPriorityIndex_AddMessage_InvalidPriorityDefaultsToNormal(t *testing.T) {
	pi := NewPriorityIndex()

	// Priority(99) is invalid; index should treat it as PriorityNormal.
	pi.AddMessage(&storage.Message{Offset: 7, Timestamp: 123, Priority: storage.Priority(99), Value: []byte("v")})

	if off, ok := pi.GetNextOffset(storage.PriorityNormal, 0); !ok || off != 7 {
		t.Fatalf("GetNextOffset(normal)=(%d,%v), want (7,true)", off, ok)
	}
}

func TestPriorityIndex_GetNextAcrossPriorities_IsStrictPriority(t *testing.T) {
	pi := NewPriorityIndex()

	// Low priority message at offset 0, high priority message at offset 10.
	pi.AddEntry(storage.PriorityLow, PriorityIndexEntry{Offset: 0, Timestamp: 1, Size: 1})
	pi.AddEntry(storage.PriorityHigh, PriorityIndexEntry{Offset: 10, Timestamp: 2, Size: 1})

	off, p, ok := pi.GetNextAcrossPriorities(0)
	if !ok {
		t.Fatalf("GetNextAcrossPriorities expected ok")
	}
	if off != 10 || p != storage.PriorityHigh {
		t.Fatalf("GetNextAcrossPriorities=(off=%d,p=%v), want (10,High)", off, p)
	}
}

func TestPriorityIndex_Compaction_RemovesConsumedEntriesAndResetsCounters(t *testing.T) {
	pi := NewPriorityIndex()
	pi.compactionThreshold = 2 // force compaction quickly

	pi.AddEntry(storage.PriorityNormal, PriorityIndexEntry{Offset: 0, Timestamp: 1, Size: 1})
	pi.AddEntry(storage.PriorityNormal, PriorityIndexEntry{Offset: 1, Timestamp: 2, Size: 1})
	pi.AddEntry(storage.PriorityNormal, PriorityIndexEntry{Offset: 2, Timestamp: 3, Size: 1})

	pi.MarkConsumedBatch([]int64{0, 1, 2}) // exceeds threshold -> compacts

	// After compaction, only unconsumed entries should remain (none, in this case).
	if off, ok := pi.GetNextOffset(storage.PriorityNormal, 0); ok || off != -1 {
		t.Fatalf("GetNextOffset after compaction=(%d,%v), want (-1,false)", off, ok)
	}
	if pi.Stats().ConsumedCount != 0 {
		t.Fatalf("ConsumedCount=%d, want 0", pi.Stats().ConsumedCount)
	}
}

func TestPriorityIndex_RebuildFromLog_ReplacesExistingState(t *testing.T) {
	pi := NewPriorityIndex()
	pi.AddEntry(storage.PriorityHigh, PriorityIndexEntry{Offset: 999, Timestamp: 1, Size: 1})

	// Fake reader returns 3 messages then stops.
	msgs := []*storage.Message{
		{Offset: 0, Timestamp: 10, Priority: storage.PriorityNormal, Value: []byte("a")},
		{Offset: 1, Timestamp: 11, Priority: storage.PriorityCritical, Value: []byte("b")},
		{Offset: 2, Timestamp: 12, Priority: storage.Priority(99), Value: []byte("c")}, // invalid -> Normal
	}
	reader := func(off int64) (*storage.Message, error) {
		if off < 0 || off >= int64(len(msgs)) {
			return nil, errors.New("eof")
		}
		return msgs[off], nil
	}

	if err := pi.RebuildFromLog(reader, 0); err != nil {
		t.Fatalf("RebuildFromLog failed: %v", err)
	}

	// Verify state was cleared and rebuilt.
	if pi.Len() != 3 {
		t.Fatalf("Len()=%d, want 3", pi.Len())
	}

	stats := pi.Stats()
	if stats.MessageCount[storage.PriorityCritical] != 1 {
		t.Fatalf("Critical count=%d, want 1", stats.MessageCount[storage.PriorityCritical])
	}
	if stats.MessageCount[storage.PriorityNormal] != 2 {
		t.Fatalf("Normal count=%d, want 2", stats.MessageCount[storage.PriorityNormal])
	}
}

func TestPriorityIndex_MetricsSnapshot_PendingCountsAndOldestTimestamp(t *testing.T) {
	pi := NewPriorityIndex()

	pi.AddEntry(storage.PriorityNormal, PriorityIndexEntry{Offset: 0, Timestamp: 50, Size: 1})
	pi.AddEntry(storage.PriorityNormal, PriorityIndexEntry{Offset: 1, Timestamp: 40, Size: 1}) // older
	pi.AddEntry(storage.PriorityHigh, PriorityIndexEntry{Offset: 2, Timestamp: 60, Size: 1})
	pi.MarkConsumed(0)

	snaps := pi.GetMetricsSnapshot()
	if len(snaps) != storage.PriorityCount {
		t.Fatalf("snapshots len=%d, want %d", len(snaps), storage.PriorityCount)
	}

	// Normal: 2 total, 1 pending (offset=1), oldest pending timestamp should be 40.
	normal := snaps[storage.PriorityNormal]
	if normal.TotalMessages != 2 {
		t.Fatalf("normal.TotalMessages=%d, want 2", normal.TotalMessages)
	}
	if normal.PendingMessages != 1 {
		t.Fatalf("normal.PendingMessages=%d, want 1", normal.PendingMessages)
	}
	if normal.OldestPendingTimestamp != 40 {
		t.Fatalf("normal.OldestPendingTimestamp=%d, want 40", normal.OldestPendingTimestamp)
	}

	// High: 1 total, 1 pending, oldest pending timestamp 60.
	high := snaps[storage.PriorityHigh]
	if high.TotalMessages != 1 || high.PendingMessages != 1 || high.OldestPendingTimestamp != 60 {
		t.Fatalf("high snapshot=%+v, want TotalMessages=1 PendingMessages=1 Oldest=60", high)
	}
}

func TestPriorityIndex_Clear_ResetsAllState(t *testing.T) {
	pi := NewPriorityIndex()
	pi.AddEntry(storage.PriorityNormal, PriorityIndexEntry{Offset: 0, Timestamp: time.Now().UnixNano(), Size: 1})
	pi.MarkConsumed(0)

	pi.Clear()

	if pi.Len() != 0 {
		t.Fatalf("Len()=%d, want 0", pi.Len())
	}
	if pi.Stats().ConsumedCount != 0 {
		t.Fatalf("ConsumedCount=%d, want 0", pi.Stats().ConsumedCount)
	}
}

func TestPriorityIndex_LenByPriority_UnconsumedCount_AndCompact(t *testing.T) {
	pi := NewPriorityIndex()

	pi.AddEntry(storage.PriorityNormal, PriorityIndexEntry{Offset: 0, Timestamp: 1, Size: 1})
	pi.AddEntry(storage.PriorityNormal, PriorityIndexEntry{Offset: 1, Timestamp: 2, Size: 1})
	pi.AddEntry(storage.PriorityHigh, PriorityIndexEntry{Offset: 2, Timestamp: 3, Size: 1})

	pi.MarkConsumed(1)

	byPrio := pi.LenByPriority()
	if byPrio[storage.PriorityNormal] != 2 || byPrio[storage.PriorityHigh] != 1 {
		t.Fatalf("LenByPriority=%v, want normal=2 high=1", byPrio)
	}

	unconsumed := pi.UnconsumedCount()
	if unconsumed[storage.PriorityNormal] != 1 || unconsumed[storage.PriorityHigh] != 1 {
		t.Fatalf("UnconsumedCount=%v, want normal=1 high=1", unconsumed)
	}

	pi.Compact()

	byPrioAfter := pi.LenByPriority()
	if byPrioAfter[storage.PriorityNormal] != 1 || byPrioAfter[storage.PriorityHigh] != 1 {
		t.Fatalf("LenByPriority after Compact=%v, want normal=1 high=1", byPrioAfter)
	}
	if pi.Stats().ConsumedCount != 0 {
		t.Fatalf("ConsumedCount after Compact=%d, want 0", pi.Stats().ConsumedCount)
	}
}
