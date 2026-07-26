// =============================================================================
// PRIORITY INDEX OFFSET-ORDERING TESTS
// =============================================================================
//
// Every read path on PriorityIndex starts by calling searchOffsetInclusive, a
// binary search. Binary search over an unsorted slice does not fail loudly: it
// returns a plausible index, the scan proceeds from there, and any entry
// sitting before that index is skipped. Because the caller then advances its
// cursor past the offset it did get, the skipped entry is never revisited.
//
// The index could become unsorted because producers allocate an offset inside
// Log.Append (under the log's lock) and index it in AddMessage (under pi.mu).
// Two separate critical sections, so two concurrent producers can allocate 2
// and 3 in that order and index 3 first.
//
// The symptom was a message that was durably appended, whose offset was
// returned to the publisher in a 200 response, and which no consumer could
// ever read. Nothing errored anywhere.
//
// =============================================================================

package broker

import (
	"fmt"
	"sync"
	"testing"

	"goqueue/internal/storage"
)

// drainIndex walks the index the way Partition.Consume does and returns every
// offset it can reach.
func drainIndex(pi *PriorityIndex) []int64 {
	var got []int64
	cursor := int64(0)
	for {
		offset, _, found := pi.GetNextAcrossPriorities(cursor)
		if !found {
			return got
		}
		got = append(got, offset)
		cursor = offset + 1
	}
}

// TestPriorityIndex_OutOfOrderInsertStaysReachable is the direct statement of
// the bug: index 3 before 2 and both must still be readable.
func TestPriorityIndex_OutOfOrderInsertStaysReachable(t *testing.T) {
	pi := NewPriorityIndex()

	// Arrival order deliberately inverted for the middle pair, exactly what a
	// producer interleaving between Log.Append and AddMessage produces.
	for _, off := range []int64{0, 1, 3, 2, 4} {
		msg := storage.NewMessage([]byte(fmt.Sprintf("k%d", off)), []byte("v"))
		msg.Offset = off
		pi.AddMessage(msg)
	}

	got := drainIndex(pi)

	if len(got) != 5 {
		t.Fatalf("reachable offsets = %v (%d), want all 5", got, len(got))
	}
	for i, off := range got {
		if off != int64(i) {
			t.Fatalf("reachable offsets = %v, want ascending 0..4", got)
		}
	}
}

// TestPriorityIndex_ConcurrentAddKeepsEveryOffsetReachable is the property
// version: whatever order concurrent producers index in, nothing is lost.
func TestPriorityIndex_ConcurrentAddKeepsEveryOffsetReachable(t *testing.T) {
	const total = 500

	pi := NewPriorityIndex()

	// A shared counter stands in for Log.Append allocating the offset under a
	// different lock than the one AddMessage takes.
	var allocMu sync.Mutex
	next := int64(0)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				allocMu.Lock()
				if next >= total {
					allocMu.Unlock()
					return
				}
				off := next
				next++
				allocMu.Unlock()

				// The gap between allocating the offset and indexing it is the
				// whole race window.
				msg := storage.NewMessage([]byte(fmt.Sprintf("k%d", off)), []byte("v"))
				msg.Offset = off
				pi.AddMessage(msg)
			}
		}()
	}
	wg.Wait()

	got := drainIndex(pi)

	if len(got) != total {
		seen := make(map[int64]bool, len(got))
		for _, off := range got {
			seen[off] = true
		}
		var missing []int64
		for off := int64(0); off < total; off++ {
			if !seen[off] {
				missing = append(missing, off)
			}
		}
		t.Fatalf("reachable offsets = %d, want %d; unreachable despite being indexed: %v",
			len(got), total, missing)
	}

	for i, off := range got {
		if off != int64(i) {
			t.Fatalf("offset at position %d = %d, want %d (index is not offset-ordered)", i, off, i)
		}
	}
}
