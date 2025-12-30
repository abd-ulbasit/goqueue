package broker

import (
	"testing"
)

// =============================================================================
// PARTITIONER TESTS
// =============================================================================
//
// These tests verify:
// 1. Hash partitioner produces consistent results (same key → same partition)
// 2. Hash partitioner distributes keys evenly
// 3. Round-robin partitioner cycles correctly
// 4. Manual partitioner respects bounds
// 5. Edge cases (nil keys, single partition, etc.)
//

// TestHashPartitionerConsistency verifies that the same key always maps
// to the same partition. This is CRITICAL for ordering guarantees.
func TestHashPartitionerConsistency(t *testing.T) {
	p := NewHashPartitioner()
	numPartitions := 10

	testKeys := []string{
		"user-123",
		"order-456",
		"session-789",
		"",
		"a",
		"ab",
		"abc",
		"this-is-a-very-long-key-that-should-still-hash-consistently",
	}

	for _, key := range testKeys {
		keyBytes := []byte(key)
		if key == "" {
			keyBytes = nil // Test nil key separately
		}

		// Get partition 10 times - should always be the same for non-nil keys
		first := p.Partition(keyBytes, nil, numPartitions)
		for i := 0; i < 10; i++ {
			got := p.Partition(keyBytes, nil, numPartitions)
			if keyBytes != nil && got != first {
				t.Errorf("HashPartitioner inconsistent for key %q: first=%d, got=%d",
					key, first, got)
			}
		}
	}
}

// TestHashPartitionerDistribution verifies that keys are distributed
// reasonably evenly across partitions.
func TestHashPartitionerDistribution(t *testing.T) {
	p := NewHashPartitioner()
	numPartitions := 10
	numKeys := 10000

	// Count how many keys map to each partition
	counts := make([]int, numPartitions)
	for i := 0; i < numKeys; i++ {
		key := []byte(string(rune('a'+(i%26))) + string(rune(i)))
		partition := p.Partition(key, nil, numPartitions)
		counts[partition]++
	}

	// Each partition should have roughly numKeys/numPartitions keys
	expected := numKeys / numPartitions
	tolerance := expected / 2 // Allow 50% deviation

	for i, count := range counts {
		if count < expected-tolerance || count > expected+tolerance {
			t.Errorf("Partition %d has %d keys, expected ~%d (±%d)",
				i, count, expected, tolerance)
		}
	}

	// Log distribution for visibility
	t.Logf("Distribution across %d partitions:", numPartitions)
	for i, count := range counts {
		t.Logf("  Partition %d: %d keys (%.1f%%)", i, count, float64(count)/float64(numKeys)*100)
	}
}

// TestHashPartitionerBounds verifies partition index is always valid.
func TestHashPartitionerBounds(t *testing.T) {
	p := NewHashPartitioner()

	tests := []struct {
		name          string
		numPartitions int
	}{
		{"single partition", 1},
		{"two partitions", 2},
		{"ten partitions", 10},
		{"hundred partitions", 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				key := []byte(string(rune(i)))
				partition := p.Partition(key, nil, tt.numPartitions)
				if partition < 0 || partition >= tt.numPartitions {
					t.Errorf("Partition %d out of bounds [0, %d)",
						partition, tt.numPartitions)
				}
			}
		})
	}
}

// TestHashPartitionerNilKeyFallback verifies nil keys use round-robin.
func TestHashPartitionerNilKeyFallback(t *testing.T) {
	p := NewHashPartitioner()
	numPartitions := 3

	// With nil keys, should cycle through partitions
	seen := make(map[int]bool)
	for i := 0; i < numPartitions*2; i++ {
		partition := p.Partition(nil, []byte("value"), numPartitions)
		seen[partition] = true
	}

	// Should have seen all partitions
	if len(seen) != numPartitions {
		t.Errorf("Expected to see %d partitions, got %d", numPartitions, len(seen))
	}
}

// TestRoundRobinPartitioner verifies round-robin cycling behavior.
func TestRoundRobinPartitioner(t *testing.T) {
	p := NewRoundRobinPartitioner()
	numPartitions := 4

	// Should cycle: 1, 2, 3, 0, 1, 2, 3, 0, ... (starts at 1 because counter is post-increment)
	expected := []int{1, 2, 3, 0, 1, 2, 3, 0}
	for i, want := range expected {
		got := p.Partition([]byte("ignored"), nil, numPartitions)
		if got != want {
			t.Errorf("Message %d: expected partition %d, got %d", i, want, got)
		}
	}
}

// TestRoundRobinPartitionerEvenDistribution verifies all partitions get equal messages.
func TestRoundRobinPartitionerEvenDistribution(t *testing.T) {
	p := NewRoundRobinPartitioner()
	numPartitions := 5
	numMessages := 1000

	counts := make([]int, numPartitions)
	for i := 0; i < numMessages; i++ {
		partition := p.Partition(nil, nil, numPartitions)
		counts[partition]++
	}

	expected := numMessages / numPartitions
	for i, count := range counts {
		if count != expected {
			t.Errorf("Partition %d got %d messages, expected exactly %d",
				i, count, expected)
		}
	}
}

// TestManualPartitioner verifies explicit partition selection.
func TestManualPartitioner(t *testing.T) {
	tests := []struct {
		name          string
		targetPart    int
		numPartitions int
		expected      int
	}{
		{"valid partition", 2, 5, 2},
		{"partition 0", 0, 5, 0},
		{"last partition", 4, 5, 4},
		{"exceeds bounds", 10, 5, 0}, // 10 % 5 = 0
		{"negative", -1, 5, 0},       // bounds check
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewManualPartitioner(tt.targetPart)
			got := p.Partition([]byte("key"), []byte("value"), tt.numPartitions)
			if got != tt.expected {
				t.Errorf("Expected partition %d, got %d", tt.expected, got)
			}
		})
	}
}

// TestMurmur3KnownValues verifies our murmur3 implementation against known values.
// These values should match other murmur3 implementations.
func TestMurmur3KnownValues(t *testing.T) {
	// Test vectors (verified against reference implementations)
	tests := []struct {
		input    string
		expected uint32
	}{
		{"", 0},
		{"a", 1009084850},
		{"ab", 2613040991},
		{"abc", 3017643002},
		{"hello", 613153351},
		{"hello world", 1586663183},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := murmur3Hash([]byte(tt.input))
			if got != tt.expected {
				t.Errorf("murmur3(%q) = %d, expected %d", tt.input, got, tt.expected)
			}
		})
	}
}

// TestPartitionerEdgeCases tests edge cases that could cause panics.
func TestPartitionerEdgeCases(t *testing.T) {
	partitioners := []struct {
		name string
		p    Partitioner
	}{
		{"hash", NewHashPartitioner()},
		{"roundrobin", NewRoundRobinPartitioner()},
		{"manual(0)", NewManualPartitioner(0)},
	}

	for _, pt := range partitioners {
		t.Run(pt.name, func(t *testing.T) {
			// Zero partitions should not panic
			partition := pt.p.Partition([]byte("key"), []byte("value"), 0)
			if partition != 0 {
				t.Errorf("Zero partitions should return 0, got %d", partition)
			}

			// Negative partitions (shouldn't happen but test anyway)
			partition = pt.p.Partition([]byte("key"), []byte("value"), -1)
			if partition != 0 {
				t.Errorf("Negative partitions should return 0, got %d", partition)
			}

			// Empty value should work
			partition = pt.p.Partition([]byte("key"), nil, 5)
			if partition < 0 || partition >= 5 {
				t.Errorf("Partition out of bounds: %d", partition)
			}
		})
	}
}

// BenchmarkHashPartitioner measures hashing performance.
func BenchmarkHashPartitioner(b *testing.B) {
	p := NewHashPartitioner()
	key := []byte("user-12345678")
	value := []byte("some payload data here")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Partition(key, value, 100)
	}
}

// BenchmarkRoundRobinPartitioner measures round-robin performance.
func BenchmarkRoundRobinPartitioner(b *testing.B) {
	p := NewRoundRobinPartitioner()
	key := []byte("user-12345678")
	value := []byte("some payload data here")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Partition(key, value, 100)
	}
}

// BenchmarkMurmur3 measures raw hash performance.
func BenchmarkMurmur3(b *testing.B) {
	data := []byte("user-12345678-session-abcdefgh")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		murmur3Hash(data)
	}
}
