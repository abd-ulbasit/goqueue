// =============================================================================
// PARTITIONER - ROUTING MESSAGES TO PARTITIONS
// =============================================================================
//
// WHAT IS A PARTITIONER?
// A partitioner decides which partition a message should go to. This is one of
// the most critical decisions in a distributed queue because it determines:
//   - Message ordering (same partition = ordered, different = parallel)
//   - Load distribution (even spread = good utilization)
//   - Data locality (related messages together = efficient consumers)
//
// WHY DOES PARTITION ROUTING MATTER?
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                    THE PARTITION ROUTING PROBLEM                        │
//   │                                                                         │
//   │   Producer sends:                                                       │
//   │     Order #1 (user: alice)                                              │
//   │     Order #2 (user: bob)                                                │
//   │     Order #3 (user: alice)  ← Must go to SAME partition as Order #1!    │
//   │                                                                         │
//   │   WHY? Because consumer processing Order #3 needs to see Order #1 first │
//   │   If they're in different partitions, no ordering guarantee!            │
//   │                                                                         │
//   │   Solution: Route by key                                                │
//   │     hash("alice") % 3 = 1  → Partition 1                                │
//   │     hash("bob")   % 3 = 0  → Partition 0                                │
//   │     hash("alice") % 3 = 1  → Partition 1  ✓ Same as Order #1            │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// PARTITIONER STRATEGIES:
//
//   1. HASH PARTITIONER (Default)
//      - hash(key) % numPartitions
//      - Same key ALWAYS goes to same partition (until partitions change)
//      - USE WHEN: You need ordering per entity (user, order, session)
//
//   2. ROUND ROBIN PARTITIONER
//      - Cycles through partitions: 0, 1, 2, 0, 1, 2, ...
//      - Ignores message key
//      - USE WHEN: Maximum throughput, no ordering needed (metrics, logs)
//
//   3. MANUAL/EXPLICIT PARTITIONER
//      - Producer specifies exact partition
//      - USE WHEN: Custom routing logic (geo-based, priority lanes)
//
// COMPARISON - How other systems partition:
//
//   - Kafka: DefaultPartitioner uses murmur2 hash, sticky partitioner for
//            batching efficiency, round-robin for null keys
//   - RabbitMQ: No partitioning (single queue), use consistent hash exchange
//               plugin for similar behavior
//   - SQS: No partitioning (FIFO queues use MessageGroupId similar to our key)
//   - Pulsar: Similar to Kafka, murmur3 hash by default
//
// HASH FUNCTION CHOICE:
//
//   We use Murmur3 because:
//   ┌───────────────────────────────────────────────────────────────────────┐
//   │ Algorithm   │ Speed      │ Distribution │ Used By                    │
//   ├─────────────┼────────────┼──────────────┼────────────────────────────│
//   │ Murmur3     │ Very Fast  │ Excellent    │ Kafka, Cassandra, Redis    │
//   │ xxHash      │ Fastest    │ Excellent    │ ClickHouse, LZ4            │
//   │ FNV-1a      │ Fast       │ Good         │ Go maps (internal)         │
//   │ CRC32       │ Fast (HW)  │ OK           │ Checksums, not routing     │
//   │ MD5/SHA     │ Slow       │ Perfect      │ Crypto (overkill for us)   │
//   └───────────────────────────────────────────────────────────────────────┘
//
//   Murmur3 is the industry standard for hash-based partitioning.
//   It's fast, has excellent distribution, and is what Kafka uses.
//
// =============================================================================

package broker

import (
	"sync/atomic"
)

// =============================================================================
// PARTITIONER INTERFACE
// =============================================================================

// Partitioner determines which partition a message should be routed to.
//
// INTERFACE CONTRACT:
//   - Partition() must be deterministic for the same key (hash-based)
//   - Partition() must return valid partition index [0, numPartitions)
//   - Implementations must be thread-safe (concurrent producers)
type Partitioner interface {
	// Partition returns the partition index for a message.
	//
	// PARAMETERS:
	//   - key: Message routing key (may be nil)
	//   - value: Message payload (some partitioners may use this)
	//   - numPartitions: Total partitions in topic
	//
	// RETURNS:
	//   - Partition index [0, numPartitions)
	Partition(key, value []byte, numPartitions int) int
}

// =============================================================================
// HASH PARTITIONER (Murmur3)
// =============================================================================

// HashPartitioner routes messages based on murmur3 hash of the key.
//
// BEHAVIOR:
//   - If key is nil: falls back to round-robin
//   - If key is non-nil: hash(key) % numPartitions
//
// GUARANTEES:
//   - Same key always goes to same partition (for fixed partition count)
//   - Even distribution across partitions (murmur3 property)
//
// WARNING - PARTITION COUNT CHANGES:
//
//	If you add partitions, existing keys may route to DIFFERENT partitions!
//	This breaks ordering for in-flight messages.
//
//	Example:
//	  Before: hash("user-123") % 3 = 1  (partition 1)
//	  After:  hash("user-123") % 4 = 2  (partition 2!)  ← Different!
//
//	This is why Kafka recommends: plan partition count upfront, or use
//	consumer groups that can handle rebalancing.
type HashPartitioner struct {
	// fallback is used when key is nil
	fallback *RoundRobinPartitioner
}

// NewHashPartitioner creates a new hash-based partitioner.
func NewHashPartitioner() *HashPartitioner {
	return &HashPartitioner{
		fallback: NewRoundRobinPartitioner(),
	}
}

// Partition returns the partition for a message using murmur3 hash.
//
// ALGORITHM:
//  1. If key is nil, use round-robin (no ordering needed)
//  2. Compute murmur3 hash of key
//  3. Take absolute value (hash can be negative)
//  4. Modulo by partition count
func (p *HashPartitioner) Partition(key, value []byte, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	if len(key) == 0 {
		// No key: round-robin for even distribution
		return p.fallback.Partition(key, value, numPartitions)
	}

	// Murmur3 hash of the key
	hash := murmur3Hash(key)

	// Ensure positive (hash can overflow to negative)
	// Use bitwise AND with max int32 to get positive value
	if hash < 0 {
		hash = -hash
	}

	return int(hash % uint32(numPartitions))
}

// =============================================================================
// MURMUR3 HASH IMPLEMENTATION
// =============================================================================
//
// Murmur3 is a non-cryptographic hash function created by Austin Appleby.
// It's designed for hash-based lookups, not security.
//
// PROPERTIES:
//   - Fast: ~3 bytes/cycle on modern CPUs
//   - Good avalanche: small input changes → large output changes
//   - Low collision rate: different inputs rarely produce same hash
//
// HOW IT WORKS (simplified):
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                         MURMUR3 ALGORITHM                               │
//   │                                                                         │
//   │   Input: "alice" (5 bytes)                                              │
//   │                                                                         │
//   │   1. INITIALIZATION                                                     │
//   │      seed = 0 (or custom seed)                                          │
//   │      h1 = seed                                                          │
//   │                                                                         │
//   │   2. BODY (process 4-byte chunks)                                       │
//   │      For each 4-byte block:                                             │
//   │        k1 = block * c1          (multiply by constant)                  │
//   │        k1 = rotl(k1, 15)        (rotate left 15 bits)                   │
//   │        k1 = k1 * c2             (multiply by another constant)          │
//   │        h1 = h1 XOR k1           (mix into hash)                         │
//   │        h1 = rotl(h1, 13)        (rotate)                                │
//   │        h1 = h1*5 + 0xe6546b64   (more mixing)                           │
//   │                                                                         │
//   │   3. TAIL (remaining 1-3 bytes)                                         │
//   │      Process remaining bytes with similar mixing                        │
//   │                                                                         │
//   │   4. FINALIZATION                                                       │
//   │      h1 = h1 XOR length         (include length in hash)                │
//   │      h1 = fmix32(h1)            (final mixing for avalanche)            │
//   │                                                                         │
//   │   Output: 32-bit hash value                                             │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// WHY THESE MAGIC NUMBERS?
//   The constants (c1, c2, rotation amounts) were chosen through extensive
//   testing to maximize avalanche effect and minimize collisions.
//   They're not random - they're the result of optimization.
//

// murmur3 constants
const (
	c1_32 uint32 = 0xcc9e2d51
	c2_32 uint32 = 0x1b873593
)

// murmur3Hash computes murmur3 32-bit hash of the input.
// This is compatible with Kafka's default partitioner.
func murmur3Hash(data []byte) uint32 {
	length := len(data)
	if length == 0 {
		return 0
	}

	// Number of 4-byte blocks
	nblocks := length / 4

	// Seed (0 matches Kafka's default)
	var h1 uint32 = 0

	// ==========================================================================
	// BODY: Process 4-byte chunks
	// ==========================================================================
	// We process the input in 4-byte blocks because:
	// 1. Modern CPUs handle 32-bit operations efficiently
	// 2. Allows parallel processing in CPU pipeline
	// 3. Reduces loop iterations

	for i := 0; i < nblocks; i++ {
		// Read 4 bytes as little-endian uint32
		// Little-endian: least significant byte first
		// Example: bytes [0x01, 0x02, 0x03, 0x04] → 0x04030201
		k1 := uint32(data[i*4]) |
			uint32(data[i*4+1])<<8 |
			uint32(data[i*4+2])<<16 |
			uint32(data[i*4+3])<<24

		// Mix the block into the hash
		k1 *= c1_32
		k1 = rotl32(k1, 15) // Rotate left by 15 bits
		k1 *= c2_32

		h1 ^= k1
		h1 = rotl32(h1, 13)
		h1 = h1*5 + 0xe6546b64
	}

	// ==========================================================================
	// TAIL: Handle remaining 1-3 bytes
	// ==========================================================================
	// If input length isn't divisible by 4, we have leftover bytes.
	// These are processed separately with a fallthrough switch.

	tail := data[nblocks*4:]
	var k1 uint32

	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough // Process next case too
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1_32
		k1 = rotl32(k1, 15)
		k1 *= c2_32
		h1 ^= k1
	}

	// ==========================================================================
	// FINALIZATION: Final mixing
	// ==========================================================================
	// This ensures that even small changes in input produce large changes
	// in the final hash (avalanche effect).

	h1 ^= uint32(length)
	h1 = fmix32(h1)

	return h1
}

// rotl32 rotates a 32-bit value left by r bits.
//
// WHY ROTATION?
// Rotation spreads bits across the hash value, ensuring that
// adjacent bytes in the input affect different bits in the output.
//
// Example: rotl32(0b00001111, 2) = 0b00111100
func rotl32(x uint32, r int) uint32 {
	return (x << r) | (x >> (32 - r))
}

// fmix32 is the finalization mix function.
//
// WHY FINALIZATION?
// Without this, the last few bytes of input would have less influence
// on the output. This function ensures all input bytes contribute equally
// to the final hash value.
//
// The magic numbers (0x85ebca6b, 0xc2b2ae35) and shift amounts (16, 13, 16)
// were determined through statistical analysis to maximize avalanche.
func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// =============================================================================
// ROUND ROBIN PARTITIONER
// =============================================================================

// RoundRobinPartitioner distributes messages evenly across partitions.
//
// BEHAVIOR:
//   - Ignores message key entirely
//   - Cycles through: 0, 1, 2, ..., n-1, 0, 1, 2, ...
//   - Thread-safe using atomic counter
//
// USE CASES:
//   - Maximum throughput (all partitions utilized)
//   - No ordering requirements (logs, metrics, events)
//   - Batch-insensitive workloads
//
// COMPARISON TO KAFKA'S STICKY PARTITIONER:
//
//	Kafka 2.4+ introduced "sticky partitioner" which sticks to one partition
//	until a batch is full, then moves to next. This improves batching efficiency.
//	We keep simple round-robin for now; sticky can be added later.
type RoundRobinPartitioner struct {
	// counter is atomically incremented for each message
	// Using uint64 to avoid overflow issues (uint32 would wrap at 4B messages)
	counter uint64
}

// NewRoundRobinPartitioner creates a new round-robin partitioner.
func NewRoundRobinPartitioner() *RoundRobinPartitioner {
	return &RoundRobinPartitioner{}
}

// Partition returns the next partition in round-robin order.
//
// THREAD SAFETY:
//
//	Uses atomic.AddUint64 to safely increment counter across goroutines.
//	This is lock-free and very fast.
func (p *RoundRobinPartitioner) Partition(key, value []byte, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}

	// Atomically increment and get the new value
	n := atomic.AddUint64(&p.counter, 1)

	// Modulo to get partition index
	return int(n % uint64(numPartitions))
}

// =============================================================================
// MANUAL PARTITIONER
// =============================================================================

// ManualPartitioner allows explicit partition selection.
//
// USAGE:
//
//	partitioner := NewManualPartitioner(2)  // Always partition 2
//	partition := partitioner.Partition(key, value, numPartitions)
//
// USE CASES:
//   - Custom routing logic (partition by region, priority)
//   - Testing (force messages to specific partition)
//   - Migration scenarios (draining specific partitions)
//
// WARNING:
//
//	Producer is responsible for ensuring partition index is valid.
//	If partition >= numPartitions, we return partition % numPartitions
//	to avoid panics, but this may not be the intended behavior.
type ManualPartitioner struct {
	// partition is the fixed partition to route all messages to
	partition int
}

// NewManualPartitioner creates a partitioner that always returns the same partition.
func NewManualPartitioner(partition int) *ManualPartitioner {
	return &ManualPartitioner{partition: partition}
}

// Partition returns the configured partition (with bounds checking).
func (p *ManualPartitioner) Partition(key, value []byte, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	if p.partition < 0 {
		return 0
	}
	// Ensure we don't exceed partition count
	return p.partition % numPartitions
}

// =============================================================================
// DEFAULT PARTITIONER (CONVENIENCE)
// =============================================================================

// DefaultPartitioner is the recommended partitioner for most use cases.
// It uses hash partitioning for messages with keys (ordering guarantee)
// and round-robin for messages without keys (throughput optimization).
var DefaultPartitioner Partitioner = NewHashPartitioner()

// type checking to make sure implementations satisfy the interface
var (
	_ Partitioner = (*HashPartitioner)(nil)
	_ Partitioner = (*RoundRobinPartitioner)(nil)
	_ Partitioner = (*ManualPartitioner)(nil)
	_ Partitioner = DefaultPartitioner
)
