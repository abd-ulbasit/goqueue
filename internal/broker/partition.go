// =============================================================================
// PARTITION - THE UNIT OF PARALLELISM
// =============================================================================
//
// WHAT IS A PARTITION?
// A partition is a totally ordered, immutable sequence of messages. Each topic
// has one or more partitions. Within a partition:
//   - Messages have sequential offsets (0, 1, 2, 3, ...)
//   - Order is guaranteed (read order = write order)
//   - Each message is written exactly once
//
// WHY PARTITIONS?
//
//   1. PARALLELISM
//      - Different partitions can be read/written by different machines
//      - More partitions = more parallelism = higher throughput
//      - Example: 3 partitions → 3 consumers can work in parallel
//
//   2. ORDERING TRADE-OFF
//      - Within partition: STRICT ordering (message A before B always)
//      - Across partitions: NO ordering (partition 1's offset 5 might be
//        before or after partition 2's offset 3)
//
//   3. SCALABILITY
//      - Partitions can be distributed across brokers (nodes)
//      - Add more partitions → spread load across more machines
//
// PARTITION vs LOG:
//   - Log: Storage abstraction (segments, indexes, bytes on disk)
//   - Partition: Business abstraction (offsets, consumers, replication)
//
// =============================================================================

package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"goqueue/internal/storage"
)

// =============================================================================
// PARTITION STRUCT
// =============================================================================

// Partition represents a single partition within a topic.
type Partition struct {
	topic             string             // topic is the name of the topic this partition belongs to
	id                int                // id is the partition number within the topic (0, 1, 2, ...)
	log               *storage.Log       // log is the underlying storage engine
	dir               string             // dir is the directory containing partition data
	priorityIndex     *PriorityIndex     // priorityIndex tracks messages by priority
	priorityScheduler *PriorityScheduler // priorityScheduler for WFQ consumption
	mu                sync.RWMutex       // mu protects metadata access
	createdAt         time.Time          // createdAt is when the partition was created
	closed            bool               // closed tracks if partition is closed
}

// =============================================================================
// PARTITION CREATION & LOADING
// =============================================================================

// NewPartition creates a new partition.
func NewPartition(baseDir string, topic string, id int) (*Partition, error) {
	// Create partition directory
	dir := filepath.Join(baseDir, topic, fmt.Sprintf("%d", id))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory: %w", err)
	}

	// Create log
	log, err := storage.NewLog(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to create log: %w", err)
	}

	return &Partition{
		topic:             topic,
		id:                id,
		log:               log,
		dir:               dir,
		priorityIndex:     NewPriorityIndex(),
		priorityScheduler: NewPriorityScheduler(DefaultPrioritySchedulerConfig()),
		createdAt:         time.Now(),
	}, nil
}

// LoadPartition opens an existing partition.
func LoadPartition(baseDir string, topic string, id int) (*Partition, error) {
	dir := filepath.Join(baseDir, topic, fmt.Sprintf("%d", id))

	// Check directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, fmt.Errorf("partition directory not found: %s", dir)
	}

	// Load log
	log, err := storage.LoadLog(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to load log: %w", err)
	}

	p := &Partition{
		topic:             topic,
		id:                id,
		log:               log,
		dir:               dir,
		priorityIndex:     NewPriorityIndex(),
		priorityScheduler: NewPriorityScheduler(DefaultPrioritySchedulerConfig()),
		// TODO: Can we load actual creation time from file metadata
		createdAt: time.Now(), //  We don't persist creation time yet
	}

	// Rebuild priority index and scheduler from log
	if err := p.rebuildPriorityState(); err != nil {
		log.Close()
		return nil, fmt.Errorf("failed to rebuild priority state: %w", err)
	}

	return p, nil
}

// =============================================================================
// PRODUCER OPERATIONS
// =============================================================================

// Produce appends a message to the partition and returns the assigned offset.
func (p *Partition) Produce(key, value []byte) (int64, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return 0, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	// Create message (defaults to priority 0 - highest)
	msg := storage.NewMessage(key, value)

	// Append to log
	offset, err := p.log.Append(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to append message: %w", err)
	}

	// Update priority index and scheduler
	// The log.Append sets msg.Offset, so we can use the full message
	msg.Offset = offset
	p.priorityIndex.AddMessage(msg)
	p.priorityScheduler.Enqueue(msg)

	return offset, nil
}

// =============================================================================
// BATCH PRODUCE - HIGH THROUGHPUT PATH
// =============================================================================
//
// ProduceBatch writes multiple messages efficiently using batch append.
// This is 10-100x faster than calling Produce() in a loop.
//
// PERFORMANCE:
//   - Single Produce():   ~10,000 msg/sec (syscall per message)
//   - ProduceBatch():     ~500,000 msg/sec (single flush for batch)
//
// WHY:
//   - Amortizes lock acquisition (one lock for entire batch)
//   - Batches I/O operations (one flush instead of N flushes)
//   - Better cache utilization (sequential memory access)
//
// RETURNS:
//   - Slice of assigned offsets (same order as input)
//   - Error if any message fails (partial writes NOT returned)
func (p *Partition) ProduceBatch(messages []*storage.Message) ([]int64, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	if len(messages) == 0 {
		return nil, nil
	}

	// Use batch append for efficiency
	offsets, err := p.log.AppendBatch(messages)
	if err != nil {
		return nil, fmt.Errorf("failed to batch append: %w", err)
	}

	// Update priority structures for all messages
	// This is still sequential but much faster than interleaved I/O
	for i, msg := range messages {
		msg.Offset = offsets[i]
		if !msg.IsControlRecord() {
			p.priorityIndex.AddMessage(msg)
			p.priorityScheduler.Enqueue(msg)
		}
	}

	return offsets, nil
}

// ProduceBatchSimple creates messages from key/value pairs and writes them.
// Convenience method for the common case where you have raw data.
func (p *Partition) ProduceBatchSimple(items []struct{ Key, Value []byte }) ([]int64, error) {
	msgs := make([]*storage.Message, len(items))
	for i, item := range items {
		msgs[i] = storage.NewMessage(item.Key, item.Value)
	}
	return p.ProduceBatch(msgs)
}

// ProduceMessage appends a pre-built message to the partition.
// This is the primary path for priority-aware message production.
func (p *Partition) ProduceMessage(msg *storage.Message) (int64, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return 0, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	offset, err := p.log.Append(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to append message: %w", err)
	}

	// Update priority index and scheduler
	// The log.Append sets msg.Offset, so we can use the full message
	msg.Offset = offset

	// =========================================================================
	// CONTROL RECORD HANDLING
	// =========================================================================
	//
	// Control records (commit/abort markers) should NOT be added to the priority
	// index or scheduler because:
	//   1. Consumers should never see control records
	//   2. They would pollute the priority queues
	//   3. Their "Critical" priority would cause issues (returned before data messages)
	//
	// Control records are only read via:
	//   - Log.ReadFrom() during transaction recovery
	//   - Explicit offset reads for abort filtering
	//
	// =========================================================================
	if !msg.IsControlRecord() {
		p.priorityIndex.AddMessage(msg)
		p.priorityScheduler.Enqueue(msg)
	}

	return offset, nil
}

// =============================================================================
// CONSUMER OPERATIONS
// =============================================================================
//
// DEFAULT BEHAVIOR: Priority-aware consumption (highest priority first).
// Messages with the same priority are returned in FIFO order.
//
// WHY PRIORITY-FIRST AS DEFAULT?
//   - Most queue systems expect higher priority = delivered first
//   - SQS, RabbitMQ, traditional MQs all prioritize by default
//   - Use ConsumeByOffset() for explicit FIFO (log-like) consumption
//
// =============================================================================

// Consume reads messages in priority order (highest priority first).
// This is the default consumption mode - messages are returned with
// Priority 0 (Critical) before Priority 1 (High), etc.
// Within the same priority level, messages are returned in FIFO order.
//
// PARAMETERS:
//   - fromOffset: Starting offset (inclusive) - messages from this offset onwards
//   - maxMessages: Maximum messages to return
//
// For offset-sequential consumption (Kafka-like), use ConsumeByOffset().
func (p *Partition) Consume(fromOffset int64, maxMessages int) ([]*storage.Message, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	// ============================================================================
	// PRIORITY-AWARE CONSUMPTION (DEFAULT)
	// ============================================================================
	//
	// BEHAVIOR: Returns messages in strict priority order (highest priority first).
	//           Within the same priority, messages are returned in FIFO order.
	//
	// WHY DEFAULT: Most queue use cases want "important messages first". This aligns
	//              with SQS FIFO's high-throughput mode and traditional MQ semantics.
	//              For offset-ordered (Kafka-like) consumption, use ConsumeByOffset().
	//
	// PARAMETERS:
	//   - fromOffset: Starting offset (inclusive) - offset N returns message at N
	//   - maxMessages: Maximum messages to return (0 = no limit, return all available)
	//
	// OFFSET SEMANTICS: All methods in goqueue use INCLUSIVE offsets.
	//   fromOffset = 0 → returns message at offset 0 (if exists)
	//   fromOffset = 5 → returns messages starting from offset 5
	//
	// FLOW:
	//   Priority 0 (Critical):  [msg@5, msg@12] ──► Consumed first
	//   Priority 1 (High):      [msg@3, msg@8]  ──► Consumed second
	//   Priority 2 (Normal):    [msg@1, msg@10] ──► Consumed third
	//   ...
	//

	// Use strict priority ordering: highest priority first, FIFO within priority
	initialCapacity := maxMessages
	if initialCapacity == 0 {
		initialCapacity = 64 // Reasonable default for unlimited reads
	}
	messages := make([]*storage.Message, 0, initialCapacity)

	// Track current position for iteration (starts at fromOffset)
	currentOffset := fromOffset

	// Loop until we hit maxMessages limit (or no more messages if maxMessages == 0)
	for {
		// Check limit: maxMessages == 0 means no limit
		if maxMessages > 0 && len(messages) >= maxMessages {
			break
		}

		offset, _, found := p.priorityIndex.GetNextAcrossPriorities(currentOffset)
		if !found {
			break
		}

		msg, err := p.log.Read(offset)
		if err != nil {
			// Skip unreadable messages (may have been deleted by retention)
			currentOffset = offset + 1
			continue
		}

		// ============================================================================
		// CONTROL RECORD FILTERING
		// ============================================================================
		//
		// WHAT: Control records are internal transaction markers (commit/abort).
		//       They are NOT real messages - consumers should never see them.
		//
		// WHY SKIP: Control records are:
		//   1. Internal bookkeeping (consumers don't care about transaction status)
		//   2. Meta-information for transaction coordinator only
		//   3. Similar to Kafka: consumers skip control records when read_committed
		//
		// COMPARISON:
		//   - Kafka with read_committed: Filters out control records automatically
		//   - RabbitMQ: No control records (different transaction model)
		//   - SQS: No concept of control records (fire-and-forget)
		//   - goqueue: We filter here to keep consumer API clean
		//
		if msg.IsControlRecord() {
			// Skip control record, move to next message
			currentOffset = offset + 1
			continue
		}

		messages = append(messages, msg)
		currentOffset = offset + 1 // Move past this offset for next iteration
	}

	return messages, nil
}

// ConsumeByOffset reads messages sequentially by offset (FIFO, ignoring priority).
// This is Kafka-like behavior where you consume the log in offset order.
//
// Use this when:
//   - You need strict offset ordering (e.g., replication, replay)
//   - You want to consume ALL messages regardless of priority
//   - You're treating the partition as an append-only log
func (p *Partition) ConsumeByOffset(fromOffset int64, maxMessages int) ([]*storage.Message, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	messages, err := p.log.ReadFrom(fromOffset, maxMessages)
	if err != nil {
		return nil, fmt.Errorf("failed to read messages: %w", err)
	}

	// ============================================================================
	// CONTROL RECORD FILTERING
	// ============================================================================
	//
	// WHAT: Control records are internal transaction markers.
	//       Consumers reading with ConsumeByOffset should not see them.
	//
	// WHY: Control records are implementation details:
	//   1. Kafka's read_committed isolates control records from consumers
	//   2. They represent transaction boundaries, not actual data
	//   3. Consuming them would confuse application logic
	//
	// HOW WE FILTER:
	//   - ReadFrom returns all messages (including control records)
	//   - We scan through and skip IsControlRecord() == true
	//   - This preserves offset ordering while hiding internals
	//
	filtered := make([]*storage.Message, 0, len(messages))
	for _, msg := range messages {
		if !msg.IsControlRecord() {
			filtered = append(filtered, msg)
		}
	}

	return filtered, nil
}

// ReadMessage reads a single message at the given offset.
func (p *Partition) ReadMessage(offset int64) (*storage.Message, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	msg, err := p.log.Read(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message at offset %d: %w", offset, err)
	}

	return msg, nil
}

// =============================================================================
// PARTITION METADATA
// =============================================================================

// Topic returns the topic name.
func (p *Partition) Topic() string {
	return p.topic
}

// ID returns the partition number.
func (p *Partition) ID() int {
	return p.id
}

// Name returns the full partition identifier (topic-partition).
func (p *Partition) Name() string {
	return fmt.Sprintf("%s-%d", p.topic, p.id)
}

// EarliestOffset returns the first available offset.
func (p *Partition) EarliestOffset() int64 {
	return p.log.EarliestOffset()
}

// LatestOffset returns the last written offset.
// Returns -1 if partition is empty.
func (p *Partition) LatestOffset() int64 {
	return p.log.LatestOffset()
}

// NextOffset returns the next offset that will be assigned.
func (p *Partition) NextOffset() int64 {
	return p.log.NextOffset()
}

// AppendAtOffset appends a message at a specific offset.
// Used for follower replication where offsets must match the leader.
// Returns an error if the offset doesn't match expected position.
//
// FOLLOWER REPLICATION SEMANTICS:
//   - Followers must append at exact offsets to maintain consistency
//   - If offset < LEO: Skip (already have this message)
//   - If offset == LEO: Append normally
//   - If offset > LEO: Error (gap in log)
func (p *Partition) AppendAtOffset(msg *storage.Message, expectedOffset int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, fmt.Errorf("partition is closed")
	}

	return p.log.AppendAtOffset(msg, expectedOffset)
}

// MessageCount returns the approximate number of messages.
func (p *Partition) MessageCount() int64 {
	earliest := p.log.EarliestOffset()
	next := p.log.NextOffset()
	if next <= earliest {
		return 0
	}
	return next - earliest
}

// Size returns the total size of partition data in bytes.
func (p *Partition) Size() int64 {
	return p.log.Size()
}

// SegmentCount returns the number of log segments.
func (p *Partition) SegmentCount() int {
	return p.log.SegmentCount()
}

// Dir returns the partition directory path.
func (p *Partition) Dir() string {
	return p.dir
}

// Log returns the underlying storage log.
// This is used by internal components that need direct log access.
func (p *Partition) Log() *storage.Log {
	return p.log
}

// GetOffsetByTimestamp returns the first offset with timestamp >= given timestamp.
//
// TIME-BASED OFFSET LOOKUP:
//
// USE CASES:
//   - Consumer group offset reset by time
//   - "Replay from 2 hours ago"
//   - Disaster recovery: reprocess from specific point in time
//
// COMPARISON:
//   - Kafka: offsetsForTimes() on KafkaConsumer
//   - SQS: Not applicable (no offset concept)
//   - RabbitMQ: Not applicable (queue-based, not log-based)
//
// RETURNS:
//   - Offset of first message with timestamp >= given timestamp
//   - 0 if partition is empty or timestamp is before all messages
func (p *Partition) GetOffsetByTimestamp(timestamp int64) (int64, error) {
	return p.log.GetOffsetByTimestamp(timestamp)
}

// =============================================================================
// PARTITION MANAGEMENT
// =============================================================================

// Sync forces all data to be written to disk.
func (p *Partition) Sync() error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	return p.log.Sync()
}

// Close closes the partition, releasing resources.
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	if err := p.log.Close(); err != nil {
		return fmt.Errorf("failed to close log: %w", err)
	}

	p.closed = true
	return nil
}

// Delete closes and removes all partition data.
func (p *Partition) Delete() error {
	if err := p.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(p.dir); err != nil {
		return fmt.Errorf("failed to delete partition directory: %w", err)
	}
	return nil
}

// =============================================================================
// PRIORITY STATE REBUILD
// =============================================================================
//
// WHY: On partition load (restart/recovery), we need to rebuild in-memory
// priority structures from the persisted log. This ensures:
//   1. PriorityIndex tracks all message offsets by priority
//   2. PriorityScheduler is populated with pending messages for WFQ
//
// HOW IT WORKS:
//   1. Scan all messages in the log from earliest to latest
//   2. For each message, add its offset to the priority index
//   3. Enqueue each message into the WFQ scheduler
//
// NOTE: This scans the entire log, which can be slow for large partitions.
// Future optimization: persist priority index separately or use checkpoints.
//
// =============================================================================

// rebuildPriorityState scans the log and rebuilds priority index + scheduler.
// Called on partition load to restore in-memory state from persisted messages.
func (p *Partition) rebuildPriorityState() error {
	// Get offset range to scan
	earliest := p.log.EarliestOffset()
	latest := p.log.LatestOffset()

	// Empty partition - nothing to rebuild
	if latest < 0 || earliest > latest {
		return nil
	}

	// Scan all messages and rebuild state
	for offset := earliest; offset <= latest; offset++ {
		msg, err := p.log.Read(offset)
		if err != nil {
			// Log gap or corrupted message - skip but log warning
			// TODO: Add proper logging
			continue
		}

		// Add to priority index (tracks offsets by priority for range queries)
		p.priorityIndex.AddMessage(msg)

		// Enqueue to scheduler for WFQ consumption
		// The scheduler tracks pending messages and applies WFQ ordering
		p.priorityScheduler.Enqueue(msg)
	}

	return nil
}

// =============================================================================
// RETENTION & CLEANUP
// =============================================================================

// DeleteMessagesBefore removes messages older than the given offset.
func (p *Partition) DeleteMessagesBefore(offset int64) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	return p.log.DeleteSegmentsBefore(offset)
}

// =============================================================================
// PRIORITY-AWARE PRODUCE
// =============================================================================
//
// WHY: Standard Produce() uses default priority (0). ProduceWithPriority allows
// callers to specify a priority level for the message, enabling priority-based
// consumption via WFQ or strict priority ordering.
//
// =============================================================================

// ProduceWithPriority appends a message with the specified priority level.
// Priority must be 0-4 where 0 is highest (Critical) and 4 is lowest (BestEffort).
func (p *Partition) ProduceWithPriority(key, value []byte, priority storage.Priority) (int64, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return 0, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	// Create message with specified priority
	msg := storage.NewMessageWithPriority(key, value, priority)

	// Append to log
	offset, err := p.log.Append(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to append message: %w", err)
	}

	// Update priority index and scheduler
	msg.Offset = offset
	p.priorityIndex.AddMessage(msg)
	p.priorityScheduler.Enqueue(msg)

	return offset, nil
}

// =============================================================================
// PRIORITY-AWARE CONSUME
// =============================================================================
//
// WHY: Normal Consume() reads sequentially by offset. Priority-aware consumption
// allows reading messages in priority order using either strict priority or WFQ.
//
// DESIGN: The scheduler is owned by the partition and populated on produce.
// Consume just dequeues from the scheduler and reads from log.
//
// =============================================================================

// ConsumeByPriority reads messages using strict priority ordering.
// Messages are returned highest priority first (0 before 1 before 2, etc.)
// Messages at the same priority are returned in FIFO order.
//
// PARAMETERS:
//   - fromOffset: Starting offset (inclusive) - message at this offset can be returned
//   - maxMessages: Maximum messages to return
func (p *Partition) ConsumeByPriority(fromOffset int64, maxMessages int) ([]*storage.Message, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	// Query the priority index for unconsumed offsets in priority order
	// This uses strict priority - highest first, FIFO within same priority
	messages := make([]*storage.Message, 0, maxMessages)
	currentOffset := fromOffset

	for len(messages) < maxMessages {
		offset, _, found := p.priorityIndex.GetNextAcrossPriorities(currentOffset)
		if !found {
			break
		}

		msg, err := p.log.Read(offset)
		if err != nil {
			// Skip unreadable messages (may have been deleted by retention)
			currentOffset = offset + 1
			continue
		}

		// Skip control records (internal transaction markers)
		if msg.IsControlRecord() {
			currentOffset = offset + 1
			continue
		}

		messages = append(messages, msg)
		currentOffset = offset + 1 // Move past this offset for next iteration
	}

	return messages, nil
}

// ConsumeByPriorityWFQ reads messages using Weighted Fair Queuing.
// The scheduler is owned by the partition and maintains deficit counters
// for fair distribution across priorities based on configured weights.
//
// PARAMETERS:
//   - fromOffset: Starting offset (inclusive) - messages below this are filtered out
//   - maxMessages: Maximum messages to return
//
// NOTE: The scheduler parameter is ignored - we use the partition's own scheduler.
// This signature is kept for compatibility during refactoring.
func (p *Partition) ConsumeByPriorityWFQ(fromOffset int64, maxMessages int, _ *PriorityScheduler) ([]*storage.Message, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("partition %s-%d is closed", p.topic, p.id)
	}
	p.mu.RUnlock()

	// Dequeue from WFQ scheduler (applies deficit round robin)
	msgs := p.priorityScheduler.DequeueN(maxMessages)

	// Filter messages below fromOffset and read from log
	// (scheduler stores *storage.Message which may be stale, so we re-read)
	messages := make([]*storage.Message, 0, len(msgs))
	for _, msg := range msgs {
		if msg.Offset < fromOffset {
			continue
		}

		// Re-read from log to get fresh data
		freshMsg, err := p.log.Read(msg.Offset)
		if err != nil {
			// Skip unreadable messages
			continue
		}

		// Skip control records (internal transaction markers)
		if freshMsg.IsControlRecord() {
			continue
		}

		messages = append(messages, freshMsg)
	}

	return messages, nil
}

// =============================================================================
// PRIORITY INDEX OPERATIONS
// =============================================================================

// MarkConsumed marks a message as consumed in the priority index.
// This removes it from future priority queries.
func (p *Partition) MarkConsumed(offset int64) {
	p.priorityIndex.MarkConsumed(offset)
}

// PriorityMetrics returns per-priority metrics for this partition.
func (p *Partition) PriorityMetrics() []PriorityMetricsSnapshot {
	return p.priorityIndex.GetMetricsSnapshot()
}
