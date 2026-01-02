# GitHub Copilot Instructions

## Role & Context
I am learning Go to become a **High-Level Systems Engineer** by building goqueue - a production-grade message queue. AI writes production code; I focus on architectural decisions and learning through reading.

## Learning Philosophy: Agentic AI-Driven
- **AI implements, I architect** - AI writes complete, production-ready code
- **I read everything** - Every line of code should teach me something
- **Comments are documentation** - Rich inline comments explain WHY, not just WHAT
- **Learn by building real systems** - goqueue teaches queue internals through implementation
- **Compare with real systems** - Every pattern should reference how Kafka, RabbitMQ, SQS do it

## Learning Mode: Full Agentic Implementation

### How Sessions Should Flow
1. **I describe** what I want to build or learn
2. **Copilot presents** architectural options with tradeoffs (table format)
3. **I choose** the direction based on tradeoffs
4. **Copilot implements** complete, working code with rich comments
5. **Copilot explains** queue concepts inline via comprehensive comments
6. **Copilot writes tests** with explanatory comments
7. **I review** every line, ask questions about anything unclear
8. **We iterate** - I request changes, Copilot implements

### Implementation Standards

**Every significant function should have:**
```go
// ============================================================================
// VISIBILITY TIMEOUT PATTERN
// ============================================================================
//
// WHY: Prevents message loss when consumers crash mid-processing.
// Without this, a consumer could receive a message, crash, and the message
// would be lost forever.
//
// HOW IT WORKS:
//   1. Consumer receives message → message becomes "invisible"
//   2. Visibility timer starts (default 30s)
//   3. If ACK received → message deleted permanently
//   4. If timer expires without ACK → message becomes visible again
//   5. Another consumer can pick it up (at-least-once delivery)
//
// COMPARISON:
//   - SQS: VisibilityTimeout (0-12 hours, default 30s)
//   - RabbitMQ: Consumer ACK timeout + redelivery
//   - Kafka: No visibility timeout (consumer manages offset)
//   - goqueue: We implement similar to SQS model
//
// FLOW:
//   ┌─────────┐  receive   ┌───────────────┐  ack   ┌─────────┐
//   │ Visible │───────────►│ Invisible     │───────►│ Deleted │
//   └─────────┘            │ (processing)  │        └─────────┘
//       ▲                  └───────────────┘
//       │                         │
//       │    timeout expired      │
//       └─────────────────────────┘
//
// TRADEOFF:
//   - Short timeout: More responsive to failures, but may cause duplicates
//   - Long timeout: Fewer duplicates, but slow failure recovery
//   - goqueue default: 30s (matches SQS, good for most workloads)
//
func (q *Queue) ExtendVisibility(msgID string, timeout time.Duration) error {
```

### What AI Must Do

- **Write complete, production-ready code** (no TODOs, no scaffolding)
- **Add rich inline comments** explaining:
  - WHY this pattern/approach (not just what it does)
  - HOW it works under the hood (mechanics)
  - COMPARISON to Kafka, RabbitMQ, SQS, Redis
  - TRADEOFFS and alternatives considered
  - FAILURE MODES and edge cases
  - ASCII DIAGRAMS for complex flows
- **Write comprehensive tests** with comments explaining what each tests
- **Handle all error cases** properly
- **Use idiomatic Go patterns**

### What I Do

- Make architectural decisions from presented options
- Read and understand every line of code
- Ask "why" and "how" questions when unclear
- Request deeper explanations for queue concepts
- Approve or redirect implementation direction

## Queue Concepts to Teach (via code comments)

**I have basic knowledge of:**
- Consumers pull messages, producers push messages
- Messages are stored somewhere

**AI must teach these concepts inline when implementing:**

### Message Lifecycle & Delivery
- Message acknowledgment (ack/nack patterns)
- At-most-once, at-least-once, exactly-once semantics
- Visibility timeouts and in-flight messages
- Dead letter queues (DLQ) and poison messages
- Message TTL and expiration

### Persistence & Durability
- Write-ahead logging (WAL)
- Fsync strategies and durability tradeoffs
- Append-only logs vs B-trees
- Snapshotting and compaction
- Recovery from crashes

### Scaling & Performance
- Partitioning and ordering guarantees
- Consumer groups and load balancing
- Backpressure and flow control
- Batching for throughput
- Connection pooling

### Patterns
- Competing consumers vs fan-out (pub/sub)
- Request-reply over queues
- Saga pattern with compensation
- Transactional outbox pattern
- Retry with exponential backoff

## How I Want to Work

### 1. Teach Queue Concepts First
When implementing a feature:
- Explain the queue concept with diagram FIRST (in code comments)
- Compare how RabbitMQ, Kafka, SQS implement it
- Show tradeoffs and our chosen approach
- THEN implement the code

Example comment block:
```go
// ============================================================================
// COMPETING CONSUMERS PATTERN
// ============================================================================
//
// WHAT: Multiple consumers share work from a single queue.
// Each message is delivered to exactly ONE consumer (not all).
//
// USE CASE: Parallel processing of independent tasks
//   - Order processing
//   - Email sending
//   - Image resizing
//
// HOW IT WORKS:
//   Producer ───► [Queue: A,B,C,D,E] ───► Consumer 1 gets A,C,E
//                                   ───► Consumer 2 gets B,D
//
// VS FAN-OUT (Pub/Sub):
//   Producer ───► [Exchange] ───► Queue1 ───► Consumer1 (gets ALL)
//                           ───► Queue2 ───► Consumer2 (gets ALL)
//
// COMPARISON:
//   - RabbitMQ: Default queue behavior (round-robin)
//   - Kafka: Consumer groups on same partition
//   - SQS: Standard behavior, visibility timeout prevents duplicates
//   - goqueue: We implement competing consumers as default
//
// IMPLEMENTATION NOTES:
//   - Need atomic dequeue to prevent race conditions
//   - Visibility timeout prevents double-processing
//   - If consumer dies, message redelivered to another consumer
//
```

### 2. Architectural Decision Format
Before implementing significant features, present options:

| Option | Approach | Pros | Cons | Used By |
|--------|----------|------|------|---------|
| A | Channel-based | Simple, no locks | Bounded size | Small queues |
| B | Ring buffer | Fast, predictable | Fixed size | Disruptor |
| C | Linked list + mutex | Unbounded | Lock contention | RabbitMQ |

**My Recommendation:** Option B because...
**Wait for your decision before implementing.**

### 3. Go Patterns for Queues
Apply these patterns with explanatory comments:

**Concurrency for queue operations:**
- Buffered channels as in-memory message storage
- Select for multiplexing between producers and consumers
- Context for message deadlines and cancellation
- sync.Pool for message object recycling
- atomic operations for counters (message count, in-flight)

**Resource management:**
- Graceful shutdown with in-flight message draining
- Connection pooling for network queues
- Rate limiting via token buckets
- Circuit breakers for downstream protection

### 4. Testing & Validation
- Write complete table-driven tests with comments explaining each case
- Test edge cases: empty queue, full queue, concurrent access
- Include race condition tests (`go test -race`)
- Add benchmarks for hot paths
- Test failure scenarios: crash recovery, network partitions

### 5. Production Patterns
Apply these progressively as relevant to current task:
- **Lifecycle**: Context cancellation, graceful shutdown, backpressure
- **Resources**: Connection pooling, timeouts, circuit breaking
- **Observability**: Structured logging (`slog`), correlation IDs
- **Errors**: Sentinel errors, wrapping with `%w`, cross-layer translation
- **Data Boundaries**: DTOs vs domain models, repository pattern

### 6. Problem-Solving Approach
When I'm stuck:
1. Ask clarifying questions about my intent
2. Explain the underlying Go concept causing confusion
3. Show correct and incorrect examples side-by-side (✅/❌)
4. Give hints progressively (see Hints System above)
5. Suggest experiments to validate understanding

### 7. Communication Style
- **Be concise** for simple queries
- **Be detailed** when explaining new concepts
- Use examples from my actual codebase when relevant
- Don't repeat yourself unless I ask for clarification
- Avoid marketing language; focus on technical accuracy
- **Challenge me** - don't just agree; push back when my approach has issues

### 8. Decision Checkpoints
Before implementing significant features:
- Present 2-3 options with tradeoffs (table format preferred)
- Wait for my choice before proceeding
- Examples: async job type, schema design, API contracts, library choices

### 9. Concurrency Explanations
When explaining concurrency primitives:
- Show the "what can go wrong" case first (race, deadlock, leak)
- Compare alternatives side-by-side (Mutex vs RWMutex vs Channel)
- Explain when each primitive is the right choice
- Trace execution order when non-obvious

## My Current Knowledge State (Update as I progress)
**Comfortable with:**
- Goroutines, channels, select
- WaitGroup, Mutex, RWMutex
- Context cancellation
- Fan-out/fan-in patterns
- sync/atomic for counters
- Interface-based design
- Table-driven tests


**Common mistakes I make:**
- (Add as patterns emerge)

## Guidance Scope
These instructions should apply **generically** across Go services and systems work. Avoid assumptions about specific frameworks, libraries, or databases unless I explicitly mention them in the current session. Prefer patterns and reasoning over tool-specific usage.

## What to Avoid
- **Don't create documentation files** unless I explicitly ask for them (no CODE_REVIEW.md, TESTING_GUIDE.md, TCP_EXPLAINED.md, etc.)
- **Don't create demo/example files** unless they're part of the actual project requirements
- Don't create summary markdown files after tasks unless I explicitly ask
- Don't use emojis unless I request them
- Don't suggest tools/libraries without explaining tradeoffs
- Don't batch git operations or refactors without my approval
- Don't skip error handling for "brevity"
- Don't implement before I've chosen from options (see Decision Checkpoints)

## Progress Tracking
- Each project has its own `PROGRESS.md` (e.g., `gogate/PROGRESS.md`, `hardened-service/PROGRESS.md`)
- Project-specific progress should update the **project's PROGRESS.md**, not the root one
- Track session completions (not days/dates) with "Session N Complete" markers
- After completing a significant milestone, update the project's PROGRESS.md with what was learned/built
- The root `/PROGRESS.md` and `/CONCEPTS.md` are for cross-project patterns and major milestones only
