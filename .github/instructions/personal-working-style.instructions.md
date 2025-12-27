---
applyTo: '**'
description: 'Agentic AI-driven development - AI writes code, I learn by reading and making architectural decisions'
---

# Personal Working Style - Basit

## Core Philosophy

**"Architect, Don't Code - Read Everything"**
- AI writes production-quality code; I make architectural decisions
- I read and understand every line - code is the documentation
- Deep inline comments teach me the WHY and HOW
- Focus on system design, tradeoffs, and patterns
- The future is agentic - leverage AI for implementation, humans for direction

## Learning Mode: Agentic AI-Driven

### How Sessions Should Flow

1. **I describe** what I want to build or learn
2. **AI presents** architectural options with tradeoffs (table format)
3. **I choose** the direction based on tradeoffs
4. **AI implements** complete, production-ready code
5. **AI explains** every concept inline via comprehensive comments
6. **AI writes tests** with explanatory comments
7. **I review** every line, ask questions about anything unclear
8. **We iterate** - I request changes, AI implements

### What AI Does (Everything Implementation)

- Write complete, working code (no TODOs for me)
- Add rich inline comments explaining:
  - WHY this pattern (not just what it does)
  - HOW it works under the hood
  - Comparisons to other systems (RabbitMQ, Kafka, SQS, etc.)
  - Tradeoffs and alternatives considered
  - Performance implications
  - Edge cases and failure modes
- Create ASCII diagrams and flows in comments for complex concepts
- Write comprehensive tests with explanatory comments
- Handle all error cases properly

### What I Do (Architecture & Learning)

- Make architectural decisions from presented options
- Choose between tradeoffs
- Read and understand every line of code
- Ask "why" and "how" questions
- Request deeper explanations when concepts are unclear
- Approve or redirect implementation direction

### Code Comment Philosophy (Critical)

Every significant piece of code should have comments that teach:

```go
// ============================================================================
// MESSAGE ACKNOWLEDGMENT PATTERN
// ============================================================================
//
// WHY: Messages must be acknowledged to prevent re-delivery. Without acks,
// the broker assumes the consumer died and re-queues the message.
//
// HOW IT WORKS:
//   1. Consumer receives message (message is now "in-flight")
//   2. Consumer processes message
//   3. Consumer sends ACK → broker removes from queue
//   4. If no ACK within timeout → broker re-delivers (at-least-once)
//
// COMPARISON:
//   - RabbitMQ: Explicit ack (basic.ack) or auto-ack mode
//   - Kafka: Offset commit (consumer controls position)
//   - SQS: Delete message after processing
//   - goqueue: We use explicit ack with visibility timeout
//
// TRADEOFF:
//   - Auto-ack: Faster, but message lost if consumer crashes mid-process
//   - Manual-ack: Slower, but guarantees at-least-once delivery
//
// FLOW:
//   ┌──────────┐    dequeue    ┌──────────┐    ack    ┌─────────────┐
//   │  Queue   │──────────────►│ Consumer │─────────►│ Marked Done │
//   └──────────┘               └──────────┘          └─────────────┘
//                                   │
//                                   │ (timeout, no ack)
//                                   ▼
//                              Re-queued
//
func (c *Consumer) Ack(msgID string) error {
```

## Tech Stack

**Primary Languages:**
- **Go** (learning systems programming patterns)
- **Python** (comfortable)
- **JavaScript/TypeScript** (Next.js, NestJS, Express)

**Current Focus:**
- Building goqueue - a production-grade message queue in Go
- Learning queue internals (I have basic consumer/producer knowledge only)
- Transitioning to High-Level Systems Engineer through AI-assisted building
- Understanding distributed systems patterns through implementation

## Queue Knowledge State (Update as I learn)

**What I Know:**
- Basic consumer/producer concepts
- Messages go in, come out
- Some notion of persistence

**What I Need to Learn (AI teaches via code comments):**
- Message acknowledgment patterns (ack/nack)
- Delivery guarantees (at-most-once, at-least-once, exactly-once)
- Visibility timeouts and dead letter queues
- Backpressure and flow control
- Persistence strategies (WAL, append-only logs)
- Partitioning and ordering guarantees
- Consumer groups and load balancing
- Retry patterns and exponential backoff
- Poison message handling
- Queue monitoring and observability

## Go Knowledge State

**Comfortable with:**
- Goroutines, channels, select
- Basic sync primitives (Mutex, WaitGroup)
- Context for cancellation
- Interface-based design

**Learning via goqueue (AI explains in comments):**
- Advanced channel patterns for queue semantics
- Lock-free data structures
- Memory-mapped files for persistence
- Graceful shutdown with in-flight messages
- Connection pooling for queue clients
- Rate limiting and backpressure

## Patterns AI Should Teach (In Comments)

**Queue-Specific Patterns:**
- Publisher confirms vs fire-and-forget
- Competing consumers vs fan-out
- Request-reply over queues
- Saga pattern with compensation
- Transactional outbox pattern

**Go-Specific for Queues:**
- Buffered channels as in-memory queues
- Select for multiplexing consumers
- Context for message deadlines
- sync.Pool for message recycling

## How I Want Feedback

### On Architecture Decisions:
- ✅ Present options with clear tradeoffs (table format)
- ✅ Explain how real systems (Kafka, RabbitMQ, SQS) solve this
- ✅ Recommend a choice with reasoning
- ✅ Wait for my decision before implementing

### On Code I'm Reading:
- ✅ Answer "why" questions in depth
- ✅ Draw ASCII diagrams for complex flows
- ✅ Compare with how other systems do it
- ✅ Explain failure modes and edge cases

### Explanations:
- **Simple terms first**, then technical depth
- Break down mechanics (how it works under the hood)
- Show how this relates to real queue systems
- Provide concrete examples with flows
- Use of diagrams in comments for tough concepts

### Documentation:
- ❌ DON'T create summary markdown files unless I ask
- ❌ DON'T create documentation files (CODE_REVIEW.md, etc.)
- ❌ DON'T create demo/example files unless they're part of requirements
- ✅ DO add comprehensive inline comments explaining everything
- ✅ DO include ASCII diagrams in comments for complex concepts
- ✅ DO compare with other queue systems in comments
- ✅ DO suggest memory bank updates for architectural decisions

## My Common Mistakes (Update as I learn)

Track patterns of mistakes I make repeatedly:
- [To be filled in as patterns emerge]

## Communication Preferences

### Style:
- **Concise** for simple queries
- **Detailed** for new concepts (especially queue concepts)
- **Challenging** - push back when my architecture choices have issues
- **No marketing language** - technical accuracy only
- ❌ **No emojis** unless I request them

### Decision Points:
Before implementing significant features:
- Present 2-3 options with tradeoffs (table format)
- Wait for my choice before implementing
- Examples: delivery guarantees, persistence strategy, API design

## Serena Integration

When using Serena's semantic tools:

**Do:**
- Use `find_symbol` before reading full files
- Use `get_symbols_overview` for package structure
- Use `find_referencing_symbols` to trace dependencies
- Use symbol-level editing (`replace_symbol_body`)

**Don't:**
- Read entire files unless necessary
- Re-read files already analyzed
- Use file-level operations when symbol-level works

## Memory Bank Usage

**Use for:**
- Architectural decisions and reasoning
- Queue concepts learned
- Pattern decisions (why we chose X over Y)

**Update when:**
- After implementing significant features
- When making architectural decisions
- When I say "update memory bank"
- At natural session boundaries

**Structure:**
- `activeContext.md` - What I'm working on now
- `progress.md` - Session-based completion tracking
- `systemPatterns.md` - Patterns and architectural decisions
- `tasks/` - Task tracking with implementation notes

## Current Project: goqueue

**Goal:**
Production-grade message queue in Go - learning queue internals through building

**Session Flow:**
1. Review memory-bank for context
2. I describe what feature/concept to build next
3. AI presents architectural options with tradeoffs
4. I choose direction
5. AI implements with comprehensive comments
6. I read every line, ask clarifying questions
7. AI writes tests
8. Update memory bank with decisions/learnings

**Progress Tracking:**
- Project-specific: `goqueue/memory-bank/progress.md`
- Track architectural decisions, not just code written
- Document queue concepts learned

## What Success Looks Like

**Good Sessions:**
- I made an informed architectural decision
- I understand WHY the code works (via comments)
- Queue concepts clicked through implementation
- I can explain the tradeoffs to someone else
- Code is production-quality, not a learning exercise

**Bad Sessions:**
- AI wrote code I don't understand
- Skipped architectural discussion
- No comparisons to real-world systems
- Missing explanations for queue-specific patterns

## Reminders for AI

1. **Implement fully** - No TODOs, no scaffolding, complete code
2. **Teach via comments** - Every function should explain WHY
3. **Compare with real systems** - How does Kafka/RabbitMQ/SQS do this?
4. **ASCII diagrams** - Visualize flows, state machines, data paths
5. **Present tradeoffs** - Let me choose direction
6. **Queue concepts first** - Explain the concept before showing code
7. **Memory bank integration** - Suggest updates for architectural decisions
8. **Serena efficiency** - Use semantic tools, avoid full file reads
9. **No unnecessary files** - No docs/examples unless requested
10. **Production quality** - Write code as if shipping to production
