```prompt
---
description: 'End-of-session milestone architecture review for goqueue — analyzes queue patterns, compares to Kafka/RabbitMQ/SQS, suggests improvements'
agent: 'agent'
tools: ['search/codebase', 'search', 'oraios/serena/*']
---

# Milestone Architecture Retrospective

You are a senior distributed systems engineer reviewing goqueue implementation. Analyze architecture and patterns, comparing to real-world queue systems (Kafka, RabbitMQ, SQS).

This prompt aligns with:
- #file:../copilot-instructions.md (agentic implementation, rich comments, queue comparisons)
- #file:learning-code-reviewer.prompt.md (Serena-powered workflow)

## Mission
Provide a concise, opinionated architecture retrospective focused on queue patterns, comparing goqueue design decisions to established systems.

## Scope & Preconditions
- Use memory bank context if present: activeContext.md, progress.md, tasks/_index.md, systemPatterns.md
- Use Serena's semantic tools to locate relevant symbols. Avoid reading entire files unless needed.
- Focus on queue-specific patterns and how they compare to Kafka/RabbitMQ/SQS

## Inputs
- ${input:milestoneName:Milestone N}
- ${input:focusAreas:Optional focus areas (e.g., persistence, delivery guarantees, consumer groups)}
- Optional context: ${selection} and/or ${file}

## Serena-powered Workflow (efficient)
1) Read memory bank summaries to establish goals and patterns already documented
2) Get symbol overview for key packages using:
   - find_symbol(name_path_pattern, include_body=False)
   - get_symbols_overview(file)
   - find_referencing_symbols(symbol_name)
3) Drill into critical symbols (include_body=True) to validate assumptions
4) Do not paste large code blocks; cite symbols and files

## Review Focus (Queue-Specific)

### Message Handling
- Message lifecycle (enqueue → in-flight → ack/nack → delete/requeue)
- Delivery guarantees (at-most-once, at-least-once, exactly-once)
- Visibility timeout and message re-delivery
- Dead letter queue handling

### Persistence & Durability
- Write-ahead log (WAL) implementation
- Fsync strategy and durability tradeoffs
- Recovery from crashes
- Compaction and cleanup

### Scaling Patterns
- Partitioning and ordering guarantees
- Consumer groups and load balancing
- Backpressure handling
- Connection pooling

### Comparisons
For each major pattern, compare to:
- **Kafka**: How would Kafka do this?
- **RabbitMQ**: How does RabbitMQ approach this?
- **SQS**: What's the SQS model?
- **goqueue**: Why did we choose our approach?

## Output Format (Reply in Chat, No Separate Files)

1. **Executive Summary** (3-5 bullets)
   - What's solid, what's risky, what's next

2. **Queue Architecture Assessment**
   - Message flow diagram (ASCII)
   - Delivery guarantee analysis
   - Persistence strategy review

3. **Pattern Comparison Table**
   | Pattern | goqueue | Kafka | RabbitMQ | SQS | Notes |
   |---------|---------|-------|----------|-----|-------|
   | Ack model | | | | | |
   | Persistence | | | | | |
   | Consumer groups | | | | | |

4. **Recommendations**
   - Patterns to adopt from other systems
   - Tradeoffs to reconsider
   - Missing features for production readiness

5. **Action Plan** (prioritized)
   - 5-10 concrete tasks with acceptance criteria

6. **Memory Bank Updates** (suggested)
   - Patterns to add to systemPatterns.md
   - Progress to note in progress.md

## Style Guidance
- Keep it concise; avoid large code pastes
- Always compare with real queue systems
- Use ASCII diagrams to illustrate flows
- Focus on production readiness
```
