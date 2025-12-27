---
name: summarizeSessionAndPlanNext
description: Update memory-bank after each session - track architectural decisions and queue concepts learned
---

Review conversation history and update memory-bank files.

---

## Step 1: Analyze Session

**What was implemented?** Complete features, not just scaffolding

**What architectural decisions were made?** Document the decision AND the tradeoffs considered

**What queue concepts were taught?** Concepts explained in code comments or discussion

**What comparisons to other systems?** Kafka, RabbitMQ, SQS references

**What patterns emerged?** Patterns that should be documented for future reference

---

## Step 2: Update systemPatterns.md

Add new patterns following this format:
```markdown
## Pattern Name

**WHY**: Why this pattern exists
**HOW**: How it works (with ASCII diagram if complex)
**COMPARISON**: How Kafka/RabbitMQ/SQS do it
**TRADEOFF**: What we chose and alternatives considered
```

---

## Step 3: Update progress.md

- Add what was implemented this session
- Track queue concepts learned
- Note architectural decisions made
- Update completion status

---

## Step 4: Update activeContext.md

- Current implementation state
- Next feature to implement
- Open questions or decisions pending

---

## Step 5: Apply Updates

Directly edit memory-bank files (don't show proposed changes first).

**After editing, confirm:**
- Features implemented this session
- Queue concepts learned
- Next implementation focus

**File purposes:**
- `systemPatterns.md`: Architectural patterns, queue patterns, comparisons
- `progress.md`: What's built, decisions made, concepts learned
- `activeContext.md`: Current focus and next steps
- `productContext.md`: High-level project goals (rarely updated)
