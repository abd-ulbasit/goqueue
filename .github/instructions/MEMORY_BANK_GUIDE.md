# Memory Bank Workflow Guide

## What is Memory Bank?

Memory Bank is a **structured documentation system** that helps AI agents (and you) maintain context across sessions. Think of it as your project's "external brain" - when the AI's memory resets, it reads these files to understand where things are and what needs to happen next.

## Why It Matters for You

**Your Problem**: You're learning Go, building multiple projects, and each time you start a session:
- Copilot doesn't remember what you learned last time
- You have to re-explain project context
- Progress tracking is scattered

**Memory Bank Solution**: Structured files that answer:
- What is this project? (projectbrief.md)
- What am I working on now? (activeContext.md)
- What have I built? (progress.md)
- What patterns did I discover? (systemPatterns.md)

## Memory Bank Structure

```
memory-bank/
├── projectbrief.md       # Project purpose & requirements
├── activeContext.md      # Current work focus (updated frequently)
├── progress.md           # What works, what's left
├── systemPatterns.md     # Architecture & design decisions
├── techContext.md        # Tech stack, dependencies, setup
├── productContext.md     # Why this exists, user goals
└── tasks/
    ├── _index.md         # All tasks with statuses
    ├── TASK001-setup.md
    └── TASK002-auth.md
```

## How to Use It: Practical Workflows

### Workflow 1: Starting a Session

**Instead of:**
```
You: "Hey Copilot, remember I was working on the fan-in pattern?"
Copilot: "I don't have context from previous sessions..."
```

**With Memory Bank:**
```
You: "Read my memory bank and continue where I left off"
Copilot: [Reads activeContext.md] "I see you completed fan-out/fan-in 
and were debugging the worker pool. Shall we continue with backpressure?"
```

### Workflow 2: Tracking Progress (Learning Mode)

**Your current setup:**
```
gogate/PROGRESS.md
hardened-service/PROGRESS.md
fanout-pipeline/ [no progress tracking]
```

**With Memory Bank:**
```
gogate/memory-bank/
├── projectbrief.md       # "API Gateway in Go - Learn proxying, middleware"
├── activeContext.md      # "Currently: Circuit breaker failing under load"
├── progress.md           # Session 1: ✅ Routing, Session 2: ✅ Auth, Session 3: 🔄 Circuit breaker
└── tasks/
    ├── _index.md         # TASK001: ✅ Completed, TASK002: 🔄 In Progress
    └── TASK002-circuit-breaker.md
```

**Commands you'd use:**
```
"update memory bank" → AI reviews all files, updates activeContext & progress
"add task: Implement rate limiting" → Creates TASK003-rate-limiting.md
"show tasks active" → Lists in-progress tasks
```

### Workflow 3: Learning Pattern Discovery

**Scenario**: You discover Go's `sync.Pool` is great for reducing allocations.

**Without Memory Bank**: You forget this insight next session.

**With Memory Bank**:
```
systemPatterns.md gets updated:
## Performance Patterns

### Object Pooling with sync.Pool
We use sync.Pool for request/response objects in the proxy layer.
Reduces GC pressure by ~40% under load.

Example: internal/proxy/pool.go

Learned: December 21, 2025 - Session 8
```

Next session, Copilot sees this and suggests: "Should we use sync.Pool for the worker pool too?"

## Commands & Triggers

| Command | What It Does |
|---------|--------------|
| `"read memory bank"` | AI reads all memory bank files |
| `"update memory bank"` | AI reviews and updates all files |
| `"add task: <description>"` | Creates new task file |
| `"update task TASK002"` | Updates specific task progress |
| `"show tasks active"` | Lists in-progress tasks |
| Auto-trigger: After significant changes | AI suggests memory bank update |

## Setting Up Memory Bank for Your Projects

### For Learning Projects (like learning-go-v1)

Create `memory-bank/projectbrief.md`:
```markdown
# Learning Go - Systems Engineering Path

## Purpose
Progress from Junior/Mid-level to High-Level Systems Engineer by building
production-grade distributed systems in Go.

## Learning Goals
1. Master Go concurrency (goroutines, channels, sync primitives)
2. Build production-quality services (graceful shutdown, observability)
3. Understand distributed systems patterns
4. Learn through implementation and debugging

## Projects
- fanout-pipeline: Concurrency patterns
- gogate: API Gateway
- hardened-service: Production patterns
- lru-cache: Data structures
- goqueue: Job processing
```

Create `memory-bank/activeContext.md`:
```markdown
# Active Context

## Current Focus
Building LRU cache with eviction policies and thread-safety.

## Recent Changes
- Implemented basic Get/Put operations
- Added mutex for thread-safety
- Working on: Eviction policy

## Next Steps
1. Implement LRU eviction with doubly-linked list
2. Add benchmarks comparing with sync.Map
3. Test under concurrent load

## Blockers
None currently

## Questions
- Should eviction happen inline or in background goroutine?
- How to balance lock granularity vs complexity?
```

### For Production Projects (like gogate)

Create `memory-bank/systemPatterns.md`:
```markdown
# System Patterns - GoGate

## Architecture
Layered architecture: Router → Middleware Chain → Proxy → Backend

## Key Decisions

### Middleware as Chain of Responsibility
Each middleware wraps the next, forming a pipeline.
Allows composition and easy testing.

### Circuit Breaker per Backend
Each backend gets its own circuit breaker state.
Prevents cascading failures.

### Connection Pooling
HTTP client with custom transport: 100 max idle conns, 10 per host.
Reduces latency by reusing connections.
```

## Integrating with Your Current Setup

### Option 1: Migrate Existing PROGRESS.md

```bash
# For each project:
cd gogate
mkdir memory-bank
mv PROGRESS.md memory-bank/progress.md

# Create other files:
# memory-bank/projectbrief.md
# memory-bank/activeContext.md
# memory-bank/systemPatterns.md
```

### Option 2: Start Fresh for New Work

Next project you start, initialize with:
```bash
mkdir memory-bank memory-bank/tasks
touch memory-bank/{projectbrief,activeContext,progress,systemPatterns,techContext}.md
touch memory-bank/tasks/_index.md
```

## Memory Bank + Serena Workflow

With Serena's semantic tools + Memory Bank:

1. **Session Start**: "Read memory bank, find symbols related to circuit breaker"
   - Serena reads memory bank → knows context
   - Uses `find_symbol` to locate relevant code
   - Shows you symbol overview without reading full file

2. **Implementation**: "Update circuit breaker with exponential backoff"
   - Serena uses `find_referencing_symbols` → finds all usages
   - Uses `replace_symbol_body` → precise edits
   - Updates `activeContext.md` → documents change

3. **Session End**: "Update memory bank"
   - AI reviews changes made
   - Updates `progress.md` with what was completed
   - Updates `activeContext.md` with next steps
   - Updates `systemPatterns.md` if new patterns emerged

## Should You Use Memory Bank?

### ✅ Use it if:
- Working on projects longer than a few sessions
- Want AI to remember context across sessions
- Learning and want to track insights
- Building complex systems with many patterns

### ❌ Skip it if:
- Quick scripts or one-off projects
- Prefer to explain context each session
- Already have your own system that works

## Quick Start (5 minutes)

Try it with your `gogate` project:

1. Create structure:
```bash
cd gogate
mkdir -p memory-bank/tasks
```

2. Create minimal files:
```bash
# memory-bank/projectbrief.md
echo "# GoGate - API Gateway in Go" > memory-bank/projectbrief.md

# memory-bank/activeContext.md  
echo "# Currently working on: Circuit breaker optimization" > memory-bank/activeContext.md
```

3. Next session, start with:
```
"Read my memory bank and continue"
```

4. When done:
```
"Update memory bank with what we accomplished"
```

## My Recommendation for You

Based on your learning style (scaffolding, explain-back, progressive hints):

**Use Memory Bank for:**
- `gogate`, `hardened-service`, `goqueue` (ongoing production projects)
- Track patterns you discover (sync.Pool, graceful shutdown, etc.)

**Don't need Memory Bank for:**
- Small exercises like `lru-cache` (unless it grows big)
- One-off experiments

**Create a template** for new projects that includes:
- `memory-bank/projectbrief.md` with learning goals
- `copilot-instructions.md` with your learning philosophy
- `.github/instructions/` with relevant instruction files
