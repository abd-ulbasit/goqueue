---
name: summarizeSessionAndPlanNext
description: Update PROGRESS.md and CONCEPTS.md after each session, plan next steps
---

Review conversation history and update tracking files.

---

## Step 1: Analyze Session

**What was built?** Features, endpoints, packages created

**What patterns were applied?** Architecture decisions, Go idioms used

**What concepts were explained?** Deep-dives, comparisons, "aha moments"

**What questions did I ask?** Capture curiosity points for future reference

**What experiments ran?** Tests, benchmarks, race detection

---

## Step 2: Update CONCEPTS.md

Add new concepts following existing format in file:
- **What**: One-line definition
- **Key Insight**: The "why" or surprising behavior  
- **Pattern**: Idiomatic Go code with comments
- **Antipattern**: Common mistake with fix

Update Quick Reference table at bottom.

---

## Step 3: Update PROGRESS.md

- Add new features to "What We've Built" with brief code snippets
- Update "Patterns Used" list
- Mark completed tasks, add new tasks from roadmap

---

## Step 4: Apply Updates

Directly edit both files (don't show proposed changes first).

**After editing, confirm:**
- Current position: Month X, Week Y
- Week progress: X/Y tasks complete
- Next session focus

**File purposes:**
- `CONCEPTS.md`: Patterns, mechanics, examples (learning reference)
- `PROGRESS.md`: What's built, decisions made, roadmap status
