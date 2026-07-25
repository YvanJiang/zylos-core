---
name: zylos-memory
description: >-
  Core memory system. Maintains persistent memory across sessions via tiered
  markdown files following the Inside Out model. Handles Memory Sync (processing
  conversations into structured memory), session rotation, consolidation, and
  context-aware state saving. Runs inside the current Core-owned detached task;
  it must not create a second provider-native background agent.
disable-model-invocation: true
user-invocable: false
---

# Memory System

Maintains persistent memory across sessions via tiered markdown files. The
current provider execution is already a durable Core-owned background task.
Run Memory Sync in that task and do not create a provider-native child merely
to detach it again.

## Architecture

```text
~/zylos/memory/
├── identity.md              # Bot soul + digital assets (always loaded)
├── state.md                 # Active working state (always loaded)
├── references.md            # Pointers to config files (always loaded)
├── users/
│   └── <id>/profile.md      # Per-user preferences
├── reference/
│   ├── decisions.md         # Key decisions with rationale
│   ├── projects.md          # Active/planned projects
│   ├── preferences.md       # Shared team preferences
│   └── ideas.md             # Uncommitted plans and ideas
├── sessions/
│   ├── current.md           # Today's session log
│   └── YYYY-MM-DD.md        # Past session logs
└── archive/                 # Cold storage
```

## Memory Sync

### Priority

Memory Sync is the highest-priority internal maintenance task.
When triggered, run it before handling queued user messages.

### Trigger Paths

Memory sync is explicit and scoped to the current task/conversation. It may be
requested by the user or by a canonical Core lifecycle interaction. Do not infer
a trigger from provider files, terminal state, or a global conversation backlog.

### Execution Ownership

Keep sync work attached to the current Core background task. A long-running
command may use an async exec session only when its result is collected before
the task ends. Bare `nohup ... &`, PM2 sidecars, an extra `codex exec`, and
provider-native subagents are not durable task ownership and must not be used
for sync.

### Sync Flow

1. Rotate session log if needed:
   `node ~/zylos/.claude/skills/zylos-memory/scripts/rotate-session.js`
2. Read the current authorized conversation/task context and memory files
   (`identity.md`, `state.md`, `references.md`, user profiles, `reference/*`,
   `sessions/current.md`). Never scan other conversations or choose a latest
   provider session.
3. Extract and classify updates from the current scoped context into the correct files.
4. Write memory updates (always,
   update `state.md` and `sessions/current.md` with current context).
5. Audit `references.md` against its content rules
   (`references/references-file-format.md`): relocate rule-violating
   entries to their routed destination (`reference/decisions.md`,
   `archive/`, or a pointer to the config file) instead of leaving or
   appending them. If the file exceeds the 8KB warn threshold
   (`memory-status.js` reports WARN), trim until it is back under.
6. Audit `state.md` against its content rules
   (`references/state-format.md`): relocate rule-violating content to its
   routed destination (`reference/projects.md`, `reference/decisions.md`,
   `archive/`, or a pointer to the on-demand file that already holds it)
   instead of leaving or appending it. If the file exceeds the 10KB warn
   threshold (`memory-status.js` reports WARN), trim until it is back
   under.
7. Confirm completion in the current task only.

## Classification Rules

- `reference/decisions.md`: committed choices that close alternatives.
- `reference/projects.md`: scoped work efforts with status.
- `reference/preferences.md`: standing team-wide preferences.
- `reference/ideas.md`: uncommitted proposals.
- `users/<id>/profile.md`: user-specific preferences.
- `state.md`: active focus, pending items, and blockers only, per the
  content rules in `references/state-format.md`; completed-task narrative,
  decisions, and run history are routed out, never accumulated.
- `references.md`: pointers and stable identifiers only, per the content
  rules in `references/references-file-format.md`; never duplicate config
  values, never accumulate narrative history.

## File Formats and Examples

Each memory file type has a format definition in `references/` and a
worked example in `examples/`:

| File | Format | Example |
|------|--------|---------|
| `identity.md` | `references/identity-format.md` | `examples/identity.md` |
| `state.md` | `references/state-format.md` | `examples/state.md` |
| `references.md` | `references/references-file-format.md` | `examples/references.md` |
| `users/<id>/profile.md` | `references/user-profile-format.md` | `examples/user-profile.md` |
| `reference/decisions.md` | `references/decisions-format.md` | `examples/decisions.md` |
| `reference/projects.md` | `references/projects-format.md` | `examples/projects.md` |
| `reference/preferences.md` | `references/preferences-format.md` | `examples/preferences.md` |
| `reference/ideas.md` | `references/ideas-format.md` | `examples/ideas.md` |
| `sessions/current.md` | `references/session-log-format.md` | `examples/session-log.md` |

## Supporting Scripts

- `session-start-inject.js`: prints core memory context blocks for hooks.
- `rotate-session.js`: rotates `sessions/current.md` at day boundary.
- `daily-commit.js`: local git snapshot for `memory/` if changed.
- `consolidate.js`: JSON consolidation report (sizes, age, budget checks).
  Use for deliberate memory maintenance, or for scheduler-triggered
  consolidation when such a task is configured. Review the report and apply
  the Consolidation Review rules below.
- `memory-status.js`: quick health summary.
  Use when you need a fast manual check of core file sizes and budget status.
  If it reports `OVER`, run `consolidate.js` and perform the needed cleanup.

## Consolidation Review

The weekly consolidation task runs `consolidate.js` and outputs a JSON report.
Review the report and apply these rules:

### Core File Budgets
- Files over 100% budget: summarize and trim older entries.
  Move historical content to `reference/` or `archive/`.
- `identity.md`, `state.md`, and `references.md` must stay under 16KB.
- Apply file-specific cleanup:
  - `identity.md`: keep only stable identity traits, principles, durable
    collaboration style, and digital asset references. Move operational state
    and one-off lessons elsewhere.
  - `state.md`: keep active focus, pending tasks, and recent completions.
    Move completed or historical detail to `sessions/current.md` or
    `reference/`. Apply the content rules in `references/state-format.md`;
    the sync-time audit (Sync Flow step 7) should keep it under the 10KB
    warn threshold.
  - `references.md`: keep pointers and lookup facts only. Move prose,
    project history, and detailed decisions to `reference/`. Apply the
    content rules in `references/references-file-format.md`; the sync-time
    audit (Sync Flow step 6) should keep it under the 8KB warn threshold.

### Session Logs
- Logs in `archiveCandidatesOlderThan30Days`: move from `sessions/` to `archive/`.

### Reference Files (`reference/*.md`)
These files have no size cap. Maintenance is at the entry level.
Freshness is reported by file mtime (Phase 1 limitation):
- **active** (< 7 days): no action.
- **aging** (7–30 days): no action.
- **fading** (30–90 days): open the file. Review entries by their dates
  and status fields. Update or confirm still-relevant entries; move
  obsolete entries (superseded/completed/abandoned/dropped) to `archive/`.
- **stale** (> 90 days): same as fading, but prioritize review.
  Entries that are clearly still critical may remain.

**Immunity:** Entries with importance 1-2 (defined in entry metadata) are
immune to automatic fading suggestions. They may still be reviewed but
should not be archived based on age alone.

### User Profiles
- Profiles over ~1KB: summarize older notes.

### General Rules
1. Never delete — always move to `archive/`. Content is recoverable
   from `archive/` or git history.
2. Log consolidation actions in `sessions/current.md`.

## Best Practices

1. Keep `state.md` lean (tight context budget).
2. Prefer updates over duplication.
3. Use explicit dates/timestamps for entries.
4. Archive instead of deleting historical data.
5. Route user data to user profiles.
6. Keep configuration values in config files; use `references.md` as an index.
