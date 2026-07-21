---
name: check-context
description: Accurately check current context window and token usage. Use when the user asks about context usage, token consumption, or when monitoring context levels.
user-invocable: false
---

# Check Context Skill

Use only provider-neutral Core observability. Provider session files, terminal
state, and retired host-side artifacts are not health or token authority.

## When to Use

- When the user asks about context usage
- When the user wants to know token consumption

## How to Use

Read the validated Core health/observability response:

```bash
zylos doctor --check --json
```

Report token or context figures only when the response contains explicit,
provider-neutral token facts for the requested conversation or turn. If those
facts are absent, report that context usage is unavailable. Never guess from a
runtime type, choose a most-recent provider session, or treat local files as a
substitute source of truth.
