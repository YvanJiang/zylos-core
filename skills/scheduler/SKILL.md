---
name: scheduler
description: Use when a user wants durable future, recurring, or interval work through the canonical Core scheduler queue.
---

# Task Scheduler

Scheduled occurrences are persisted locally, then idempotently admitted through
the canonical Core scheduler contract. Core queue and maintenance state govern
execution. The daemon reconciles completion only from the provider-neutral Core
observability snapshot; local age or host liveness never abandons an occurrence.

## CLI

`~/zylos/.claude/skills/scheduler/scripts/cli.js <command>`

| Command | Description | Reference |
|---------|-------------|-----------|
| `add <prompt> [options]` | Add a new task | `references/add.md` |
| `update <task-id> [options]` | Update an existing task | `references/update.md` |
| `list` / `next` / `running` / `history` | Query durable scheduling and Core projection state | `references/query.md` |
| `remove` / `pause` / `resume` | Manage future occurrences | `references/lifecycle.md` |

There is no caller-controlled completion command. Core terminal turn state is
the sole completion authority.

## Timezone

Timezone resolution is `~/zylos/.env` then `process.env.TZ`, then `UTC`. Times
are parsed and displayed in the configured timezone; the database stores UTC.
See `references/config.md` for details.
