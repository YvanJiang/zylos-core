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

## Bound Conversations

`--bound-conversation-json` must contain the complete durable channel identity:
`channel`, `chat_type`, `chat_id`, `native_thread_or_topic_id`, `message_id`,
and `root_message_id`. For a native thread, the last three values are all
non-null: `message_id` is the exact reply-target message and
`root_message_id` is the immutable thread root. Outside a thread, the native
thread ID and root message ID are null. The scheduler never queries a latest
message or falls back to the parent chat.

Rows migrated from retired idle/reply controls remain fenced. Reconfigure them
with `update --bound-conversation-json` or explicitly select the scheduler-owned
synthetic conversation with `update --use-synthetic-conversation`; only then
may `resume` make the next occurrence pending.

The task captures `region`, `tenant_id`, and `bot_id` as durable Core scope at
creation (or one-time migration). Retries and restarts reuse that stored scope;
later process-environment changes cannot fork an occurrence's idempotency key.

## Timezone

Timezone resolution is `~/zylos/.env` then `process.env.TZ`, then `UTC`. Times
are parsed and displayed in the configured timezone; the database stores UTC.
See `references/config.md` for details.
