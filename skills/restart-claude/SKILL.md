---
name: restart-claude
description: Use when the user asks to restart Claude Code, or after changing settings/hooks/keybindings.
---

# Restart Claude Code Skill

Restart the long-lived Core executor service through the authoritative Zylos
service lifecycle. The executor owns the Claude provider prerequisite and
re-establishes it as part of its healthy service generation.

## When to Use

- After changing Claude Code settings, hooks, or keybindings
- When Claude needs to reload configuration
- To clear temporary state without upgrading
- User explicitly asks to restart

## Pre-Restart Checklist

Before restarting the executor, complete these steps **in order**:

### 1. Stop background tasks

Check for running background agents (Task tool). If any are active, stop them to avoid orphaned work.

### 2. Sync memory

Update memory files (state.md, sessions/current.md, etc.) to preserve important context that would otherwise be lost on restart.

### 3. Write a session handoff summary

Write a brief message covering:
- **What was being worked on** (active tasks, user requests in progress)
- **Current state** (what's done, what's pending, any blockers)
- **What the next session should pick up** (if anything)

### 4. Persist the handoff summary

Write the summary into the scoped durable memory files identified in step 2,
normally `~/zylos/memory/state.md` and `sessions/current.md`. Do not create a
hidden global conversation, invoke a channel send command, or depend on a
provider session startup hook.

Do not send the full handoff summary to an external user channel
(Telegram, Lark, Feishu, HXA, etc.). Handoff summaries are operational context
for the next agent session and may contain task state from outside the current
conversation. If the user is actively waiting, return only a short user-facing
notice in the current turn, without internal task inventory or cross-channel
context. Core owns any user-visible durable delivery target.

### 5. Restart through Core service control

```bash
zylos restart
zylos status
```

Treat a non-zero exit from either command as a failed restart. Do not invoke
PM2 directly: the CLI verifies the exact executor registration, performs the
durable shutdown handshake, and accepts only a new healthy Core service
identity.

## How It Works

1. **Durable shutdown**: Core fences new work and closes provider ownership.
2. **Exact supervisor control**: Zylos restarts only its owned executor registration.
3. **Generation proof**: Success requires a different healthy executor service identity.
