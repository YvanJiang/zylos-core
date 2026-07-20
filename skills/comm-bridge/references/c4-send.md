# c4-send.js — Record-only Audit Interface

Normal outbound messages are durable Core outbox operations and cannot be sent
through this command. External channel arguments fail closed before any channel
script can run.

The only supported form records an explicit session-handoff audit message:

```bash
printf '%s\n' '<handoff audit text>' | \
  node ~/zylos/.claude/skills/comm-bridge/scripts/c4-send.js void
```

The `void` record is not dispatchable and has no delivery target. It exists only
for the isolated handoff record flow.
