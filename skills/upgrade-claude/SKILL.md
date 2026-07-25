---
name: upgrade-claude
description: Upgrade the Claude prerequisite while the authoritative executor service is stopped.
---

# Upgrade Claude Code

Use only after the user explicitly authorizes the provider upgrade. The helper
stops the Core executor through its durable shutdown control, upgrades Claude,
then starts the executor and requires authoritative health.

```bash
node ~/zylos/.claude/skills/upgrade-claude/scripts/upgrade.js
```

Never enqueue provider slash commands, invoke a retired dispatcher, or manage
the supervisor directly. A non-zero helper exit means the upgrade or lifecycle
handoff failed and must be reported as such.
