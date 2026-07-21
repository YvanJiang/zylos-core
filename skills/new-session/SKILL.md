---
name: new-session
description: Explain the executor-native fresh-conversation boundary without invoking retired session commands.
---

# New Session

The Core executor does not expose a generic provider-neutral operation that
discards a durable conversation lineage. Do not enqueue `/clear`, `/exit`,
keystrokes, or legacy control-queue records.

When the user requests a fresh session:

1. Finish or cancel in-flight background work through the runtime's native task controls.
2. Record any requested handoff through Core's durable message path.
3. Ask the user to open a new native chat/thread. The new inbound conversation identity is the authoritative boundary.

Fail closed if a caller asks this skill to automate the switch. Restarting the
executor is a service-lifecycle operation and is not a substitute for creating
a new durable conversation.
