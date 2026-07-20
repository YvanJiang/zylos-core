---
name: comm-bridge
description: >-
  Compatibility ingress and historical conversation/checkpoint queries. Normal
  outbound replies are durable Core outbox commands delivered by channel owners.
---

# Communication Bridge (C4)

```
channel ingress -> Core queue -> executor -> Core outbox -> channel delivery owner
```

`c4-receive.js` accepts authenticated compatibility-channel text into the same
canonical Core ingress used by channel adapters. It requires a stable native
message ID and actor ID. Core persists the turn and the initial
`zylos.delivery-command@1.1` `send_text` command atomically.

Normal outbound replies must never invoke `c4-send.js`. Core owns their durable
outbox record, retry state, delivery target mapping, and reply lineage; a channel
owner supplies rendering and delivery. `c4-send.js` is restricted to the
record-only `void` channel used by explicit session-handoff audit flows.

| Script | Purpose | Reference |
|--------|---------|-----------|
| `c4-receive.js` | Compatibility ingress into Core | `references/c4-receive.md` |
| `c4-send.js` | Record-only `void` audit message | `references/c4-send.md` |
| `c4-fetch.js` | Query historical conversation records | `references/c4-fetch.md` |
| `c4-db.js` | Historical record/checkpoint database CLI | `references/c4-db.md` |
| `c4-checkpoint.js` | Create/query sync checkpoints | `references/c4-checkpoint.md` |

Use `zylos status` or `zylos doctor --check --json` for Core health. Provider
session and user-interface state are not health or routing authority.
