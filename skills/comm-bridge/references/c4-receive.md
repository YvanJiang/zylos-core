# c4-receive.js — Compatibility Ingress

The command validates a compatibility-channel event and atomically persists a
Core turn plus its initial durable `send_text` outbox command.

```bash
node ~/zylos/.claude/skills/comm-bridge/scripts/c4-receive.js \
  --channel <channel> --endpoint <chat_id> \
  --message-id <stable_native_message_id> \
  --actor-id <authenticated_actor_id> \
  [--chat-type dm|group|thread] \
  [--thread-id <native_thread_id> --root-message-id <native_root_message_id>] \
  [--occurred-at <RFC3339>] --content <text> [--json]
```

Retries must preserve the stable message ID and original occurrence timestamp.
For a native thread, both its conversation/thread identity and durable root
message target are required. They are separate from any later reply-target
message ID persisted by channel delivery results.

The command does not select a runtime session, inspect a prompt or interface,
inject terminal input, append routing instructions, or deliver a reply.
