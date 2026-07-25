# c4-receive.js — Compatibility Ingress

The command validates a compatibility-channel event and atomically persists a
Core turn plus its initial durable `send_text` outbox command.

```bash
node ~/zylos/.claude/skills/comm-bridge/scripts/c4-receive.js \
  --channel <channel> --endpoint <chat_id> \
  --message-id <stable_native_message_id> \
  --actor-id <authenticated_actor_id> \
  [--actor-type user|service] \
  [--chat-type dm|group|thread] \
  [--thread-id <native_thread_id> --root-message-id <native_root_message_id>] \
  [--occurred-at <RFC3339>] \
  [--attachments-json <validated_public_attachment_array>] \
  [--task-summary <short_user_visible_description>] \
  --content <text> [--json]
```

`--task-summary` is optional channel-supplied display text for queue status.
Core stores it with the inbound content and only lists it back to the same
source conversation. It never broadens queue visibility across chats.

Channel owners may supply canonical public attachment facts with
`--attachments-json`; their `attachment_id` and `content_ref` must be stable
channel-owned capabilities. Display names, local paths, and URLs are never
routing or download authority. A channel renderer must validate its capability
again and construct any user-visible download href at its own boundary.

Retries must preserve the stable message ID and original occurrence timestamp.
For a native thread, both its conversation/thread identity and durable root
message target are required. They are separate from any later reply-target
message ID persisted by channel delivery results.

The command does not select a runtime session, inspect a prompt or interface,
inject terminal input, append routing instructions, or deliver a reply.
