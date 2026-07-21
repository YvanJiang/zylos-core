# add

`cli.js add <prompt> [options]`

Creates a new scheduled task. Exactly one timing option is required.

## Timing Options

| Option | Type | Example |
|--------|------|---------|
| `--in "<duration>"` | One-time | `--in "30 minutes"` |
| `--at "<time>"` | One-time | `--at "tomorrow 9am"` |
| `--cron "<expression>"` | Recurring | `--cron "0 9 * * *"` |
| `--every "<interval>"` | Interval | `--every "2 hours"` |

### Duration Formats (`--in`, `--every`)

- Natural language: "30 minutes", "2 hours", "2.5 hours", "1 hour 30 minutes", "an hour"
- Short forms: "30m", "2h", "1d"
- Pure numbers: "7200" (seconds)

## Other Options

| Option | Description | Default |
|--------|-------------|---------|
| `--priority <1-3>` | 1=urgent, 2=high, 3=normal | 3 |
| `--name "<name>"` | Task display name | Truncated prompt |
| `--miss-threshold <seconds>` | Skip if overdue by more than this | 300 |
| `--bound-conversation-json "<json>"` | Complete durable Core conversation identity | synthetic schedule conversation |

## Examples

```bash
# One-time (delay)
cli.js add "Check emails" --in "30 minutes" --priority 2

# One-time (absolute time)
cli.js add "Send report" --at "tomorrow 9am"

# Recurring (cron)
cli.js add "Health check" --cron "0 9 * * *"

# Interval
cli.js add "Check updates" --every "2 hours"
cli.js add "Check updates" --every "90 minutes"

# Bound to an existing native thread with durable root and reply target
cli.js add "Weekly report" --cron "0 9 * * 1" \
  --bound-conversation-json '{"channel":"lark","chat_type":"thread","chat_id":"chat_xxx","native_thread_or_topic_id":"thread_yyy","message_id":"message_reply_target","root_message_id":"message_thread_root"}'

# Long miss threshold (backup: must execute even if delayed)
cli.js add "Backup data" --cron "0 2 * * *" --miss-threshold 86400
```

## Best Practices

### --miss-threshold

- **Default 300s**: health checks, heartbeats, real-time notifications
- **Long (explicit)**: backups (`86400`), reports (`14400`), batch processing
- Default (5 min) is suitable for most tasks.

### Bound Conversation

The JSON value is validated before the task is persisted. It must carry
`channel`, `chat_type`, `chat_id`, `native_thread_or_topic_id`, `message_id`,
and `root_message_id`. A thread requires three distinct durable anchors: its
native conversation ID, immutable root message, and exact reply-target message.
Outside a thread, both the native thread ID and root message are `null`.

```bash
--bound-conversation-json '{"channel":"telegram","chat_type":"dm","chat_id":"user_123","native_thread_or_topic_id":null,"message_id":"message_456","root_message_id":null}'
```

Primitive channel/endpoint pairs and local idle gates are retired and rejected.
Core queue and maintenance state are the only execution authority.
