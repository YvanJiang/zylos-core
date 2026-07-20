# update

`cli.js update <task-id> [options]`

Updates an existing task. Supports partial ID matching.

## Options

All `add` timing options (`--in`, `--at`, `--cron`, `--every`) plus:

| Option | Description |
|--------|-------------|
| `--name "<name>"` | Update task name |
| `--prompt "<prompt>"` | Update task content |
| `--priority <1-3>` | Update priority |
| `--bound-conversation-json "<json>"` | Replace the complete durable Core conversation identity |
| `--miss-threshold <seconds>` | Update miss threshold |

When the schedule is changed, the timezone column is automatically synced to the current configured TZ.

## Examples

```bash
# Update priority
cli.js update task-abc --priority 1

# Change schedule
cli.js update task-abc --cron "0 10 * * *"

# Replace the complete bound conversation identity
cli.js update task-abc --bound-conversation-json '{"channel":"telegram","chat_type":"dm","chat_id":"user_123","native_thread_or_topic_id":null,"message_id":"message_456","root_message_id":null}'

# Switch from cron to interval
cli.js update task-abc --every "2 hours"
```

Retired local idle and primitive reply options are rejected. Existing database
rows that contain them are cleared and paused during schema initialization so
they cannot execute silently. An already-admitted Core turn remains `running`
until its authoritative terminal state is reconciled, then pauses before any
future occurrence. An operator must explicitly reconfigure and resume the task.
