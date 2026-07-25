# Configuration

## Components

| File | Purpose |
|------|---------|
| `daemon.js` | Main scheduler daemon |
| `daemon-tasks.js` | Daemon task processing logic |
| `cli.js` | CLI for task management |
| `runtime.js` | Runtime monitor and IPC |
| `database.js` | SQLite persistence layer |
| `cron-utils.js` | Cron expression utilities |
| `time-utils.js` | Time parsing utilities |
| `tz.js` | Timezone loading and validation |

## Timezone

Resolution order:
1. `~/zylos/.env` (`TZ=...`)
2. External `process.env.TZ` (e.g., PM2 env block)
3. `UTC` only when both are unset

Example `.env`:
```bash
TZ=Asia/Shanghai
```

Behavior:
- Natural language times (`--at "tomorrow 9am"`) are parsed in configured timezone
- Cron expressions are evaluated in configured timezone
- CLI display uses configured timezone
- Database timestamps remain UTC Unix seconds
- If `TZ` is present but invalid, CLI/daemon exits with a clear error (fail-fast)

After changing timezone config:
```bash
pm2 restart scheduler
```

## Database

SQLite at `~/zylos/scheduler/scheduler.db`

Each task durably captures the Core `region`, `tenant_id`, and `bot_id` scope
when it is created. A one-time schema migration captures the current configured
scope for older tasks. Daemon restarts and later environment changes reuse the
stored values so an occurrence's idempotency identity cannot fork.

## Priority Levels

| Priority | Type | Description |
|----------|------|-------------|
| 1 | Urgent | Highest priority, immediate execution |
| 2 | High | Important tasks, execute soon |
| 3 | Normal | Default priority, standard execution |

Priority only affects canonical Core queue ordering. Authoritative Core
maintenance state controls admission; the scheduler has no host-idle gate.

## Retry / Missed Task Behavior

Scheduler uses an implicit retry mechanism based on `miss_threshold` (default 300s), not an explicit retry counter:

1. Task reaches `next_run_at` but Core admission is temporarily unavailable → task stays `pending`
2. Daemon retries with durable bounded backoff while within the `miss_threshold` window
3. Core accepts within the window → task is idempotently enqueued
4. Window expires → one-time tasks marked `failed`, recurring/interval skip to next schedule

A missed-occurrence delivery notice rejected only for `queue_full` receives
durable exponential backoff. Any non-retryable Core rejection terminalizes the
local task and its history instead of polling the same rejected notice forever.

The `retry_count` / `max_retries` columns in the database are reserved but unused. Adjust `--miss-threshold <seconds>` per task to control the retry window.

## Service Management

```bash
pm2 status scheduler
pm2 logs scheduler
pm2 restart scheduler
```
