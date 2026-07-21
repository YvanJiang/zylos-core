---
name: health-check
description: |
  Report Core service, executor, turn, queue, delivery, disk, and memory health.
  Runtime facts come only from the provider-neutral Core observability contract.
user-invocable: false
allowed-tools: Bash, Read
---

# System Health Check

## Runtime health

Run the read-only Core diagnostic:

```bash
zylos doctor --check --json
```

Use its observability snapshot facts for service health, maintenance/draining,
executor queues and wait reasons, turn states, workspace leases, and outbox
retry/dead-letter counts. An unavailable or degraded snapshot is itself the
health result; do not infer missing facts from a process or user interface.

## Host capacity

Disk usage is read-only:

```bash
df -h /
```

For memory, select the host-native read-only diagnostics:

```bash
case "$(uname -s)" in
  Darwin) vm_stat; vm.swapusage; memory_pressure ;;
  Linux)  sed -n '1,30p' /proc/meminfo ;;
esac
```

Report warnings at 80% usage and critical capacity at 90% usage. Do not restart
or mutate a service as part of a health check.

## Result delivery

Return the structured findings in the current turn. Core owns the durable
outbox and the channel delivery owner renders and delivers that result. Never
select a channel target or call a channel sender from this skill.
