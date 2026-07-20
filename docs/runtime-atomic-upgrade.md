# Atomic runtime upgrade host

The installed runtime owns upgrades through
`runtime/migration/installed-runtime-upgrade-host.js`. A host constructs
`createInstalledRuntimeUpgradeHost()` with the canonical runtime SQLite connection and the
snapshot, release, legacy-source, executor, and notice adapters. It then:

1. calls `attach(preflight)` with a stable `upgrade_id`;
2. calls `advance(upgrade_id, input)` repeatedly, reopening SQLite and reconstructing the host
   whenever the process restarts;
3. supplies the same legacy batch while the run is `drained` until the durable migration event
   exists.

`attach()` takes `runtime/upgrade-owner.lock`, records the owning host/PID/UUID, safely adopts a
crashed same-host owner, then atomically writes
`runtime/atomic-upgrade-owner.json` before starting or reattaching the durable run. The obsolete
self-upgrade launcher and its installed finalizer now fail closed unconditionally; they contain no
executable upgrade route. A live lock owner is never displaced merely because its lock is old.

External adapter effects use `upgrade_id:step_key` as their idempotency key and take a durable
SQLite claim before invocation. The coordinator renews the claim while the adapter is live; only a
genuinely expired claim can be replayed by a new coordinator. The snapshot adapter
must create an openable SQLite online backup and verify its SHA-256, integrity, and foreign keys.
The release adapter must atomically activate and restore package artifacts, and may remove
explicit legacy paths only after commit. Before activating the target release, the legacy-source
adapter proves the old dispatcher stopped and atomically renames the exact hash-matched queue to a
read-only audit file. The durable invalidation proof is
required before any legacy record can be imported, so runtime-control records cannot remain at an
executable source path. After the transaction has written immutable migration records, compact facts,
and fixed-class audit payloads, a second durable effect verifies and removes the temporary source file;
the mixed raw batch therefore cannot outlive its 30-day/detail and 180-day/control split in SQLite.

Rollback verifies the durable restore effect, revokes every target-release executor registration,
and parks only unexecuted turns imported by that upgrade by cancelling their queue rows while leaving
the attemptless turns outside active states. A later upgrade may
adopt the exact payload and turn after proving the prior run rolled back and no provider attempt exists.
It does not replace the live SQLite database, so ingress accepted during maintenance remains durable.

Physical activation also advances the durable release-generation fence. After commit, a service
may claim a conversation only when its registered `upgrade_id` and release ref match the applicable
installation or bot fence; already-loaded old services therefore remain unable to execute.
Fresh executor processes may register after commit only for that exact active fence. On rollback the
restored old executor can resume, while revoked target registrations remain unable to claim after reopen.

Raw legacy payload detail follows the 30-day terminal-detail class. Runtime-control audit payloads
follow the 180-day security-audit class. Normalized C4 terminal facts, scheduler definition/history
facts, and global-lineage handoff context remain durable without an ordinary TTL.
