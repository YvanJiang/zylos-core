# Atomic runtime upgrade host

The installed runtime owns upgrades through
`runtime/migration/installed-runtime-upgrade-host.js`. A host constructs
`createInstalledRuntimeUpgradeHost()` with the canonical runtime SQLite connection and the
snapshot, release, legacy-source, executor, and notice adapters. It then:

1. calls `attach(preflight)` with a stable `upgrade_id`;
2. calls `advance(upgrade_id, input)` repeatedly, reopening SQLite and reconstructing the host
   whenever the process restarts;
3. supplies the same legacy batch while the run is `drained` until the durable migration event
   exists;
4. calls `requestRollback(upgrade_id, failure)` when an adapter or validation failure is permanent.

`attach()` takes `runtime/upgrade-owner.lock`, records the owning host/PID/UUID, safely adopts a
crashed same-host owner, then atomically writes
`runtime/atomic-upgrade-owner.json` before starting or reattaching the durable run. The obsolete
self-upgrade launcher and its installed finalizer now fail closed unconditionally; they contain no
executable upgrade route. A live lock owner is never displaced merely because its lock is old.

The executor service writes the installed orchestration inputs to
`runtime/upgrade-plans/<upgrade_id>.json` before attaching the Global26 run. On every daemon start,
it queries Core SQLite for the oldest resumable run and reconstructs the same snapshot, release,
legacy-source, and target-health adapters before normal turn polling begins. A missing or
conflicting plan fails closed. A committed run retains its plan until post-commit cleanup is
durably recorded; a rolled-back run resumes only the restored release. The active-release launcher
then lets the supervisor restart from the exact durable release pointer, preventing old and new
runtime paths from executing concurrently.

The one-time exact-base bootstrap also requires the installed channel prerequisite to atomically
publish `runtime/channel-authority.json` before the installer runs. It uses contract
`zylos.channel-authority`, schema version 1, and exactly one authenticated-event scope per migrated
channel. Each scope records `channel`, `region`, `tenant_id`, `bot_id`, `verified_at`,
`verification_source`, and `provider_instance_id`; Feishu requires region `cn` and Lark requires
region `global`. Core copies the validated non-secret facts and their canonical SHA-256 into the
durable upgrade plan before fencing the old source. A missing, changed, duplicate, or mismatched
scope fails before source mutation. Provider execution activity is a separate plan fact and never
selects or synthesizes a user reply target.

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
Invalidation also materializes a hash-bound rollback queue selected by Core's canonical legacy
classifier and public envelope/scheduler validators: only exact-identity, positive-FIFO, uniquely
routed pending C4 and safe next occurrences of one-time, recurring, or interval schedules may enter
it. A rollback after an activation or migration failure first reads the hash-bound full audit into
immutable Core migration records, compact facts, 30-day detail, and 180-day runtime-control audit;
unknown-side-effect notices must have durable delivery proof before release recovery can begin. The
temporary mixed audit is then sealed. Queue restoration and dispatcher restart are separate durable
effects: the former finishes while the dispatcher is stopped, and the latter requires a
dispatcher-owned `step_id` proof that is replayable even if the restored queue has already progressed.
Runtime control, ambiguous/running work, history, and unknown side effects can never re-enter the
executable source. Commit deletes the unused rollback queue.

Rollback verifies the durable restore effect, revokes every target-release executor registration,
and parks only unexecuted turns imported by that upgrade by cancelling their queue rows while leaving
the attemptless turns outside active states. A later upgrade may
adopt the exact payload and turn after proving the prior run rolled back and no provider attempt exists.
It does not replace the live SQLite database, so ingress accepted during maintenance remains durable.
The service transaction refuses to terminalize `rolled_back` when a completed source invalidation
lacks audit sealing, queue restoration, dispatcher restart, or delivered notice evidence. A claimed
invalidation that crashed after stopping or renaming the source is resumed from its durable input
before any rollback code consumes its result.

Physical activation also advances the durable release-generation fence. After commit, a service
may claim a conversation only when its registered `upgrade_id` and release ref match the applicable
installation or bot fence; already-loaded old services therefore remain unable to execute.
Fresh executor processes may register after commit only for that exact active fence. On rollback the
restored old executor can resume, while revoked target registrations remain unable to claim after reopen.

Raw legacy payload detail follows the 30-day terminal-detail class. Runtime-control audit payloads
follow the 180-day security-audit class. Normalized C4 terminal facts, scheduler definition/history
facts, and global-lineage handoff context remain durable without an ordinary TTL.
