# Detached conversation workspaces

Only the internal execution conversation created by detached
`acceptNormalInbound` receives an independent task workspace. The origin/channel
conversation remains a durable delivery and permission identity and has no
workspace binding. `acceptQueuedInbound` retains its compatibility FIFO
semantics and does not create a binding.

## Durable contract

Core SQLite owns `runtime_conversation_workspaces`. `conversation_id` is unique
and binds one workspace identity:

- `workspace_id`;
- controlled absolute `workspace_root`;
- `base_snapshot_root`, `base_snapshot_ref`, and the durable
  `base_snapshot_manifest_json`;
- positive `generation`;
- `state`;
- provisioning, ready, quarantine, failure, retirement, error, and retention
  evidence.

The exact states are `requested`, `provisioning`, `ready`, `quarantined`,
`failed`, and `retired`. Callers use
`getConversationWorkspaceBinding(database, conversationId)`:

| state | wait_reason | claimable |
|---|---|---|
| `requested` | `workspace_provisioning` | false |
| `provisioning` | `workspace_provisioning` | false |
| `ready` | null | true |
| `quarantined` | `workspace_quarantined` | false |
| `failed` | `workspace_failed` | false |
| `retired` | `workspace_retired` | false |

No binding returns `null`. An executor must distinguish that result using
`runtime_background_tasks.execution_conversation_id`: a detached execution
conversation without its required binding fails closed; a conversation that is
not a detached execution conversation uses the explicit legacy/shared
workspace contract.

`runtime_lineage_workspace_bindings` durably records the exact workspace root
and generation first assigned to a lineage. Executor claim calls
`bindLineageWorkspaceInTransaction` before starting or resuming a provider
session. Exact replay is idempotent; a different workspace identity is a
conflict. `getLineageWorkspaceBinding` provides restart proof. SQLite triggers
make a ready or lineage-bound root/generation immutable. This foundation does
not support in-place reprovisioning of an existing provider lineage.

## Transaction and provisioning boundary

Detached inbound admission inserts only a `requested` row in the same SQLite
transaction as the background task, execution conversation, inbound event,
turn, queue, events, outbox, and idempotency result. It performs no filesystem
operation. A database guard rejects bindings for any conversation that is not
the `execution_conversation_id` of a detached background task.

Schema initialization backfills missing bindings for pre-existing nonterminal
detached tasks. Safe queued/running work becomes `requested`; pre-existing
`recovering` or `side_effect_status=unknown` work becomes `quarantined` with
durable evidence. Terminal history without uncertainty is not provisioned.

`createConversationWorkspaceProvisioner` owns the later filesystem state
machine:

1. reserve deterministic final and staging paths under one canonical,
   pre-existing, non-symlink store root;
2. durably transition `requested -> provisioning` with an expiring owner;
3. populate a private `0700` staging directory from the durable snapshot
   root/ref and its explicit file manifest;
4. write Core ownership evidence and atomically rename staging to the final
   root;
5. commit `provisioning -> ready`.

If activation wins but the ready commit fails, reopen validates the ownership
marker and commits ready without copying again. An expired provisioning owner
allows a new process to fence the old owner, remove only the exact recorded and
contained staging directory, and resume the same generation. That recovery
cleanup runs inside a provisioning-only SQLite `IMMEDIATE` transaction so
neither a new owner nor an uncertainty trigger can observe `requested` before
the deterministic staging path is gone. A crash rolls SQLite back to the
expired provisioning row, making a partial or missing staging tree safe to
clean again. The inbound admission transaction never crosses this filesystem
boundary. A live foreign provisioning owner remains `workspace_provisioning`.

`snapshotFiles` is a default-deny allowlist of canonical relative regular-file
paths. Core persists the sorted manifest on the first provisioning attempt and
all reopen/retry work uses that durable manifest and snapshot identity, even if
the process's current defaults differ. Unlisted files and directories are never
walked or copied. Manifest entries that traverse, use symlinks, or name
environment files, credentials/secrets, SQLite/socket data, PM2 state,
source-control metadata, provider/control homes, SSH/cloud credential homes,
dependency caches, or private-key formats are rejected. Every directory is
`0700`; files are `0600` or `0700` when executable.

The snapshot root must therefore be a dedicated, immutable, Core-produced task
snapshot. `ZYLOS_DIR`, Core SQLite, sockets, provider homes, PM2 state, home
directories, and arbitrary working directories are control-plane or
machine-local data and must never be supplied as a base snapshot.

## Runtime bootstrap

The executor daemon creates two controlled paths beneath `ZYLOS_DIR/runtime`:

- `conversation-workspaces/` is the private store for final and staging roots;
- `conversation-workspace-base-v1/` is a read-only empty snapshot whose durable
  reference is `zylos-empty-conversation-workspace@1`.

The empty snapshot is deliberate. Core does not copy `.env`, credentials,
provider homes, SQLite, sockets, PM2 state, global memory, dependencies, or the
installation directory into a conversation workspace. Managed identity,
skills, credentials, and memory retain their existing global ownership while
the provider cwd and output paths are isolated per detached execution
conversation.

Executor startup reconciles every durable binding. Before reservation, a
`requested` or reclaimable `provisioning` binding is passed through
`createConversationWorkspaceProvisioner.ensure`; only the resulting `ready`
root can reach lease acquisition and provider execution. A live foreign
provisioner remains `workspace_provisioning`, while a durable failure or
quarantine keeps only that conversation unclaimable.

`acceptQueuedInbound` remains an explicit compatibility seam and uses the
legacy shared root. Because that root can contain the conversation-workspace
store, its writable or uncertain lease can conservatively overlap isolated
roots. Detached conversation roots are siblings and do not overlap each other.

## Recovery, quarantine, and retirement

`side_effect_status=unknown`, a recovering execution/background task, an
uncertain workspace lease, or unknown background work quarantines the binding.
Quarantine never removes or reuses its final or staging path. Missing/tampered
roots, marker mismatch, and path escape also quarantine with durable error
evidence. Unsafe snapshot content fails the binding and records the error.

Retirement is allowed only from `ready` after nonterminal turns, active or
uncertain leases, and active/unknown background work are absent. Retirement
records retention evidence but does not delete the root. Physical disposal is
intentionally outside this module until a separate retention design can prove
all recovery, lineage, and audit references are safe.
