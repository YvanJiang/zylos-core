import crypto from 'node:crypto';

import {
  createDeliveryLaneKey,
  createDeliveryLaneKeyFromIdentity,
} from './delivery-lane-key.js';

const OUTBOX_TABLE_SCHEMA = `(
    outbox_id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL UNIQUE,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    turn_id TEXT REFERENCES runtime_turns(turn_id),
    control_id TEXT,
    lane_key TEXT,
    predecessor_delivery_id TEXT,
    aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
    status TEXT NOT NULL,
    command_json TEXT NOT NULL,
    claimed_command_hash TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    supersedable INTEGER NOT NULL DEFAULT 0 CHECK (supersedable IN (0, 1)),
    terminal INTEGER NOT NULL DEFAULT 0 CHECK (terminal IN (0, 1)),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    delivery_attempt_id TEXT,
    delivery_attempt_no INTEGER CHECK (
      delivery_attempt_no IS NULL OR delivery_attempt_no > 0
    ),
    outbox_lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK (outbox_lease_epoch >= 0),
    lease_owner TEXT,
    lease_expires_at TEXT,
    lease_expires_epoch_ms INTEGER,
    pre_action_fenced_at TEXT,
    last_attempt_at TEXT,
    next_attempt_at TEXT,
    last_error_json TEXT,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT
  )`;

const SAFE_RETRY_WORKSPACE_MIGRATION_ID = 'conversation-workspace-safe-provider-retry-v1';
const SAFE_RETRY_WORKSPACE_REPAIR_MIGRATION_ID = 'conversation-workspace-safe-provider-retry-repair-v2';

const CONVERSATION_WORKSPACE_STATE_TRANSITION_TRIGGER = `
  CREATE TRIGGER IF NOT EXISTS runtime_conversation_workspace_state_transition
  BEFORE UPDATE OF state ON runtime_conversation_workspaces
  WHEN NEW.state IS NOT OLD.state
    AND NOT (
      (OLD.state = 'requested' AND NEW.state IN ('provisioning', 'quarantined'))
      OR (
        OLD.state = 'provisioning'
        AND NEW.state IN ('requested', 'ready', 'quarantined', 'failed')
      )
      OR (OLD.state = 'ready' AND NEW.state IN ('quarantined', 'retired'))
    )
  BEGIN
    SELECT RAISE(ABORT, 'invalid conversation workspace state transition');
  END;
`;

const CONVERSATION_WORKSPACE_RUNTIME_QUARANTINE_TRIGGERS = `
  CREATE TRIGGER IF NOT EXISTS runtime_workspace_quarantine_background_task
  AFTER UPDATE OF state, side_effect_status ON runtime_background_tasks
  WHEN NEW.side_effect_status = 'unknown'
    OR (
      NEW.state = 'recovering'
      AND NOT EXISTS (
        SELECT 1
        FROM runtime_turn_queue AS retry_queue
        JOIN runtime_turns AS retry_turn
          ON retry_turn.turn_id = retry_queue.turn_id
        JOIN runtime_provider_attempts AS retry_attempt
          ON retry_attempt.turn_id = retry_queue.turn_id
          AND retry_attempt.attempt_id = retry_turn.attempt_id
          AND retry_attempt.attempt_no = retry_turn.attempt_no
          AND retry_attempt.lease_epoch = retry_turn.lease_epoch
        WHERE retry_queue.turn_id = NEW.execution_turn_id
          AND retry_queue.status = 'queued'
          AND retry_queue.wait_reason = 'provider_retry'
          AND retry_attempt.state = 'retry_wait'
          AND retry_attempt.side_effect_status = 'none'
      )
    )
  BEGIN
    UPDATE runtime_conversation_workspaces
    SET state = 'quarantined',
      provisioning_owner = NULL,
      provisioning_expires_at = NULL,
      quarantined_at = COALESCE(
        quarantined_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      ),
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      last_error_json = json_object(
        'code', 'workspace_runtime_uncertain',
        'message', 'Detached background execution entered uncertain recovery.',
        'terminal', json('true'),
        'occurred_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      )
    WHERE conversation_id = NEW.execution_conversation_id
      AND state IN ('requested', 'provisioning', 'ready');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_workspace_quarantine_background_task_insert
  AFTER INSERT ON runtime_background_tasks
  WHEN NEW.side_effect_status = 'unknown'
    OR (
      NEW.state = 'recovering'
      AND NOT EXISTS (
        SELECT 1
        FROM runtime_turn_queue AS retry_queue
        JOIN runtime_turns AS retry_turn
          ON retry_turn.turn_id = retry_queue.turn_id
        JOIN runtime_provider_attempts AS retry_attempt
          ON retry_attempt.turn_id = retry_queue.turn_id
          AND retry_attempt.attempt_id = retry_turn.attempt_id
          AND retry_attempt.attempt_no = retry_turn.attempt_no
          AND retry_attempt.lease_epoch = retry_turn.lease_epoch
        WHERE retry_queue.turn_id = NEW.execution_turn_id
          AND retry_queue.status = 'queued'
          AND retry_queue.wait_reason = 'provider_retry'
          AND retry_attempt.state = 'retry_wait'
          AND retry_attempt.side_effect_status = 'none'
      )
    )
  BEGIN
    UPDATE runtime_conversation_workspaces
    SET state = 'quarantined',
      provisioning_owner = NULL,
      provisioning_expires_at = NULL,
      quarantined_at = COALESCE(
        quarantined_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      ),
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      last_error_json = json_object(
        'code', 'workspace_runtime_uncertain',
        'message', 'Detached background execution entered uncertain recovery.',
        'terminal', json('true'),
        'occurred_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      )
    WHERE conversation_id = NEW.execution_conversation_id
      AND state IN ('requested', 'provisioning', 'ready');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_workspace_quarantine_recovering_turn
  AFTER UPDATE OF state ON runtime_turns
  WHEN NEW.state = 'recovering'
    AND NOT EXISTS (
      SELECT 1
      FROM runtime_turn_queue AS retry_queue
      JOIN runtime_provider_attempts AS retry_attempt
        ON retry_attempt.turn_id = retry_queue.turn_id
        AND retry_attempt.attempt_id = NEW.attempt_id
        AND retry_attempt.attempt_no = NEW.attempt_no
        AND retry_attempt.lease_epoch = NEW.lease_epoch
      WHERE retry_queue.turn_id = NEW.turn_id
        AND retry_queue.status = 'queued'
        AND retry_queue.wait_reason = 'provider_retry'
        AND retry_attempt.state = 'retry_wait'
        AND retry_attempt.side_effect_status = 'none'
    )
  BEGIN
    UPDATE runtime_conversation_workspaces
    SET state = 'quarantined',
      provisioning_owner = NULL,
      provisioning_expires_at = NULL,
      quarantined_at = COALESCE(
        quarantined_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      ),
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      last_error_json = json_object(
        'code', 'workspace_runtime_uncertain',
        'message', 'Conversation execution entered recovering state.',
        'terminal', json('true'),
        'occurred_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      )
    WHERE conversation_id = NEW.conversation_id
      AND state IN ('requested', 'provisioning', 'ready');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_workspace_quarantine_recovering_turn_insert
  AFTER INSERT ON runtime_turns
  WHEN NEW.state = 'recovering'
    AND NOT EXISTS (
      SELECT 1
      FROM runtime_turn_queue AS retry_queue
      JOIN runtime_provider_attempts AS retry_attempt
        ON retry_attempt.turn_id = retry_queue.turn_id
        AND retry_attempt.attempt_id = NEW.attempt_id
        AND retry_attempt.attempt_no = NEW.attempt_no
        AND retry_attempt.lease_epoch = NEW.lease_epoch
      WHERE retry_queue.turn_id = NEW.turn_id
        AND retry_queue.status = 'queued'
        AND retry_queue.wait_reason = 'provider_retry'
        AND retry_attempt.state = 'retry_wait'
        AND retry_attempt.side_effect_status = 'none'
    )
  BEGIN
    UPDATE runtime_conversation_workspaces
    SET state = 'quarantined',
      provisioning_owner = NULL,
      provisioning_expires_at = NULL,
      quarantined_at = COALESCE(
        quarantined_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      ),
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      last_error_json = json_object(
        'code', 'workspace_runtime_uncertain',
        'message', 'Conversation execution entered recovering state.',
        'terminal', json('true'),
        'occurred_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      )
    WHERE conversation_id = NEW.conversation_id
      AND state IN ('requested', 'provisioning', 'ready');
  END;
`;

const RUNTIME_SCHEMA = `
  CREATE TABLE IF NOT EXISTS runtime_schema_migrations (
    migration_id TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_conversations (
    conversation_id TEXT PRIMARY KEY,
    conversation_key TEXT NOT NULL UNIQUE,
    region TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    chat_type TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    native_thread_or_topic_id TEXT,
    last_queue_sequence INTEGER NOT NULL DEFAULT 0 CHECK (last_queue_sequence >= 0),
    queue_version INTEGER NOT NULL DEFAULT 1 CHECK (queue_version > 0),
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_lineages (
    lineage_id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    lineage_kind TEXT NOT NULL,
    is_default INTEGER NOT NULL CHECK (is_default IN (0, 1)),
    created_at TEXT NOT NULL,
    provider TEXT CHECK (provider IS NULL OR provider IN ('claude', 'codex')),
    provider_native_id TEXT,
    provider_native_id_bound_at TEXT,
    provider_native_state TEXT NOT NULL DEFAULT 'unknown'
      CHECK (provider_native_state IN ('unknown', 'valid', 'invalid')),
    recovery_of_lineage_id TEXT REFERENCES runtime_lineages(lineage_id),
    CHECK (
      (provider IS NULL AND provider_native_id IS NULL AND provider_native_id_bound_at IS NULL)
      OR (provider IS NOT NULL AND provider_native_id IS NOT NULL
          AND provider_native_id_bound_at IS NOT NULL)
    )
  );

  CREATE UNIQUE INDEX IF NOT EXISTS runtime_lineages_one_default
    ON runtime_lineages(conversation_id)
    WHERE is_default = 1;

  CREATE TABLE IF NOT EXISTS runtime_inbound_events (
    inbound_event_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    message_id TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    envelope_json TEXT NOT NULL,
    received_at TEXT NOT NULL,
    committed_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_scheduler_occurrences (
    schedule_id TEXT NOT NULL,
    occurrence_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    bound_conversation INTEGER NOT NULL CHECK (bound_conversation IN (0, 1)),
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    turn_id TEXT NOT NULL REFERENCES runtime_turns(turn_id),
    status TEXT NOT NULL CHECK (status IN ('accepted', 'rejected')),
    envelope_json TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    PRIMARY KEY (schedule_id, occurrence_id),
    UNIQUE (turn_id)
  );

  CREATE TABLE IF NOT EXISTS runtime_turns (
    turn_id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    lineage_id TEXT REFERENCES runtime_lineages(lineage_id),
    inbound_event_id TEXT NOT NULL UNIQUE REFERENCES runtime_inbound_events(inbound_event_id),
    state TEXT NOT NULL,
    turn_version INTEGER NOT NULL CHECK (turn_version > 0),
    attempt_id TEXT,
    attempt_no INTEGER CHECK (attempt_no IS NULL OR attempt_no > 0),
    lease_epoch INTEGER CHECK (lease_epoch IS NULL OR lease_epoch > 0),
    queue_sequence INTEGER NOT NULL CHECK (queue_sequence > 0),
    provider_input_json TEXT,
    redirected_from_turn_id TEXT UNIQUE REFERENCES runtime_turns(turn_id),
    created_at TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    CHECK (
      (attempt_id IS NULL AND attempt_no IS NULL AND lease_epoch IS NULL)
      OR (attempt_id IS NOT NULL AND attempt_no IS NOT NULL AND lease_epoch IS NOT NULL)
    ),
    UNIQUE (conversation_id, queue_sequence)
  );

  CREATE TABLE IF NOT EXISTS runtime_turn_queue (
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    queue_sequence INTEGER NOT NULL CHECK (queue_sequence > 0),
    turn_id TEXT NOT NULL UNIQUE REFERENCES runtime_turns(turn_id),
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority IN (0, 1)),
    wait_reason TEXT,
    wait_detail_json TEXT,
    enqueued_at TEXT NOT NULL,
    available_at TEXT,
    PRIMARY KEY (conversation_id, queue_sequence)
  );

  CREATE TABLE IF NOT EXISTS runtime_background_tasks (
    background_task_id TEXT PRIMARY KEY,
    origin_conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    dispatch_turn_id TEXT NOT NULL UNIQUE
      REFERENCES runtime_turns(turn_id) ON DELETE CASCADE,
    execution_conversation_id TEXT NOT NULL UNIQUE
      REFERENCES runtime_conversations(conversation_id),
    execution_turn_id TEXT NOT NULL UNIQUE
      REFERENCES runtime_turns(turn_id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (
      state IN (
        'queued', 'starting', 'running', 'waiting_user', 'redirecting',
        'recovering', 'completed', 'stopped', 'cancelled', 'failed',
        'interrupted', 'timed_out'
      )
    ),
    side_effect_status TEXT NOT NULL DEFAULT 'none'
      CHECK (side_effect_status IN ('none', 'known', 'unknown')),
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    CHECK (origin_conversation_id != execution_conversation_id),
    CHECK (dispatch_turn_id != execution_turn_id)
  );

  CREATE INDEX IF NOT EXISTS runtime_background_tasks_origin
    ON runtime_background_tasks(origin_conversation_id, created_at, background_task_id);

  CREATE INDEX IF NOT EXISTS runtime_background_tasks_state
    ON runtime_background_tasks(state, created_at, background_task_id);

  CREATE TABLE IF NOT EXISTS runtime_conversation_workspaces (
    workspace_id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL UNIQUE
      REFERENCES runtime_conversations(conversation_id) ON DELETE RESTRICT,
    workspace_root TEXT UNIQUE,
    staging_root TEXT UNIQUE,
    base_snapshot_root TEXT,
    base_snapshot_ref TEXT,
    base_snapshot_manifest_json TEXT,
    generation INTEGER NOT NULL DEFAULT 1 CHECK (generation > 0),
    state TEXT NOT NULL CHECK (
      state IN ('requested', 'provisioning', 'ready', 'quarantined', 'failed', 'retired')
    ),
    provisioning_owner TEXT,
    provisioning_expires_at TEXT,
    requested_at TEXT NOT NULL,
    provisioning_started_at TEXT,
    ready_at TEXT,
    quarantined_at TEXT,
    failed_at TEXT,
    retired_at TEXT,
    updated_at TEXT NOT NULL,
    last_error_json TEXT,
    retention_reason TEXT,
    retain_until TEXT,
    UNIQUE (workspace_id, conversation_id, workspace_root, generation),
    CHECK (
      state IN ('requested', 'quarantined')
      OR (
        workspace_root IS NOT NULL
        AND base_snapshot_root IS NOT NULL
        AND base_snapshot_ref IS NOT NULL
        AND base_snapshot_manifest_json IS NOT NULL
      )
    ),
    CHECK (
      state != 'provisioning'
      OR (
        staging_root IS NOT NULL
        AND provisioning_owner IS NOT NULL
        AND provisioning_expires_at IS NOT NULL
        AND provisioning_started_at IS NOT NULL
      )
    ),
    CHECK (state != 'ready' OR ready_at IS NOT NULL),
    CHECK (state != 'quarantined' OR quarantined_at IS NOT NULL),
    CHECK (state != 'failed' OR failed_at IS NOT NULL),
    CHECK (state != 'retired' OR retired_at IS NOT NULL)
  );

  CREATE INDEX IF NOT EXISTS runtime_conversation_workspaces_state
    ON runtime_conversation_workspaces(state, updated_at, workspace_id);

  CREATE TRIGGER IF NOT EXISTS runtime_conversation_workspace_detached_only
  BEFORE INSERT ON runtime_conversation_workspaces
  WHEN NOT EXISTS (
    SELECT 1
    FROM runtime_background_tasks
    WHERE execution_conversation_id = NEW.conversation_id
  )
  BEGIN
    SELECT RAISE(ABORT, 'conversation workspace requires a detached execution conversation');
  END;

  INSERT INTO runtime_conversation_workspaces (
    workspace_id, conversation_id, workspace_root, staging_root,
    base_snapshot_root, base_snapshot_ref, base_snapshot_manifest_json,
    generation, state, provisioning_owner, provisioning_expires_at,
    requested_at, provisioning_started_at, ready_at, quarantined_at,
    failed_at, retired_at, updated_at, last_error_json,
    retention_reason, retain_until
  )
  SELECT
    'conversation-workspace-backfill-' || lower(hex(randomblob(16))),
    task.execution_conversation_id,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    1,
    CASE
      WHEN task.state = 'recovering' OR task.side_effect_status = 'unknown'
        THEN 'quarantined'
      ELSE 'requested'
    END,
    NULL,
    NULL,
    task.created_at,
    NULL,
    NULL,
    CASE
      WHEN task.state = 'recovering' OR task.side_effect_status = 'unknown'
        THEN task.created_at
      ELSE NULL
    END,
    NULL,
    NULL,
    task.created_at,
    CASE
      WHEN task.state = 'recovering' OR task.side_effect_status = 'unknown'
        THEN json_object(
          'code', 'workspace_runtime_uncertain',
          'message', 'Pre-existing detached execution requires manual recovery.',
          'terminal', json('true'),
          'occurred_at', task.created_at
        )
      ELSE NULL
    END,
    NULL,
    NULL
  FROM runtime_background_tasks AS task
  WHERE (
    task.state IN (
      'queued', 'starting', 'running', 'waiting_user', 'redirecting', 'recovering'
    )
    OR task.side_effect_status = 'unknown'
  )
    AND NOT EXISTS (
      SELECT 1
      FROM runtime_conversation_workspaces AS workspace
      WHERE workspace.conversation_id = task.execution_conversation_id
    );

  ${CONVERSATION_WORKSPACE_STATE_TRANSITION_TRIGGER}

  CREATE TRIGGER IF NOT EXISTS runtime_conversation_workspace_primary_identity_immutable
  BEFORE UPDATE OF workspace_id, conversation_id ON runtime_conversation_workspaces
  WHEN NEW.workspace_id IS NOT OLD.workspace_id
    OR NEW.conversation_id IS NOT OLD.conversation_id
  BEGIN
    SELECT RAISE(ABORT, 'conversation workspace identity is immutable');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_conversation_workspace_delete_forbidden
  BEFORE DELETE ON runtime_conversation_workspaces
  BEGIN
    SELECT RAISE(ABORT, 'conversation workspace disposal requires a retention migration');
  END;

  CREATE TABLE IF NOT EXISTS runtime_lineage_workspace_bindings (
    lineage_id TEXT PRIMARY KEY
      REFERENCES runtime_lineages(lineage_id) ON DELETE RESTRICT,
    workspace_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL
      REFERENCES runtime_conversations(conversation_id) ON DELETE RESTRICT,
    workspace_root TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    bound_at TEXT NOT NULL,
    FOREIGN KEY (workspace_id, conversation_id, workspace_root, generation)
      REFERENCES runtime_conversation_workspaces(
        workspace_id, conversation_id, workspace_root, generation
      ) ON DELETE RESTRICT
  );

  CREATE INDEX IF NOT EXISTS runtime_lineage_workspace_bindings_workspace
    ON runtime_lineage_workspace_bindings(workspace_id, generation, lineage_id);

  CREATE TRIGGER IF NOT EXISTS runtime_lineage_workspace_binding_guard
  BEFORE INSERT ON runtime_lineage_workspace_bindings
  BEGIN
    SELECT CASE
      WHEN (
        SELECT conversation_id
        FROM runtime_lineages
        WHERE lineage_id = NEW.lineage_id
      ) IS NOT NEW.conversation_id
      THEN RAISE(ABORT, 'lineage and workspace conversations must match')
    END;
    SELECT CASE
      WHEN (
        SELECT state
        FROM runtime_conversation_workspaces
        WHERE workspace_id = NEW.workspace_id
      ) IS NOT 'ready'
      THEN RAISE(ABORT, 'lineage workspace binding requires a ready workspace')
    END;
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_lineage_workspace_binding_immutable_update
  BEFORE UPDATE ON runtime_lineage_workspace_bindings
  BEGIN
    SELECT RAISE(ABORT, 'lineage workspace binding is immutable');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_lineage_workspace_binding_immutable_delete
  BEFORE DELETE ON runtime_lineage_workspace_bindings
  BEGIN
    SELECT RAISE(ABORT, 'lineage workspace binding is immutable');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_conversation_workspace_ready_identity_immutable
  BEFORE UPDATE OF workspace_root, base_snapshot_root, base_snapshot_ref,
    base_snapshot_manifest_json, generation
    ON runtime_conversation_workspaces
  WHEN OLD.state IN ('ready', 'quarantined', 'failed', 'retired')
    AND (
      NEW.workspace_root IS NOT OLD.workspace_root
      OR NEW.base_snapshot_root IS NOT OLD.base_snapshot_root
      OR NEW.base_snapshot_ref IS NOT OLD.base_snapshot_ref
      OR NEW.base_snapshot_manifest_json IS NOT OLD.base_snapshot_manifest_json
      OR NEW.generation IS NOT OLD.generation
    )
  BEGIN
    SELECT RAISE(ABORT, 'ready workspace identity is immutable');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_conversation_workspace_bound_identity_immutable
  BEFORE UPDATE OF conversation_id, workspace_root, generation
    ON runtime_conversation_workspaces
  WHEN EXISTS (
    SELECT 1
    FROM runtime_lineage_workspace_bindings
    WHERE workspace_id = OLD.workspace_id
  )
    AND (
      NEW.conversation_id IS NOT OLD.conversation_id
      OR NEW.workspace_root IS NOT OLD.workspace_root
      OR NEW.generation IS NOT OLD.generation
    )
  BEGIN
    SELECT RAISE(ABORT, 'lineage-bound workspace identity is immutable');
  END;

  CREATE TABLE IF NOT EXISTS runtime_input_groups (
    input_group_id TEXT PRIMARY KEY,
    origin_conversation_id TEXT NOT NULL
      REFERENCES runtime_conversations(conversation_id),
    actor_id TEXT NOT NULL,
    routing_intent_key TEXT NOT NULL,
    routing_intent_json TEXT NOT NULL,
    background_task_id TEXT NOT NULL UNIQUE
      REFERENCES runtime_background_tasks(background_task_id) ON DELETE CASCADE,
    execution_turn_id TEXT NOT NULL UNIQUE
      REFERENCES runtime_turns(turn_id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN ('collecting', 'sealed', 'cancelled')),
    member_count INTEGER NOT NULL CHECK (member_count > 0),
    opened_at TEXT NOT NULL,
    last_member_at TEXT NOT NULL,
    collect_until TEXT NOT NULL,
    max_collect_until TEXT NOT NULL,
    sealed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
      (state = 'sealed' AND sealed_at IS NOT NULL)
      OR (state != 'sealed' AND sealed_at IS NULL)
    )
  );

  CREATE INDEX IF NOT EXISTS runtime_input_groups_collecting
    ON runtime_input_groups (
      origin_conversation_id, actor_id, state, collect_until, input_group_id
    );

  CREATE TABLE IF NOT EXISTS runtime_input_group_members (
    input_group_id TEXT NOT NULL
      REFERENCES runtime_input_groups(input_group_id) ON DELETE CASCADE,
    member_ordinal INTEGER NOT NULL CHECK (member_ordinal > 0),
    origin_conversation_id TEXT NOT NULL
      REFERENCES runtime_conversations(conversation_id),
    inbound_event_id TEXT NOT NULL UNIQUE
      REFERENCES runtime_inbound_events(inbound_event_id) ON DELETE CASCADE,
    dispatch_turn_id TEXT NOT NULL UNIQUE
      REFERENCES runtime_turns(turn_id) ON DELETE CASCADE,
    message_id TEXT NOT NULL,
    actor_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    content_json TEXT NOT NULL,
    reply_json TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    PRIMARY KEY (input_group_id, member_ordinal),
    UNIQUE (input_group_id, message_id)
  );

  CREATE UNIQUE INDEX IF NOT EXISTS runtime_turns_one_active_per_conversation
    ON runtime_turns(conversation_id)
    WHERE state IN ('starting', 'running', 'waiting_user', 'redirecting', 'recovering');

  CREATE TABLE IF NOT EXISTS runtime_permission_revision_sequence (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    current_revision INTEGER NOT NULL CHECK (current_revision >= 0)
  );

  INSERT OR IGNORE INTO runtime_permission_revision_sequence (singleton_id, current_revision)
    VALUES (1, 0);

  CREATE TABLE IF NOT EXISTS runtime_permission_grants (
    grant_id TEXT PRIMARY KEY,
    grant_kind TEXT NOT NULL CHECK (
      grant_kind IN ('next_turn', 'timed_conversation', 'persistent_bot')
    ),
    state TEXT NOT NULL CHECK (state IN ('active', 'consumed', 'revoked', 'expired')),
    region TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    conversation_id TEXT REFERENCES runtime_conversations(conversation_id),
    issued_by_actor_id TEXT NOT NULL,
    source TEXT NOT NULL,
    policy_revision INTEGER NOT NULL UNIQUE CHECK (policy_revision > 0),
    issued_at TEXT NOT NULL,
    expires_at TEXT,
    consumed_by_turn_id TEXT UNIQUE REFERENCES runtime_turns(turn_id),
    consumed_at TEXT,
    revoked_at TEXT,
    expired_at TEXT,
    notice_target_json TEXT NOT NULL,
    CHECK (
      (grant_kind = 'next_turn' AND conversation_id IS NOT NULL AND expires_at IS NULL)
      OR (grant_kind = 'timed_conversation' AND conversation_id IS NOT NULL
          AND expires_at IS NOT NULL)
      OR (grant_kind = 'persistent_bot' AND conversation_id IS NULL AND expires_at IS NULL)
    ),
    CHECK (
      (state = 'consumed' AND consumed_by_turn_id IS NOT NULL AND consumed_at IS NOT NULL)
      OR (state != 'consumed' AND consumed_by_turn_id IS NULL AND consumed_at IS NULL)
    )
  );

  CREATE INDEX IF NOT EXISTS runtime_permission_grants_conversation
    ON runtime_permission_grants(conversation_id, grant_kind, state, policy_revision);
  CREATE INDEX IF NOT EXISTS runtime_permission_grants_bot
    ON runtime_permission_grants(region, tenant_id, bot_id, grant_kind, state, policy_revision);
  CREATE UNIQUE INDEX IF NOT EXISTS runtime_permission_one_active_bot_grant
    ON runtime_permission_grants(region, tenant_id, bot_id)
    WHERE grant_kind = 'persistent_bot' AND state = 'active';

  CREATE TABLE IF NOT EXISTS runtime_permission_revocations (
    revocation_id TEXT PRIMARY KEY,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('conversation', 'bot')),
    region TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    conversation_id TEXT REFERENCES runtime_conversations(conversation_id),
    actor_id TEXT NOT NULL,
    source TEXT NOT NULL,
    policy_revision INTEGER NOT NULL UNIQUE CHECK (policy_revision > 0),
    reason TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    CHECK (
      (scope_kind = 'conversation' AND conversation_id IS NOT NULL)
      OR (scope_kind = 'bot' AND conversation_id IS NULL)
    )
  );

  CREATE INDEX IF NOT EXISTS runtime_permission_revocations_scope
    ON runtime_permission_revocations(
      region, tenant_id, bot_id, conversation_id, scope_kind, policy_revision
    );

  CREATE TABLE IF NOT EXISTS runtime_turn_permissions (
    turn_id TEXT PRIMARY KEY REFERENCES runtime_turns(turn_id),
    actor_id TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('safe', 'trusted')),
    basis_kind TEXT NOT NULL CHECK (
      basis_kind IN ('default_safe', 'next_turn', 'timed_conversation', 'persistent_bot')
    ),
    grant_id TEXT REFERENCES runtime_permission_grants(grant_id),
    grant_policy_revision INTEGER,
    accepted_policy_revision INTEGER NOT NULL CHECK (accepted_policy_revision >= 0),
    accepted_at TEXT NOT NULL,
    CHECK (
      (basis_kind = 'default_safe' AND mode = 'safe' AND grant_id IS NULL
        AND grant_policy_revision IS NULL)
      OR (basis_kind != 'default_safe' AND mode = 'trusted' AND grant_id IS NOT NULL
        AND grant_policy_revision IS NOT NULL)
    )
  );

  CREATE TABLE IF NOT EXISTS runtime_permission_controls (
    control_id TEXT PRIMARY KEY,
    inbound_event_id TEXT NOT NULL UNIQUE REFERENCES runtime_inbound_events(inbound_event_id),
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    command_kind TEXT NOT NULL CHECK (
      command_kind IN ('next_turn', 'timed_conversation', 'persistent_bot', 'safe')
    ),
    command_text TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('completed', 'pending_confirmation', 'rejected')),
    policy_revision INTEGER CHECK (policy_revision IS NULL OR policy_revision > 0),
    grant_id TEXT REFERENCES runtime_permission_grants(grant_id),
    interaction_id TEXT UNIQUE REFERENCES runtime_interactions(interaction_id),
    result_json TEXT NOT NULL,
    final_result_json TEXT,
    committed_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_permission_confirmations (
    control_id TEXT PRIMARY KEY REFERENCES runtime_permission_controls(control_id),
    interaction_id TEXT NOT NULL UNIQUE REFERENCES runtime_interactions(interaction_id),
    action_kind TEXT NOT NULL CHECK (
      action_kind IN ('grant_persistent_bot', 'revoke_persistent_bot')
    ),
    expected_command TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    region TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    target_grant_id TEXT REFERENCES runtime_permission_grants(grant_id),
    requested_policy_revision INTEGER NOT NULL CHECK (requested_policy_revision >= 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'denied', 'expired')),
    expires_at TEXT NOT NULL,
    resolved_at TEXT,
    effect_policy_revision INTEGER CHECK (effect_policy_revision IS NULL OR effect_policy_revision > 0),
    CHECK (
      (action_kind = 'grant_persistent_bot' AND target_grant_id IS NULL)
      OR (action_kind = 'revoke_persistent_bot' AND target_grant_id IS NOT NULL)
    )
  );

  CREATE TABLE IF NOT EXISTS runtime_permission_audit (
    audit_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    source TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    policy_revision INTEGER NOT NULL CHECK (policy_revision >= 0),
    reason TEXT NOT NULL,
    redacted_context_json TEXT NOT NULL,
    turn_id TEXT REFERENCES runtime_turns(turn_id),
    grant_id TEXT REFERENCES runtime_permission_grants(grant_id),
    control_id TEXT REFERENCES runtime_permission_controls(control_id),
    action_ref TEXT,
    committed_at TEXT NOT NULL
  );

  CREATE INDEX IF NOT EXISTS runtime_permission_audit_policy
    ON runtime_permission_audit(policy_revision, committed_at);

  CREATE TABLE IF NOT EXISTS runtime_permission_action_decisions (
    decision_id TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL REFERENCES runtime_turns(turn_id),
    action_ref TEXT NOT NULL,
    action_kind TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('trusted', 'requires_approval')),
    basis_kind TEXT NOT NULL,
    grant_id TEXT REFERENCES runtime_permission_grants(grant_id),
    checked_policy_revision INTEGER NOT NULL CHECK (checked_policy_revision >= 0),
    checked_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_stop_controls (
    stop_id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    stop_cutoff_queue_sequence INTEGER NOT NULL CHECK (stop_cutoff_queue_sequence >= 0),
    active_turn_id TEXT REFERENCES runtime_turns(turn_id),
    result_json TEXT NOT NULL,
    committed_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_steer_controls (
    steer_id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    target_turn_id TEXT NOT NULL UNIQUE REFERENCES runtime_turns(turn_id),
    inbound_event_id TEXT UNIQUE REFERENCES runtime_inbound_events(inbound_event_id),
    priority_turn_id TEXT UNIQUE REFERENCES runtime_turns(turn_id),
    stop_barrier_id TEXT UNIQUE REFERENCES runtime_stop_controls(stop_id),
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_steer_requests (
    steer_id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    target_turn_id TEXT NOT NULL REFERENCES runtime_turns(turn_id),
    inbound_event_id TEXT NOT NULL UNIQUE REFERENCES runtime_inbound_events(inbound_event_id),
    winner_control_id TEXT UNIQUE REFERENCES runtime_steer_controls(steer_id),
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_executor_residents (
    conversation_id TEXT PRIMARY KEY REFERENCES runtime_conversations(conversation_id),
    bot_id TEXT NOT NULL,
    provider TEXT NOT NULL CHECK (provider IN ('claude', 'codex')),
    owner_service_instance_id TEXT,
    owner_epoch INTEGER NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    owner_expires_at TEXT,
    admitted_at TEXT NOT NULL,
    last_used_at TEXT NOT NULL
  );

  CREATE INDEX IF NOT EXISTS runtime_executor_residents_by_bot
    ON runtime_executor_residents(bot_id, provider);

  CREATE TABLE IF NOT EXISTS runtime_workspace_lease_fences (
    workspace_root TEXT PRIMARY KEY,
    last_epoch INTEGER NOT NULL CHECK (last_epoch > 0),
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_workspace_leases (
    workspace_lease_id TEXT PRIMARY KEY,
    workspace_root TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('writable', 'read_only')),
    holder_service_instance_id TEXT NOT NULL,
    holder_conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    holder_turn_id TEXT NOT NULL REFERENCES runtime_turns(turn_id),
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch > 0),
    lease_expires_at TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'uncertain', 'released', 'expired')),
    acquired_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    released_at TEXT,
    UNIQUE (workspace_root, lease_epoch),
    CHECK (
      (state IN ('active', 'uncertain') AND released_at IS NULL)
      OR (state IN ('released', 'expired') AND released_at IS NOT NULL)
    )
  );

  CREATE UNIQUE INDEX IF NOT EXISTS runtime_workspace_leases_active_turn
    ON runtime_workspace_leases(holder_turn_id)
    WHERE state IN ('active', 'uncertain');

  CREATE INDEX IF NOT EXISTS runtime_workspace_leases_active_root
    ON runtime_workspace_leases(state, workspace_root, lease_expires_at);

  CREATE TABLE IF NOT EXISTS runtime_workspace_background_work (
    background_work_id TEXT PRIMARY KEY,
    workspace_lease_id TEXT NOT NULL REFERENCES runtime_workspace_leases(workspace_lease_id),
    holder_turn_id TEXT NOT NULL REFERENCES runtime_turns(turn_id),
    provider_task_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'completed', 'failed', 'unknown')),
    started_at TEXT NOT NULL,
    ended_at TEXT,
    error_json TEXT,
    UNIQUE (workspace_lease_id, provider_task_id),
    CHECK (
      (state = 'active' AND ended_at IS NULL)
      OR (state IN ('completed', 'failed', 'unknown') AND ended_at IS NOT NULL)
    )
  );

  CREATE INDEX IF NOT EXISTS runtime_workspace_background_work_active
    ON runtime_workspace_background_work(holder_turn_id, state);

  CREATE TRIGGER IF NOT EXISTS runtime_workspace_quarantine_uncertain_lease_insert
  AFTER INSERT ON runtime_workspace_leases
  WHEN NEW.state = 'uncertain'
  BEGIN
    UPDATE runtime_conversation_workspaces
    SET state = 'quarantined',
      provisioning_owner = NULL,
      provisioning_expires_at = NULL,
      quarantined_at = COALESCE(
        quarantined_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      ),
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      last_error_json = json_object(
        'code', 'workspace_runtime_uncertain',
        'message', 'Conversation workspace lease is uncertain.',
        'terminal', json('true'),
        'occurred_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      )
    WHERE conversation_id = NEW.holder_conversation_id
      AND state IN ('requested', 'provisioning', 'ready');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_workspace_quarantine_uncertain_lease_update
  AFTER UPDATE OF state ON runtime_workspace_leases
  WHEN NEW.state = 'uncertain'
  BEGIN
    UPDATE runtime_conversation_workspaces
    SET state = 'quarantined',
      provisioning_owner = NULL,
      provisioning_expires_at = NULL,
      quarantined_at = COALESCE(
        quarantined_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      ),
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      last_error_json = json_object(
        'code', 'workspace_runtime_uncertain',
        'message', 'Conversation workspace lease is uncertain.',
        'terminal', json('true'),
        'occurred_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      )
    WHERE conversation_id = NEW.holder_conversation_id
      AND state IN ('requested', 'provisioning', 'ready');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_workspace_quarantine_unknown_work_insert
  AFTER INSERT ON runtime_workspace_background_work
  WHEN NEW.state = 'unknown'
  BEGIN
    UPDATE runtime_conversation_workspaces
    SET state = 'quarantined',
      provisioning_owner = NULL,
      provisioning_expires_at = NULL,
      quarantined_at = COALESCE(
        quarantined_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      ),
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      last_error_json = json_object(
        'code', 'workspace_runtime_uncertain',
        'message', 'Conversation workspace background work is uncertain.',
        'terminal', json('true'),
        'occurred_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      )
    WHERE conversation_id = (
      SELECT holder_conversation_id
      FROM runtime_workspace_leases
      WHERE workspace_lease_id = NEW.workspace_lease_id
    )
      AND state IN ('requested', 'provisioning', 'ready');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_workspace_quarantine_unknown_work_update
  AFTER UPDATE OF state ON runtime_workspace_background_work
  WHEN NEW.state = 'unknown'
  BEGIN
    UPDATE runtime_conversation_workspaces
    SET state = 'quarantined',
      provisioning_owner = NULL,
      provisioning_expires_at = NULL,
      quarantined_at = COALESCE(
        quarantined_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      ),
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      last_error_json = json_object(
        'code', 'workspace_runtime_uncertain',
        'message', 'Conversation workspace background work is uncertain.',
        'terminal', json('true'),
        'occurred_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      )
    WHERE conversation_id = (
      SELECT holder_conversation_id
      FROM runtime_workspace_leases
      WHERE workspace_lease_id = NEW.workspace_lease_id
    )
      AND state IN ('requested', 'provisioning', 'ready');
  END;

  CREATE TABLE IF NOT EXISTS runtime_executor_leases (
    conversation_id TEXT PRIMARY KEY REFERENCES runtime_conversations(conversation_id),
    lease_owner TEXT,
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch > 0),
    turn_id TEXT UNIQUE REFERENCES runtime_turns(turn_id),
    attempt_id TEXT,
    attempt_no INTEGER CHECK (attempt_no IS NULL OR attempt_no > 0),
    lease_expires_at TEXT,
    updated_at TEXT NOT NULL,
    CHECK (
      (
        lease_owner IS NULL AND turn_id IS NULL AND attempt_id IS NULL
        AND attempt_no IS NULL AND lease_expires_at IS NULL
      )
      OR (
        lease_owner IS NOT NULL AND turn_id IS NOT NULL AND attempt_id IS NOT NULL
        AND attempt_no IS NOT NULL AND lease_expires_at IS NOT NULL
      )
    )
  );

  CREATE TABLE IF NOT EXISTS runtime_provider_attempts (
    attempt_id TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL REFERENCES runtime_turns(turn_id),
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch > 0),
    provider TEXT NOT NULL CHECK (provider IN ('claude', 'codex')),
    service_instance_id TEXT NOT NULL,
    executor_instance_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
      state IN (
        'starting', 'running', 'waiting_user', 'redirecting',
        'retry_wait', 'retrying', 'recovering',
        'completed', 'stopped', 'cancelled', 'interrupted', 'failed', 'timed_out'
      )
    ),
    runtime_instance_id TEXT,
    runtime_evidence_json TEXT,
    last_provider_event_at TEXT,
    last_lease_renewed_at TEXT NOT NULL,
    retry_backoff_ms INTEGER CHECK (retry_backoff_ms IS NULL OR retry_backoff_ms >= 0),
    next_retry_at TEXT,
    side_effect_status TEXT NOT NULL DEFAULT 'none'
      CHECK (side_effect_status IN ('none', 'known', 'unknown')),
    error_json TEXT,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (turn_id, attempt_no),
    UNIQUE (turn_id, lease_epoch)
  );

  CREATE INDEX IF NOT EXISTS runtime_provider_attempts_active
    ON runtime_provider_attempts(turn_id, state, service_instance_id);

  ${CONVERSATION_WORKSPACE_RUNTIME_QUARANTINE_TRIGGERS}

  CREATE TRIGGER IF NOT EXISTS runtime_background_task_turn_state_update
  AFTER UPDATE OF state ON runtime_turns
  BEGIN
    UPDATE runtime_background_tasks
    SET
      state = NEW.state,
      started_at = CASE
        WHEN NEW.state IN (
          'starting', 'running', 'waiting_user', 'redirecting', 'recovering',
          'completed', 'stopped', 'cancelled', 'failed', 'interrupted', 'timed_out'
        ) THEN COALESCE(started_at, NEW.committed_at)
        ELSE started_at
      END,
      completed_at = CASE
        WHEN NEW.state IN (
          'completed', 'stopped', 'cancelled', 'failed', 'interrupted', 'timed_out'
        ) THEN COALESCE(completed_at, NEW.committed_at)
        ELSE NULL
      END
    WHERE execution_turn_id = NEW.turn_id;
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_background_task_attempt_update
  AFTER UPDATE OF side_effect_status ON runtime_provider_attempts
  BEGIN
    UPDATE runtime_background_tasks
    SET side_effect_status = NEW.side_effect_status
    WHERE execution_turn_id = NEW.turn_id;
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_input_group_turn_cancel
  AFTER UPDATE OF state ON runtime_turns
  WHEN NEW.state IN ('stopped', 'cancelled', 'interrupted', 'failed', 'timed_out')
  BEGIN
    UPDATE runtime_input_groups
    SET state = 'cancelled', updated_at = NEW.committed_at
    WHERE execution_turn_id = NEW.turn_id AND state = 'collecting';
  END;

  CREATE TABLE IF NOT EXISTS runtime_execution_recoveries (
    recovery_id TEXT PRIMARY KEY,
    recovery_version INTEGER NOT NULL DEFAULT 1 CHECK (recovery_version > 0),
    turn_id TEXT NOT NULL UNIQUE REFERENCES runtime_turns(turn_id),
    attempt_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch > 0),
    provider TEXT NOT NULL CHECK (provider IN ('claude', 'codex')),
    prior_service_instance_id TEXT,
    prior_executor_instance_id TEXT,
    prior_runtime_instance_id TEXT,
    recovery_kind TEXT NOT NULL CHECK (
      recovery_kind IN ('provider_failure', 'startup_reconciliation', 'sweep_reconciliation')
    ),
    state TEXT NOT NULL CHECK (state IN ('waiting_decision', 'authorized', 'stopped')),
    side_effect_status TEXT NOT NULL CHECK (side_effect_status = 'unknown'),
    notice_event_sequence INTEGER NOT NULL CHECK (notice_event_sequence > 0),
    interaction_id TEXT NOT NULL UNIQUE REFERENCES runtime_interactions(interaction_id),
    error_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE INDEX IF NOT EXISTS runtime_execution_recovery_state
    ON runtime_execution_recoveries(state, created_at);

  CREATE TABLE IF NOT EXISTS runtime_provider_stop_incidents (
    incident_id TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL UNIQUE REFERENCES runtime_turns(turn_id),
    attempt_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch > 0),
    provider_stop_status TEXT NOT NULL,
    side_effect_status TEXT NOT NULL CHECK (side_effect_status = 'unknown'),
    disposition TEXT NOT NULL CHECK (disposition = 'manual_recovery_required'),
    error_json TEXT NOT NULL,
    outbox_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_provider_event_diagnostics (
    diagnostic_id TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL REFERENCES runtime_turns(turn_id),
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    provider TEXT NOT NULL CHECK (provider IN ('claude', 'codex')),
    attempt_id TEXT,
    attempt_no INTEGER CHECK (attempt_no IS NULL OR attempt_no > 0),
    lease_epoch INTEGER CHECK (lease_epoch IS NULL OR lease_epoch > 0),
    current_turn_state TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    descriptor_json TEXT NOT NULL,
    observed_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_observability_instances (
    service_instance_id TEXT PRIMARY KEY,
    host_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    service_version INTEGER NOT NULL DEFAULT 1 CHECK (service_version > 0),
    snapshot_version INTEGER NOT NULL DEFAULT 0 CHECK (snapshot_version >= 0),
    last_reconciliation_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_executor_service_instances (
    service_instance_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL CHECK (provider IN ('claude', 'codex')),
    provider_transport TEXT NOT NULL CHECK (
      provider_transport IN ('claude_agent_sdk', 'official_app_server', 'injected_test_seam')
    ),
    release_ref TEXT,
    upgrade_id TEXT REFERENCES runtime_upgrade_runs(upgrade_id),
    started_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    revoked_at TEXT,
    CHECK (
      (upgrade_id IS NULL AND release_ref IS NULL)
      OR (upgrade_id IS NOT NULL AND release_ref IS NOT NULL)
    )
  );

  CREATE TABLE IF NOT EXISTS runtime_upgrade_runs (
    upgrade_id TEXT PRIMARY KEY,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('installation', 'bot')),
    bot_id TEXT,
    from_release TEXT NOT NULL,
    to_release TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
      state IN (
        'preflight', 'snapshotted', 'maintenance', 'drained', 'migrating',
        'health_check', 'ready_to_commit', 'rollback_required',
        'committed', 'rolled_back'
      )
    ),
    state_version INTEGER NOT NULL CHECK (state_version > 0),
    preflight_json TEXT NOT NULL,
    snapshot_json TEXT,
    migration_json TEXT,
    health_json TEXT,
    failure_json TEXT,
    maintenance_started_at TEXT,
    committed_at TEXT,
    rolled_back_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
      (scope_kind = 'installation' AND bot_id IS NULL)
      OR (scope_kind = 'bot' AND bot_id IS NOT NULL AND length(bot_id) > 0)
    )
  );

  CREATE UNIQUE INDEX IF NOT EXISTS runtime_upgrade_one_active_run
    ON runtime_upgrade_runs((1))
    WHERE state NOT IN ('committed', 'rolled_back');

  CREATE TABLE IF NOT EXISTS runtime_upgrade_events (
    upgrade_id TEXT NOT NULL REFERENCES runtime_upgrade_runs(upgrade_id),
    step_key TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    result_json TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    PRIMARY KEY (upgrade_id, step_key)
  );

  CREATE TABLE IF NOT EXISTS runtime_upgrade_effects (
    upgrade_id TEXT NOT NULL REFERENCES runtime_upgrade_runs(upgrade_id),
    step_key TEXT NOT NULL,
    step_id TEXT NOT NULL UNIQUE,
    input_hash TEXT NOT NULL,
    input_json TEXT NOT NULL DEFAULT '{}',
    state TEXT NOT NULL CHECK (state IN ('claimed', 'completed')),
    claim_owner TEXT NOT NULL,
    claim_attempt INTEGER NOT NULL CHECK (claim_attempt > 0),
    claim_expires_at TEXT,
    result_json TEXT,
    committed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (upgrade_id, step_key),
    CHECK (step_id = upgrade_id || ':' || step_key),
    CHECK (
      (state = 'claimed' AND claim_expires_at IS NOT NULL AND result_json IS NULL)
      OR (state = 'completed' AND claim_expires_at IS NULL AND result_json IS NOT NULL)
    )
  );

  CREATE TABLE IF NOT EXISTS runtime_active_release_fences (
    scope_key TEXT PRIMARY KEY,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('installation', 'bot')),
    bot_id TEXT,
    upgrade_id TEXT NOT NULL UNIQUE REFERENCES runtime_upgrade_runs(upgrade_id),
    release_ref TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    activated_at TEXT NOT NULL,
    CHECK (
      (scope_kind = 'installation' AND bot_id IS NULL AND scope_key = 'installation')
      OR (scope_kind = 'bot' AND bot_id IS NOT NULL AND scope_key = 'bot:' || bot_id)
    )
  );

  CREATE TABLE IF NOT EXISTS runtime_upgrade_release_fence_history (
    upgrade_id TEXT PRIMARY KEY REFERENCES runtime_upgrade_runs(upgrade_id),
    scope_key TEXT NOT NULL,
    previous_fence_json TEXT,
    target_generation INTEGER NOT NULL CHECK (target_generation > 0),
    activated_at TEXT NOT NULL,
    restored_at TEXT
  );

  CREATE TABLE IF NOT EXISTS runtime_upgrade_force_notices (
    upgrade_id TEXT NOT NULL REFERENCES runtime_upgrade_runs(upgrade_id),
    turn_id TEXT NOT NULL REFERENCES runtime_turns(turn_id),
    outbox_id TEXT NOT NULL UNIQUE REFERENCES runtime_outbox(outbox_id),
    delivery_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    PRIMARY KEY (upgrade_id, turn_id)
  );

  CREATE TABLE IF NOT EXISTS runtime_legacy_migration_records (
    upgrade_id TEXT NOT NULL REFERENCES runtime_upgrade_runs(upgrade_id),
    legacy_kind TEXT NOT NULL CHECK (
      legacy_kind IN ('c4', 'global_provider_lineage', 'scheduler', 'runtime_control')
    ),
    legacy_record_id TEXT NOT NULL,
    legacy_state TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (
      disposition IN (
        'migrated_pending', 'quarantined_ambiguous',
        'quarantined_invalid_identity', 'quarantined_side_effect_unknown', 'retained_delivered',
        'retained_failed', 'archived_unmapped', 'retained_history',
        'invalidated_audit_only', 'migrated_scheduler', 'skipped_missed',
        'restored_pending', 'restored_scheduler'
      )
    ),
    payload_hash TEXT NOT NULL,
    audit_json TEXT NOT NULL,
    migrated_turn_id TEXT REFERENCES runtime_turns(turn_id),
    imported_by_upgrade INTEGER NOT NULL DEFAULT 0 CHECK (imported_by_upgrade IN (0, 1)),
    executable INTEGER NOT NULL DEFAULT 0 CHECK (executable = 0),
    read_only INTEGER NOT NULL DEFAULT 1 CHECK (read_only = 1),
    created_at TEXT NOT NULL,
    PRIMARY KEY (upgrade_id, legacy_kind, legacy_record_id),
    CHECK (
      legacy_kind != 'runtime_control'
      OR (disposition = 'invalidated_audit_only' AND migrated_turn_id IS NULL)
    ),
    CHECK (
      disposition IN ('migrated_pending', 'migrated_scheduler') OR migrated_turn_id IS NULL
    ),
    CHECK (
      imported_by_upgrade = 0 OR migrated_turn_id IS NOT NULL
    )
  );

  CREATE TABLE IF NOT EXISTS runtime_legacy_migration_payloads (
    upgrade_id TEXT NOT NULL,
    legacy_kind TEXT NOT NULL,
    legacy_record_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (upgrade_id, legacy_kind, legacy_record_id),
    FOREIGN KEY (upgrade_id, legacy_kind, legacy_record_id)
      REFERENCES runtime_legacy_migration_records(upgrade_id, legacy_kind, legacy_record_id)
  );

  CREATE TABLE IF NOT EXISTS runtime_legacy_migration_audit_payloads (
    upgrade_id TEXT NOT NULL,
    legacy_kind TEXT NOT NULL,
    legacy_record_id TEXT NOT NULL,
    audit_payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (upgrade_id, legacy_kind, legacy_record_id),
    FOREIGN KEY (upgrade_id, legacy_kind, legacy_record_id)
      REFERENCES runtime_legacy_migration_records(upgrade_id, legacy_kind, legacy_record_id),
    CHECK (legacy_kind = 'runtime_control')
  );

  CREATE TABLE IF NOT EXISTS runtime_legacy_migration_durable_facts (
    upgrade_id TEXT NOT NULL,
    legacy_kind TEXT NOT NULL CHECK (legacy_kind IN ('c4', 'scheduler')),
    legacy_record_id TEXT NOT NULL,
    fact_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (upgrade_id, legacy_kind, legacy_record_id),
    FOREIGN KEY (upgrade_id, legacy_kind, legacy_record_id)
      REFERENCES runtime_legacy_migration_records(upgrade_id, legacy_kind, legacy_record_id)
  );

  CREATE TABLE IF NOT EXISTS runtime_legacy_unmapped_messages (
    region TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    chat_type TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    native_thread_or_topic_id TEXT,
    platform_message_id TEXT NOT NULL,
    upgrade_id TEXT NOT NULL,
    legacy_kind TEXT NOT NULL CHECK (legacy_kind = 'global_provider_lineage'),
    legacy_record_id TEXT NOT NULL,
    recent_c4_context_json TEXT NOT NULL,
    memory_handoff TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (
      region, tenant_id, channel, bot_id, platform_message_id, upgrade_id
    ),
    FOREIGN KEY (upgrade_id, legacy_kind, legacy_record_id)
      REFERENCES runtime_legacy_migration_records(upgrade_id, legacy_kind, legacy_record_id)
  );

  CREATE TABLE IF NOT EXISTS runtime_legacy_migration_notices (
    upgrade_id TEXT NOT NULL,
    legacy_kind TEXT NOT NULL,
    legacy_record_id TEXT NOT NULL,
    outbox_id TEXT NOT NULL UNIQUE REFERENCES runtime_outbox(outbox_id),
    delivery_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('pending', 'delivered')),
    proof_hash TEXT,
    proof_json TEXT,
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    PRIMARY KEY (upgrade_id, legacy_kind, legacy_record_id),
    FOREIGN KEY (upgrade_id, legacy_kind, legacy_record_id)
      REFERENCES runtime_legacy_migration_records(upgrade_id, legacy_kind, legacy_record_id),
    CHECK (
      (state = 'pending' AND proof_hash IS NULL AND proof_json IS NULL AND delivered_at IS NULL)
      OR (state = 'delivered' AND proof_hash IS NOT NULL AND proof_json IS NOT NULL
        AND delivered_at IS NOT NULL)
    )
  );

  CREATE TRIGGER IF NOT EXISTS runtime_legacy_migration_audit_update_immutable
  BEFORE UPDATE ON runtime_legacy_migration_records
  BEGIN
    SELECT RAISE(ABORT, 'legacy migration audit is immutable');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_legacy_migration_audit_delete_immutable
  BEFORE DELETE ON runtime_legacy_migration_records
  BEGIN
    SELECT RAISE(ABORT, 'legacy migration audit is immutable');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_legacy_migration_payload_update_immutable
  BEFORE UPDATE ON runtime_legacy_migration_payloads
  BEGIN
    SELECT RAISE(ABORT, 'legacy migration payload is immutable');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_legacy_migration_audit_payload_update_immutable
  BEFORE UPDATE ON runtime_legacy_migration_audit_payloads
  BEGIN
    SELECT RAISE(ABORT, 'legacy migration audit payload is immutable');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_legacy_migration_durable_fact_update_immutable
  BEFORE UPDATE ON runtime_legacy_migration_durable_facts
  BEGIN
    SELECT RAISE(ABORT, 'legacy migration durable fact is immutable');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_legacy_migration_durable_fact_delete_immutable
  BEFORE DELETE ON runtime_legacy_migration_durable_facts
  BEGIN
    SELECT RAISE(ABORT, 'legacy migration durable fact is immutable');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_legacy_unmapped_message_update_immutable
  BEFORE UPDATE ON runtime_legacy_unmapped_messages
  BEGIN
    SELECT RAISE(ABORT, 'legacy unmapped message identity is immutable');
  END;

  CREATE TRIGGER IF NOT EXISTS runtime_legacy_unmapped_message_delete_immutable
  BEFORE DELETE ON runtime_legacy_unmapped_messages
  BEGIN
    SELECT RAISE(ABORT, 'legacy unmapped message identity is immutable');
  END;

  CREATE TABLE IF NOT EXISTS runtime_operations_policies (
    policy_id TEXT NOT NULL,
    policy_version INTEGER NOT NULL CHECK (policy_version > 0),
    artifact_hash TEXT NOT NULL,
    policy_json TEXT NOT NULL,
    registered_at TEXT NOT NULL,
    PRIMARY KEY (policy_id, policy_version)
  );

  CREATE TABLE IF NOT EXISTS runtime_operations_controls (
    caller_namespace TEXT NOT NULL,
    control_id TEXT NOT NULL,
    action TEXT NOT NULL,
    target_json TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    normalized_request_json TEXT NOT NULL,
    control_result_version INTEGER NOT NULL CHECK (control_result_version > 0),
    latest_result_json TEXT NOT NULL,
    audit_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (caller_namespace, control_id)
  );

  CREATE TABLE IF NOT EXISTS runtime_operations_audit (
    audit_id TEXT PRIMARY KEY,
    caller_namespace TEXT NOT NULL,
    control_id TEXT NOT NULL,
    action TEXT NOT NULL,
    outcome TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    capability TEXT,
    grant_id TEXT,
    policy_id TEXT NOT NULL,
    policy_version INTEGER NOT NULL CHECK (policy_version > 0),
    target_json TEXT NOT NULL,
    expected_version_json TEXT,
    previous_target_version INTEGER,
    target_version INTEGER,
    reason TEXT NOT NULL,
    error_json TEXT,
    committed_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_operations_idempotency_conflicts (
    caller_namespace TEXT NOT NULL,
    control_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    result_json TEXT NOT NULL,
    audit_id TEXT NOT NULL UNIQUE,
    committed_at TEXT NOT NULL,
    PRIMARY KEY (caller_namespace, control_id, request_hash)
  );

  CREATE TABLE IF NOT EXISTS runtime_operations_reconciliation_intents (
    intent_id TEXT PRIMARY KEY,
    service_instance_id TEXT NOT NULL,
    caller_namespace TEXT NOT NULL,
    expected_service_version INTEGER NOT NULL CHECK (expected_service_version > 0),
    state TEXT NOT NULL CHECK (state IN ('pending', 'completed', 'failed')),
    control_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (service_instance_id, caller_namespace, control_id)
  );

  CREATE TABLE IF NOT EXISTS runtime_normalized_events (
    event_id TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL REFERENCES runtime_turns(turn_id),
    event_sequence INTEGER NOT NULL CHECK (event_sequence > 0),
    turn_version INTEGER NOT NULL CHECK (turn_version > 0),
    event_json TEXT NOT NULL,
    persisted_at TEXT NOT NULL,
    UNIQUE (turn_id, event_sequence),
    UNIQUE (turn_id, turn_version)
  );

  CREATE TABLE IF NOT EXISTS runtime_interactions (
    interaction_id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    turn_id TEXT REFERENCES runtime_turns(turn_id),
    lineage_id TEXT REFERENCES runtime_lineages(lineage_id),
    parent_type TEXT NOT NULL CHECK (
      parent_type IN ('provider_turn', 'security_control', 'recovery_control')
    ),
    parent_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    state TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    handoff_state TEXT NOT NULL,
    handoff_version INTEGER CHECK (handoff_version IS NULL OR handoff_version > 0),
    request_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (parent_type, parent_id, ordinal),
    CHECK (
      (parent_type = 'security_control' AND turn_id IS NULL AND lineage_id IS NULL)
      OR (parent_type != 'security_control' AND turn_id IS NOT NULL)
    )
  );

  CREATE TABLE IF NOT EXISTS runtime_interaction_answers (
    answer_id TEXT PRIMARY KEY,
    interaction_id TEXT NOT NULL UNIQUE REFERENCES runtime_interactions(interaction_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    payload_hash TEXT NOT NULL,
    answer_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    committed_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_interaction_handoffs (
    handoff_id TEXT PRIMARY KEY,
    interaction_id TEXT NOT NULL UNIQUE REFERENCES runtime_interactions(interaction_id),
    answer_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL,
    parent_type TEXT NOT NULL CHECK (
      parent_type IN ('provider_turn', 'security_control', 'recovery_control')
    ),
    provider_attempt_id TEXT,
    handoff_attempt_id TEXT,
    handoff_attempt_no INTEGER CHECK (
      handoff_attempt_no IS NULL OR handoff_attempt_no > 0
    ),
    lease_epoch INTEGER CHECK (lease_epoch IS NULL OR lease_epoch > 0),
    record_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
      (parent_type = 'provider_turn' AND provider_attempt_id IS NOT NULL AND lease_epoch IS NOT NULL)
      OR (parent_type != 'provider_turn' AND provider_attempt_id IS NULL AND lease_epoch IS NULL)
    )
  );

  CREATE TABLE IF NOT EXISTS runtime_interaction_audit (
    audit_id TEXT PRIMARY KEY,
    interaction_id TEXT NOT NULL REFERENCES runtime_interactions(interaction_id),
    handoff_id TEXT NOT NULL REFERENCES runtime_interaction_handoffs(handoff_id),
    outcome TEXT NOT NULL,
    provider_attempt_id TEXT,
    lease_epoch INTEGER CHECK (lease_epoch IS NULL OR lease_epoch > 0),
    acknowledgement_json TEXT NOT NULL,
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_outbox ${OUTBOX_TABLE_SCHEMA};

  CREATE TABLE IF NOT EXISTS runtime_outbox_claim_snapshots (
    outbox_id TEXT NOT NULL,
    delivery_attempt_id TEXT NOT NULL UNIQUE,
    delivery_attempt_no INTEGER NOT NULL CHECK (delivery_attempt_no > 0),
    outbox_lease_epoch INTEGER NOT NULL CHECK (outbox_lease_epoch > 0),
    lease_owner TEXT NOT NULL,
    command_json TEXT NOT NULL,
    command_hash TEXT NOT NULL,
    claimed_at TEXT NOT NULL,
    PRIMARY KEY (outbox_id, outbox_lease_epoch)
  );

  CREATE TABLE IF NOT EXISTS runtime_outbox_reconciliations (
    reconciliation_id TEXT PRIMARY KEY,
    outbox_id TEXT NOT NULL REFERENCES runtime_outbox(outbox_id) ON DELETE CASCADE,
    reconciliation_epoch INTEGER NOT NULL CHECK (reconciliation_epoch > 0),
    state TEXT NOT NULL CHECK (state IN (
      'claimed',
      'confirmed',
      'replacement_authorized',
      'replacement_fenced',
      'failed',
      'resolved'
    )),
    lease_owner TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    lease_expires_epoch_ms INTEGER NOT NULL,
    delivery_attempt_id TEXT NOT NULL,
    delivery_attempt_no INTEGER NOT NULL CHECK (delivery_attempt_no > 0),
    outbox_lease_epoch INTEGER NOT NULL CHECK (outbox_lease_epoch > 0),
    delivery_lease_owner TEXT NOT NULL,
    command_json TEXT NOT NULL,
    command_hash TEXT NOT NULL,
    target_platform_message_id TEXT NOT NULL,
    pre_action_fenced_at TEXT NOT NULL,
    evidence_json TEXT,
    evidence_recorded_at TEXT,
    replacement_fenced_at TEXT,
    result_json TEXT,
    resolved_at TEXT,
    last_error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (outbox_id, reconciliation_epoch)
  );

  CREATE INDEX IF NOT EXISTS runtime_outbox_reconciliations_by_outbox
    ON runtime_outbox_reconciliations(outbox_id, reconciliation_epoch DESC);

  CREATE TABLE IF NOT EXISTS runtime_outbox_operator_reconciliations (
    reconciliation_id TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL UNIQUE,
    outbox_id TEXT NOT NULL,
    delivery_attempt_id TEXT NOT NULL,
    delivery_attempt_no INTEGER NOT NULL CHECK (delivery_attempt_no > 0),
    outbox_lease_epoch INTEGER NOT NULL CHECK (outbox_lease_epoch > 0),
    decision TEXT NOT NULL CHECK (decision = 'platform_readback_no_effect'),
    previous_status TEXT NOT NULL,
    terminal_status TEXT NOT NULL CHECK (terminal_status = 'superseded'),
    actor_id TEXT NOT NULL,
    authorization_ref TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    evidence_hash TEXT NOT NULL,
    replacement_outbox_id TEXT,
    committed_at TEXT NOT NULL,
    UNIQUE (outbox_id, delivery_attempt_id, delivery_attempt_no, outbox_lease_epoch)
  );

  CREATE TABLE IF NOT EXISTS runtime_outbox_delivery_corrections (
    correction_id TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL UNIQUE,
    outbox_id TEXT NOT NULL,
    delivery_attempt_id TEXT NOT NULL,
    delivery_attempt_no INTEGER NOT NULL CHECK (delivery_attempt_no > 0),
    outbox_lease_epoch INTEGER NOT NULL CHECK (outbox_lease_epoch > 0),
    decision TEXT NOT NULL CHECK (decision = 'platform_readback_delivery_absent'),
    previous_status TEXT NOT NULL CHECK (previous_status = 'delivered'),
    terminal_status TEXT NOT NULL CHECK (terminal_status = 'dead_letter'),
    actor_id TEXT NOT NULL,
    authorization_ref TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    evidence_hash TEXT NOT NULL,
    fallback_outbox_id TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    UNIQUE (outbox_id, delivery_attempt_id, delivery_attempt_no, outbox_lease_epoch)
  );

  CREATE TABLE IF NOT EXISTS runtime_delivery_lanes (
    lane_key TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL UNIQUE REFERENCES runtime_turns(turn_id),
    aggregate_type TEXT NOT NULL,
    lane_identity_version INTEGER NOT NULL DEFAULT 1
      CHECK (lane_identity_version IN (0, 1)),
    delivery_mode TEXT NOT NULL DEFAULT 'main'
      CHECK (delivery_mode IN ('main', 'text')),
    target_json TEXT NOT NULL,
    mapping_json TEXT NOT NULL,
    platform_message_id TEXT,
    applied_platform_version,
    last_delivery_id TEXT,
    last_applied_version INTEGER NOT NULL DEFAULT 0 CHECK (last_applied_version >= 0),
    last_delivered_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_projection_snapshots (
    projection_id TEXT PRIMARY KEY,
    lane_key TEXT NOT NULL REFERENCES runtime_delivery_lanes(lane_key),
    turn_id TEXT NOT NULL REFERENCES runtime_turns(turn_id),
    aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
    event_sequence_through INTEGER NOT NULL CHECK (event_sequence_through > 0),
    render_model_json TEXT NOT NULL,
    critical INTEGER NOT NULL CHECK (critical IN (0, 1)),
    terminal INTEGER NOT NULL CHECK (terminal IN (0, 1)),
    status TEXT NOT NULL,
    materialized_outbox_id TEXT REFERENCES runtime_outbox(outbox_id),
    created_at TEXT NOT NULL,
    UNIQUE (lane_key, aggregate_version)
  );

  CREATE TABLE IF NOT EXISTS runtime_inbound_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    inbound_event_id TEXT NOT NULL UNIQUE REFERENCES runtime_inbound_events(inbound_event_id),
    payload_hash TEXT NOT NULL,
    first_result_json TEXT NOT NULL,
    committed_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_message_mappings (
    region TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    bot_id TEXT NOT NULL,
    platform_message_id TEXT NOT NULL,
    conversation_id TEXT REFERENCES runtime_conversations(conversation_id),
    turn_id TEXT REFERENCES runtime_turns(turn_id),
    lineage_id TEXT REFERENCES runtime_lineages(lineage_id),
    binding_state TEXT NOT NULL,
    reason TEXT,
    mapping_id TEXT NOT NULL UNIQUE,
    mapping_version INTEGER NOT NULL CHECK (mapping_version > 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY (region, tenant_id, channel, bot_id, platform_message_id)
  );

  CREATE TABLE IF NOT EXISTS runtime_reply_mapping_recoveries (
    recovery_id TEXT PRIMARY KEY,
    recovery_version INTEGER NOT NULL DEFAULT 1 CHECK (recovery_version > 0),
    turn_id TEXT NOT NULL UNIQUE REFERENCES runtime_turns(turn_id),
    mapping_id TEXT NOT NULL UNIQUE,
    source_platform_message_id TEXT NOT NULL,
    reason TEXT NOT NULL CHECK (
      reason IN (
        'mapping_missing',
        'mapping_corrupt',
        'mapping_unbound',
        'provider_lineage_invalid'
      )
    ),
    candidate_lineage_id TEXT REFERENCES runtime_lineages(lineage_id),
    side_effect_status TEXT NOT NULL
      CHECK (side_effect_status IN ('none', 'known', 'unknown')),
    state TEXT NOT NULL CHECK (
      state IN (
        'queued',
        'notice_pending',
        'native_recovery_claimed',
        'native_recovery_not_applicable',
        'waiting_decision',
        'bound',
        'rejected',
        'failed'
      )
    ),
    notice_event_sequence INTEGER CHECK (
      notice_event_sequence IS NULL OR notice_event_sequence > 0
    ),
    native_recovery_attempt_count INTEGER NOT NULL DEFAULT 0
      CHECK (native_recovery_attempt_count IN (0, 1)),
    native_recovery_attempt_id TEXT,
    native_recovery_status TEXT,
    native_recovery_result_json TEXT,
    native_recovery_owner_service_instance_id TEXT,
    native_recovery_claim_expires_at TEXT,
    bound_lineage_id TEXT REFERENCES runtime_lineages(lineage_id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_compact_turn_summaries (
    turn_id TEXT PRIMARY KEY REFERENCES runtime_turns(turn_id),
    conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
    lineage_id TEXT REFERENCES runtime_lineages(lineage_id),
    terminal_state TEXT NOT NULL CHECK (
      terminal_state IN ('completed', 'stopped', 'cancelled', 'interrupted', 'failed', 'timed_out')
    ),
    terminal_at TEXT NOT NULL,
    final_text TEXT,
    final_error_json TEXT,
    lane_key TEXT,
    mapping_id TEXT,
    aggregate_version INTEGER,
    event_sequence_through INTEGER,
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_retention_entries (
    record_kind TEXT NOT NULL,
    record_id TEXT NOT NULL,
    turn_id TEXT,
    retention_class TEXT NOT NULL CHECK (
      retention_class IN ('raw_detail_7d', 'terminal_detail_30d', 'security_audit_180d')
    ),
    anchor_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    disposal_kind TEXT NOT NULL CHECK (disposal_kind IN ('delete', 'redact')),
    registered_at TEXT NOT NULL,
    disposed_at TEXT,
    deletion_audit_id TEXT,
    PRIMARY KEY (record_kind, record_id),
    CHECK (
      (disposed_at IS NULL AND deletion_audit_id IS NULL)
      OR (disposed_at IS NOT NULL AND deletion_audit_id IS NOT NULL)
    )
  );

  CREATE INDEX IF NOT EXISTS runtime_retention_entries_due
    ON runtime_retention_entries(expires_at, record_kind)
    WHERE disposed_at IS NULL;

  CREATE TABLE IF NOT EXISTS runtime_retention_deletion_audit (
    audit_id TEXT PRIMARY KEY,
    sweep_id TEXT NOT NULL,
    record_kind TEXT NOT NULL,
    record_id TEXT NOT NULL,
    turn_id TEXT,
    retention_class TEXT NOT NULL,
    anchor_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    disposal_kind TEXT NOT NULL,
    content_digest_version INTEGER NOT NULL DEFAULT 1 CHECK (content_digest_version = 1),
    content_sha256 TEXT NOT NULL,
    deleted_at TEXT NOT NULL,
    UNIQUE (record_kind, record_id)
  );

  CREATE TABLE IF NOT EXISTS runtime_retention_sweeps (
    sweep_id TEXT PRIMARY KEY,
    transaction_time TEXT NOT NULL,
    candidate_count INTEGER NOT NULL CHECK (candidate_count >= 0),
    disposed_count INTEGER NOT NULL CHECK (disposed_count >= 0),
    oldest_expiry_at TEXT,
    max_lag_ms INTEGER NOT NULL CHECK (max_lag_ms >= 0),
    observed_lag_ms INTEGER NOT NULL CHECK (observed_lag_ms >= 0),
    max_lag_exceeded INTEGER NOT NULL CHECK (max_lag_exceeded IN (0, 1)),
    busy_retry_count INTEGER NOT NULL CHECK (busy_retry_count >= 0),
    committed_at TEXT NOT NULL
  );

`;

const OUTBOX_V2_SCHEMA = `CREATE TABLE runtime_outbox ${OUTBOX_TABLE_SCHEMA};`;

const OUTBOX_COLUMNS = Object.freeze([
  'outbox_id',
  'delivery_id',
  'aggregate_type',
  'aggregate_id',
  'turn_id',
  'control_id',
  'lane_key',
  'predecessor_delivery_id',
  'aggregate_version',
  'status',
  'command_json',
  'claimed_command_hash',
  'priority',
  'supersedable',
  'terminal',
  'attempt_count',
  'delivery_attempt_id',
  'delivery_attempt_no',
  'outbox_lease_epoch',
  'lease_owner',
  'lease_expires_at',
  'lease_expires_epoch_ms',
  'pre_action_fenced_at',
  'last_attempt_at',
  'next_attempt_at',
  'last_error_json',
  'result_json',
  'created_at',
  'updated_at',
]);

function addColumnIfMissing(database, tableName, columnName, definition) {
  const columns = database.prepare(`PRAGMA table_info(${tableName})`).all();
  if (columns.some(({ name }) => name === columnName)) return;
  database.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
}

function migrateOperationsReconciliationIntents(database) {
  const columns = database.prepare(
    "PRAGMA table_info('runtime_operations_reconciliation_intents')",
  ).all();
  if (columns.some(({ name }) => name === 'caller_namespace')) return;
  const migrate = database.transaction(() => {
    database.exec(`
      CREATE TABLE runtime_operations_reconciliation_intents_v2 (
        intent_id TEXT PRIMARY KEY,
        service_instance_id TEXT NOT NULL,
        caller_namespace TEXT NOT NULL,
        expected_service_version INTEGER NOT NULL CHECK (expected_service_version > 0),
        state TEXT NOT NULL CHECK (state IN ('pending', 'completed', 'failed')),
        control_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE (service_instance_id, caller_namespace, control_id)
      );

      INSERT INTO runtime_operations_reconciliation_intents_v2 (
        intent_id, service_instance_id, caller_namespace, expected_service_version,
        state, control_id, created_at, updated_at
      )
      SELECT intent.intent_id, intent.service_instance_id,
        (
          SELECT control.caller_namespace
          FROM runtime_operations_controls AS control
          WHERE control.control_id = intent.control_id
            AND control.action = 'reconcile'
            AND json_extract(control.target_json, '$.service_instance_id')
              = intent.service_instance_id
          ORDER BY control.created_at, control.caller_namespace
          LIMIT 1
        ),
        intent.expected_service_version, intent.state, intent.control_id,
        intent.created_at, intent.updated_at
      FROM runtime_operations_reconciliation_intents AS intent;

      DROP TABLE runtime_operations_reconciliation_intents;
      ALTER TABLE runtime_operations_reconciliation_intents_v2
        RENAME TO runtime_operations_reconciliation_intents;
    `);
  });
  migrate.immediate();
}

function migrateInteractionControlStorage(database) {
  const columns = database.prepare("PRAGMA table_info('runtime_interactions')").all();
  if (columns.some(({ name }) => name === 'parent_type')) return;

  database.pragma('foreign_keys = OFF');
  database.pragma('legacy_alter_table = ON');
  try {
    const migrate = database.transaction(() => {
      database.exec(`
        ALTER TABLE runtime_interactions RENAME TO runtime_interactions_issue16;
        ALTER TABLE runtime_interaction_answers RENAME TO runtime_interaction_answers_issue16;
        ALTER TABLE runtime_interaction_handoffs RENAME TO runtime_interaction_handoffs_issue16;
        ALTER TABLE runtime_interaction_audit RENAME TO runtime_interaction_audit_issue16;

        CREATE TABLE runtime_interactions (
          interaction_id TEXT PRIMARY KEY,
          conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
          turn_id TEXT REFERENCES runtime_turns(turn_id),
          lineage_id TEXT REFERENCES runtime_lineages(lineage_id),
          parent_type TEXT NOT NULL CHECK (
            parent_type IN ('provider_turn', 'security_control', 'recovery_control')
          ),
          parent_id TEXT NOT NULL,
          ordinal INTEGER NOT NULL CHECK (ordinal > 0),
          state TEXT NOT NULL,
          version INTEGER NOT NULL CHECK (version > 0),
          handoff_state TEXT NOT NULL,
          handoff_version INTEGER CHECK (handoff_version IS NULL OR handoff_version > 0),
          request_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE (parent_type, parent_id, ordinal),
          CHECK (
            (parent_type = 'security_control' AND turn_id IS NULL AND lineage_id IS NULL)
            OR (parent_type != 'security_control' AND turn_id IS NOT NULL)
          )
        );
        INSERT INTO runtime_interactions (
          interaction_id, conversation_id, turn_id, lineage_id, parent_type, parent_id,
          ordinal, state, version, handoff_state, handoff_version, request_json,
          created_at, updated_at
        )
        SELECT interaction_id, conversation_id, turn_id, lineage_id, 'provider_turn', turn_id,
          ordinal, state, version, handoff_state, handoff_version, request_json,
          created_at, updated_at
        FROM runtime_interactions_issue16;

        CREATE TABLE runtime_interaction_answers (
          answer_id TEXT PRIMARY KEY,
          interaction_id TEXT NOT NULL UNIQUE REFERENCES runtime_interactions(interaction_id),
          idempotency_key TEXT NOT NULL UNIQUE,
          payload_hash TEXT NOT NULL,
          answer_json TEXT NOT NULL,
          result_json TEXT NOT NULL,
          committed_at TEXT NOT NULL
        );
        INSERT INTO runtime_interaction_answers
        SELECT * FROM runtime_interaction_answers_issue16;

        CREATE TABLE runtime_interaction_handoffs (
          handoff_id TEXT PRIMARY KEY,
          interaction_id TEXT NOT NULL UNIQUE REFERENCES runtime_interactions(interaction_id),
          answer_id TEXT NOT NULL UNIQUE,
          state TEXT NOT NULL,
          parent_type TEXT NOT NULL CHECK (
            parent_type IN ('provider_turn', 'security_control', 'recovery_control')
          ),
          provider_attempt_id TEXT,
          handoff_attempt_id TEXT,
          handoff_attempt_no INTEGER CHECK (
            handoff_attempt_no IS NULL OR handoff_attempt_no > 0
          ),
          lease_epoch INTEGER CHECK (lease_epoch IS NULL OR lease_epoch > 0),
          record_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          CHECK (
            (parent_type = 'provider_turn' AND provider_attempt_id IS NOT NULL
              AND lease_epoch IS NOT NULL)
            OR (parent_type != 'provider_turn' AND provider_attempt_id IS NULL
              AND lease_epoch IS NULL)
          )
        );
        INSERT INTO runtime_interaction_handoffs (
          handoff_id, interaction_id, answer_id, state, parent_type, provider_attempt_id,
          handoff_attempt_id, handoff_attempt_no, lease_epoch, record_json, created_at, updated_at
        )
        SELECT handoff_id, interaction_id, answer_id, state, 'provider_turn', provider_attempt_id,
          handoff_attempt_id, handoff_attempt_no, lease_epoch, record_json, created_at, updated_at
        FROM runtime_interaction_handoffs_issue16;

        CREATE TABLE runtime_interaction_audit (
          audit_id TEXT PRIMARY KEY,
          interaction_id TEXT NOT NULL REFERENCES runtime_interactions(interaction_id),
          handoff_id TEXT NOT NULL REFERENCES runtime_interaction_handoffs(handoff_id),
          outcome TEXT NOT NULL,
          provider_attempt_id TEXT,
          lease_epoch INTEGER CHECK (lease_epoch IS NULL OR lease_epoch > 0),
          acknowledgement_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        INSERT INTO runtime_interaction_audit
        SELECT * FROM runtime_interaction_audit_issue16;

        DROP TABLE runtime_interaction_audit_issue16;
        DROP TABLE runtime_interaction_handoffs_issue16;
        DROP TABLE runtime_interaction_answers_issue16;
        DROP TABLE runtime_interactions_issue16;
      `);
      const violations = database.prepare('PRAGMA foreign_key_check').all();
      if (violations.length > 0) {
        throw new Error('Interaction control storage migration violated foreign keys.');
      }
    });
    migrate.immediate();
  } finally {
    database.pragma('legacy_alter_table = OFF');
    database.pragma('foreign_keys = ON');
  }
}

function migrateNullableSecurityControlInteractions(database) {
  const columns = database.prepare("PRAGMA table_info('runtime_interactions')").all();
  const turnId = columns.find(({ name }) => name === 'turn_id');
  if (!turnId || turnId.notnull === 0) return;

  database.pragma('foreign_keys = OFF');
  database.pragma('legacy_alter_table = ON');
  try {
    const migrate = database.transaction(() => {
      database.exec(`
        ALTER TABLE runtime_interactions RENAME TO runtime_interactions_issue18;
        ALTER TABLE runtime_interaction_answers RENAME TO runtime_interaction_answers_issue18;
        ALTER TABLE runtime_interaction_handoffs RENAME TO runtime_interaction_handoffs_issue18;
        ALTER TABLE runtime_interaction_audit RENAME TO runtime_interaction_audit_issue18;

        CREATE TABLE runtime_interactions (
          interaction_id TEXT PRIMARY KEY,
          conversation_id TEXT NOT NULL REFERENCES runtime_conversations(conversation_id),
          turn_id TEXT REFERENCES runtime_turns(turn_id),
          lineage_id TEXT REFERENCES runtime_lineages(lineage_id),
          parent_type TEXT NOT NULL CHECK (
            parent_type IN ('provider_turn', 'security_control', 'recovery_control')
          ),
          parent_id TEXT NOT NULL,
          ordinal INTEGER NOT NULL CHECK (ordinal > 0),
          state TEXT NOT NULL,
          version INTEGER NOT NULL CHECK (version > 0),
          handoff_state TEXT NOT NULL,
          handoff_version INTEGER CHECK (handoff_version IS NULL OR handoff_version > 0),
          request_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE (parent_type, parent_id, ordinal),
          CHECK (
            (parent_type = 'security_control' AND turn_id IS NULL AND lineage_id IS NULL)
            OR (parent_type != 'security_control' AND turn_id IS NOT NULL)
          )
        );
        INSERT INTO runtime_interactions (
          interaction_id, conversation_id, turn_id, lineage_id, parent_type, parent_id,
          ordinal, state, version, handoff_state, handoff_version, request_json,
          created_at, updated_at
        )
        SELECT interaction_id, conversation_id, turn_id, lineage_id, parent_type, parent_id,
          ordinal, state, version, handoff_state, handoff_version, request_json,
          created_at, updated_at
        FROM runtime_interactions_issue18;

        CREATE TABLE runtime_interaction_answers (
          answer_id TEXT PRIMARY KEY,
          interaction_id TEXT NOT NULL UNIQUE REFERENCES runtime_interactions(interaction_id),
          idempotency_key TEXT NOT NULL UNIQUE,
          payload_hash TEXT NOT NULL,
          answer_json TEXT NOT NULL,
          result_json TEXT NOT NULL,
          committed_at TEXT NOT NULL
        );
        INSERT INTO runtime_interaction_answers
        SELECT * FROM runtime_interaction_answers_issue18;

        CREATE TABLE runtime_interaction_handoffs (
          handoff_id TEXT PRIMARY KEY,
          interaction_id TEXT NOT NULL UNIQUE REFERENCES runtime_interactions(interaction_id),
          answer_id TEXT NOT NULL UNIQUE,
          state TEXT NOT NULL,
          parent_type TEXT NOT NULL CHECK (
            parent_type IN ('provider_turn', 'security_control', 'recovery_control')
          ),
          provider_attempt_id TEXT,
          handoff_attempt_id TEXT,
          handoff_attempt_no INTEGER CHECK (
            handoff_attempt_no IS NULL OR handoff_attempt_no > 0
          ),
          lease_epoch INTEGER CHECK (lease_epoch IS NULL OR lease_epoch > 0),
          record_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          CHECK (
            (parent_type = 'provider_turn' AND provider_attempt_id IS NOT NULL
              AND lease_epoch IS NOT NULL)
            OR (parent_type != 'provider_turn' AND provider_attempt_id IS NULL
              AND lease_epoch IS NULL)
          )
        );
        INSERT INTO runtime_interaction_handoffs
        SELECT * FROM runtime_interaction_handoffs_issue18;

        CREATE TABLE runtime_interaction_audit (
          audit_id TEXT PRIMARY KEY,
          interaction_id TEXT NOT NULL REFERENCES runtime_interactions(interaction_id),
          handoff_id TEXT NOT NULL REFERENCES runtime_interaction_handoffs(handoff_id),
          outcome TEXT NOT NULL,
          provider_attempt_id TEXT,
          lease_epoch INTEGER CHECK (lease_epoch IS NULL OR lease_epoch > 0),
          acknowledgement_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        INSERT INTO runtime_interaction_audit
        SELECT * FROM runtime_interaction_audit_issue18;

        DROP TABLE runtime_interaction_audit_issue18;
        DROP TABLE runtime_interaction_handoffs_issue18;
        DROP TABLE runtime_interaction_answers_issue18;
        DROP TABLE runtime_interactions_issue18;
      `);
      const violations = database.prepare('PRAGMA foreign_key_check').all();
      if (violations.length > 0) {
        throw new Error('Nullable security-control interaction migration violated foreign keys.');
      }
    });
    migrate.immediate();
  } finally {
    database.pragma('legacy_alter_table = OFF');
    database.pragma('foreign_keys = ON');
  }
}

function hasLegacyAggregateVersionConstraint(database) {
  return database.prepare("PRAGMA index_list('runtime_outbox')").all()
    .filter(({ unique }) => unique === 1)
    .some(({ name }) => {
      const fields = database.prepare(`PRAGMA index_info('${name}')`).all()
        .map(({ name: fieldName }) => fieldName);
      return fields.length === 3
        && fields[0] === 'aggregate_type'
        && fields[1] === 'aggregate_id'
        && fields[2] === 'aggregate_version';
    });
}

function migrateLegacyOutboxConstraint(database) {
  if (!hasLegacyAggregateVersionConstraint(database)) return;
  const reconciliationCount = database.prepare(`
    SELECT COUNT(*) AS count FROM runtime_outbox_reconciliations
  `).get().count;
  if (reconciliationCount !== 0) {
    throw new Error(
      'Legacy outbox migration cannot discard durable delivery reconciliation records.',
    );
  }
  const migrate = database.transaction(() => {
    database.exec(`
      DROP TABLE IF EXISTS runtime_projection_snapshots;
      DROP TABLE IF EXISTS runtime_delivery_lanes;
      DROP TABLE IF EXISTS runtime_outbox_reconciliations;
      ALTER TABLE runtime_outbox RENAME TO runtime_outbox_issue07;
      ${OUTBOX_V2_SCHEMA}
      INSERT INTO runtime_outbox (${OUTBOX_COLUMNS.join(', ')})
        SELECT ${OUTBOX_COLUMNS.join(', ')} FROM runtime_outbox_issue07;
      DROP TABLE runtime_outbox_issue07;
    `);
    database.exec(RUNTIME_SCHEMA);
  });
  migrate.immediate();
}

function backfillOutboxLeaseEpochs(database) {
  const rows = database.prepare(`
    SELECT outbox_id, lease_expires_at
    FROM runtime_outbox
    WHERE lease_expires_at IS NOT NULL AND lease_expires_epoch_ms IS NULL
  `).all();
  const update = database.prepare(`
    UPDATE runtime_outbox
    SET lease_expires_epoch_ms = ?
    WHERE outbox_id = ? AND lease_expires_at = ? AND lease_expires_epoch_ms IS NULL
  `);
  for (const row of rows) {
    const epochMs = Date.parse(row.lease_expires_at);
    if (Number.isFinite(epochMs)) {
      update.run(epochMs, row.outbox_id, row.lease_expires_at);
    }
  }
}

export function quarantineUnverifiableOutboxClaims(database) {
  const quarantine = () => {
    const rows = database.prepare(`
      SELECT outbox.outbox_id, outbox.status, outbox.delivery_attempt_id,
        outbox.delivery_attempt_no, outbox.outbox_lease_epoch,
        outbox.lease_owner, outbox.command_json, outbox.claimed_command_hash,
        outbox.last_attempt_at, outbox.created_at,
        snapshot.command_json AS snapshot_command_json,
        snapshot.command_hash AS snapshot_command_hash,
        snapshot.lease_owner AS snapshot_lease_owner
      FROM runtime_outbox AS outbox
      LEFT JOIN runtime_outbox_claim_snapshots AS snapshot
        ON snapshot.outbox_id = outbox.outbox_id
        AND snapshot.delivery_attempt_id = outbox.delivery_attempt_id
        AND snapshot.delivery_attempt_no = outbox.delivery_attempt_no
        AND snapshot.outbox_lease_epoch = outbox.outbox_lease_epoch
      WHERE outbox.status IN ('delivering', 'retry_wait')
    `).all();
    const update = database.prepare(`
      UPDATE runtime_outbox
      SET status = 'delivery_unknown', next_attempt_at = NULL,
        last_error_json = COALESCE(last_error_json, ?),
        updated_at = COALESCE(updated_at, last_attempt_at, created_at)
      WHERE outbox_id = ? AND status = ?
        AND delivery_attempt_id IS ? AND delivery_attempt_no IS ?
        AND outbox_lease_epoch = ? AND lease_owner IS ?
        AND command_json = ? AND claimed_command_hash IS ?
    `);
    for (const row of rows) {
      const snapshotHash = row.snapshot_command_json === null
        ? null
        : crypto.createHash('sha256').update(row.snapshot_command_json).digest('hex');
      const verified = row.snapshot_command_json !== null
        && (row.status === 'retry_wait' || row.snapshot_lease_owner === row.lease_owner)
        && row.snapshot_command_json === row.command_json
        && row.snapshot_command_hash === row.claimed_command_hash
        && snapshotHash === row.snapshot_command_hash;
      if (verified) continue;
      const occurredAt = row.last_attempt_at ?? row.created_at;
      update.run(
        JSON.stringify({
          code: 'delivery_claim_authority_unverifiable',
          category: 'internal',
          retryable: false,
          side_effect_status: 'unknown',
          user_message: 'Delivery acknowledgement is unknown and requires reconciliation.',
          occurred_at: occurredAt,
        }),
        row.outbox_id,
        row.status,
        row.delivery_attempt_id,
        row.delivery_attempt_no,
        row.outbox_lease_epoch,
        row.lease_owner,
        row.command_json,
        row.claimed_command_hash,
      );
    }
  };
  if (database.inTransaction) return quarantine();
  return database.transaction(quarantine).immediate();
}

function backfillDeliveryLanes(database) {
  const rows = database.prepare(`
    SELECT outbox_id, delivery_id, aggregate_version, status, command_json,
      result_json, created_at
    FROM runtime_outbox
    WHERE lane_key IS NULL AND aggregate_type = 'turn_main'
    ORDER BY created_at, outbox_id
  `).all();
  for (const row of rows) {
    let command;
    try {
      command = JSON.parse(row.command_json);
    } catch {
      continue;
    }
    if (
      command.aggregate_type !== 'turn_main'
      || typeof command.mapping?.turn_id !== 'string'
      || typeof command.target?.channel !== 'string'
    ) {
      continue;
    }
    const laneKey = createDeliveryLaneKey(command);
    let result = null;
    if (row.result_json !== null) {
      try {
        result = JSON.parse(row.result_json);
      } catch {
        result = null;
      }
    }
    const delivered = row.status === 'delivered' && result?.status === 'delivered';
    database.prepare(`
      INSERT OR IGNORE INTO runtime_delivery_lanes (
        lane_key, turn_id, aggregate_type, lane_identity_version, delivery_mode,
        target_json, mapping_json,
        platform_message_id, applied_platform_version, last_delivery_id,
        last_applied_version, last_delivered_at, created_at, updated_at
      ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      laneKey,
      command.mapping.turn_id,
      command.aggregate_type,
      command.operation === 'send_text' ? 'text' : 'main',
      JSON.stringify(command.target),
      JSON.stringify(command.mapping),
      delivered ? result.platform_message_id : null,
      delivered ? result.applied_platform_version : null,
      delivered ? row.delivery_id : null,
      delivered ? row.aggregate_version : 0,
      delivered ? result.delivered_at : null,
      row.created_at,
      result?.result_at ?? row.created_at,
    );
    database.prepare(`
      UPDATE runtime_outbox
      SET lane_key = ?, predecessor_delivery_id = ?, priority = ?, terminal = ?,
        updated_at = COALESCE(updated_at, created_at)
      WHERE outbox_id = ? AND lane_key IS NULL
    `).run(
      laneKey,
      command.predecessor_delivery_id ?? null,
      command.priority ?? 0,
      command.render_model?.terminal === true ? 1 : 0,
      row.outbox_id,
    );
  }
}

function migrateDeliveryLaneIdentity(database) {
  const migrate = database.transaction(() => {
    database.pragma('defer_foreign_keys = ON');
    const lanes = database.prepare(`
      SELECT lane_key, turn_id, aggregate_type, target_json
      FROM runtime_delivery_lanes
      WHERE lane_identity_version = 0
    `).all();
    for (const lane of lanes) {
      let target;
      try {
        target = JSON.parse(lane.target_json);
      } catch {
        continue;
      }
      let migratedLaneKey;
      try {
        migratedLaneKey = createDeliveryLaneKeyFromIdentity({
          target,
          turnId: lane.turn_id,
          aggregateType: lane.aggregate_type,
        });
      } catch {
        continue;
      }
      if (migratedLaneKey !== lane.lane_key) {
        database.prepare(`
          UPDATE runtime_delivery_lanes SET lane_key = ? WHERE lane_key = ?
        `).run(migratedLaneKey, lane.lane_key);
        database.prepare(`
          UPDATE runtime_outbox SET lane_key = ? WHERE lane_key = ?
        `).run(migratedLaneKey, lane.lane_key);
        database.prepare(`
          UPDATE runtime_projection_snapshots SET lane_key = ? WHERE lane_key = ?
        `).run(migratedLaneKey, lane.lane_key);
      }
      database.prepare(`
        UPDATE runtime_delivery_lanes
        SET lane_identity_version = 1
        WHERE lane_key = ?
      `).run(migratedLaneKey);
    }
  });
  migrate.immediate();
}

function repairConversationWorkspaceSafeRetryQuarantines(database) {
  database.prepare(`
    UPDATE runtime_conversation_workspaces
    SET state = CASE
        WHEN workspace_root IS NULL THEN 'requested'
        ELSE 'ready'
      END,
      quarantined_at = NULL,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      last_error_json = NULL
    WHERE state = 'quarantined'
      AND (
        (workspace_root IS NULL AND ready_at IS NULL)
        OR (workspace_root IS NOT NULL AND ready_at IS NOT NULL)
      )
      AND json_extract(last_error_json, '$.code') = 'workspace_runtime_uncertain'
      AND EXISTS (
        SELECT 1
        FROM runtime_background_tasks AS background
        JOIN runtime_turns AS turn
          ON turn.turn_id = background.execution_turn_id
        JOIN runtime_turn_queue AS retry_queue
          ON retry_queue.turn_id = background.execution_turn_id
        JOIN runtime_provider_attempts AS retry_attempt
          ON retry_attempt.turn_id = background.execution_turn_id
          AND retry_attempt.attempt_id = turn.attempt_id
          AND retry_attempt.attempt_no = turn.attempt_no
          AND retry_attempt.lease_epoch = turn.lease_epoch
        WHERE background.execution_conversation_id
            = runtime_conversation_workspaces.conversation_id
          AND background.state = 'recovering'
          AND background.side_effect_status = 'none'
          AND turn.state = 'recovering'
          AND retry_queue.status = 'queued'
          AND retry_queue.wait_reason = 'provider_retry'
          AND retry_attempt.state = 'retry_wait'
          AND retry_attempt.side_effect_status = 'none'
      )
      AND NOT EXISTS (
        SELECT 1
        FROM runtime_background_tasks AS unsafe_background
        WHERE unsafe_background.execution_conversation_id
            = runtime_conversation_workspaces.conversation_id
          AND (
            unsafe_background.side_effect_status = 'unknown'
            OR (
              unsafe_background.state = 'recovering'
              AND NOT EXISTS (
                SELECT 1
                FROM runtime_turn_queue AS safe_queue
                JOIN runtime_turns AS safe_turn
                  ON safe_turn.turn_id = safe_queue.turn_id
                JOIN runtime_provider_attempts AS safe_attempt
                  ON safe_attempt.turn_id = safe_queue.turn_id
                  AND safe_attempt.attempt_id = safe_turn.attempt_id
                  AND safe_attempt.attempt_no = safe_turn.attempt_no
                  AND safe_attempt.lease_epoch = safe_turn.lease_epoch
                WHERE safe_queue.turn_id = unsafe_background.execution_turn_id
                  AND safe_queue.status = 'queued'
                  AND safe_queue.wait_reason = 'provider_retry'
                  AND safe_attempt.state = 'retry_wait'
                  AND safe_attempt.side_effect_status = 'none'
              )
            )
          )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM runtime_turns AS unsafe_turn
        WHERE unsafe_turn.conversation_id
            = runtime_conversation_workspaces.conversation_id
          AND unsafe_turn.state = 'recovering'
          AND NOT EXISTS (
            SELECT 1
            FROM runtime_turn_queue AS safe_queue
            JOIN runtime_provider_attempts AS safe_attempt
              ON safe_attempt.turn_id = safe_queue.turn_id
              AND safe_attempt.attempt_id = unsafe_turn.attempt_id
              AND safe_attempt.attempt_no = unsafe_turn.attempt_no
              AND safe_attempt.lease_epoch = unsafe_turn.lease_epoch
            WHERE safe_queue.turn_id = unsafe_turn.turn_id
              AND safe_queue.status = 'queued'
              AND safe_queue.wait_reason = 'provider_retry'
              AND safe_attempt.state = 'retry_wait'
              AND safe_attempt.side_effect_status = 'none'
          )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM runtime_execution_recoveries AS recovery
        JOIN runtime_turns AS recovery_turn ON recovery_turn.turn_id = recovery.turn_id
        WHERE recovery_turn.conversation_id
          = runtime_conversation_workspaces.conversation_id
      )
      AND NOT EXISTS (
        SELECT 1
        FROM runtime_workspace_leases AS lease
        WHERE lease.holder_conversation_id
            = runtime_conversation_workspaces.conversation_id
          AND lease.state = 'uncertain'
      )
      AND NOT EXISTS (
        SELECT 1
        FROM runtime_workspace_background_work AS background_work
        JOIN runtime_workspace_leases AS lease
          ON lease.workspace_lease_id = background_work.workspace_lease_id
        WHERE lease.holder_conversation_id
            = runtime_conversation_workspaces.conversation_id
          AND background_work.state = 'unknown'
      )
  `).run();
}

function migrateConversationWorkspaceSafeRetryQuarantines(database) {
  const migrate = database.transaction(() => {
    const applied = database.prepare(`
      SELECT 1
      FROM runtime_schema_migrations
      WHERE migration_id = ?
    `).get(SAFE_RETRY_WORKSPACE_MIGRATION_ID);
    if (applied) return;
    const quarantineTriggers = database.prepare(`
      SELECT name, sql
      FROM sqlite_master
      WHERE type = 'trigger'
        AND name IN (
          'runtime_workspace_quarantine_background_task',
          'runtime_workspace_quarantine_background_task_insert',
          'runtime_workspace_quarantine_recovering_turn',
          'runtime_workspace_quarantine_recovering_turn_insert'
        )
    `).all();
    const requiresUpgrade = quarantineTriggers.length !== 4
      || quarantineTriggers.some(
        ({ sql }) => !sql?.includes("retry_queue.wait_reason = 'provider_retry'")
          || !sql.includes('retry_attempt.attempt_id'),
      );
    if (requiresUpgrade) {
      database.exec(`
        DROP TRIGGER IF EXISTS runtime_workspace_quarantine_background_task;
        DROP TRIGGER IF EXISTS runtime_workspace_quarantine_background_task_insert;
        DROP TRIGGER IF EXISTS runtime_workspace_quarantine_recovering_turn;
        DROP TRIGGER IF EXISTS runtime_workspace_quarantine_recovering_turn_insert;
        DROP TRIGGER IF EXISTS runtime_conversation_workspace_state_transition;
        ${CONVERSATION_WORKSPACE_RUNTIME_QUARANTINE_TRIGGERS}
      `);
      repairConversationWorkspaceSafeRetryQuarantines(database);
      database.exec(CONVERSATION_WORKSPACE_STATE_TRANSITION_TRIGGER);
    }
    database.prepare(`
      INSERT INTO runtime_schema_migrations (migration_id, applied_at)
      VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    `).run(SAFE_RETRY_WORKSPACE_MIGRATION_ID);
  });
  migrate.immediate();
}

function repairConversationWorkspaceSafeRetryQuarantinesV2(database) {
  const migrate = database.transaction(() => {
    const applied = database.prepare(`
      SELECT 1
      FROM runtime_schema_migrations
      WHERE migration_id = ?
    `).get(SAFE_RETRY_WORKSPACE_REPAIR_MIGRATION_ID);
    if (applied) return;
    database.exec(`
      DROP TRIGGER IF EXISTS runtime_conversation_workspace_state_transition;
    `);
    repairConversationWorkspaceSafeRetryQuarantines(database);
    database.exec(CONVERSATION_WORKSPACE_STATE_TRANSITION_TRIGGER);
    database.prepare(`
      INSERT INTO runtime_schema_migrations (migration_id, applied_at)
      VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    `).run(SAFE_RETRY_WORKSPACE_REPAIR_MIGRATION_ID);
  });
  migrate.immediate();
}

export function initializeRuntimePersistence(database) {
  database.pragma('journal_mode = WAL');
  database.pragma('busy_timeout = 5000');
  database.pragma('foreign_keys = ON');
  database.exec(RUNTIME_SCHEMA);
  addColumnIfMissing(database, 'runtime_turns', 'attempt_id', 'TEXT');
  addColumnIfMissing(
    database,
    'runtime_turns',
    'attempt_no',
    'INTEGER CHECK (attempt_no IS NULL OR attempt_no > 0)',
  );
  addColumnIfMissing(
    database,
    'runtime_turns',
    'lease_epoch',
    'INTEGER CHECK (lease_epoch IS NULL OR lease_epoch > 0)',
  );
  migrateConversationWorkspaceSafeRetryQuarantines(database);
  repairConversationWorkspaceSafeRetryQuarantinesV2(database);
  migrateOperationsReconciliationIntents(database);
  addColumnIfMissing(
    database,
    'runtime_conversations',
    'queue_version',
    'INTEGER NOT NULL DEFAULT 1 CHECK (queue_version > 0)',
  );
  database.prepare(`
    UPDATE runtime_conversations
    SET queue_version = last_queue_sequence + 1
    WHERE queue_version < last_queue_sequence + 1
  `).run();
  addColumnIfMissing(
    database,
    'runtime_lineages',
    'recovery_of_lineage_id',
    'TEXT REFERENCES runtime_lineages(lineage_id)',
  );
  addColumnIfMissing(database, 'runtime_turns', 'provider_input_json', 'TEXT');
  addColumnIfMissing(database, 'runtime_turns', 'terminal_at', 'TEXT');
  addColumnIfMissing(database, 'runtime_turns', 'detail_expires_at', 'TEXT');
  addColumnIfMissing(
    database,
    'runtime_retention_deletion_audit',
    'content_digest_version',
    'INTEGER NOT NULL DEFAULT 1 CHECK (content_digest_version = 1)',
  );
  addColumnIfMissing(
    database,
    'runtime_turns',
    'redirected_from_turn_id',
    'TEXT REFERENCES runtime_turns(turn_id)',
  );
  addColumnIfMissing(
    database,
    'runtime_turn_queue',
    'priority',
    'INTEGER NOT NULL DEFAULT 0 CHECK (priority IN (0, 1))',
  );
  addColumnIfMissing(database, 'runtime_turn_queue', 'wait_reason', 'TEXT');
  addColumnIfMissing(database, 'runtime_turn_queue', 'wait_detail_json', 'TEXT');
  addColumnIfMissing(database, 'runtime_turn_queue', 'available_at', 'TEXT');
  addColumnIfMissing(database, 'runtime_executor_service_instances', 'revoked_at', 'TEXT');
  addColumnIfMissing(
    database,
    'runtime_legacy_unmapped_messages',
    'recent_c4_context_json',
    "TEXT NOT NULL DEFAULT '[]'",
  );
  addColumnIfMissing(
    database,
    'runtime_legacy_unmapped_messages',
    'memory_handoff',
    "TEXT NOT NULL DEFAULT ''",
  );
  addColumnIfMissing(
    database,
    'runtime_reply_mapping_recoveries',
    'recovery_version',
    'INTEGER NOT NULL DEFAULT 1 CHECK (recovery_version > 0)',
  );
  addColumnIfMissing(
    database, 'runtime_upgrade_effects', 'input_json', "TEXT NOT NULL DEFAULT '{}'",
  );
  addColumnIfMissing(
    database,
    'runtime_execution_recoveries',
    'recovery_version',
    'INTEGER NOT NULL DEFAULT 1 CHECK (recovery_version > 0)',
  );
  database.exec(`
    CREATE TRIGGER IF NOT EXISTS runtime_turn_queue_insert_version
    AFTER INSERT ON runtime_turn_queue
    BEGIN
      UPDATE runtime_conversations
      SET queue_version = queue_version + 1
      WHERE conversation_id = NEW.conversation_id;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_turn_queue_update_version
    AFTER UPDATE ON runtime_turn_queue
    WHEN NEW.conversation_id IS NOT OLD.conversation_id
      OR NEW.queue_sequence IS NOT OLD.queue_sequence
      OR NEW.turn_id IS NOT OLD.turn_id
      OR NEW.status IS NOT OLD.status
      OR NEW.priority IS NOT OLD.priority
      OR NEW.wait_reason IS NOT OLD.wait_reason
      OR NEW.wait_detail_json IS NOT OLD.wait_detail_json
      OR NEW.enqueued_at IS NOT OLD.enqueued_at
    BEGIN
      UPDATE runtime_conversations
      SET queue_version = queue_version + 1
      WHERE conversation_id = OLD.conversation_id;
      UPDATE runtime_conversations
      SET queue_version = queue_version + 1
      WHERE conversation_id = NEW.conversation_id
        AND NEW.conversation_id IS NOT OLD.conversation_id;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_turn_queue_delete_version
    AFTER DELETE ON runtime_turn_queue
    BEGIN
      UPDATE runtime_conversations
      SET queue_version = queue_version + 1
      WHERE conversation_id = OLD.conversation_id;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_execution_recovery_state_versions
    AFTER UPDATE OF state ON runtime_execution_recoveries
    WHEN NEW.recovery_version = OLD.recovery_version AND NEW.state <> OLD.state
    BEGIN
      UPDATE runtime_execution_recoveries
      SET recovery_version = recovery_version + 1
      WHERE recovery_id = NEW.recovery_id;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_reply_mapping_recovery_state_versions
    AFTER UPDATE OF state ON runtime_reply_mapping_recoveries
    WHEN NEW.recovery_version = OLD.recovery_version AND NEW.state <> OLD.state
    BEGIN
      UPDATE runtime_reply_mapping_recoveries
      SET recovery_version = recovery_version + 1
      WHERE recovery_id = NEW.recovery_id;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_turn_terminal_retention
    AFTER UPDATE OF state ON runtime_turns
    WHEN NEW.state IN ('completed', 'stopped', 'cancelled', 'interrupted', 'failed', 'timed_out')
      AND OLD.state NOT IN ('completed', 'stopped', 'cancelled', 'interrupted', 'failed', 'timed_out')
    BEGIN
      UPDATE runtime_turns
      SET terminal_at = NEW.committed_at,
        detail_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', NEW.committed_at, '+30 days')
      WHERE turn_id = NEW.turn_id AND terminal_at IS NULL AND detail_expires_at IS NULL;
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      )
      SELECT 'provider_attempt_detail', attempt_id, NEW.turn_id, 'terminal_detail_30d',
        NEW.committed_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.committed_at, '+30 days'),
        'redact', NEW.committed_at
      FROM runtime_provider_attempts WHERE turn_id = NEW.turn_id;
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      )
      SELECT 'terminal_outbox', outbox_id, NEW.turn_id, 'terminal_detail_30d',
        NEW.committed_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.committed_at, '+30 days'),
        'delete', NEW.committed_at
      FROM runtime_outbox
      WHERE turn_id = NEW.turn_id AND status IN ('delivered', 'superseded', 'dead_letter');
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      )
      SELECT 'terminal_background_work', background_work_id, NEW.turn_id,
        'terminal_detail_30d', NEW.committed_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.committed_at, '+30 days'),
        'delete', NEW.committed_at
      FROM runtime_workspace_background_work
      WHERE holder_turn_id = NEW.turn_id AND state IN ('completed', 'failed');
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      )
      SELECT 'terminal_workspace_lease', workspace_lease_id, NEW.turn_id,
        'terminal_detail_30d', NEW.committed_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.committed_at, '+30 days'),
        'delete', NEW.committed_at
      FROM runtime_workspace_leases
      WHERE holder_turn_id = NEW.turn_id AND state IN ('released', 'expired');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_normalized_event_retention
    AFTER INSERT ON runtime_normalized_events
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      ) VALUES (
        'normalized_event', NEW.event_id, NEW.turn_id, 'raw_detail_7d', NEW.persisted_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.persisted_at, '+7 days'),
        'delete', NEW.persisted_at
      );
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_provider_raw_event_retention
    AFTER INSERT ON runtime_provider_event_diagnostics
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      ) VALUES (
        'provider_raw_event', NEW.diagnostic_id, NEW.turn_id, 'raw_detail_7d',
        NEW.observed_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.observed_at, '+7 days'),
        'delete', NEW.observed_at
      );
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_projection_snapshot_retention
    AFTER INSERT ON runtime_projection_snapshots
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      )
      SELECT
        CASE WHEN NEW.terminal = 1 THEN 'terminal_projection' ELSE 'intermediate_projection' END,
        NEW.projection_id, NEW.turn_id,
        CASE WHEN NEW.terminal = 1 THEN 'terminal_detail_30d' ELSE 'raw_detail_7d' END,
        CASE WHEN NEW.terminal = 1 THEN turn.terminal_at ELSE NEW.created_at END,
        CASE WHEN NEW.terminal = 1
          THEN strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days')
          ELSE strftime('%Y-%m-%dT%H:%M:%fZ', NEW.created_at, '+7 days')
        END,
        'delete', NEW.created_at
      FROM runtime_turns AS turn
      WHERE turn.turn_id = NEW.turn_id
        AND (NEW.terminal = 0 OR turn.terminal_at IS NOT NULL);
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_terminal_outbox_retention
    AFTER UPDATE OF status ON runtime_outbox
    WHEN NEW.turn_id IS NOT NULL
      AND NEW.status IN ('delivered', 'superseded', 'dead_letter')
      AND OLD.status NOT IN ('delivered', 'superseded', 'dead_letter')
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      )
      SELECT 'terminal_outbox', NEW.outbox_id, NEW.turn_id, 'terminal_detail_30d',
        turn.terminal_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days'),
        'delete', NEW.updated_at
      FROM runtime_turns AS turn
      WHERE turn.turn_id = NEW.turn_id AND turn.terminal_at IS NOT NULL;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_terminal_queue_retention
    AFTER UPDATE OF status ON runtime_turn_queue
    WHEN NEW.status IN ('completed', 'stopped', 'cancelled', 'interrupted', 'failed', 'timed_out')
      AND OLD.status NOT IN ('completed', 'stopped', 'cancelled', 'interrupted', 'failed', 'timed_out')
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      )
      SELECT 'terminal_turn_queue', NEW.turn_id, NEW.turn_id, 'terminal_detail_30d',
        turn.terminal_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days'),
        'delete', turn.terminal_at
      FROM runtime_turns AS turn
      WHERE turn.turn_id = NEW.turn_id AND turn.terminal_at IS NOT NULL;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_terminal_background_work_retention_insert
    AFTER INSERT ON runtime_workspace_background_work
    WHEN NEW.state IN ('completed', 'failed')
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      )
      SELECT 'terminal_background_work', NEW.background_work_id, NEW.holder_turn_id,
        'terminal_detail_30d', turn.terminal_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days'),
        'delete', NEW.ended_at
      FROM runtime_turns AS turn
      WHERE turn.turn_id = NEW.holder_turn_id AND turn.terminal_at IS NOT NULL;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_terminal_background_work_retention_update
    AFTER UPDATE OF state ON runtime_workspace_background_work
    WHEN NEW.state IN ('completed', 'failed') AND OLD.state = 'active'
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      )
      SELECT 'terminal_background_work', NEW.background_work_id, NEW.holder_turn_id,
        'terminal_detail_30d', turn.terminal_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days'),
        'delete', NEW.ended_at
      FROM runtime_turns AS turn
      WHERE turn.turn_id = NEW.holder_turn_id AND turn.terminal_at IS NOT NULL;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_terminal_workspace_lease_retention_insert
    AFTER INSERT ON runtime_workspace_leases
    WHEN NEW.state IN ('released', 'expired')
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      )
      SELECT 'terminal_workspace_lease', NEW.workspace_lease_id, NEW.holder_turn_id,
        'terminal_detail_30d', turn.terminal_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days'),
        'delete', NEW.released_at
      FROM runtime_turns AS turn
      WHERE turn.turn_id = NEW.holder_turn_id AND turn.terminal_at IS NOT NULL;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_terminal_workspace_lease_retention_update
    AFTER UPDATE OF state ON runtime_workspace_leases
    WHEN NEW.state IN ('released', 'expired') AND OLD.state IN ('active', 'uncertain')
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      )
      SELECT 'terminal_workspace_lease', NEW.workspace_lease_id, NEW.holder_turn_id,
        'terminal_detail_30d', turn.terminal_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days'),
        'delete', NEW.released_at
      FROM runtime_turns AS turn
      WHERE turn.turn_id = NEW.holder_turn_id AND turn.terminal_at IS NOT NULL;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_permission_audit_retention
    AFTER INSERT ON runtime_permission_audit
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      ) VALUES (
        'permission_audit', NEW.audit_id, NEW.turn_id, 'security_audit_180d',
        NEW.committed_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.committed_at, '+180 days'),
        'delete', NEW.committed_at
      );
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_operations_audit_retention
    AFTER INSERT ON runtime_operations_audit
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      ) VALUES (
        'operations_audit', NEW.audit_id, json_extract(NEW.target_json, '$.turn_id'),
        'security_audit_180d', NEW.committed_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.committed_at, '+180 days'),
        'delete', NEW.committed_at
      );
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_interaction_audit_retention
    AFTER INSERT ON runtime_interaction_audit
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      )
      SELECT 'interaction_audit', NEW.audit_id, interaction.turn_id,
        'security_audit_180d', NEW.created_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.created_at, '+180 days'),
        'delete', NEW.created_at
      FROM runtime_interactions AS interaction
      WHERE interaction.interaction_id = NEW.interaction_id;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_permission_action_decision_retention
    AFTER INSERT ON runtime_permission_action_decisions
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      ) VALUES (
        'permission_action_decision', NEW.decision_id, NEW.turn_id,
        'security_audit_180d', NEW.checked_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.checked_at, '+180 days'),
        'delete', NEW.checked_at
      );
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_operations_conflict_audit_retention
    AFTER INSERT ON runtime_operations_idempotency_conflicts
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      ) VALUES (
        'operations_idempotency_conflict',
        json_array(NEW.caller_namespace, NEW.control_id, NEW.request_hash),
        NULL, 'security_audit_180d', NEW.committed_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.committed_at, '+180 days'),
        'delete', NEW.committed_at
      );
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_legacy_migration_payload_retention
    AFTER INSERT ON runtime_legacy_migration_payloads
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      ) VALUES (
        'legacy_migration_payload',
        json_array(NEW.upgrade_id, NEW.legacy_kind, NEW.legacy_record_id),
        NULL, 'terminal_detail_30d', NEW.created_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.created_at, '+30 days'),
        'delete', NEW.created_at
      );
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_legacy_migration_audit_payload_retention
    AFTER INSERT ON runtime_legacy_migration_audit_payloads
    BEGIN
      INSERT OR IGNORE INTO runtime_retention_entries (
        record_kind, record_id, turn_id, retention_class, anchor_at,
        expires_at, disposal_kind, registered_at
      ) VALUES (
        'legacy_migration_audit_payload',
        json_array(NEW.upgrade_id, NEW.legacy_kind, NEW.legacy_record_id),
        NULL, 'security_audit_180d', NEW.created_at,
        strftime('%Y-%m-%dT%H:%M:%fZ', NEW.created_at, '+180 days'),
        'delete', NEW.created_at
      );
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_terminal_projection_compact_summary
    AFTER INSERT ON runtime_projection_snapshots
    WHEN NEW.terminal = 1
    BEGIN
      INSERT OR IGNORE INTO runtime_compact_turn_summaries (
        turn_id, conversation_id, lineage_id, terminal_state, terminal_at,
        final_text, final_error_json, lane_key, mapping_id, aggregate_version,
        event_sequence_through, created_at
      )
      SELECT turn.turn_id, turn.conversation_id, turn.lineage_id, turn.state,
        turn.terminal_at, json_extract(NEW.render_model_json, '$.text'),
        json_extract(NEW.render_model_json, '$.error'), NEW.lane_key,
        json_extract(lane.mapping_json, '$.mapping_id'), NEW.aggregate_version,
        NEW.event_sequence_through, NEW.created_at
      FROM runtime_turns AS turn
      JOIN runtime_delivery_lanes AS lane ON lane.lane_key = NEW.lane_key
      WHERE turn.turn_id = NEW.turn_id AND turn.terminal_at IS NOT NULL;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_compact_turn_summary_immutable_update
    BEFORE UPDATE ON runtime_compact_turn_summaries
    BEGIN
      SELECT RAISE(ABORT, 'runtime compact turn summary is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_compact_turn_summary_immutable_delete
    BEFORE DELETE ON runtime_compact_turn_summaries
    BEGIN
      SELECT RAISE(ABORT, 'runtime compact turn summary is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_retention_deletion_audit_immutable_update
    BEFORE UPDATE ON runtime_retention_deletion_audit
    BEGIN
      SELECT RAISE(ABORT, 'runtime retention deletion audit is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_retention_deletion_audit_immutable_delete
    BEFORE DELETE ON runtime_retention_deletion_audit
    BEGIN
      SELECT RAISE(ABORT, 'runtime retention deletion audit is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_retention_entry_immutable_update
    BEFORE UPDATE ON runtime_retention_entries
    WHEN NEW.record_kind IS NOT OLD.record_kind
      OR NEW.record_id IS NOT OLD.record_id
      OR NEW.turn_id IS NOT OLD.turn_id
      OR NEW.retention_class IS NOT OLD.retention_class
      OR NEW.anchor_at IS NOT OLD.anchor_at
      OR NEW.expires_at IS NOT OLD.expires_at
      OR NEW.disposal_kind IS NOT OLD.disposal_kind
      OR NEW.registered_at IS NOT OLD.registered_at
      OR OLD.disposed_at IS NOT NULL
      OR NEW.disposed_at IS NULL
      OR NEW.deletion_audit_id IS NULL
      OR NOT EXISTS (
        SELECT 1 FROM runtime_retention_deletion_audit AS audit
        WHERE audit.audit_id = NEW.deletion_audit_id
          AND audit.record_kind = OLD.record_kind
          AND audit.record_id = OLD.record_id
          AND audit.deleted_at = NEW.disposed_at
      )
    BEGIN
      SELECT RAISE(ABORT, 'runtime retention entry is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_retention_entry_immutable_delete
    BEFORE DELETE ON runtime_retention_entries
    BEGIN
      SELECT RAISE(ABORT, 'runtime retention entry is immutable');
    END;
  `);
  database.prepare(`
    UPDATE runtime_turns
    SET terminal_at = committed_at,
      detail_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', committed_at, '+30 days')
    WHERE state IN ('completed', 'stopped', 'cancelled', 'interrupted', 'failed', 'timed_out')
      AND terminal_at IS NULL AND detail_expires_at IS NULL
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_compact_turn_summaries (
      turn_id, conversation_id, lineage_id, terminal_state, terminal_at,
      final_text, final_error_json, lane_key, mapping_id, aggregate_version,
      event_sequence_through, created_at
    )
    SELECT turn.turn_id, turn.conversation_id, turn.lineage_id, turn.state,
      turn.terminal_at, json_extract(projection.render_model_json, '$.text'),
      json_extract(projection.render_model_json, '$.error'), projection.lane_key,
      json_extract(lane.mapping_json, '$.mapping_id'), projection.aggregate_version,
      projection.event_sequence_through, projection.created_at
    FROM runtime_turns AS turn
    JOIN runtime_projection_snapshots AS projection ON projection.turn_id = turn.turn_id
      AND projection.terminal = 1
      AND projection.aggregate_version = (
        SELECT MAX(candidate.aggregate_version)
        FROM runtime_projection_snapshots AS candidate
        WHERE candidate.turn_id = turn.turn_id AND candidate.terminal = 1
      )
    JOIN runtime_delivery_lanes AS lane ON lane.lane_key = projection.lane_key
    WHERE turn.terminal_at IS NOT NULL
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT 'normalized_event', event_id, turn_id, 'raw_detail_7d', persisted_at,
      strftime('%Y-%m-%dT%H:%M:%fZ', persisted_at, '+7 days'), 'delete', persisted_at
    FROM runtime_normalized_events
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT 'provider_raw_event', diagnostic_id, turn_id, 'raw_detail_7d',
      observed_at, strftime('%Y-%m-%dT%H:%M:%fZ', observed_at, '+7 days'),
      'delete', observed_at
    FROM runtime_provider_event_diagnostics
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT CASE WHEN projection.terminal = 1
        THEN 'terminal_projection' ELSE 'intermediate_projection' END,
      projection.projection_id, projection.turn_id,
      CASE WHEN projection.terminal = 1
        THEN 'terminal_detail_30d' ELSE 'raw_detail_7d' END,
      CASE WHEN projection.terminal = 1 THEN turn.terminal_at ELSE projection.created_at END,
      CASE WHEN projection.terminal = 1
        THEN strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days')
        ELSE strftime('%Y-%m-%dT%H:%M:%fZ', projection.created_at, '+7 days')
      END,
      'delete', projection.created_at
    FROM runtime_projection_snapshots AS projection
    JOIN runtime_turns AS turn ON turn.turn_id = projection.turn_id
    WHERE projection.terminal = 0 OR turn.terminal_at IS NOT NULL
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT 'provider_attempt_detail', attempt.attempt_id, attempt.turn_id,
      'terminal_detail_30d', turn.terminal_at,
      strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days'),
      'redact', turn.terminal_at
    FROM runtime_provider_attempts AS attempt
    JOIN runtime_turns AS turn ON turn.turn_id = attempt.turn_id
    WHERE turn.terminal_at IS NOT NULL
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT 'terminal_outbox', outbox.outbox_id, outbox.turn_id,
      'terminal_detail_30d', turn.terminal_at,
      strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days'),
      'delete', turn.terminal_at
    FROM runtime_outbox AS outbox
    JOIN runtime_turns AS turn ON turn.turn_id = outbox.turn_id
    WHERE turn.terminal_at IS NOT NULL
      AND outbox.status IN ('delivered', 'superseded', 'dead_letter')
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT 'terminal_turn_queue', queue.turn_id, queue.turn_id,
      'terminal_detail_30d', turn.terminal_at,
      strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days'),
      'delete', turn.terminal_at
    FROM runtime_turn_queue AS queue
    JOIN runtime_turns AS turn ON turn.turn_id = queue.turn_id
    WHERE turn.terminal_at IS NOT NULL
      AND queue.status IN ('completed', 'stopped', 'cancelled', 'interrupted', 'failed', 'timed_out')
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT 'terminal_background_work', work.background_work_id, work.holder_turn_id,
      'terminal_detail_30d', turn.terminal_at,
      strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days'),
      'delete', work.ended_at
    FROM runtime_workspace_background_work AS work
    JOIN runtime_turns AS turn ON turn.turn_id = work.holder_turn_id
    WHERE turn.terminal_at IS NOT NULL AND work.state IN ('completed', 'failed')
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT 'terminal_workspace_lease', lease.workspace_lease_id, lease.holder_turn_id,
      'terminal_detail_30d', turn.terminal_at,
      strftime('%Y-%m-%dT%H:%M:%fZ', turn.terminal_at, '+30 days'),
      'delete', lease.released_at
    FROM runtime_workspace_leases AS lease
    JOIN runtime_turns AS turn ON turn.turn_id = lease.holder_turn_id
    WHERE turn.terminal_at IS NOT NULL AND lease.state IN ('released', 'expired')
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT 'permission_audit', audit_id, turn_id, 'security_audit_180d',
      committed_at, strftime('%Y-%m-%dT%H:%M:%fZ', committed_at, '+180 days'),
      'delete', committed_at
    FROM runtime_permission_audit
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT 'operations_audit', audit_id, json_extract(target_json, '$.turn_id'),
      'security_audit_180d', committed_at,
      strftime('%Y-%m-%dT%H:%M:%fZ', committed_at, '+180 days'),
      'delete', committed_at
    FROM runtime_operations_audit
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT 'interaction_audit', audit.audit_id, interaction.turn_id,
      'security_audit_180d', audit.created_at,
      strftime('%Y-%m-%dT%H:%M:%fZ', audit.created_at, '+180 days'),
      'delete', audit.created_at
    FROM runtime_interaction_audit AS audit
    JOIN runtime_interactions AS interaction
      ON interaction.interaction_id = audit.interaction_id
  `).run();
    database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT 'permission_action_decision', decision_id, turn_id,
      'security_audit_180d', checked_at,
      strftime('%Y-%m-%dT%H:%M:%fZ', checked_at, '+180 days'),
      'delete', checked_at
    FROM runtime_permission_action_decisions
  `).run();
  database.prepare(`
    INSERT OR IGNORE INTO runtime_retention_entries (
      record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind, registered_at
    )
    SELECT 'operations_idempotency_conflict',
      json_array(caller_namespace, control_id, request_hash), NULL,
      'security_audit_180d', committed_at,
      strftime('%Y-%m-%dT%H:%M:%fZ', committed_at, '+180 days'),
      'delete', committed_at
    FROM runtime_operations_idempotency_conflicts
    `).run();
  addColumnIfMissing(
    database,
    'runtime_reply_mapping_recoveries',
    'notice_event_sequence',
    'INTEGER CHECK (notice_event_sequence IS NULL OR notice_event_sequence > 0)',
  );
  addColumnIfMissing(
    database,
    'runtime_reply_mapping_recoveries',
    'native_recovery_attempt_id',
    'TEXT',
  );
  addColumnIfMissing(
    database,
    'runtime_reply_mapping_recoveries',
    'native_recovery_status',
    'TEXT',
  );
  addColumnIfMissing(
    database,
    'runtime_reply_mapping_recoveries',
    'native_recovery_result_json',
    'TEXT',
  );
  addColumnIfMissing(
    database,
    'runtime_reply_mapping_recoveries',
    'native_recovery_owner_service_instance_id',
    'TEXT',
  );
  addColumnIfMissing(
    database,
    'runtime_reply_mapping_recoveries',
    'native_recovery_claim_expires_at',
    'TEXT',
  );
  addColumnIfMissing(
    database,
    'runtime_reply_mapping_recoveries',
    'bound_lineage_id',
    'TEXT REFERENCES runtime_lineages(lineage_id)',
  );
  addColumnIfMissing(database, 'runtime_executor_residents', 'owner_service_instance_id', 'TEXT');
  addColumnIfMissing(
    database,
    'runtime_executor_residents',
    'owner_epoch',
    'INTEGER NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0)',
  );
  addColumnIfMissing(database, 'runtime_executor_residents', 'owner_expires_at', 'TEXT');
  addColumnIfMissing(database, 'runtime_interaction_answers', 'payload_hash', 'TEXT');
  migrateInteractionControlStorage(database);
  migrateNullableSecurityControlInteractions(database);
  addColumnIfMissing(database, 'runtime_permission_controls', 'final_result_json', 'TEXT');
  addColumnIfMissing(database, 'runtime_permission_confirmations', 'target_grant_id', 'TEXT');
  addColumnIfMissing(
    database,
    'runtime_lineages',
    'provider',
    "TEXT CHECK (provider IS NULL OR provider IN ('claude', 'codex'))",
  );
  addColumnIfMissing(database, 'runtime_lineages', 'provider_native_id', 'TEXT');
  addColumnIfMissing(database, 'runtime_lineages', 'provider_native_id_bound_at', 'TEXT');
  addColumnIfMissing(
    database,
    'runtime_lineages',
    'provider_native_state',
    "TEXT NOT NULL DEFAULT 'unknown' CHECK (provider_native_state IN ('unknown', 'valid', 'invalid'))",
  );
  addColumnIfMissing(
    database,
    'runtime_delivery_lanes',
    'delivery_mode',
    "TEXT NOT NULL DEFAULT 'main' CHECK (delivery_mode IN ('main', 'text'))",
  );
  addColumnIfMissing(
    database,
    'runtime_delivery_lanes',
    'lane_identity_version',
    'INTEGER NOT NULL DEFAULT 0 CHECK (lane_identity_version IN (0, 1))',
  );
  addColumnIfMissing(
    database,
    'runtime_outbox',
    'attempt_count',
    'INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0)',
  );
  addColumnIfMissing(database, 'runtime_outbox', 'delivery_attempt_id', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'lane_key', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'predecessor_delivery_id', 'TEXT');
  addColumnIfMissing(
    database,
    'runtime_outbox',
    'delivery_attempt_no',
    'INTEGER CHECK (delivery_attempt_no IS NULL OR delivery_attempt_no > 0)',
  );
  addColumnIfMissing(
    database,
    'runtime_outbox',
    'outbox_lease_epoch',
    'INTEGER NOT NULL DEFAULT 0 CHECK (outbox_lease_epoch >= 0)',
  );
  addColumnIfMissing(database, 'runtime_outbox', 'lease_owner', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'lease_expires_at', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'lease_expires_epoch_ms', 'INTEGER');
  backfillOutboxLeaseEpochs(database);
  addColumnIfMissing(database, 'runtime_outbox', 'pre_action_fenced_at', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'last_attempt_at', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'next_attempt_at', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'last_error_json', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'result_json', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'claimed_command_hash', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'updated_at', 'TEXT');
  addColumnIfMissing(
    database,
    'runtime_outbox',
    'priority',
    'INTEGER NOT NULL DEFAULT 0',
  );
  addColumnIfMissing(
    database,
    'runtime_outbox',
    'supersedable',
    'INTEGER NOT NULL DEFAULT 0 CHECK (supersedable IN (0, 1))',
  );
  addColumnIfMissing(
    database,
    'runtime_outbox',
    'terminal',
    'INTEGER NOT NULL DEFAULT 0 CHECK (terminal IN (0, 1))',
  );
  quarantineUnverifiableOutboxClaims(database);
  migrateLegacyOutboxConstraint(database);
  backfillDeliveryLanes(database);
  migrateDeliveryLaneIdentity(database);
  database.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS runtime_lineages_provider_native_id
      ON runtime_lineages(provider, provider_native_id)
      WHERE provider IS NOT NULL AND provider_native_id IS NOT NULL;
    DROP INDEX IF EXISTS runtime_outbox_dispatch;
    CREATE INDEX IF NOT EXISTS runtime_outbox_dispatch
      ON runtime_outbox(status, next_attempt_at, lease_expires_epoch_ms, priority, created_at);
    CREATE INDEX IF NOT EXISTS runtime_outbox_lane
      ON runtime_outbox(lane_key, aggregate_version, status);
    CREATE INDEX IF NOT EXISTS runtime_outbox_claim_snapshot_attempt
      ON runtime_outbox_claim_snapshots(outbox_id, delivery_attempt_id, outbox_lease_epoch);
    CREATE INDEX IF NOT EXISTS runtime_reply_mapping_recovery_dispatch
      ON runtime_reply_mapping_recoveries(state, created_at);
    CREATE INDEX IF NOT EXISTS runtime_reply_mapping_recovery_source
      ON runtime_reply_mapping_recoveries(source_platform_message_id, state);

    DROP TRIGGER IF EXISTS runtime_bound_message_mapping_immutable;
    CREATE TRIGGER runtime_bound_message_mapping_immutable
    BEFORE UPDATE ON runtime_message_mappings
    WHEN OLD.binding_state = 'bound'
    BEGIN
      SELECT RAISE(ABORT, 'bound reply mapping is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_bound_message_mapping_delete_immutable
    BEFORE DELETE ON runtime_message_mappings
    WHEN OLD.binding_state = 'bound'
    BEGIN
      SELECT RAISE(ABORT, 'bound reply mapping cannot be deleted');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_bound_recovery_turn_lineage_immutable
    BEFORE UPDATE OF lineage_id ON runtime_turns
    WHEN NEW.lineage_id IS NOT OLD.lineage_id AND EXISTS (
      SELECT 1 FROM runtime_reply_mapping_recoveries AS recovery
      WHERE recovery.turn_id = OLD.turn_id AND recovery.state = 'bound'
        AND recovery.bound_lineage_id = OLD.lineage_id
    )
    BEGIN
      SELECT RAISE(ABORT, 'bound recovery turn lineage is immutable');
    END;

    DROP TRIGGER IF EXISTS runtime_bound_delivery_lane_mapping_immutable;
    CREATE TRIGGER runtime_bound_delivery_lane_mapping_immutable
    BEFORE UPDATE OF mapping_json ON runtime_delivery_lanes
    WHEN json_extract(OLD.mapping_json, '$.binding_state') = 'bound' AND (
      json_extract(NEW.mapping_json, '$.conversation_id')
        IS NOT json_extract(OLD.mapping_json, '$.conversation_id')
      OR json_extract(NEW.mapping_json, '$.turn_id')
        IS NOT json_extract(OLD.mapping_json, '$.turn_id')
      OR json_extract(NEW.mapping_json, '$.lineage_id')
        IS NOT json_extract(OLD.mapping_json, '$.lineage_id')
      OR json_extract(NEW.mapping_json, '$.binding_state')
        IS NOT json_extract(OLD.mapping_json, '$.binding_state')
      OR json_extract(NEW.mapping_json, '$.reason')
        IS NOT json_extract(OLD.mapping_json, '$.reason')
    )
    BEGIN
      SELECT RAISE(ABORT, 'bound delivery lane mapping is immutable');
    END;

    DROP TRIGGER IF EXISTS runtime_bound_outbox_mapping_immutable;
    CREATE TRIGGER runtime_bound_outbox_mapping_immutable
    BEFORE UPDATE OF command_json ON runtime_outbox
    WHEN json_extract(OLD.command_json, '$.mapping.binding_state') = 'bound' AND (
      json_extract(NEW.command_json, '$.mapping.mapping_id')
        IS NOT json_extract(OLD.command_json, '$.mapping.mapping_id')
      OR json_extract(NEW.command_json, '$.mapping.lineage_id')
        IS NOT json_extract(OLD.command_json, '$.mapping.lineage_id')
      OR json_extract(NEW.command_json, '$.mapping.binding_state')
        IS NOT json_extract(OLD.command_json, '$.mapping.binding_state')
      OR json_extract(NEW.command_json, '$.mapping.mapping_version')
        IS NOT json_extract(OLD.command_json, '$.mapping.mapping_version')
      OR json_extract(NEW.command_json, '$.mapping.conversation_id')
        IS NOT json_extract(OLD.command_json, '$.mapping.conversation_id')
      OR json_extract(NEW.command_json, '$.mapping.turn_id')
        IS NOT json_extract(OLD.command_json, '$.mapping.turn_id')
      OR json_extract(NEW.command_json, '$.mapping.reason')
        IS NOT json_extract(OLD.command_json, '$.mapping.reason')
    )
    BEGIN
      SELECT RAISE(ABORT, 'bound outbox mapping is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_outbox_claim_snapshot_update_immutable
    BEFORE UPDATE ON runtime_outbox_claim_snapshots
    BEGIN
      SELECT RAISE(ABORT, 'outbox claim snapshot is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_outbox_claim_snapshot_insert_once
    BEFORE INSERT ON runtime_outbox_claim_snapshots
    WHEN EXISTS (
      SELECT 1 FROM runtime_outbox_claim_snapshots
      WHERE (outbox_id = NEW.outbox_id AND outbox_lease_epoch = NEW.outbox_lease_epoch)
        OR delivery_attempt_id = NEW.delivery_attempt_id
    )
    BEGIN
      SELECT RAISE(ABORT, 'outbox claim snapshot is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_outbox_claim_snapshot_delete_immutable
    BEFORE DELETE ON runtime_outbox_claim_snapshots
    WHEN EXISTS (
      SELECT 1 FROM runtime_outbox WHERE outbox_id = OLD.outbox_id
    )
    BEGIN
      SELECT RAISE(ABORT, 'active outbox claim snapshot cannot be deleted');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_outbox_claim_snapshot_cleanup
    AFTER DELETE ON runtime_outbox
    BEGIN
      DELETE FROM runtime_outbox_claim_snapshots WHERE outbox_id = OLD.outbox_id;
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_outbox_operator_reconciliation_update_immutable
    BEFORE UPDATE ON runtime_outbox_operator_reconciliations
    BEGIN
      SELECT RAISE(ABORT, 'outbox reconciliation audit is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_outbox_operator_reconciliation_delete_immutable
    BEFORE DELETE ON runtime_outbox_operator_reconciliations
    BEGIN
      SELECT RAISE(ABORT, 'outbox reconciliation audit is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_outbox_delivery_correction_update_immutable
    BEFORE UPDATE ON runtime_outbox_delivery_corrections
    BEGIN
      SELECT RAISE(ABORT, 'outbox delivery correction audit is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_outbox_delivery_correction_delete_immutable
    BEFORE DELETE ON runtime_outbox_delivery_corrections
    BEGIN
      SELECT RAISE(ABORT, 'outbox delivery correction audit is immutable');
    END;

    DROP TRIGGER IF EXISTS runtime_bound_reply_recovery_immutable;
    CREATE TRIGGER runtime_bound_reply_recovery_immutable
    BEFORE UPDATE ON runtime_reply_mapping_recoveries
    WHEN OLD.state = 'bound' AND (
      NEW.recovery_id IS NOT OLD.recovery_id
      OR NEW.turn_id IS NOT OLD.turn_id
      OR NEW.mapping_id IS NOT OLD.mapping_id
      OR NEW.source_platform_message_id IS NOT OLD.source_platform_message_id
      OR NEW.reason IS NOT OLD.reason
      OR NEW.candidate_lineage_id IS NOT OLD.candidate_lineage_id
      OR NEW.side_effect_status IS NOT OLD.side_effect_status
      OR NEW.state IS NOT OLD.state
      OR NEW.notice_event_sequence IS NOT OLD.notice_event_sequence
      OR NEW.native_recovery_attempt_count IS NOT OLD.native_recovery_attempt_count
      OR NEW.native_recovery_attempt_id IS NOT OLD.native_recovery_attempt_id
      OR NEW.native_recovery_status IS NOT OLD.native_recovery_status
      OR NEW.native_recovery_result_json IS NOT OLD.native_recovery_result_json
      OR NEW.native_recovery_owner_service_instance_id
        IS NOT OLD.native_recovery_owner_service_instance_id
      OR NEW.native_recovery_claim_expires_at IS NOT OLD.native_recovery_claim_expires_at
      OR NEW.bound_lineage_id IS NOT OLD.bound_lineage_id
      OR NEW.created_at IS NOT OLD.created_at
      OR NEW.updated_at IS NOT OLD.updated_at
    )
    BEGIN
      SELECT RAISE(ABORT, 'bound reply recovery is immutable');
    END;

    CREATE UNIQUE INDEX IF NOT EXISTS runtime_turns_one_redirect_per_turn
      ON runtime_turns(redirected_from_turn_id)
      WHERE redirected_from_turn_id IS NOT NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS runtime_steer_controls_one_winner_per_turn
      ON runtime_steer_controls(target_turn_id);
    CREATE INDEX IF NOT EXISTS runtime_turn_queue_priority
      ON runtime_turn_queue(conversation_id, priority DESC, queue_sequence ASC, status);
    INSERT OR IGNORE INTO runtime_steer_requests (
      steer_id, conversation_id, target_turn_id, inbound_event_id,
      winner_control_id, request_json, result_json, committed_at, updated_at
    )
    SELECT steer_id, conversation_id, target_turn_id, inbound_event_id,
      steer_id, request_json, result_json, committed_at, updated_at
    FROM runtime_steer_controls
    WHERE inbound_event_id IS NOT NULL;
  `);
}
