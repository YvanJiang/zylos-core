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
    last_attempt_at TEXT,
    next_attempt_at TEXT,
    last_error_json TEXT,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT
  )`;

const RUNTIME_SCHEMA = `
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
    enqueued_at TEXT NOT NULL,
    PRIMARY KEY (conversation_id, queue_sequence)
  );

  CREATE UNIQUE INDEX IF NOT EXISTS runtime_turns_one_active_per_conversation
    ON runtime_turns(conversation_id)
    WHERE state IN ('starting', 'running', 'waiting_user', 'redirecting', 'recovering');

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
    turn_id TEXT NOT NULL REFERENCES runtime_turns(turn_id),
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
    UNIQUE (parent_type, parent_id, ordinal)
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
    bound_lineage_id TEXT REFERENCES runtime_lineages(lineage_id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
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
  'priority',
  'supersedable',
  'terminal',
  'attempt_count',
  'delivery_attempt_id',
  'delivery_attempt_no',
  'outbox_lease_epoch',
  'lease_owner',
  'lease_expires_at',
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

function migrateInteractionControlStorage(database) {
  const columns = database.prepare("PRAGMA table_info('runtime_interactions')").all();
  if (columns.some(({ name }) => name === 'parent_type')) return;

  database.pragma('foreign_keys = OFF');
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
          turn_id TEXT NOT NULL REFERENCES runtime_turns(turn_id),
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
          UNIQUE (parent_type, parent_id, ordinal)
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
  const migrate = database.transaction(() => {
    database.exec(`
      DROP TABLE IF EXISTS runtime_projection_snapshots;
      DROP TABLE IF EXISTS runtime_delivery_lanes;
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
    'runtime_lineages',
    'recovery_of_lineage_id',
    'TEXT REFERENCES runtime_lineages(lineage_id)',
  );
  addColumnIfMissing(
    database,
    'runtime_turns',
    'lease_epoch',
    'INTEGER CHECK (lease_epoch IS NULL OR lease_epoch > 0)',
  );
  addColumnIfMissing(database, 'runtime_turns', 'provider_input_json', 'TEXT');
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
  addColumnIfMissing(database, 'runtime_outbox', 'last_attempt_at', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'next_attempt_at', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'last_error_json', 'TEXT');
  addColumnIfMissing(database, 'runtime_outbox', 'result_json', 'TEXT');
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
  migrateLegacyOutboxConstraint(database);
  backfillDeliveryLanes(database);
  migrateDeliveryLaneIdentity(database);
  database.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS runtime_lineages_provider_native_id
      ON runtime_lineages(provider, provider_native_id)
      WHERE provider IS NOT NULL AND provider_native_id IS NOT NULL;
    DROP INDEX IF EXISTS runtime_outbox_dispatch;
    CREATE INDEX IF NOT EXISTS runtime_outbox_dispatch
      ON runtime_outbox(status, next_attempt_at, priority, created_at);
    CREATE INDEX IF NOT EXISTS runtime_outbox_lane
      ON runtime_outbox(lane_key, aggregate_version, status);
    CREATE INDEX IF NOT EXISTS runtime_reply_mapping_recovery_dispatch
      ON runtime_reply_mapping_recoveries(state, created_at);
    CREATE INDEX IF NOT EXISTS runtime_reply_mapping_recovery_source
      ON runtime_reply_mapping_recoveries(source_platform_message_id, state);

    CREATE TRIGGER IF NOT EXISTS runtime_bound_message_mapping_immutable
    BEFORE UPDATE ON runtime_message_mappings
    WHEN OLD.binding_state = 'bound' AND (
      NEW.mapping_id IS NOT OLD.mapping_id
      OR NEW.conversation_id IS NOT OLD.conversation_id
      OR NEW.turn_id IS NOT OLD.turn_id
      OR NEW.lineage_id IS NOT OLD.lineage_id
      OR NEW.binding_state IS NOT OLD.binding_state
      OR NEW.mapping_version IS NOT OLD.mapping_version
    )
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

    CREATE TRIGGER IF NOT EXISTS runtime_bound_delivery_lane_mapping_immutable
    BEFORE UPDATE OF mapping_json ON runtime_delivery_lanes
    WHEN json_extract(OLD.mapping_json, '$.binding_state') = 'bound' AND (
      json_extract(NEW.mapping_json, '$.lineage_id')
        IS NOT json_extract(OLD.mapping_json, '$.lineage_id')
      OR json_extract(NEW.mapping_json, '$.binding_state')
        IS NOT json_extract(OLD.mapping_json, '$.binding_state')
    )
    BEGIN
      SELECT RAISE(ABORT, 'bound delivery lane mapping is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_bound_outbox_mapping_immutable
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
    )
    BEGIN
      SELECT RAISE(ABORT, 'bound outbox mapping is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS runtime_bound_reply_recovery_immutable
    BEFORE UPDATE ON runtime_reply_mapping_recoveries
    WHEN OLD.state = 'bound' AND (
      NEW.state IS NOT OLD.state
      OR NEW.bound_lineage_id IS NOT OLD.bound_lineage_id
      OR NEW.mapping_id IS NOT OLD.mapping_id
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
