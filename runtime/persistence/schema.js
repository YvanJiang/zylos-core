import { createDeliveryLaneKey } from './delivery-lane-key.js';

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
    wait_reason TEXT,
    enqueued_at TEXT NOT NULL,
    PRIMARY KEY (conversation_id, queue_sequence)
  );

  CREATE UNIQUE INDEX IF NOT EXISTS runtime_turns_one_active_per_conversation
    ON runtime_turns(conversation_id)
    WHERE state IN ('starting', 'running', 'waiting_user', 'redirecting', 'recovering');

  CREATE TABLE IF NOT EXISTS runtime_executor_residents (
    conversation_id TEXT PRIMARY KEY REFERENCES runtime_conversations(conversation_id),
    bot_id TEXT NOT NULL,
    provider TEXT NOT NULL CHECK (provider IN ('claude', 'codex')),
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
    lineage_id TEXT NOT NULL REFERENCES runtime_lineages(lineage_id),
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    state TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    handoff_state TEXT NOT NULL,
    handoff_version INTEGER CHECK (handoff_version IS NULL OR handoff_version > 0),
    request_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (turn_id, ordinal)
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
    provider_attempt_id TEXT NOT NULL,
    handoff_attempt_id TEXT,
    handoff_attempt_no INTEGER CHECK (
      handoff_attempt_no IS NULL OR handoff_attempt_no > 0
    ),
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch > 0),
    record_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_interaction_audit (
    audit_id TEXT PRIMARY KEY,
    interaction_id TEXT NOT NULL REFERENCES runtime_interactions(interaction_id),
    handoff_id TEXT NOT NULL REFERENCES runtime_interaction_handoffs(handoff_id),
    outcome TEXT NOT NULL,
    provider_attempt_id TEXT NOT NULL,
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch > 0),
    acknowledgement_json TEXT NOT NULL,
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS runtime_outbox ${OUTBOX_TABLE_SCHEMA};

  CREATE TABLE IF NOT EXISTS runtime_delivery_lanes (
    lane_key TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL UNIQUE REFERENCES runtime_turns(turn_id),
    aggregate_type TEXT NOT NULL,
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
        lane_key, turn_id, aggregate_type, delivery_mode, target_json, mapping_json,
        platform_message_id, applied_platform_version, last_delivery_id,
        last_applied_version, last_delivered_at, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
  addColumnIfMissing(database, 'runtime_turn_queue', 'wait_reason', 'TEXT');
  addColumnIfMissing(database, 'runtime_interaction_answers', 'payload_hash', 'TEXT');
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
    'runtime_delivery_lanes',
    'delivery_mode',
    "TEXT NOT NULL DEFAULT 'main' CHECK (delivery_mode IN ('main', 'text'))",
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
  database.exec(`
    DROP INDEX IF EXISTS runtime_outbox_dispatch;
    CREATE INDEX IF NOT EXISTS runtime_outbox_dispatch
      ON runtime_outbox(status, next_attempt_at, priority, created_at);
    CREATE INDEX IF NOT EXISTS runtime_outbox_lane
      ON runtime_outbox(lane_key, aggregate_version, status);
    CREATE UNIQUE INDEX IF NOT EXISTS runtime_lineages_provider_native_id
      ON runtime_lineages(provider, provider_native_id)
      WHERE provider IS NOT NULL AND provider_native_id IS NOT NULL;
  `);
}
