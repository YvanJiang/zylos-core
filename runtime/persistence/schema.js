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
    created_at TEXT NOT NULL
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
    enqueued_at TEXT NOT NULL,
    PRIMARY KEY (conversation_id, queue_sequence)
  );

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

  CREATE TABLE IF NOT EXISTS runtime_outbox (
    outbox_id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL UNIQUE,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    turn_id TEXT REFERENCES runtime_turns(turn_id),
    control_id TEXT,
    aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
    status TEXT NOT NULL,
    command_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (aggregate_type, aggregate_id, aggregate_version)
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

function addColumnIfMissing(database, tableName, columnName, definition) {
  const columns = database.prepare(`PRAGMA table_info(${tableName})`).all();
  if (columns.some(({ name }) => name === columnName)) return;
  database.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
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
}
