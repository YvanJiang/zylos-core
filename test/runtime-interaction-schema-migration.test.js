import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { initializeRuntimePersistence } from '../runtime/persistence/schema.js';
import {
  cleanupInteractionTestDatabases,
  createRunningInteractionTurn,
  interactionAnswer,
  openInteractionTestDatabase,
} from './helpers/runtime-interaction-fixtures.js';

function downgradeInteractionStorageToIssue16(database) {
  database.pragma('foreign_keys = OFF');
  database.exec(`
    ALTER TABLE runtime_interactions RENAME TO runtime_interactions_issue17;
    ALTER TABLE runtime_interaction_answers RENAME TO runtime_interaction_answers_issue17;
    ALTER TABLE runtime_interaction_handoffs RENAME TO runtime_interaction_handoffs_issue17;
    ALTER TABLE runtime_interaction_audit RENAME TO runtime_interaction_audit_issue17;

    CREATE TABLE runtime_interactions (
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
    INSERT INTO runtime_interactions (
      interaction_id, conversation_id, turn_id, lineage_id, ordinal, state, version,
      handoff_state, handoff_version, request_json, created_at, updated_at
    )
    SELECT interaction_id, conversation_id, turn_id, lineage_id, ordinal, state, version,
      handoff_state, handoff_version, request_json, created_at, updated_at
    FROM runtime_interactions_issue17;

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
    SELECT * FROM runtime_interaction_answers_issue17;

    CREATE TABLE runtime_interaction_handoffs (
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
    INSERT INTO runtime_interaction_handoffs (
      handoff_id, interaction_id, answer_id, state, provider_attempt_id,
      handoff_attempt_id, handoff_attempt_no, lease_epoch, record_json, created_at, updated_at
    )
    SELECT handoff_id, interaction_id, answer_id, state, provider_attempt_id,
      handoff_attempt_id, handoff_attempt_no, lease_epoch, record_json, created_at, updated_at
    FROM runtime_interaction_handoffs_issue17;

    CREATE TABLE runtime_interaction_audit (
      audit_id TEXT PRIMARY KEY,
      interaction_id TEXT NOT NULL REFERENCES runtime_interactions(interaction_id),
      handoff_id TEXT NOT NULL REFERENCES runtime_interaction_handoffs(handoff_id),
      outcome TEXT NOT NULL,
      provider_attempt_id TEXT NOT NULL,
      lease_epoch INTEGER NOT NULL CHECK (lease_epoch > 0),
      acknowledgement_json TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
    INSERT INTO runtime_interaction_audit
    SELECT * FROM runtime_interaction_audit_issue17;

    DROP TABLE runtime_interaction_audit_issue17;
    DROP TABLE runtime_interaction_handoffs_issue17;
    DROP TABLE runtime_interaction_answers_issue17;
    DROP TABLE runtime_interactions_issue17;
  `);
  database.pragma('foreign_keys = ON');
}

function populateProviderInteraction(database) {
  const { store, turnContext } = createRunningInteractionTurn(database, 'schema-migration');
  const request = store.requestInteraction(turnContext, {
    provider_interaction_ref: 'provider-schema-migration',
    tool_use_id: 'tool-schema-migration',
    kind: 'tool_approval',
    prompt: 'Preserve this interaction during schema migration?',
    choices: [],
    authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
    allowed_sources: ['card_action'],
  });
  const committed = store.commitInteractionAnswer(interactionAnswer(request, 'schema-migration'));
  const claimed = store.claimInteractionHandoff(committed.handoff_id);
  const sending = store.markInteractionHandoffSendStarted(claimed);
  store.acknowledgeInteractionHandoff({
    status: 'accepted',
    handoff_id: sending.handoff.handoff_id,
    provider_attempt_id: sending.handoff.provider_attempt_id,
    handoff_attempt_id: sending.handoff.handoff_attempt_id,
    handoff_attempt_no: sending.handoff.handoff_attempt_no,
    lease_epoch: sending.handoff.lease_epoch,
  });
  return request;
}

function readInteractionRows(database, interactionId) {
  return {
    interaction: database.prepare(`
      SELECT interaction_id, conversation_id, turn_id, lineage_id, state, version,
        handoff_state, handoff_version, request_json, created_at, updated_at
      FROM runtime_interactions WHERE interaction_id = ?
    `).get(interactionId),
    answer: database.prepare(`
      SELECT * FROM runtime_interaction_answers WHERE interaction_id = ?
    `).get(interactionId),
    handoff: database.prepare(`
      SELECT handoff_id, interaction_id, answer_id, state, provider_attempt_id,
        handoff_attempt_id, handoff_attempt_no, lease_epoch, record_json, created_at, updated_at
      FROM runtime_interaction_handoffs WHERE interaction_id = ?
    `).get(interactionId),
    audit: database.prepare(`
      SELECT * FROM runtime_interaction_audit WHERE interaction_id = ?
    `).get(interactionId),
  };
}

afterEach(() => {
  cleanupInteractionTestDatabases();
});

describe('runtime interaction schema migration', () => {
  test('reopens populated issue16 storage without losing rows and remains idempotent', () => {
    const database = openInteractionTestDatabase('interaction-schema-migration');
    initializeRuntimePersistence(database);
    const request = populateProviderInteraction(database);
    downgradeInteractionStorageToIssue16(database);
    const before = readInteractionRows(database, request.interaction_id);
    const file = database.prepare('PRAGMA database_list').all()
      .find(({ name }) => name === 'main').file;
    database.close();

    const reopened = new Database(file);
    initializeRuntimePersistence(reopened);
    expect(readInteractionRows(reopened, request.interaction_id)).toEqual(before);
    expect(reopened.prepare(`
      SELECT parent_type, parent_id FROM runtime_interactions WHERE interaction_id = ?
    `).get(request.interaction_id)).toEqual({
      parent_type: 'provider_turn',
      parent_id: request.turn_id,
    });
    expect(reopened.prepare(`
      SELECT parent_type FROM runtime_interaction_handoffs WHERE interaction_id = ?
    `).get(request.interaction_id)).toEqual({ parent_type: 'provider_turn' });
    expect(reopened.prepare('PRAGMA foreign_key_check').all()).toEqual([]);

    initializeRuntimePersistence(reopened);
    expect(readInteractionRows(reopened, request.interaction_id)).toEqual(before);
    expect(reopened.prepare('PRAGMA foreign_key_check').all()).toEqual([]);
    reopened.close();
  });

  test('rolls back the issue16 rewrite when migrated rows violate a foreign key', () => {
    const database = openInteractionTestDatabase('interaction-schema-migration-rollback');
    initializeRuntimePersistence(database);
    const request = populateProviderInteraction(database);
    downgradeInteractionStorageToIssue16(database);
    database.pragma('foreign_keys = OFF');
    database.prepare(`
      UPDATE runtime_interaction_audit SET interaction_id = 'missing-interaction'
      WHERE interaction_id = ?
    `).run(request.interaction_id);

    expect(() => initializeRuntimePersistence(database))
      .toThrow('Interaction control storage migration violated foreign keys.');
    expect(database.prepare("PRAGMA table_info('runtime_interactions')").all()
      .some(({ name }) => name === 'parent_type')).toBe(false);
    expect(database.prepare(`
      SELECT interaction_id FROM runtime_interaction_audit
    `).get()).toEqual({ interaction_id: 'missing-interaction' });
    database.close();
  });
});
