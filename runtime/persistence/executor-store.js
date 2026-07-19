import {
  admitNormalizedEvent,
  createNormalizedEventStreamState,
  validateDeliveryCommand,
  validateNormalizedEvent,
} from '../../contracts/public/index.js';
import { initializeRuntimePersistence } from './schema.js';

const CANONICAL_TRANSITIONS = Object.freeze({
  queued: 'starting',
  starting: 'running',
  running: 'completed',
});

export class ExecutorPersistenceError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'ExecutorPersistenceError';
    this.code = code;
  }
}

function conflict(code, message) {
  throw new ExecutorPersistenceError(code, message);
}

function sameFence(row, fence) {
  return row.attempt_id === fence.attempt_id
    && row.attempt_no === fence.attempt_no
    && row.lease_epoch === fence.lease_epoch;
}

function loadTurn(database, turnId) {
  const turn = database.prepare(`
    SELECT
      turn.turn_id,
      turn.conversation_id,
      turn.lineage_id,
      turn.state,
      turn.turn_version,
      turn.attempt_id,
      turn.attempt_no,
      turn.lease_epoch,
      inbound.envelope_json
    FROM runtime_turns AS turn
    JOIN runtime_inbound_events AS inbound
      ON inbound.inbound_event_id = turn.inbound_event_id
    WHERE turn.turn_id = ?
  `).get(turnId);
  if (!turn) conflict('turn_not_found', `Turn ${turnId} does not exist.`);
  return turn;
}

function loadStreamState(database, turnId) {
  let state = createNormalizedEventStreamState();
  const rows = database.prepare(`
    SELECT event_json
    FROM runtime_normalized_events
    WHERE turn_id = ?
    ORDER BY event_sequence ASC
  `).all(turnId);
  for (const { event_json: eventJson } of rows) {
    state = admitNormalizedEvent(state, JSON.parse(eventJson));
  }
  return state;
}

function loadLastEvent(database, turnId) {
  return database.prepare(`
    SELECT event_id, event_sequence
    FROM runtime_normalized_events
    WHERE turn_id = ?
    ORDER BY event_sequence DESC
    LIMIT 1
  `).get(turnId);
}

function assertActiveFence(database, turn, fence, serviceInstanceId) {
  if (!sameFence(turn, fence)) {
    conflict('stale_attempt', 'The turn no longer matches this provider attempt fence.');
  }
  const lease = database.prepare(`
    SELECT lease_owner, lease_epoch, turn_id, attempt_id, attempt_no
    FROM runtime_executor_leases
    WHERE conversation_id = ?
  `).get(turn.conversation_id);
  if (
    !lease
    || lease.lease_owner !== serviceInstanceId
    || lease.turn_id !== turn.turn_id
    || !sameFence(lease, fence)
  ) {
    conflict('stale_attempt', 'The executor lease no longer matches this provider attempt fence.');
  }
}

function buildEvent({
  turn,
  lastEvent,
  fence,
  provider,
  descriptor,
  occurredAt,
  generateId,
}) {
  const envelope = JSON.parse(turn.envelope_json);
  const event = {
    contract: 'zylos.normalized-event',
    contract_version: '1.0',
    event_id: generateId('event'),
    trace_id: envelope.trace_id,
    conversation_id: turn.conversation_id,
    turn_id: turn.turn_id,
    lineage_id: turn.lineage_id,
    event_sequence: lastEvent.event_sequence + 1,
    turn_version: turn.turn_version + 1,
    attempt_id: fence.attempt_id,
    attempt_no: fence.attempt_no,
    lease_epoch: fence.lease_epoch,
    kind: descriptor.kind,
    phase: descriptor.phase,
    occurred_at: occurredAt,
    persisted_at: occurredAt,
    provider,
    provider_native_id: descriptor.provider_native_id ?? null,
    payload: descriptor.payload,
    causation_event_id: lastEvent.event_id,
    error: descriptor.error ?? null,
  };
  validateNormalizedEvent(event);
  return event;
}

function projectRenderModel(renderModel, event) {
  let text = renderModel.text;
  if (event.kind === 'text_snapshot') {
    text = event.payload.text;
  } else if (event.kind === 'text_delta') {
    const currentText = event.payload.start_offset === 0 ? '' : (renderModel.text ?? '');
    text = `${currentText.slice(0, event.payload.start_offset)}${event.payload.text}`;
  }
  return {
    ...renderModel,
    phase: event.phase,
    text,
    error: event.error,
    terminal: event.phase === 'completed',
  };
}

function projectPendingDelivery(database, turn, event) {
  const pending = database.prepare(`
    SELECT outbox_id, aggregate_version, command_json
    FROM runtime_outbox
    WHERE turn_id = ?
      AND aggregate_type = 'turn_main'
      AND status = 'pending'
    ORDER BY aggregate_version DESC
    LIMIT 1
  `).get(turn.turn_id);
  if (!pending) {
    conflict(
      'outbox_not_pending',
      `Turn ${turn.turn_id} has no pending main delivery to project.`,
    );
  }
  const currentCommand = JSON.parse(pending.command_json);
  if (
    currentCommand.contract !== 'zylos.delivery-command'
    || currentCommand.aggregate_type !== 'turn_main'
    || currentCommand.aggregate_id !== turn.turn_id
    || currentCommand.operation !== 'create_main'
  ) {
    conflict(
      'outbox_contract_mismatch',
      `Turn ${turn.turn_id} has an incompatible pending main delivery.`,
    );
  }
  const projectedCommand = {
    ...currentCommand,
    aggregate_version: event.turn_version,
    event_sequence_through: event.event_sequence,
    render_model: projectRenderModel(currentCommand.render_model, event),
  };
  validateDeliveryCommand(projectedCommand);
  const projected = database.prepare(`
    UPDATE runtime_outbox
    SET aggregate_version = ?, command_json = ?
    WHERE outbox_id = ?
      AND aggregate_type = 'turn_main'
      AND status = 'pending'
      AND aggregate_version = ?
  `).run(
    event.turn_version,
    JSON.stringify(projectedCommand),
    pending.outbox_id,
    pending.aggregate_version,
  );
  if (projected.changes !== 1) {
    conflict('outbox_version_conflict', 'The pending main delivery changed concurrently.');
  }
}

function persistEvent(database, turn, event) {
  const streamState = loadStreamState(database, turn.turn_id);
  admitNormalizedEvent(streamState, event);
  database.prepare(`
    INSERT INTO runtime_normalized_events (
      event_id, turn_id, event_sequence, turn_version, event_json, persisted_at
    ) VALUES (?, ?, ?, ?, ?, ?)
  `).run(
    event.event_id,
    turn.turn_id,
    event.event_sequence,
    event.turn_version,
    JSON.stringify(event),
    event.persisted_at,
  );
  projectPendingDelivery(database, turn, event);
}

function commitTurnEvent(database, {
  turn,
  event,
  fence,
  nextState,
  staleMessage,
}) {
  const updated = database.prepare(`
    UPDATE runtime_turns
    SET state = ?, turn_version = ?, committed_at = ?
    WHERE turn_id = ? AND state = ?
      AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
  `).run(
    nextState,
    event.turn_version,
    event.persisted_at,
    turn.turn_id,
    turn.state,
    fence.attempt_id,
    fence.attempt_no,
    fence.lease_epoch,
  );
  if (updated.changes !== 1) conflict('stale_attempt', staleMessage);
  persistEvent(database, turn, event);
}

function transitionInTransaction(database, {
  turnId,
  fromState,
  toState,
  fence,
  provider,
  serviceInstanceId,
  occurredAt,
  generateId,
}) {
  if (CANONICAL_TRANSITIONS[fromState] !== toState) {
    conflict('illegal_transition', `Canonical transition ${fromState} -> ${toState} is not allowed.`);
  }
  const turn = loadTurn(database, turnId);
  if (turn.state !== fromState) {
    conflict(
      'illegal_transition',
      `Turn ${turnId} is ${turn.state}; expected ${fromState} before ${toState}.`,
    );
  }
  assertActiveFence(database, turn, fence, serviceInstanceId);
  const event = buildEvent({
    turn,
    lastEvent: loadLastEvent(database, turnId),
    fence,
    provider,
    descriptor: {
      kind: 'turn_state_changed',
      phase: toState,
      payload: {
        from_state: fromState,
        to_state: toState,
        reason_code: `executor_${toState}`,
      },
    },
    occurredAt,
    generateId,
  });
  commitTurnEvent(database, {
    turn,
    event,
    fence,
    nextState: toState,
    staleMessage: 'The canonical turn transition lost its attempt fence.',
  });
  if (toState === 'completed') {
    const completedQueueEntry = database.prepare(`
      UPDATE runtime_turn_queue
      SET status = 'completed'
      WHERE turn_id = ? AND status = 'claimed'
    `).run(turnId);
    if (completedQueueEntry.changes !== 1) {
      conflict('stale_attempt', 'The canonical turn completion lost its queue claim.');
    }
    const released = database.prepare(`
      UPDATE runtime_executor_leases
      SET
        lease_owner = NULL,
        turn_id = NULL,
        attempt_id = NULL,
        attempt_no = NULL,
        lease_expires_at = NULL,
        updated_at = ?
      WHERE conversation_id = ? AND lease_owner = ? AND turn_id = ?
        AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
    `).run(
      occurredAt,
      turn.conversation_id,
      serviceInstanceId,
      turnId,
      fence.attempt_id,
      fence.attempt_no,
      fence.lease_epoch,
    );
    if (released.changes !== 1) {
      conflict('stale_attempt', 'The canonical turn completion lost its executor lease.');
    }
  }
  return event;
}

export function createExecutorStore({
  database,
  provider,
  serviceInstanceId,
  now,
  generateId,
  leaseDurationMs = 10_000,
}) {
  initializeRuntimePersistence(database);

  function rebuildExecutorCache() {
    const rows = database.prepare(`
      SELECT conversation_id, turn_id, status, queue_sequence
      FROM runtime_turn_queue
      WHERE status IN ('queued', 'claimed')
      ORDER BY conversation_id ASC, queue_sequence ASC
    `).all();
    const executors = new Map();
    for (const row of rows) {
      let projection = executors.get(row.conversation_id);
      if (!projection) {
        projection = {
          conversation_id: row.conversation_id,
          active_turn_id: null,
          queued_turn_ids: [],
        };
        executors.set(row.conversation_id, projection);
      }
      if (row.status === 'claimed') projection.active_turn_id = row.turn_id;
      else projection.queued_turn_ids.push(row.turn_id);
    }
    return [...executors.values()];
  }

  function claimNextQueuedTurn() {
    const claim = database.transaction(() => {
      const turn = database.prepare(`
        SELECT turn.turn_id
        FROM runtime_turn_queue AS queue
        JOIN runtime_turns AS turn ON turn.turn_id = queue.turn_id
        WHERE queue.status = 'queued' AND turn.state = 'queued'
          AND NOT EXISTS (
            SELECT 1
            FROM runtime_turn_queue AS earlier
            WHERE earlier.conversation_id = queue.conversation_id
              AND earlier.queue_sequence < queue.queue_sequence
              AND earlier.status IN ('queued', 'claimed')
          )
          AND NOT EXISTS (
            SELECT 1
            FROM runtime_executor_leases AS lease
            WHERE lease.conversation_id = queue.conversation_id
              AND lease.lease_owner IS NOT NULL
          )
        ORDER BY turn.created_at ASC, turn.conversation_id ASC, queue.queue_sequence ASC
        LIMIT 1
      `).get();
      if (!turn) return null;

      const claimedAt = now();
      const current = loadTurn(database, turn.turn_id);
      const existingLease = database.prepare(`
        SELECT lease_epoch
        FROM runtime_executor_leases
        WHERE conversation_id = ?
      `).get(current.conversation_id);
      const fence = {
        attempt_id: generateId('attempt'),
        attempt_no: 1,
        lease_epoch: (existingLease?.lease_epoch ?? 0) + 1,
      };
      const leaseExpiresAt = new Date(
        Date.parse(claimedAt) + leaseDurationMs,
      ).toISOString();
      const leaseWrite = database.prepare(`
        INSERT INTO runtime_executor_leases (
          conversation_id, lease_owner, lease_epoch, turn_id, attempt_id,
          attempt_no, lease_expires_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(conversation_id) DO UPDATE SET
          lease_owner = excluded.lease_owner,
          lease_epoch = excluded.lease_epoch,
          turn_id = excluded.turn_id,
          attempt_id = excluded.attempt_id,
          attempt_no = excluded.attempt_no,
          lease_expires_at = excluded.lease_expires_at,
          updated_at = excluded.updated_at
        WHERE runtime_executor_leases.lease_owner IS NULL
      `).run(
        current.conversation_id,
        serviceInstanceId,
        fence.lease_epoch,
        current.turn_id,
        fence.attempt_id,
        fence.attempt_no,
        leaseExpiresAt,
        claimedAt,
      );
      if (leaseWrite.changes !== 1) {
        conflict('stale_attempt', 'The executor lease was claimed concurrently.');
      }
      const turnUpdate = database.prepare(`
        UPDATE runtime_turns
        SET attempt_id = ?, attempt_no = ?, lease_epoch = ?
        WHERE turn_id = ? AND state = 'queued' AND attempt_id IS NULL
      `).run(
        fence.attempt_id,
        fence.attempt_no,
        fence.lease_epoch,
        current.turn_id,
      );
      const queueUpdate = database.prepare(`
        UPDATE runtime_turn_queue
        SET status = 'claimed'
        WHERE turn_id = ? AND status = 'queued'
      `).run(current.turn_id);
      if (turnUpdate.changes !== 1 || queueUpdate.changes !== 1) {
        conflict('stale_attempt', 'The durable queue entry was claimed concurrently.');
      }
      transitionInTransaction(database, {
        turnId: current.turn_id,
        fromState: 'queued',
        toState: 'starting',
        fence,
        provider,
        serviceInstanceId,
        occurredAt: claimedAt,
        generateId,
      });
      const envelope = JSON.parse(current.envelope_json);
      return {
        conversation_id: current.conversation_id,
        turn_id: current.turn_id,
        lineage_id: current.lineage_id,
        trace_id: envelope.trace_id,
        input: envelope.content,
        attempt: fence,
      };
    });
    return claim.immediate();
  }

  function transitionTurn(turnContext, fromState, toState) {
    const transition = database.transaction(() => transitionInTransaction(database, {
      turnId: turnContext.turn_id,
      fromState,
      toState,
      fence: turnContext.attempt,
      provider,
      serviceInstanceId,
      occurredAt: now(),
      generateId,
    }));
    return transition.immediate();
  }

  function appendAdapterEvent(turnContext, descriptor) {
    if (descriptor.kind === 'turn_state_changed') {
      conflict('illegal_transition', 'Provider adapters cannot author canonical state transitions.');
    }
    const append = database.transaction(() => {
      const turn = loadTurn(database, turnContext.turn_id);
      if (turn.state !== 'running') {
        conflict('illegal_transition', `Adapter output is invalid while turn is ${turn.state}.`);
      }
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      const event = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence: turnContext.attempt,
        provider,
        descriptor: {
          ...descriptor,
          phase: 'running',
        },
        occurredAt: now(),
        generateId,
      });
      commitTurnEvent(database, {
        turn,
        event,
        fence: turnContext.attempt,
        nextState: 'running',
        staleMessage: 'The adapter event lost its provider attempt fence.',
      });
      return event;
    });
    return append.immediate();
  }

  return Object.freeze({
    appendAdapterEvent,
    claimNextQueuedTurn,
    rebuildExecutorCache,
    transitionTurn,
  });
}
