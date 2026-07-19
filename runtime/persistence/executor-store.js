import {
  admitNormalizedEvent,
  createNormalizedEventStreamState,
  validateInteractionAnswer,
  validateInteractionAnswerResult,
  validateInteractionHandoff,
  validateInteractionHandoffTransition,
  validateInteractionRequest,
  validateInteractionTransition,
  validateNormalizedEvent,
} from '../../contracts/public/index.js';
import { stageMainProjection } from './main-projection.js';
import { initializeRuntimePersistence } from './schema.js';

const CANONICAL_TRANSITIONS = Object.freeze({
  queued: Object.freeze(['starting']),
  starting: Object.freeze(['running', 'stopped', 'failed']),
  running: Object.freeze(['waiting_user', 'recovering', 'completed', 'stopped', 'failed']),
  waiting_user: Object.freeze(['running', 'recovering', 'stopped', 'timed_out', 'failed']),
  recovering: Object.freeze(['running', 'stopped', 'failed', 'interrupted']),
});

const TERMINAL_STATES = new Set(['completed', 'stopped', 'failed', 'interrupted', 'timed_out']);

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
      lineage.provider,
      lineage.provider_native_id,
      inbound.envelope_json
    FROM runtime_turns AS turn
    JOIN runtime_inbound_events AS inbound
      ON inbound.inbound_event_id = turn.inbound_event_id
    LEFT JOIN runtime_lineages AS lineage
      ON lineage.lineage_id = turn.lineage_id
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

function assertBoundProviderNativeId(turn, provider, providerNativeId) {
  if (
    turn.provider !== provider
    || turn.provider_native_id !== providerNativeId
  ) {
    conflict(
      'stale_attempt',
      'Provider output cannot reference a native ID before the lineage binding is durable.',
    );
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
    attempt_id: fence?.attempt_id ?? null,
    attempt_no: fence?.attempt_no ?? null,
    lease_epoch: fence?.lease_epoch ?? null,
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

function persistEvent(database, turn, event, generateId) {
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
  stageMainProjection(database, turn, event, { generateId });
}

function commitTurnEvent(database, {
  turn,
  event,
  fence,
  nextState,
  staleMessage,
  generateId,
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
  persistEvent(database, turn, event, generateId);
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
  error = null,
  reasonCode = `executor_${toState}`,
}) {
  if (!CANONICAL_TRANSITIONS[fromState]?.includes(toState)) {
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
        reason_code: reasonCode,
      },
      error: error === null ? null : {
        ...error,
        occurred_at: error.occurred_at ?? occurredAt,
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
    generateId,
  });
  if (TERMINAL_STATES.has(toState)) {
    const terminalQueueEntry = database.prepare(`
      UPDATE runtime_turn_queue
      SET status = ?
      WHERE turn_id = ? AND status = 'claimed'
    `).run(toState, turnId);
    if (terminalQueueEntry.changes !== 1) {
      conflict('stale_attempt', 'The canonical terminal transition lost its queue claim.');
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
      conflict('stale_attempt', 'The canonical terminal transition lost its executor lease.');
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
  interactionTimeoutMs = 10 * 60_000,
}) {
  initializeRuntimePersistence(database);

  function rebuildExecutorCache() {
    const rows = database.prepare(`
      SELECT conversation_id, turn_id, status, wait_reason, queue_sequence
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
          wait_reason: null,
        };
        executors.set(row.conversation_id, projection);
      }
      if (row.status === 'claimed') projection.active_turn_id = row.turn_id;
      else {
        projection.queued_turn_ids.push(row.turn_id);
        projection.wait_reason ??= row.wait_reason;
      }
    }
    return [...executors.values()];
  }

  function listClaimableQueuedTurns() {
    return database.prepare(`
      SELECT
        turn.turn_id,
        turn.conversation_id,
        conversation.bot_id,
        turn.created_at,
        queue.queue_sequence
      FROM runtime_turn_queue AS queue
      JOIN runtime_turns AS turn ON turn.turn_id = queue.turn_id
      JOIN runtime_conversations AS conversation
        ON conversation.conversation_id = turn.conversation_id
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
    `).all();
  }

  function markCapacityWaitInTransaction(turnId, occurredAt) {
    const queued = database.prepare(`
      SELECT turn.turn_id, turn.conversation_id, queue.wait_reason
      FROM runtime_turns AS turn
      JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      WHERE turn.turn_id = ? AND turn.state = 'queued' AND queue.status = 'queued'
    `).get(turnId);
    if (!queued) return null;
    const result = {
      conversation_id: queued.conversation_id,
      turn_id: queued.turn_id,
      wait_reason: 'executor_capacity',
    };
    if (queued.wait_reason === 'executor_capacity') return result;

    const turn = loadTurn(database, turnId);
    const event = buildEvent({
      turn,
      lastEvent: loadLastEvent(database, turnId),
      fence: null,
      provider: null,
      descriptor: {
        kind: 'turn_state_changed',
        phase: 'queued',
        payload: {
          from_state: 'queued',
          to_state: 'queued',
          reason_code: 'executor_capacity',
        },
      },
      occurredAt,
      generateId,
    });
    const turnUpdate = database.prepare(`
      UPDATE runtime_turns
      SET turn_version = ?, committed_at = ?
      WHERE turn_id = ? AND state = 'queued' AND turn_version = ?
    `).run(event.turn_version, occurredAt, turnId, turn.turn_version);
    const queueUpdate = database.prepare(`
      UPDATE runtime_turn_queue
      SET wait_reason = 'executor_capacity'
      WHERE turn_id = ? AND status = 'queued' AND wait_reason IS NOT 'executor_capacity'
    `).run(turnId);
    if (turnUpdate.changes !== 1 || queueUpdate.changes !== 1) {
      conflict('stale_attempt', 'The capacity-wait projection changed concurrently.');
    }
    persistEvent(database, turn, event, generateId);
    return result;
  }

  function isResidentConversation(conversationId) {
    return database.prepare(`
      SELECT owner_service_instance_id, owner_epoch, owner_expires_at
      FROM runtime_executor_residents
      WHERE conversation_id = ? AND provider = 'claude'
    `).get(conversationId) ?? null;
  }

  function residentOwnerExpiresAt(ownedAt) {
    const timestamp = Date.parse(ownedAt);
    if (!Number.isFinite(timestamp)) {
      throw new TypeError('now must return an ISO timestamp for resident ownership');
    }
    return new Date(timestamp + leaseDurationMs).toISOString();
  }

  function reconcileExpiredResidents() {
    if (provider !== 'claude') return 0;
    const reconciledAt = now();
    const released = database.prepare(`
      DELETE FROM runtime_executor_residents
      WHERE provider = 'claude'
        AND (
          owner_service_instance_id IS NULL
          OR (
            owner_service_instance_id != ?
            AND (owner_expires_at IS NULL OR owner_expires_at <= ?)
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM runtime_turns
          WHERE runtime_turns.conversation_id = runtime_executor_residents.conversation_id
            AND state IN (
              'queued', 'starting', 'running', 'waiting_user',
              'redirecting', 'recovering', 'retrying'
            )
        )
    `).run(serviceInstanceId, reconciledAt);
    return released.changes;
  }

  function residentCountForBot(botId) {
    return database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_executor_residents
      WHERE bot_id = ? AND provider = 'claude'
    `).get(botId).count;
  }

  function reserveNextExecutor({ maxResidentExecutorsPerBot, markCapacityWait = true }) {
    const reserve = database.transaction(() => {
      reconcileExpiredResidents();
      const candidates = listClaimableQueuedTurns();
      if (candidates.length === 0) return { status: 'idle' };
      if (provider !== 'claude') {
        return {
          status: 'ready',
          conversation_id: candidates[0].conversation_id,
        };
      }

      let selected = candidates.find((candidate) => {
        const resident = isResidentConversation(candidate.conversation_id);
        return resident?.owner_service_instance_id === serviceInstanceId;
      });
      if (!selected) {
        const takeoverAt = now();
        for (const candidate of candidates) {
          const resident = isResidentConversation(candidate.conversation_id);
          if (!resident || (resident.owner_expires_at && resident.owner_expires_at > takeoverAt)) {
            continue;
          }
          const taken = database.prepare(`
            UPDATE runtime_executor_residents
            SET owner_service_instance_id = ?, owner_epoch = owner_epoch + 1,
              owner_expires_at = ?, last_used_at = ?
            WHERE conversation_id = ? AND provider = 'claude'
              AND owner_epoch = ?
              AND (owner_expires_at IS NULL OR owner_expires_at <= ?)
          `).run(
            serviceInstanceId,
            residentOwnerExpiresAt(takeoverAt),
            takeoverAt,
            candidate.conversation_id,
            resident.owner_epoch,
            takeoverAt,
          );
          if (taken.changes === 1) {
            selected = candidate;
            break;
          }
        }
      }
      if (!selected) {
        for (const candidate of candidates) {
          if (isResidentConversation(candidate.conversation_id)) continue;
          if (residentCountForBot(candidate.bot_id) >= maxResidentExecutorsPerBot) continue;
          const admittedAt = now();
          database.prepare(`
            INSERT INTO runtime_executor_residents (
              conversation_id, bot_id, provider, owner_service_instance_id,
              owner_epoch, owner_expires_at, admitted_at, last_used_at
            ) VALUES (?, ?, 'claude', ?, 1, ?, ?, ?)
          `).run(
            candidate.conversation_id,
            candidate.bot_id,
            serviceInstanceId,
            residentOwnerExpiresAt(admittedAt),
            admittedAt,
            admittedAt,
          );
          selected = candidate;
          break;
        }
      }
      if (selected) {
        const usedAt = now();
        database.prepare(`
          UPDATE runtime_executor_residents
          SET last_used_at = ?, owner_expires_at = ?
          WHERE conversation_id = ? AND provider = 'claude'
            AND owner_service_instance_id = ?
        `).run(
          usedAt,
          residentOwnerExpiresAt(usedAt),
          selected.conversation_id,
          serviceInstanceId,
        );
      }

      const waits = [];
      const blocked = [];
      for (const candidate of candidates) {
        if (candidate.conversation_id === selected?.conversation_id) continue;
        const resident = isResidentConversation(candidate.conversation_id);
        if (resident?.owner_service_instance_id === serviceInstanceId) continue;
        if (resident) {
          blocked.push(candidate);
          if (markCapacityWait || selected) {
            const wait = markCapacityWaitInTransaction(candidate.turn_id, now());
            if (wait) waits.push(wait);
          }
          continue;
        }
        if (residentCountForBot(candidate.bot_id) < maxResidentExecutorsPerBot) continue;
        blocked.push(candidate);
        if (markCapacityWait || selected) {
          const wait = markCapacityWaitInTransaction(candidate.turn_id, now());
          if (wait) waits.push(wait);
        }
      }

      if (selected) {
        return { status: 'ready', conversation_id: selected.conversation_id };
      }
      if (waits.length > 0) return { status: 'capacity_wait', ...waits[0] };
      if (blocked.length > 0) {
        return {
          status: 'capacity_wait',
          conversation_id: blocked[0].conversation_id,
          turn_id: blocked[0].turn_id,
          wait_reason: 'executor_capacity',
        };
      }
      return { status: 'idle' };
    });
    return reserve.immediate();
  }

  function claimNextQueuedTurn({ conversationId = null } = {}) {
    const claim = database.transaction(() => {
      const turn = database.prepare(`
        SELECT turn.turn_id
        FROM runtime_turn_queue AS queue
        JOIN runtime_turns AS turn ON turn.turn_id = queue.turn_id
        WHERE queue.status = 'queued' AND turn.state = 'queued'
          AND (? IS NULL OR queue.conversation_id = ?)
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
      `).get(conversationId, conversationId);
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
        SET status = 'claimed', wait_reason = NULL
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
        provider_native_id: current.provider_native_id,
        trace_id: envelope.trace_id,
        input: envelope.content,
        interaction_authority: envelope.actor?.authenticated === true
          ? [{ type: 'actor', actor_id: envelope.actor.actor_id }]
          : [],
        attempt: fence,
      };
    });
    return claim.immediate();
  }

  function transitionTurn(turnContext, fromState, toState, { error = null } = {}) {
    const transition = database.transaction(() => transitionInTransaction(database, {
      turnId: turnContext.turn_id,
      fromState,
      toState,
      fence: turnContext.attempt,
      provider,
      serviceInstanceId,
      occurredAt: now(),
      generateId,
      error,
    }));
    return transition.immediate();
  }

  function assertCurrentFence(turnContext) {
    const turn = loadTurn(database, turnContext.turn_id);
    assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
    return Object.freeze({ state: turn.state });
  }

  function bindProviderNativeId(turnContext, providerNativeId) {
    if (typeof providerNativeId !== 'string' || providerNativeId.length === 0) {
      throw new TypeError('providerNativeId must be a non-empty string');
    }
    const bind = database.transaction(() => {
      const turn = loadTurn(database, turnContext.turn_id);
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      if (turn.lineage_id === null) {
        conflict('lineage_resolution_pending', 'A provider native ID requires a bound lineage.');
      }
      if (turn.provider !== null || turn.provider_native_id !== null) {
        if (turn.provider === provider && turn.provider_native_id === providerNativeId) {
          return { status: 'already_bound', provider_native_id: providerNativeId };
        }
        conflict(
          'version_conflict',
          'The lineage is already bound to a different provider native ID.',
        );
      }
      const boundAt = now();
      const updated = database.prepare(`
        UPDATE runtime_lineages
        SET provider = ?, provider_native_id = ?, provider_native_id_bound_at = ?
        WHERE lineage_id = ? AND conversation_id = ?
          AND provider IS NULL AND provider_native_id IS NULL
      `).run(
        provider,
        providerNativeId,
        boundAt,
        turn.lineage_id,
        turn.conversation_id,
      );
      if (updated.changes !== 1) {
        conflict('version_conflict', 'The provider lineage binding changed concurrently.');
      }
      return {
        status: 'bound',
        provider_native_id: providerNativeId,
        bound_at: boundAt,
      };
    });
    return bind.immediate();
  }

  function appendAdapterEvent(turnContext, descriptor) {
    if (descriptor.kind === 'turn_state_changed') {
      conflict('illegal_transition', 'Provider adapters cannot author canonical state transitions.');
    }
    const append = database.transaction(() => {
      let turn = loadTurn(database, turnContext.turn_id);
      const restoreWaitingUser = turn.state === 'waiting_user';
      if (restoreWaitingUser) {
        transitionInTransaction(database, {
          turnId: turnContext.turn_id,
          fromState: 'waiting_user',
          toState: 'running',
          fence: turnContext.attempt,
          provider,
          serviceInstanceId,
          occurredAt: now(),
          generateId,
        });
        turn = loadTurn(database, turnContext.turn_id);
      }
      if (turn.state !== 'running') {
        conflict('illegal_transition', `Adapter output is invalid while turn is ${turn.state}.`);
      }
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      if (descriptor.provider_native_id !== null) {
        assertBoundProviderNativeId(turn, provider, descriptor.provider_native_id);
      }
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
        generateId,
      });
      if (restoreWaitingUser) {
        transitionInTransaction(database, {
          turnId: turnContext.turn_id,
          fromState: 'running',
          toState: 'waiting_user',
          fence: turnContext.attempt,
          provider,
          serviceInstanceId,
          occurredAt: now(),
          generateId,
        });
      }
      return event;
    });
    return append.immediate();
  }

  function isConversationEvictable(conversationId) {
    if (typeof conversationId !== 'string' || conversationId.length === 0) {
      throw new TypeError('conversationId must be a non-empty string');
    }
    const blockingTurn = database.prepare(`
      SELECT 1
      FROM runtime_turns
      WHERE conversation_id = ?
        AND state IN (
          'queued', 'starting', 'running', 'waiting_user',
          'redirecting', 'recovering', 'retrying'
        )
      LIMIT 1
    `).get(conversationId);
    return blockingTurn === undefined;
  }

  function releaseExecutorResident(conversationId) {
    if (typeof conversationId !== 'string' || conversationId.length === 0) {
      throw new TypeError('conversationId must be a non-empty string');
    }
    const released = database.prepare(`
      DELETE FROM runtime_executor_residents
      WHERE conversation_id = ? AND provider = ?
        AND (owner_service_instance_id = ? OR owner_service_instance_id IS NULL)
        AND NOT EXISTS (
          SELECT 1
          FROM runtime_turns
          WHERE conversation_id = ?
            AND state IN (
              'queued', 'starting', 'running', 'waiting_user',
              'redirecting', 'recovering', 'retrying'
            )
        )
    `).run(conversationId, provider, serviceInstanceId, conversationId);
    return released.changes === 1;
  }

  function requestInteraction(turnContext, descriptor) {
    const request = database.transaction(() => {
      const requestedAt = now();
      let turn = loadTurn(database, turnContext.turn_id);
      if (!['running', 'waiting_user'].includes(turn.state)) {
        conflict('illegal_transition', `Interaction requests are invalid while turn is ${turn.state}.`);
      }
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      const ordinal = database.prepare(`
        SELECT COALESCE(MAX(ordinal), 0) + 1 AS ordinal
        FROM runtime_interactions
        WHERE turn_id = ?
      `).get(turn.turn_id).ordinal;
      const envelope = JSON.parse(turn.envelope_json);
      const interaction = {
        contract: 'zylos.interaction-request',
        contract_version: '1.0',
        trace_id: envelope.trace_id,
        interaction_id: generateId('interaction'),
        conversation_id: turn.conversation_id,
        turn_id: turn.turn_id,
        lineage_id: turn.lineage_id,
        control_id: null,
        parent_type: 'provider_turn',
        tool_use_id: descriptor.tool_use_id ?? null,
        ordinal,
        kind: descriptor.kind,
        prompt: descriptor.prompt,
        choices: descriptor.choices,
        authorized_subjects: descriptor.authorized_subjects,
        allowed_sources: descriptor.allowed_sources,
        runtime_fence: {
          provider_attempt_id: turnContext.attempt.attempt_id,
          lease_epoch: turnContext.attempt.lease_epoch,
          provider_interaction_ref: descriptor.provider_interaction_ref,
        },
        state: 'pending',
        version: 1,
        handoff_state: 'not_started',
        created_at: requestedAt,
        expires_at: new Date(
          Date.parse(requestedAt) + interactionTimeoutMs,
        ).toISOString(),
        card_delivery_id: null,
      };
      validateInteractionRequest(interaction, { occurredAt: requestedAt });
      database.prepare(`
        INSERT INTO runtime_interactions (
          interaction_id, conversation_id, turn_id, lineage_id, ordinal,
          state, version, handoff_state, handoff_version, request_json,
          created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', 1, 'not_started', NULL, ?, ?, ?)
      `).run(
        interaction.interaction_id,
        interaction.conversation_id,
        interaction.turn_id,
        interaction.lineage_id,
        interaction.ordinal,
        JSON.stringify(interaction),
        requestedAt,
        requestedAt,
      );
      if (turn.state === 'running') {
        transitionInTransaction(database, {
          turnId: turn.turn_id,
          fromState: 'running',
          toState: 'waiting_user',
          fence: turnContext.attempt,
          provider,
          serviceInstanceId,
          occurredAt: requestedAt,
          generateId,
          reasonCode: 'interaction_requested',
        });
        turn = loadTurn(database, turn.turn_id);
      }
      const event = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence: turnContext.attempt,
        provider,
        descriptor: {
          kind: 'interaction_requested',
          phase: 'waiting_user',
          payload: {
            interaction_id: interaction.interaction_id,
            ordinal: interaction.ordinal,
            interaction_version: interaction.version,
            handoff_version: null,
            kind: interaction.kind,
            prompt: interaction.prompt,
            choices: structuredClone(interaction.choices),
            allowed_sources: [...interaction.allowed_sources],
          },
        },
        occurredAt: requestedAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn,
        event,
        fence: turnContext.attempt,
        nextState: 'waiting_user',
        staleMessage: 'The interaction request lost its provider attempt fence.',
        generateId,
      });
      return interaction;
    });
    return request.immediate();
  }

  function commitInteractionAnswer(answer) {
    const commit = database.transaction(() => {
      const committedAt = now();
      validateInteractionAnswer(answer, { occurredAt: committedAt });
      const row = database.prepare(`
        SELECT request_json
        FROM runtime_interactions
        WHERE interaction_id = ?
      `).get(answer.interaction_id);
      if (!row) conflict('interaction_not_found', `Interaction ${answer.interaction_id} does not exist.`);
      const request = JSON.parse(row.request_json);
      if (request.state !== 'pending') {
        conflict('interaction_already_answered', 'Only a pending interaction can accept an answer.');
      }
      if (answer.interaction_version !== request.version) {
        conflict('version_conflict', 'The answer does not match the current interaction version.');
      }
      const turn = loadTurn(database, request.turn_id);
      if (turn.state !== 'waiting_user') {
        conflict('illegal_transition', `Interaction answers are invalid while turn is ${turn.state}.`);
      }
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      if (
        request.runtime_fence.provider_attempt_id !== fence.attempt_id
        || request.runtime_fence.lease_epoch !== fence.lease_epoch
      ) {
        conflict('stale_attempt', 'The interaction no longer matches the current runtime fence.');
      }
      assertActiveFence(database, turn, fence, serviceInstanceId);

      validateInteractionTransition({
        from: 'pending',
        to: 'answer_committed',
        occurredAt: committedAt,
      });
      const handoffId = generateId('handoff');
      const updatedRequest = {
        ...request,
        state: 'answer_committed',
        version: request.version + 1,
        handoff_state: 'pending',
      };
      validateInteractionRequest(updatedRequest, { occurredAt: committedAt });
      const handoff = {
        handoff_id: handoffId,
        interaction_id: request.interaction_id,
        answer_id: answer.answer_id,
        parent_type: 'provider_turn',
        state: 'pending',
        provider_attempt_id: fence.attempt_id,
        handoff_attempt_id: null,
        handoff_attempt_no: null,
        lease_epoch: fence.lease_epoch,
        claimed_by: null,
        claimed_at: null,
        last_send_started_at: null,
        provider_acked_at: null,
        handoff_deadline_at: request.expires_at,
        reason_code: null,
        error: null,
        side_effect_status: 'none',
      };
      validateInteractionHandoff(handoff, { occurredAt: committedAt });

      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'answer_committed', version = ?, handoff_state = 'pending',
          handoff_version = 1, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'pending' AND version = ?
      `).run(
        updatedRequest.version,
        JSON.stringify(updatedRequest),
        committedAt,
        request.interaction_id,
        request.version,
      );
      if (interactionUpdate.changes !== 1) {
        conflict('version_conflict', 'The interaction answer lost its state/version compare-and-swap.');
      }
      database.prepare(`
        INSERT INTO runtime_interaction_handoffs (
          handoff_id, interaction_id, answer_id, state, provider_attempt_id,
          handoff_attempt_id, handoff_attempt_no, lease_epoch, record_json,
          created_at, updated_at
        ) VALUES (?, ?, ?, 'pending', ?, NULL, NULL, ?, ?, ?, ?)
      `).run(
        handoff.handoff_id,
        handoff.interaction_id,
        handoff.answer_id,
        handoff.provider_attempt_id,
        handoff.lease_epoch,
        JSON.stringify(handoff),
        committedAt,
        committedAt,
      );

      const event = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence,
        provider,
        descriptor: {
          kind: 'interaction_answer_committed',
          phase: 'waiting_user',
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: 1,
          },
        },
        occurredAt: committedAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn,
        event,
        fence,
        nextState: 'waiting_user',
        staleMessage: 'The interaction answer commit lost its provider attempt fence.',
        generateId,
      });
      const result = {
        contract: 'zylos.interaction-answer-result',
        contract_version: '1.0',
        trace_id: answer.trace_id,
        interaction_id: request.interaction_id,
        answer_id: answer.answer_id,
        idempotency_key: answer.idempotency_key,
        status: 'accepted',
        interaction_state: 'answer_committed',
        interaction_version: updatedRequest.version,
        handoff_state: 'pending',
        handoff_id: handoff.handoff_id,
        turn_id: turn.turn_id,
        turn_version: event.turn_version,
        control_id: null,
        error: null,
        received_at: committedAt,
        committed_at: committedAt,
      };
      validateInteractionAnswerResult(result, { occurredAt: committedAt });
      database.prepare(`
        INSERT INTO runtime_interaction_answers (
          answer_id, interaction_id, idempotency_key, answer_json, result_json,
          committed_at
        ) VALUES (?, ?, ?, ?, ?, ?)
      `).run(
        answer.answer_id,
        answer.interaction_id,
        answer.idempotency_key,
        JSON.stringify(answer),
        JSON.stringify(result),
        committedAt,
      );
      return result;
    });
    return commit.immediate();
  }

  function claimInteractionHandoff(handoffId) {
    const claim = database.transaction(() => {
      const claimedAt = now();
      const row = database.prepare(`
        SELECT interaction.request_json, answer.answer_json, handoff.record_json
        FROM runtime_interaction_handoffs AS handoff
        JOIN runtime_interactions AS interaction
          ON interaction.interaction_id = handoff.interaction_id
        JOIN runtime_interaction_answers AS answer
          ON answer.answer_id = handoff.answer_id
        WHERE handoff.handoff_id = ?
      `).get(handoffId);
      if (!row) conflict('handoff_not_found', `Interaction handoff ${handoffId} does not exist.`);
      const request = JSON.parse(row.request_json);
      const answer = JSON.parse(row.answer_json);
      const handoff = JSON.parse(row.record_json);
      if (request.state !== 'answer_committed' || handoff.state !== 'pending') {
        conflict('illegal_transition', 'Only a committed answer with a pending handoff can be claimed.');
      }
      const turn = loadTurn(database, request.turn_id);
      if (turn.state !== 'waiting_user') {
        conflict('illegal_transition', `Interaction handoff is invalid while turn is ${turn.state}.`);
      }
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      if (
        handoff.provider_attempt_id !== fence.attempt_id
        || handoff.lease_epoch !== fence.lease_epoch
      ) {
        conflict('stale_attempt', 'The pending handoff no longer matches the current runtime fence.');
      }
      assertActiveFence(database, turn, fence, serviceInstanceId);
      validateInteractionTransition({
        from: 'answer_committed',
        to: 'answer_delivering',
        occurredAt: claimedAt,
      });
      validateInteractionHandoffTransition({
        from: 'pending',
        to: 'delivering',
        occurredAt: claimedAt,
      });

      const updatedRequest = {
        ...request,
        state: 'answer_delivering',
        version: request.version + 1,
        handoff_state: 'delivering',
      };
      const updatedHandoff = {
        ...handoff,
        state: 'delivering',
        handoff_attempt_id: generateId('handoff-attempt'),
        handoff_attempt_no: 1,
        claimed_by: serviceInstanceId,
        claimed_at: claimedAt,
        last_send_started_at: claimedAt,
      };
      validateInteractionRequest(updatedRequest, { occurredAt: claimedAt });
      validateInteractionHandoff(updatedHandoff, { occurredAt: claimedAt });

      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'answer_delivering', version = ?, handoff_state = 'delivering',
          handoff_version = 2, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'answer_committed' AND version = ?
      `).run(
        updatedRequest.version,
        JSON.stringify(updatedRequest),
        claimedAt,
        request.interaction_id,
        request.version,
      );
      const handoffUpdate = database.prepare(`
        UPDATE runtime_interaction_handoffs
        SET state = 'delivering', handoff_attempt_id = ?, handoff_attempt_no = 1,
          record_json = ?, updated_at = ?
        WHERE handoff_id = ? AND state = 'pending' AND handoff_attempt_id IS NULL
      `).run(
        updatedHandoff.handoff_attempt_id,
        JSON.stringify(updatedHandoff),
        claimedAt,
        handoff.handoff_id,
      );
      if (interactionUpdate.changes !== 1 || handoffUpdate.changes !== 1) {
        conflict('version_conflict', 'The interaction handoff claim lost its state/version fence.');
      }

      const event = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence,
        provider,
        descriptor: {
          kind: 'interaction_answer_handoff_started',
          phase: 'waiting_user',
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: 2,
          },
        },
        occurredAt: claimedAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn,
        event,
        fence,
        nextState: 'waiting_user',
        staleMessage: 'The interaction handoff claim lost its provider attempt fence.',
        generateId,
      });
      return {
        request: updatedRequest,
        answer,
        handoff: updatedHandoff,
      };
    });
    return claim.immediate();
  }

  function acknowledgeInteractionHandoff(acknowledgement, { holdForPermission = false } = {}) {
    const acknowledge = database.transaction(() => {
      const acknowledgedAt = now();
      if (!['accepted', 'deny'].includes(acknowledgement?.status)) {
        conflict('invalid_acknowledgement', 'The happy-path handler acknowledgement must be accepted or deny.');
      }
      const row = database.prepare(`
        SELECT interaction.request_json, handoff.record_json
        FROM runtime_interaction_handoffs AS handoff
        JOIN runtime_interactions AS interaction
          ON interaction.interaction_id = handoff.interaction_id
        WHERE handoff.handoff_id = ?
      `).get(acknowledgement.handoff_id);
      if (!row) {
        conflict('handoff_not_found', `Interaction handoff ${acknowledgement.handoff_id} does not exist.`);
      }
      const request = JSON.parse(row.request_json);
      const handoff = JSON.parse(row.record_json);
      const turn = loadTurn(database, request.turn_id);
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      if (
        request.state !== 'answer_delivering'
        || handoff.state !== 'delivering'
        || acknowledgement.provider_attempt_id !== handoff.provider_attempt_id
        || acknowledgement.handoff_attempt_id !== handoff.handoff_attempt_id
        || acknowledgement.handoff_attempt_no !== handoff.handoff_attempt_no
        || acknowledgement.lease_epoch !== handoff.lease_epoch
      ) {
        conflict('stale_attempt', 'The handler acknowledgement does not match the delivering handoff fence.');
      }
      assertActiveFence(database, turn, fence, serviceInstanceId);
      validateInteractionTransition({
        from: 'answer_delivering',
        to: 'answered',
        sendStarted: true,
        occurredAt: acknowledgedAt,
      });
      validateInteractionHandoffTransition({
        from: 'delivering',
        to: 'accepted',
        sendStarted: true,
        occurredAt: acknowledgedAt,
      });

      const updatedRequest = {
        ...request,
        state: 'answered',
        version: request.version + 1,
        handoff_state: 'accepted',
      };
      const updatedHandoff = {
        ...handoff,
        state: 'accepted',
        provider_acked_at: acknowledgedAt,
        side_effect_status: 'known',
      };
      validateInteractionRequest(updatedRequest, { occurredAt: acknowledgedAt });
      validateInteractionHandoff(updatedHandoff, { occurredAt: acknowledgedAt });
      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'answered', version = ?, handoff_state = 'accepted',
          handoff_version = 3, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'answer_delivering' AND version = ?
      `).run(
        updatedRequest.version,
        JSON.stringify(updatedRequest),
        acknowledgedAt,
        request.interaction_id,
        request.version,
      );
      const handoffUpdate = database.prepare(`
        UPDATE runtime_interaction_handoffs
        SET state = 'accepted', record_json = ?, updated_at = ?
        WHERE handoff_id = ? AND state = 'delivering'
          AND handoff_attempt_id = ? AND handoff_attempt_no = ?
          AND provider_attempt_id = ? AND lease_epoch = ?
      `).run(
        JSON.stringify(updatedHandoff),
        acknowledgedAt,
        handoff.handoff_id,
        handoff.handoff_attempt_id,
        handoff.handoff_attempt_no,
        handoff.provider_attempt_id,
        handoff.lease_epoch,
      );
      if (interactionUpdate.changes !== 1 || handoffUpdate.changes !== 1) {
        conflict('stale_attempt', 'The handler acknowledgement lost its interaction/handoff fence.');
      }
      const auditId = generateId('audit');
      database.prepare(`
        INSERT INTO runtime_interaction_audit (
          audit_id, interaction_id, handoff_id, outcome, provider_attempt_id,
          lease_epoch, acknowledgement_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        auditId,
        request.interaction_id,
        handoff.handoff_id,
        acknowledgement.status,
        handoff.provider_attempt_id,
        handoff.lease_epoch,
        JSON.stringify(acknowledgement),
        acknowledgedAt,
      );

      const hasNextBlockingInteraction = database.prepare(`
        SELECT 1
        FROM runtime_interactions
        WHERE turn_id = ? AND interaction_id != ?
          AND state IN ('pending', 'answer_committed', 'answer_delivering', 'delivery_unknown')
        LIMIT 1
      `).get(turn.turn_id, request.interaction_id) !== undefined;
      const remainsBlocked = hasNextBlockingInteraction || holdForPermission;
      let currentTurn = turn;
      if (!remainsBlocked) {
        transitionInTransaction(database, {
          turnId: turn.turn_id,
          fromState: 'waiting_user',
          toState: 'running',
          fence,
          provider,
          serviceInstanceId,
          occurredAt: acknowledgedAt,
          generateId,
          reasonCode: 'interaction_answered',
        });
        currentTurn = loadTurn(database, turn.turn_id);
      }
      const event = buildEvent({
        turn: currentTurn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence,
        provider,
        descriptor: {
          kind: 'interaction_answered',
          phase: remainsBlocked ? 'waiting_user' : 'running',
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: 3,
            state: 'answered',
            handoff_state: 'accepted',
          },
        },
        occurredAt: acknowledgedAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn: currentTurn,
        event,
        fence,
        nextState: remainsBlocked ? 'waiting_user' : 'running',
        staleMessage: 'The handler acknowledgement lost its provider attempt fence.',
        generateId,
      });
      return {
        status: acknowledgement.status,
        interaction_id: request.interaction_id,
        handoff_id: handoff.handoff_id,
        audit_id: auditId,
        resumed: !remainsBlocked,
        turn_state: remainsBlocked ? 'waiting_user' : 'running',
        turn_version: event.turn_version,
      };
    });
    return acknowledge.immediate();
  }

  function resumeTurnAfterPermission(turnContext) {
    const resume = database.transaction(() => {
      const turn = loadTurn(database, turnContext.turn_id);
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      if (turn.state !== 'waiting_user') {
        return { resumed: turn.state === 'running', turn_state: turn.state };
      }
      const hasBlockingInteraction = database.prepare(`
        SELECT 1
        FROM runtime_interactions
        WHERE turn_id = ?
          AND state IN ('pending', 'answer_committed', 'answer_delivering', 'delivery_unknown')
        LIMIT 1
      `).get(turn.turn_id) !== undefined;
      if (hasBlockingInteraction) {
        return { resumed: false, turn_state: 'waiting_user' };
      }
      transitionInTransaction(database, {
        turnId: turn.turn_id,
        fromState: 'waiting_user',
        toState: 'running',
        fence: turnContext.attempt,
        provider,
        serviceInstanceId,
        occurredAt: now(),
        generateId,
        reasonCode: 'permission_resolved',
      });
      return { resumed: true, turn_state: 'running' };
    });
    return resume.immediate();
  }

  return Object.freeze({
    acknowledgeInteractionHandoff,
    appendAdapterEvent,
    assertCurrentFence,
    bindProviderNativeId,
    claimNextQueuedTurn,
    claimInteractionHandoff,
    commitInteractionAnswer,
    isConversationEvictable,
    releaseExecutorResident,
    reconcileExpiredResidents,
    rebuildExecutorCache,
    requestInteraction,
    resumeTurnAfterPermission,
    reserveNextExecutor,
    transitionTurn,
  });
}
