import crypto from 'node:crypto';

import { createContractError } from '../../contracts/public/index.js';
import { createExecutorStore } from '../persistence/executor-store.js';

function defaultGenerateId(kind) {
  return `${kind}-${crypto.randomUUID()}`;
}

function deepFreeze(value) {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
}

function normalizeProviderError(error, occurredAt) {
  const descriptor = error?.providerError;
  if (descriptor && typeof descriptor === 'object') {
    try {
      return createContractError({
        code: descriptor.code,
        category: descriptor.category,
        retryable: descriptor.retryable,
        sideEffectStatus: descriptor.side_effect_status,
        userMessage: descriptor.user_message,
        occurredAt,
      });
    } catch {
      // Invalid adapter error metadata is replaced with the safe generic provider failure below.
    }
  }
  return createContractError({
    code: 'side_effect_unknown',
    category: 'provider',
    retryable: false,
    sideEffectStatus: 'unknown',
    userMessage: 'The provider execution failed after side effects may have occurred.',
    occurredAt,
  });
}

function normalizePreSendProviderError(error, occurredAt) {
  const descriptor = error?.providerError;
  if (descriptor && typeof descriptor === 'object') {
    try {
      return createContractError({
        code: descriptor.code,
        category: descriptor.category,
        retryable: descriptor.retryable === true,
        sideEffectStatus: 'none',
        userMessage: descriptor.user_message,
        occurredAt,
      });
    } catch {
      // Invalid adapter metadata is replaced with the safe non-retryable failure below.
    }
  }
  return createContractError({
    code: 'provider_context_invalid',
    category: 'provider',
    retryable: false,
    sideEffectStatus: 'none',
    userMessage: 'The provider rejected the answer before delivery began.',
    occurredAt,
  });
}

function isExplicitProviderError(error) {
  return error !== null
    && typeof error === 'object'
    && error.providerError !== null
    && typeof error.providerError === 'object';
}

export function createExecutorService({
  database,
  adapter,
  provider,
  serviceInstanceId,
  now = () => new Date().toISOString(),
  generateId = defaultGenerateId,
  leaseDurationMs,
  residentLeaseDurationMs = 60_000,
  residentHeartbeatIntervalMs = 20_000,
  scheduleResidentHeartbeat = setInterval,
  cancelResidentHeartbeat = clearInterval,
  permissionHandler = null,
  interactionHandoffDispositionAuthorizer = null,
  interactionTimeoutMs,
  maxResidentExecutorsPerBot = 20,
  setTimeoutFn = setTimeout,
  clearTimeoutFn = clearTimeout,
  onDeadlineError = () => {},
}) {
  if (!database || typeof database.transaction !== 'function') {
    throw new TypeError('database must be a better-sqlite3 connection');
  }
  if (!adapter || typeof adapter.execute !== 'function') {
    throw new TypeError('adapter.execute must be a function');
  }
  if (!['claude', 'codex'].includes(provider)) {
    throw new TypeError('provider must be claude or codex');
  }
  if (typeof serviceInstanceId !== 'string' || serviceInstanceId.length === 0) {
    throw new TypeError('serviceInstanceId must be a non-empty string');
  }
  if (typeof generateId !== 'function') {
    throw new TypeError('generateId must be a function');
  }
  if (permissionHandler !== null && typeof permissionHandler !== 'function') {
    throw new TypeError('permissionHandler must be a function or null');
  }
  if (
    interactionHandoffDispositionAuthorizer !== null
    && typeof interactionHandoffDispositionAuthorizer !== 'function'
  ) {
    throw new TypeError('interactionHandoffDispositionAuthorizer must be a function or null');
  }
  if (!Number.isFinite(residentLeaseDurationMs) || residentLeaseDurationMs <= 0) {
    throw new TypeError('residentLeaseDurationMs must be a positive finite number');
  }
  if (
    !Number.isFinite(residentHeartbeatIntervalMs)
    || residentHeartbeatIntervalMs <= 0
    || residentHeartbeatIntervalMs >= residentLeaseDurationMs
  ) {
    throw new TypeError('residentHeartbeatIntervalMs must be positive and below the resident lease');
  }
  if (typeof scheduleResidentHeartbeat !== 'function') {
    throw new TypeError('scheduleResidentHeartbeat must be a function');
  }
  if (typeof cancelResidentHeartbeat !== 'function') {
    throw new TypeError('cancelResidentHeartbeat must be a function');
  }
  if (
    !Number.isSafeInteger(maxResidentExecutorsPerBot)
    || maxResidentExecutorsPerBot <= 0
  ) {
    throw new TypeError('maxResidentExecutorsPerBot must be a positive safe integer');
  }

  const store = createExecutorStore({
    database,
    provider,
    serviceInstanceId,
    now,
    generateId,
    leaseDurationMs,
    residentLeaseDurationMs,
    interactionTimeoutMs,
  });
  let executors = [];
  let started = false;
  const activeRuns = new Map();
  const activeTurns = new Map();
  const cancelledTurnIds = new Set();
  const closingPermissionTurnIds = new Set();
  const permissionControllers = new Map();
  const activeRunSettlements = new Set();
  const interactionDeliverySettlements = new Set();
  const cancellationSettlements = new Map();
  const endedResidentFences = new Map();
  const pendingInteractionRecoveries = new Map();
  const pendingRecoveryIsolations = new Map();
  const recoveringOwnershipReleases = new Map();
  const timedOutLeaseReleases = new Map();
  const uncertainTurnIds = new Set();
  let lifecycle = 'open';
  let closePromise = null;
  let residentHeartbeat = null;
  let residentHeartbeatFailure = null;

  function persistenceFailure(cause) {
    const error = new Error(`Runtime persistence failed: ${cause.message}`, { cause });
    error.persistenceFailure = true;
    if (typeof cause?.code === 'string') error.code = cause.code;
    return error;
  }

  function persist(operation) {
    try {
      return operation();
    } catch (cause) {
      throw persistenceFailure(cause);
    }
  }

  const deadlineTimers = new Map();

  function clearInteractionDeadline(interactionId) {
    const timer = deadlineTimers.get(interactionId);
    if (timer === undefined) return;
    clearTimeoutFn(timer);
    deadlineTimers.delete(interactionId);
  }

  function scheduleInteractionDeadline(request) {
    if (lifecycle !== 'open') return;
    clearInteractionDeadline(request.interaction_id);
    const interactionVersion = request.interaction_version ?? request.version;
    const delay = Math.max(0, Date.parse(request.expires_at) - Date.parse(now()));
    const timer = setTimeoutFn(async () => {
      deadlineTimers.delete(request.interaction_id);
      try {
        return await expireInteraction({
          interaction_id: request.interaction_id,
          interaction_version: interactionVersion,
        });
      } catch (error) {
        onDeadlineError(error, request);
        return null;
      }
    }, delay);
    timer?.unref?.();
    deadlineTimers.set(request.interaction_id, timer);
  }

  function reschedulePendingInteractionDeadlines() {
    if (lifecycle !== 'open') return;
    const deadlines = store.listPendingInteractionDeadlines();
    const scheduledInteractionIds = new Set(
      deadlines.map(({ interaction_id: interactionId }) => interactionId),
    );
    for (const interactionId of deadlineTimers.keys()) {
      if (!scheduledInteractionIds.has(interactionId)) {
        clearInteractionDeadline(interactionId);
      }
    }
    for (const deadline of deadlines) scheduleInteractionDeadline(deadline);
  }

  function clearAllInteractionDeadlines() {
    for (const interactionId of [...deadlineTimers.keys()]) {
      clearInteractionDeadline(interactionId);
    }
  }

  function refresh() {
    executors = store.rebuildExecutorCache();
  }

  function snapshot() {
    return {
      service_instance_id: serviceInstanceId,
      executors: executors.map((executor) => ({
        ...executor,
        queued_turn_ids: [...executor.queued_turn_ids],
      })),
    };
  }

  function start() {
    if (lifecycle !== 'open') {
      throw new Error(`Executor service is ${lifecycle}; it cannot be started.`);
    }
    if (started) return snapshot();
    store.reconcileExpiredResidents();
    if (residentHeartbeat === null) {
      residentHeartbeat = scheduleResidentHeartbeat(() => {
        try {
          store.heartbeatOwnedResidents();
          residentHeartbeatFailure = null;
        } catch (error) {
          residentHeartbeatFailure = error;
          return;
        }
        for (const [conversationId, ownerEpoch] of endedResidentFences) {
          try {
            if (store.releaseExecutorResident(conversationId, ownerEpoch)) {
              endedResidentFences.delete(conversationId);
            }
          } catch (error) {
            residentHeartbeatFailure = error;
          }
        }
        for (const [turnId, expiration] of timedOutLeaseReleases) {
          try {
            store.releaseTimedOutExecutorLease(expiration);
            timedOutLeaseReleases.delete(turnId);
          } catch (error) {
            residentHeartbeatFailure = error;
          }
        }
        for (const turnContext of recoveringOwnershipReleases.values()) {
          try {
            releaseRecoveringOwnership(turnContext);
          } catch (error) {
            residentHeartbeatFailure = error;
          }
        }
      }, residentHeartbeatIntervalMs);
      residentHeartbeat?.unref?.();
    }
    refresh();
    reschedulePendingInteractionDeadlines();
    started = true;
    return snapshot();
  }

  function resultFor(activeRun, status, extra = {}) {
    return {
      status,
      conversation_id: activeRun.turnContext.conversation_id,
      turn_id: activeRun.turnContext.turn_id,
      ...activeRun.turnContext.attempt,
      ...extra,
    };
  }

  function cleanupActiveRun(activeRun) {
    const { turnContext } = activeRun;
    if (activeRuns.get(turnContext.turn_id) === activeRun) {
      activeRuns.delete(turnContext.turn_id);
    }
    if (activeTurns.get(turnContext.conversation_id) === turnContext) {
      activeTurns.delete(turnContext.conversation_id);
    }
    cancelledTurnIds.delete(turnContext.turn_id);
    uncertainTurnIds.delete(turnContext.turn_id);
    cancellationSettlements.delete(turnContext.turn_id);
    const controllers = permissionControllers.get(turnContext.turn_id);
    if (!controllers || controllers.size === 0) {
      permissionControllers.delete(turnContext.turn_id);
      closingPermissionTurnIds.delete(turnContext.turn_id);
    }
    if (!activeRun.settled) {
      activeRun.settled = true;
      activeRunSettlements.delete(activeRun.settlement);
      activeRun.resolveSettlement();
    }
  }

  function releaseAbsentResident(turnContext) {
    if (
      typeof adapter.hasResident === 'function'
      && !adapter.hasResident(turnContext.conversation_id)
    ) {
      const ownerEpoch = turnContext.resident?.owner_epoch;
      endedResidentFences.set(turnContext.conversation_id, ownerEpoch);
      const released = store.releaseExecutorResident(turnContext.conversation_id, ownerEpoch);
      if (released) endedResidentFences.delete(turnContext.conversation_id);
    }
  }

  function releaseRecoveringOwnership(turnContext) {
    recoveringOwnershipReleases.set(turnContext.turn_id, turnContext);
    const result = store.releaseRecoveringExecutorOwnership(turnContext);
    recoveringOwnershipReleases.delete(turnContext.turn_id);
    pendingRecoveryIsolations.delete(turnContext.turn_id);
    endedResidentFences.delete(turnContext.conversation_id);
    return result;
  }

  function recordRecoveringIsolation(turnContext) {
    pendingRecoveryIsolations.delete(turnContext.turn_id);
    recoveringOwnershipReleases.set(turnContext.turn_id, turnContext);
  }

  async function isolateInteractionRecovery(activeRun) {
    if (typeof adapter.abort === 'function') {
      await adapter.abort(activeRun.turnContext);
      if (provider === 'codex' && typeof activeRun.iterator?.return === 'function') {
        try {
          await activeRun.iterator.return();
        } catch {
          // Provider terminal is the isolation proof; iterator cleanup is best-effort.
        }
      }
      return true;
    }
    if (typeof activeRun.iterator?.return === 'function') {
      await activeRun.iterator.return();
      return true;
    }
    return false;
  }

  async function awaitCancellationSettlement(turnId) {
    const settlement = cancellationSettlements.get(turnId);
    if (settlement) await settlement.promise;
  }

  function transitionToRecovery(activeRun, reasonCode = 'provider_execution_uncertain') {
    const { state } = store.assertCurrentFence(activeRun.turnContext);
    persist(() => store.transitionTurn(activeRun.turnContext, state, 'recovering', {
      error: null,
      reasonCode,
    }));
    activeRun.durableSettled = true;
    refresh();
    cleanupActiveRun(activeRun);
    pendingRecoveryIsolations.set(activeRun.turnContext.turn_id, activeRun.turnContext);
    return resultFor(activeRun, 'recovering');
  }

  async function retainRecoveringRun(activeRun) {
    try {
      await activeRun.iterator?.return?.();
    } catch {
      // The durable recovery fence is authoritative; iterator cleanup is best-effort.
    }
    activeRun.durableSettled = true;
    cleanupActiveRun(activeRun);
    refresh();
    reschedulePendingInteractionDeadlines();
    return resultFor(activeRun, 'recovering');
  }

  async function handleRunFailure(activeRun, error) {
    const { turnContext } = activeRun;
    if (activeRun.providerFailureOutcome?.status === 'recovering') {
      return retainRecoveringRun(activeRun);
    }
    if (activeRun.providerFailurePersistenceError) {
      activeRun.durableSettled = true;
      cleanupActiveRun(activeRun);
      refresh();
      throw activeRun.providerFailurePersistenceError;
    }
    if (activeRun.durableSettled) {
      cleanupActiveRun(activeRun);
      throw error;
    }
    if (error.persistenceFailure) {
      if (typeof adapter.abort === 'function') {
        await adapter.abort(turnContext);
      }
      cleanupActiveRun(activeRun);
      throw error;
    }
    await awaitCancellationSettlement(turnContext.turn_id);
    closingPermissionTurnIds.add(turnContext.turn_id);
    for (const controller of permissionControllers.get(turnContext.turn_id) ?? []) {
      controller.abort();
    }
    if (uncertainTurnIds.has(turnContext.turn_id)) {
      const isolationProven = cancellationSettlements.get(turnContext.turn_id)?.isolationProven;
      const recovery = transitionToRecovery(activeRun);
      if (isolationProven) {
        try {
          releaseRecoveringOwnership(turnContext);
        } catch (ownershipFailure) {
          residentHeartbeatFailure = ownershipFailure;
        }
      }
      return recovery;
    }
    const cancelled = cancelledTurnIds.has(turnContext.turn_id);
    if (isExplicitProviderError(error) && !cancelled) {
      const { state } = store.assertCurrentFence(turnContext);
      persist(() => store.transitionTurn(turnContext, state, 'failed', {
        error: normalizeProviderError(error, now()),
        reasonCode: 'executor_failed',
      }));
      activeRun.durableSettled = true;
      refresh();
      cleanupActiveRun(activeRun);
      releaseAbsentResident(turnContext);
      reschedulePendingInteractionDeadlines();
      return resultFor(activeRun, 'failed');
    }
    const terminalState = cancelled ? 'stopped' : 'failed';
    const { state } = store.assertCurrentFence(turnContext);
    store.transitionTurn(turnContext, state, terminalState, {
      error: cancelled ? null : {
        code: 'provider_stream_failed',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The provider stream ended before the turn completed.',
      },
    });
    activeRun.durableSettled = true;
    refresh();
    cleanupActiveRun(activeRun);
    releaseAbsentResident(turnContext);
    reschedulePendingInteractionDeadlines();
    return resultFor(activeRun, terminalState);
  }

  async function finishRun(activeRun) {
    activeRun.ensureProviderStarted();
    await awaitCancellationSettlement(activeRun.turnContext.turn_id);
    if (uncertainTurnIds.has(activeRun.turnContext.turn_id)) {
      const { turnContext } = activeRun;
      const isolationProven = cancellationSettlements.get(turnContext.turn_id)?.isolationProven;
      const recovery = transitionToRecovery(activeRun);
      if (isolationProven) {
        try {
          releaseRecoveringOwnership(turnContext);
        } catch (ownershipFailure) {
          residentHeartbeatFailure = ownershipFailure;
        }
      }
      return recovery;
    }
    const terminalState = activeRun.outcome === 'cancelled'
      ? 'stopped'
      : (activeRun.outcome === 'failed' ? 'failed' : 'completed');
    const failure = activeRun.outcome === 'failed'
      || (activeRun.usesManagedRecords && activeRun.outcome === null);
    const { state } = store.assertCurrentFence(activeRun.turnContext);
    persist(() => store.transitionTurn(
      activeRun.turnContext,
      state,
      failure ? 'failed' : terminalState,
      {
        error: failure ? {
          code: activeRun.outcome === null
            ? 'provider_stream_ended'
            : 'provider_execution_failed',
          category: 'provider',
          retryable: false,
          side_effect_status: 'unknown',
          user_message: 'The provider turn did not complete successfully.',
        } : null,
      },
    ));
    activeRun.durableSettled = true;
    refresh();
    cleanupActiveRun(activeRun);
    releaseAbsentResident(activeRun.turnContext);
    reschedulePendingInteractionDeadlines();
    return resultFor(activeRun, failure ? 'failed' : terminalState);
  }

  async function driveRun(activeRun) {
    try {
      while (true) {
        const next = await activeRun.iterator.next();
        if (activeRun.providerFailureOutcome?.status === 'recovering') {
          return retainRecoveringRun(activeRun);
        }
        if (next.done) return await finishRun(activeRun);
        const record = next.value;
        if (record?.type === 'interaction_persisted') {
          reschedulePendingInteractionDeadlines();
          if (cancelledTurnIds.has(activeRun.turnContext.turn_id)) continue;
          activeRun.pauseKind = 'interaction';
          refresh();
          return resultFor(activeRun, 'waiting_user', { request: record.request });
        }
        if (record?.kind === 'interaction_requested') {
          activeRun.ensureProviderStarted();
          const request = persist(() => store.requestInteraction(
            activeRun.turnContext,
            record.payload,
          ));
          reschedulePendingInteractionDeadlines();
          if (cancelledTurnIds.has(activeRun.turnContext.turn_id)) continue;
          activeRun.pauseKind = 'interaction';
          refresh();
          return resultFor(activeRun, 'waiting_user', { request });
        }
        if (record?.type === 'provider_native_id') {
          activeRun.usesManagedRecords = true;
          try {
            persist(() => store.bindProviderNativeId(
              activeRun.turnContext,
              record.provider_native_id,
            ));
            activeRun.currentProviderNativeId = record.provider_native_id;
            activeRun.ensureProviderStarted();
            record.acknowledge();
          } catch (error) {
            record.acknowledge(error);
            throw error;
          }
          continue;
        }
        if (record?.type === 'normalized_event') {
          activeRun.usesManagedRecords = true;
          activeRun.ensureProviderStarted();
          persist(() => store.appendAdapterEvent(activeRun.turnContext, record.event));
          continue;
        }
        if (record?.type === 'turn_result') {
          activeRun.usesManagedRecords = true;
          activeRun.ensureProviderStarted();
          activeRun.outcome = record.outcome;
          continue;
        }
        activeRun.ensureProviderStarted();
        persist(() => store.appendAdapterEvent(activeRun.turnContext, record));
      }
    } catch (error) {
      return handleRunFailure(activeRun, error);
    }
  }

  function advanceRun(activeRun) {
    if (activeRun.advancing) return activeRun.advancing;
    const advancing = driveRun(activeRun);
    activeRun.advancing = advancing;
    advancing.finally(() => {
      if (activeRun.advancing === advancing) activeRun.advancing = null;
    }).catch(() => {});
    return advancing;
  }

  function createPermissionControls(activeRun, adapterContext) {
    const { turnContext } = activeRun;
    return Object.freeze({
      interactionPolicy: Object.freeze({
        allowed_sources: Object.freeze(['main_card_reply', 'card_action']),
        authorized_subjects: Object.freeze(
          turnContext.interaction_authority.map((subject) => Object.freeze({ ...subject })),
        ),
      }),
      persistInteraction(descriptor) {
        const request = persist(() => store.requestInteraction(turnContext, descriptor));
        reschedulePendingInteractionDeadlines();
        return Object.freeze(request);
      },
      residentEnded(context) {
        const ownerEpoch = turnContext.resident?.owner_epoch;
        endedResidentFences.set(context.conversation_id, ownerEpoch);
        const released = store.releaseExecutorResident(
          context.conversation_id,
          ownerEpoch,
        );
        if (released) endedResidentFences.delete(context.conversation_id);
      },
      async requestPermission(request, { signal } = {}) {
        if (permissionHandler === null) {
          return Object.freeze({
            behavior: 'deny',
            message: 'No permission handler is configured for this executor service.',
            interrupt: false,
          });
        }
        let controllers = permissionControllers.get(turnContext.turn_id);
        if (!controllers) {
          controllers = new Set();
          permissionControllers.set(turnContext.turn_id, controllers);
          activeRun.ensureProviderStarted();
          persist(() => store.transitionTurn(turnContext, 'running', 'waiting_user'));
        }
        const controller = new AbortController();
        controllers.add(controller);
        const abort = () => controller.abort();
        signal?.addEventListener?.('abort', abort, { once: true });
        let decision;
        try {
          decision = await Promise.race([
            permissionHandler(
              Object.freeze({ ...request }),
              adapterContext,
              Object.freeze({ signal: controller.signal }),
            ),
            new Promise((resolve, reject) => {
              controller.signal.addEventListener('abort', () => {
                const error = new Error('Permission request was cancelled.');
                error.name = 'AbortError';
                reject(error);
              }, { once: true });
            }),
          ]);
        } finally {
          signal?.removeEventListener?.('abort', abort);
          controllers.delete(controller);
          if (controllers.size === 0) {
            permissionControllers.delete(turnContext.turn_id);
            if (
              !cancelledTurnIds.has(turnContext.turn_id)
              && !closingPermissionTurnIds.has(turnContext.turn_id)
            ) {
              const resumed = persist(() => store.resumeTurnAfterPermission(turnContext));
              if (resumed.resumed && activeRun.pauseKind === 'interaction') {
                activeRun.pauseKind = null;
                queueMicrotask(() => advanceRun(activeRun).catch(() => {}));
              }
            }
            closingPermissionTurnIds.delete(turnContext.turn_id);
          }
        }
        if (!decision || !['allow', 'deny'].includes(decision.behavior)) {
          throw new TypeError('permissionHandler must return an allow or deny decision');
        }
        return Object.freeze({ ...decision });
      },
    });
  }

  async function runNext() {
    if (lifecycle !== 'open') {
      throw new Error(`Executor service is ${lifecycle}; it cannot claim another turn.`);
    }
    if (!started) start();
    if (residentHeartbeatFailure) throw persistenceFailure(residentHeartbeatFailure);
    let reservation = store.reserveNextExecutor({
      maxResidentExecutorsPerBot,
      markCapacityWait: false,
    });
    if (reservation.status === 'idle') return reservation;
    if (reservation.status === 'capacity_wait') {
      await evictIdleExecutors();
      if (lifecycle !== 'open') {
        throw new Error(`Executor service is ${lifecycle}; it cannot claim another turn.`);
      }
      reservation = store.reserveNextExecutor({ maxResidentExecutorsPerBot });
    }
    if (reservation.status === 'capacity_wait') {
      refresh();
      return reservation;
    }
    const turnContext = store.claimNextQueuedTurn({
      conversationId: reservation.conversation_id,
      requireResident: provider === 'claude',
    });
    if (!turnContext) {
      refresh();
      return { status: 'idle' };
    }
    endedResidentFences.delete(turnContext.conversation_id);
    let resolveSettlement;
    const settlement = new Promise((resolve) => {
      resolveSettlement = resolve;
    });
    const activeRun = {
      advancing: null,
      currentProviderNativeId: turnContext.provider_native_id,
      durableSettled: false,
      ensureProviderStarted: null,
      iterator: null,
      outcome: null,
      pauseKind: null,
      providerFailureOutcome: null,
      providerFailurePersistenceError: null,
      providerStarted: false,
      resolveSettlement,
      settled: false,
      settlement,
      timedOutExpiration: null,
      turnContext,
      usesManagedRecords: false,
    };
    activeRunSettlements.add(settlement);
    activeRuns.set(turnContext.turn_id, activeRun);
    activeTurns.set(turnContext.conversation_id, turnContext);
    activeRun.ensureProviderStarted = () => {
      if (activeRun.providerStarted) return { status: 'already_started' };
      persist(() => store.transitionTurn(turnContext, 'starting', 'running', {
        reasonCode: 'provider_started',
      }));
      activeRun.providerStarted = true;
      refresh();
      return { status: 'running' };
    };
    try {
      refresh();
      const adapterContext = Object.freeze({
        conversation_id: turnContext.conversation_id,
        turn_id: turnContext.turn_id,
        lineage_id: turnContext.lineage_id,
        provider_native_id: turnContext.provider_native_id,
        trace_id: turnContext.trace_id,
        input: turnContext.input,
        interaction: Object.freeze({
          authorized_subjects: Object.freeze(
            turnContext.interaction.authorized_subjects.map((subject) => Object.freeze({
              ...subject,
            })),
          ),
          allowed_sources: Object.freeze([...turnContext.interaction.allowed_sources]),
        }),
        lineage: Object.freeze({ ...turnContext.lineage }),
        bindProviderNativeId(providerNativeId) {
          const binding = persist(() => store.bindProviderNativeId(
            turnContext,
            providerNativeId,
          ));
          activeRun.currentProviderNativeId = providerNativeId;
          return binding;
        },
        reportProviderState(providerState) {
          if (
            !providerState
            || providerState.state !== 'started'
            || !Object.hasOwn(providerState, 'provider_native_id')
            || providerState.provider_native_id !== activeRun.currentProviderNativeId
          ) {
            throw new TypeError('provider state must match the current started lineage');
          }
          return activeRun.ensureProviderStarted();
        },
        reportProviderFailure(providerFailure) {
          if (activeRun.durableSettled) {
            throw new Error('Provider failure arrived after the executor run settled.');
          }
          const normalizedFailure = normalizeProviderError(providerFailure, now());
          let outcome;
          try {
            outcome = persist(() => store.markProviderFailure(
              turnContext,
              normalizedFailure,
            ));
          } catch (error) {
            activeRun.providerFailurePersistenceError = error;
            throw error;
          }
          activeRun.providerFailureOutcome = outcome;
          if (outcome.status === 'recovering') {
            reschedulePendingInteractionDeadlines();
            refresh();
            if (!activeRun.advancing) {
              activeRun.durableSettled = true;
              cleanupActiveRun(activeRun);
            }
          }
          return outcome;
        },
        attempt: Object.freeze({ ...turnContext.attempt }),
      });
      const events = adapter.execute(
        adapterContext,
        createPermissionControls(activeRun, adapterContext),
      );
      if (!events || typeof events[Symbol.asyncIterator] !== 'function') {
        throw new TypeError('adapter.execute must return an async iterable');
      }
      activeRun.iterator = events[Symbol.asyncIterator]();
    } catch (error) {
      return handleRunFailure(activeRun, error);
    }
    return advanceRun(activeRun);
  }

  async function cancel(conversationId) {
    if (typeof conversationId !== 'string' || conversationId.length === 0) {
      throw new TypeError('conversationId must be a non-empty string');
    }
    const turnContext = activeTurns.get(conversationId);
    if (!turnContext) return { status: 'idle', conversation_id: conversationId };
    store.assertCurrentFence(turnContext);
    if (typeof adapter.cancel !== 'function') {
      throw new TypeError('adapter.cancel must be a function to cancel an active turn');
    }
    if (cancellationSettlements.has(turnContext.turn_id)) {
      throw new Error('Cancellation is already pending for the active turn.');
    }
    let resolveCancellation;
    const cancellation = {
      isolationProven: false,
      promise: new Promise((resolve) => { resolveCancellation = resolve; }),
      resolve: () => resolveCancellation(),
      status: 'pending',
    };
    cancellationSettlements.set(turnContext.turn_id, cancellation);
    cancelledTurnIds.add(turnContext.turn_id);
    for (const controller of permissionControllers.get(turnContext.turn_id) ?? []) {
      controller.abort();
    }
    try {
      await adapter.cancel(turnContext);
    } catch (error) {
      cancelledTurnIds.delete(turnContext.turn_id);
      if (error.cancellationUncertain) {
        uncertainTurnIds.add(turnContext.turn_id);
        cancellation.status = 'uncertain';
        if (typeof adapter.abort === 'function') {
          try {
            await adapter.abort(turnContext);
            cancellation.isolationProven = true;
          } catch (abortFailure) {
            error.abortFailure = abortFailure;
          }
        }
        cancellation.resolve();
      } else {
        cancellation.status = 'failed';
        cancellation.resolve();
      }
      throw error;
    }
    cancellation.status = 'confirmed';
    cancellation.resolve();
    const activeRun = activeRuns.get(turnContext.turn_id);
    let execution = null;
    if (activeRun?.pauseKind === 'interaction') {
      activeRun.pauseKind = null;
      execution = await advanceRun(activeRun);
    }
    return {
      status: 'cancellation_requested',
      conversation_id: turnContext.conversation_id,
      turn_id: turnContext.turn_id,
      ...turnContext.attempt,
      ...(execution ? { execution } : {}),
    };
  }

  async function evictIdleExecutors() {
    if (typeof adapter.evictIdle !== 'function') return [];
    const evicted = await adapter.evictIdle({
      canEvict: (conversationId) => store.isConversationEvictable(conversationId),
      maxCount: 1,
    });
    for (const conversationId of evicted) {
      store.releaseExecutorResident(conversationId);
      endedResidentFences.delete(conversationId);
    }
    return evicted;
  }

  function submitInteractionAnswer(answer, sourceEvidence) {
    const result = store.commitInteractionAnswer(answer, sourceEvidence);
    if (
      ['accepted', 'duplicate'].includes(result.status)
      || (result.interaction_state !== null && result.interaction_state !== 'pending')
    ) {
      clearInteractionDeadline(result.interaction_id);
    }
    reschedulePendingInteractionDeadlines();
    return result;
  }

  async function expireInteraction(expiration) {
    const result = store.expireInteraction(expiration);
    if (result.status !== 'expired' || result.turn_state !== 'timed_out') {
      if (result.status === 'not_pending') clearInteractionDeadline(result.interaction_id);
      reschedulePendingInteractionDeadlines();
      return result;
    }
    clearInteractionDeadline(result.interaction_id);

    const timedOutRun = activeRuns.get(result.turn_id);
    if (timedOutRun) timedOutRun.timedOutExpiration = result;
    if (!timedOutRun || typeof timedOutRun.iterator.return !== 'function') {
      store.markTimedOutProviderStopUnknown(result, 'not_current');
      reschedulePendingInteractionDeadlines();
      return {
        ...result,
        lease_released: false,
        provider_stop_status: 'not_current',
      };
    }
    if (typeof adapter.interrupt === 'function') {
      let interruption;
      try {
        interruption = await adapter.interrupt({
          turn_id: result.turn_id,
          attempt: result.attempt,
          reason: 'timeout',
        });
      } catch {
        store.markTimedOutProviderStopUnknown(result, 'uncertain');
        reschedulePendingInteractionDeadlines();
        return { ...result, lease_released: false, provider_stop_status: 'uncertain' };
      }
      if (interruption?.status !== 'provider_stopped') {
        store.markTimedOutProviderStopUnknown(result, interruption?.status ?? 'uncertain');
        reschedulePendingInteractionDeadlines();
        return {
          ...result,
          lease_released: false,
          provider_stop_status: interruption?.status ?? 'uncertain',
        };
      }
    }
    if (typeof adapter.abort === 'function') {
      try {
        await adapter.abort(timedOutRun.turnContext);
      } catch (error) {
        lifecycle = 'close_failed';
        throw error;
      }
    }
    const cleanupFailures = [];
    try {
      await timedOutRun.iterator.return();
    } catch (error) {
      cleanupFailures.push(error);
    }
    timedOutRun.durableSettled = true;
    timedOutRun.timedOutExpiration = null;
    cleanupActiveRun(timedOutRun);
    try {
      releaseAbsentResident(timedOutRun.turnContext);
    } catch (error) {
      cleanupFailures.push(error);
    }
    let leaseReleased = false;
    try {
      leaseReleased = store.releaseTimedOutExecutorLease(result);
      timedOutLeaseReleases.delete(result.turn_id);
    } catch (error) {
      timedOutLeaseReleases.set(result.turn_id, result);
      cleanupFailures.push(error);
    }
    refresh();
    reschedulePendingInteractionDeadlines();
    if (cleanupFailures.length > 0) {
      const [failure] = cleanupFailures;
      failure.cleanupFailures = cleanupFailures.slice(1);
      throw failure;
    }
    return { ...result, lease_released: leaseReleased };
  }

  async function executeInteractionDelivery(handoffId) {
    if (typeof adapter.prepareInteractionAnswer !== 'function') {
      throw new TypeError('adapter.prepareInteractionAnswer must be a function');
    }
    const delivery = store.claimInteractionHandoff(handoffId);
    const activeRun = activeRuns.get(delivery.request.turn_id);
    let prepared;
    try {
      prepared = await adapter.prepareInteractionAnswer(deepFreeze(delivery));
      if (!prepared || typeof prepared.send !== 'function') {
        throw new TypeError('adapter.prepareInteractionAnswer must return a send function');
      }
    } catch (error) {
      const providerError = normalizePreSendProviderError(error, now());
      const resolution = persist(
        () => store.markInteractionHandoffPreSendFailure(delivery, providerError),
      );
      if (resolution.status === 'recovering' && activeRun && !activeRun.durableSettled) {
        const isolationProven = await isolateInteractionRecovery(activeRun);
        if (!isolationProven) {
          lifecycle = 'close_failed';
          throw new Error('The non-retryable pre-send failure could not isolate its provider run.');
        }
        activeRun.durableSettled = true;
        cleanupActiveRun(activeRun);
        releaseRecoveringOwnership(activeRun.turnContext);
      }
      reschedulePendingInteractionDeadlines();
      refresh();
      return resolution;
    }
    const sendingDelivery = persist(
      () => store.markInteractionHandoffSendStarted(delivery),
    );
    let handlerAcknowledgement;
    try {
      handlerAcknowledgement = await prepared.send(
        deepFreeze(sendingDelivery),
      );
    } catch (error) {
      if (provider === 'codex' && isExplicitProviderError(error)) {
        const providerError = normalizeProviderError(error, now());
        if (providerError.side_effect_status === 'unknown') {
          const deliveryUnknown = persist(
            () => store.markInteractionHandoffDeliveryUnknown(sendingDelivery, providerError),
          );
          if (activeRun && !activeRun.durableSettled) {
            activeRun.durableSettled = true;
            cleanupActiveRun(activeRun);
          }
          reschedulePendingInteractionDeadlines();
          refresh();
          return deliveryUnknown;
        }
      }
      const recovery = {
        activeRun,
        delivery: sendingDelivery,
        deliveryUnknown: null,
        isolationProven: false,
        marked: false,
      };
      pendingInteractionRecoveries.set(delivery.request.turn_id, recovery);
      try {
        recovery.deliveryUnknown = persist(
          () => store.markInteractionHandoffDeliveryUnknown(sendingDelivery),
        );
        recovery.marked = true;
      } catch (markFailure) {
        lifecycle = 'close_failed';
        error.markFailure = markFailure;
      }
      if (activeRun && !activeRun.durableSettled) {
        try {
          recovery.isolationProven = await isolateInteractionRecovery(activeRun);
        } catch (isolationFailure) {
          lifecycle = 'close_failed';
          error.isolationFailure = isolationFailure;
        }
      }
      if (!recovery.isolationProven) lifecycle = 'close_failed';
      if (recovery.marked && activeRun && !activeRun.durableSettled) {
        activeRun.durableSettled = true;
        refresh();
        cleanupActiveRun(activeRun);
      }
      if (recovery.marked && recovery.isolationProven) {
        try {
          releaseRecoveringOwnership(activeRun.turnContext);
        } catch (ownershipFailure) {
          lifecycle = 'close_failed';
          error.ownershipFailure = ownershipFailure;
        }
        pendingInteractionRecoveries.delete(delivery.request.turn_id);
      }
      reschedulePendingInteractionDeadlines();
      if (recovery.deliveryUnknown) error.deliveryUnknown = recovery.deliveryUnknown;
      throw error;
    }
    const managedPermissions = permissionControllers.get(delivery.request.turn_id)?.size ?? 0;
    let acknowledgement;
    try {
      acknowledgement = store.acknowledgeInteractionHandoff(handlerAcknowledgement, {
        holdForPermission: managedPermissions > 0
          || handlerAcknowledgement.blocking_interactions_remaining === true,
      });
    } catch (error) {
      if (activeRun && !activeRun.durableSettled) {
        let recoveryPersisted = false;
        try {
          transitionToRecovery(activeRun, 'interaction_ack_persistence_failed');
          recoveryPersisted = true;
        } catch (recoveryFailure) {
          lifecycle = 'close_failed';
          error.recoveryFailure = recoveryFailure;
        }
        try {
          const isolationProven = await isolateInteractionRecovery(activeRun);
          if (recoveryPersisted && isolationProven) {
            releaseRecoveringOwnership(activeRun.turnContext);
          }
          if (!isolationProven) lifecycle = 'close_failed';
        } catch (isolationFailure) {
          lifecycle = 'close_failed';
          error.isolationFailure = isolationFailure;
        }
      }
      throw error;
    }
    reschedulePendingInteractionDeadlines();
    const shouldAdvance = acknowledgement.resumed
      || handlerAcknowledgement.blocking_interactions_remaining === true;
    if (activeRun && shouldAdvance) activeRun.pauseKind = null;
    const execution = activeRun && shouldAdvance ? await advanceRun(activeRun) : null;
    return { acknowledgement, execution };
  }

  function deliverInteractionAnswer(handoffId) {
    if (lifecycle !== 'open') {
      return Promise.reject(new Error(
        `Executor service is ${lifecycle}; it cannot deliver an interaction answer.`,
      ));
    }
    const delivery = executeInteractionDelivery(handoffId);
    interactionDeliverySettlements.add(delivery);
    delivery.finally(() => {
      interactionDeliverySettlements.delete(delivery);
    }).catch(() => {});
    return delivery;
  }

  async function reconcileInteractionHandoff(handoffId) {
    if (lifecycle !== 'open') {
      throw new Error(
        `Executor service is ${lifecycle}; it cannot reconcile an interaction handoff.`,
      );
    }
    if (typeof adapter.queryInteractionHandoffAcceptance !== 'function') {
      throw new TypeError('adapter.queryInteractionHandoffAcceptance must be a function');
    }
    const delivery = store.getInteractionHandoffForRecovery(handoffId);
    const proof = await adapter.queryInteractionHandoffAcceptance(deepFreeze(delivery));
    if (proof?.status === 'accepted') {
      const acknowledgement = persist(
        () => store.acknowledgeInteractionHandoffFromQuery(delivery, proof),
      );
      refresh();
      return acknowledgement;
    }
    return persist(() => store.recordInteractionHandoffQueryUnproven(delivery, proof));
  }

  async function resolveInteractionHandoff(handoffId, disposition) {
    if (lifecycle !== 'open') {
      throw new Error(
        `Executor service is ${lifecycle}; it cannot resolve an interaction handoff.`,
      );
    }
    if (interactionHandoffDispositionAuthorizer === null) {
      throw new TypeError('No interaction handoff disposition authorizer is configured.');
    }
    const delivery = store.getInteractionHandoffForRecovery(handoffId);
    const decision = Object.freeze({
      capability: 'interaction.handoff.resolve',
      scope: Object.freeze({
        conversation_id: delivery.request.conversation_id,
        turn_id: delivery.request.turn_id,
        handoff_id: delivery.handoff.handoff_id,
        action: disposition?.action,
        replacement_interaction_id: disposition?.replacement_interaction_id,
      }),
    });
    const authorization = await interactionHandoffDispositionAuthorizer(
      decision,
      deepFreeze(delivery),
    );
    const result = persist(() => store.resolveInteractionHandoffDisposition(
      delivery,
      disposition,
      authorization,
    ));
    reschedulePendingInteractionDeadlines();
    refresh();
    return result;
  }

  async function close() {
    if (closePromise) return closePromise;
    lifecycle = 'closing';
    const closing = (async () => {
      try {
        clearAllInteractionDeadlines();
        const shutdownFailures = [];
        const providerClosing = (async () => {
          if (typeof adapter.close !== 'function') return [];
          const result = await adapter.close();
          return Array.isArray(result) ? result : [];
        })().then(
          (closedConversationIds) => ({ closedConversationIds, error: null }),
          (error) => ({
            closedConversationIds: error.closedConversationIds ?? [],
            error,
          }),
        );
        await Promise.allSettled([...interactionDeliverySettlements]);
        const providerCloseResult = await providerClosing;
        const { closedConversationIds } = providerCloseResult;
        if (providerCloseResult.error) shutdownFailures.push(providerCloseResult.error);
        const closedConversationIdSet = new Set(closedConversationIds);
        const skippedSettlements = new Set();
        const pendingRecoveryTurnIds = new Set();
        for (const [turnId, recovery] of pendingInteractionRecoveries) {
          pendingRecoveryTurnIds.add(turnId);
          if (!recovery.activeRun) {
            shutdownFailures.push(new Error(
              `Interaction recovery ${turnId} has no local provider run to isolate.`,
            ));
            continue;
          }
          if (closedConversationIdSet.has(recovery.activeRun.turnContext.conversation_id)) {
            recovery.isolationProven = true;
          } else if (!recovery.isolationProven && recovery.activeRun) {
            try {
              recovery.isolationProven = await isolateInteractionRecovery(recovery.activeRun);
            } catch (error) {
              shutdownFailures.push(error);
            }
          }
          if (!recovery.marked) {
            try {
              recovery.deliveryUnknown = store.markInteractionHandoffDeliveryUnknown(
                recovery.delivery,
              );
              recovery.marked = true;
            } catch (error) {
              shutdownFailures.push(error);
            }
          }
          if (recovery.marked && !recovery.activeRun.durableSettled) {
            recovery.activeRun.durableSettled = true;
            refresh();
            cleanupActiveRun(recovery.activeRun);
          }
          if (recovery.marked && recovery.isolationProven) {
            recordRecoveringIsolation(recovery.activeRun.turnContext);
            pendingInteractionRecoveries.delete(turnId);
          } else {
            if (!recovery.activeRun.settled) {
              skippedSettlements.add(recovery.activeRun.settlement);
            }
            if (recovery.marked && !recovery.isolationProven) {
              shutdownFailures.push(new Error(
                `Provider close did not prove isolation for interaction recovery ${turnId}.`,
              ));
            }
          }
        }
        for (const activeRun of activeRuns.values()) {
          if (pendingRecoveryTurnIds.has(activeRun.turnContext.turn_id)) continue;
          if (activeRun.timedOutExpiration) {
            if (closedConversationIdSet.has(activeRun.turnContext.conversation_id)) {
              const expiration = activeRun.timedOutExpiration;
              activeRun.timedOutExpiration = null;
              activeRun.durableSettled = true;
              cleanupActiveRun(activeRun);
              timedOutLeaseReleases.set(activeRun.turnContext.turn_id, expiration);
            } else {
              skippedSettlements.add(activeRun.settlement);
              shutdownFailures.push(new Error(
                `Provider close did not prove isolation for timed-out turn ${activeRun.turnContext.turn_id}.`,
              ));
            }
            continue;
          }
          if (activeRun.pauseKind === 'interaction') {
            let isolationProven = closedConversationIdSet.has(
              activeRun.turnContext.conversation_id,
            );
            if (
              !isolationProven
              && provider !== 'codex'
              && typeof activeRun.iterator?.return === 'function'
            ) {
              try {
                await activeRun.iterator.return();
                isolationProven = true;
              } catch (error) {
                shutdownFailures.push(error);
              }
            }
            transitionToRecovery(activeRun, 'executor_shutdown_uncertain');
            if (isolationProven) {
              recordRecoveringIsolation(activeRun.turnContext);
            } else {
              shutdownFailures.push(new Error(
                `Provider close did not prove isolation for recovering turn ${activeRun.turnContext.turn_id}.`,
              ));
            }
          } else if (!activeRun.advancing && activeRun.iterator) {
            advanceRun(activeRun);
          }
        }
        for (const [turnId, turnContext] of pendingRecoveryIsolations) {
          if (closedConversationIdSet.has(turnContext.conversation_id)) {
            recordRecoveringIsolation(turnContext);
          } else {
            shutdownFailures.push(new Error(
              `Provider close did not prove isolation for recovering turn ${turnId}.`,
            ));
          }
        }
        await Promise.allSettled(
          [...activeRunSettlements].filter((settlement) => !skippedSettlements.has(settlement)),
        );
        for (const conversationId of closedConversationIds) {
          if (!endedResidentFences.has(conversationId)) {
            endedResidentFences.set(conversationId, null);
          }
        }
        for (const turnContext of recoveringOwnershipReleases.values()) {
          try {
            releaseRecoveringOwnership(turnContext);
          } catch (error) {
            shutdownFailures.push(error);
          }
        }
        for (const [conversationId, ownerEpoch] of endedResidentFences) {
          try {
            store.releaseExecutorResident(conversationId, ownerEpoch);
            endedResidentFences.delete(conversationId);
          } catch (error) {
            shutdownFailures.push(error);
          }
        }
        for (const [turnId, expiration] of timedOutLeaseReleases) {
          try {
            store.releaseTimedOutExecutorLease(expiration);
            timedOutLeaseReleases.delete(turnId);
          } catch (error) {
            shutdownFailures.push(error);
          }
        }
        if (shutdownFailures.length === 1) throw shutdownFailures[0];
        if (shutdownFailures.length > 1) {
          throw new AggregateError(
            shutdownFailures,
            'Executor shutdown did not release all durable ownership.',
          );
        }
        lifecycle = 'closed';
      } catch (error) {
        lifecycle = 'close_failed';
        throw error;
      } finally {
        clearAllInteractionDeadlines();
        if (lifecycle === 'closed' && residentHeartbeat !== null) {
          cancelResidentHeartbeat(residentHeartbeat);
          residentHeartbeat = null;
        }
        if (lifecycle === 'close_failed' && closePromise === closing) {
          closePromise = null;
        }
      }
    })();
    closePromise = closing;
    return closing;
  }

  return Object.freeze({
    cancel,
    close,
    deliverInteractionAnswer,
    evictIdleExecutors,
    expireInteraction,
    reconcileInteractionHandoff,
    resolveInteractionHandoff,
    runNext,
    snapshot,
    start,
    submitInteractionAnswer,
  });
}
