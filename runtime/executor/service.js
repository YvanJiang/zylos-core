import crypto from 'node:crypto';

import {
  canonicalizeJson,
  createContractError,
} from '../../contracts/public/index.js';
import { createOperationsControlService } from '../control/operations-control-service.js';
import { createRuntimeSnapshotPublisher } from '../observability/snapshot-publisher.js';
import { createPermissionService } from '../permissions/permission-service.js';
import { createExecutorStore } from '../persistence/executor-store.js';
import { resolveProviderWorkspaceAccess } from '../workspace/lease-coordinator.js';

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
        retryable: descriptor.retryable === true
          && descriptor.side_effect_status === 'none',
        sideEffectStatus: descriptor.side_effect_status,
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
    sideEffectStatus: 'unknown',
    userMessage: 'Provider preparation failed without proving that no side effect occurred.',
    occurredAt,
  });
}

function isExplicitProviderError(error) {
  return error !== null
    && typeof error === 'object'
    && error.providerError !== null
    && typeof error.providerError === 'object';
}

const NEVER_RETRY_PROVIDER_CODES = new Set([
  'authentication_failed',
  'authorization_failed',
  'cancelled',
  'permission_denied',
  'provider_context_invalid',
  'provider_stop_failed',
  'side_effect_unknown',
  'stopped',
  'turn_interrupted',
  'turn_terminal',
  'unauthenticated',
  'unauthorized',
]);

function isSafeProviderRetry(error) {
  return error.retryable === true
    && error.side_effect_status === 'none'
    && !['authentication', 'authorization', 'validation'].includes(error.category)
    && !NEVER_RETRY_PROVIDER_CODES.has(error.code)
    && !error.code.includes('permission')
    && !error.code.includes('context')
    && !error.code.includes('stop');
}

function isProviderTerminalRecord(record) {
  return record?.type === 'turn_result'
    && ['cancelled', 'completed', 'failed'].includes(record.outcome);
}

export function createExecutorService({
  database,
  adapter,
  provider,
  serviceInstanceId,
  hostId = serviceInstanceId,
  serviceStartedAt = null,
  now = () => new Date().toISOString(),
  generateId = defaultGenerateId,
  leaseDurationMs = 30_000,
  residentLeaseDurationMs = 60_000,
  residentHeartbeatIntervalMs = 20_000,
  workspaceRoot = process.cwd(),
  workspaceLeaseDurationMs = 10_000,
  workspaceHeartbeatIntervalMs = Math.min(3_000, workspaceLeaseDurationMs / 3),
  scheduleResidentHeartbeat = setInterval,
  cancelResidentHeartbeat = clearInterval,
  scheduleWorkspaceHeartbeat = setInterval,
  cancelWorkspaceHeartbeat = clearInterval,
  isolateOrphanedWorkspace = null,
  turnLeaseRenewalIntervalMs = Math.min(10_000, leaseDurationMs / 3),
  scheduleTurnLeaseRenewal = setInterval,
  cancelTurnLeaseRenewal = clearInterval,
  nonterminalSweepIntervalMs = 30_000,
  scheduleNonterminalSweep = setInterval,
  cancelNonterminalSweep = clearInterval,
  permissionSweepIntervalMs = 1_000,
  schedulePermissionSweep = setInterval,
  cancelPermissionSweep = clearInterval,
  permissionHandler = null,
  interactionHandoffDispositionAuthorizer = null,
  interactionTimeoutMs,
  providerStopTimeoutMs = 10_000,
  providerRetryBaseDelayMs = 1_000,
  providerRetryJitterRatio = 0.2,
  providerRetryRandom = Math.random,
  maxResidentExecutorsPerBot = 20,
  operationsPolicy = null,
  operationsServiceNamespace = null,
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
  if (!Number.isFinite(providerStopTimeoutMs) || providerStopTimeoutMs <= 0) {
    throw new TypeError('providerStopTimeoutMs must be a positive finite number');
  }
  if (!Number.isSafeInteger(providerRetryBaseDelayMs) || providerRetryBaseDelayMs <= 0) {
    throw new TypeError('providerRetryBaseDelayMs must be a positive safe integer');
  }
  if (
    !Number.isFinite(providerRetryJitterRatio)
    || providerRetryJitterRatio < 0
    || providerRetryJitterRatio > 1
  ) {
    throw new TypeError('providerRetryJitterRatio must be between zero and one');
  }
  if (typeof providerRetryRandom !== 'function') {
    throw new TypeError('providerRetryRandom must be a function');
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
  if (!Number.isFinite(turnLeaseRenewalIntervalMs) || turnLeaseRenewalIntervalMs <= 0) {
    throw new TypeError('turnLeaseRenewalIntervalMs must be a positive finite number');
  }
  if (!Number.isFinite(leaseDurationMs) || leaseDurationMs <= 0) {
    throw new TypeError('leaseDurationMs must be a positive finite number');
  }
  if (turnLeaseRenewalIntervalMs >= leaseDurationMs) {
    throw new TypeError('turn lease renewal interval must be below the turn lease duration');
  }
  if (typeof scheduleTurnLeaseRenewal !== 'function') {
    throw new TypeError('scheduleTurnLeaseRenewal must be a function');
  }
  if (typeof cancelTurnLeaseRenewal !== 'function') {
    throw new TypeError('cancelTurnLeaseRenewal must be a function');
  }
  if (!Number.isFinite(nonterminalSweepIntervalMs) || nonterminalSweepIntervalMs <= 0) {
    throw new TypeError('nonterminalSweepIntervalMs must be a positive finite number');
  }
  if (typeof scheduleNonterminalSweep !== 'function') {
    throw new TypeError('scheduleNonterminalSweep must be a function');
  }
  if (typeof cancelNonterminalSweep !== 'function') {
    throw new TypeError('cancelNonterminalSweep must be a function');
  }
  if (!Number.isFinite(permissionSweepIntervalMs) || permissionSweepIntervalMs <= 0) {
    throw new TypeError('permissionSweepIntervalMs must be a positive finite number');
  }
  if (typeof schedulePermissionSweep !== 'function') {
    throw new TypeError('schedulePermissionSweep must be a function');
  }
  if (typeof cancelPermissionSweep !== 'function') {
    throw new TypeError('cancelPermissionSweep must be a function');
  }
  if (
    !Number.isFinite(workspaceLeaseDurationMs)
    || workspaceLeaseDurationMs <= 0
  ) {
    throw new TypeError('workspaceLeaseDurationMs must be a positive finite number');
  }
  if (
    !Number.isFinite(workspaceHeartbeatIntervalMs)
    || workspaceHeartbeatIntervalMs <= 0
    || workspaceHeartbeatIntervalMs >= workspaceLeaseDurationMs
  ) {
    throw new TypeError(
      'workspaceHeartbeatIntervalMs must be positive and below the workspace lease',
    );
  }
  if (typeof scheduleWorkspaceHeartbeat !== 'function') {
    throw new TypeError('scheduleWorkspaceHeartbeat must be a function');
  }
  if (typeof cancelWorkspaceHeartbeat !== 'function') {
    throw new TypeError('cancelWorkspaceHeartbeat must be a function');
  }
  if (isolateOrphanedWorkspace !== null && typeof isolateOrphanedWorkspace !== 'function') {
    throw new TypeError('isolateOrphanedWorkspace must be a function or null');
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
    workspaceLeaseDurationMs,
    interactionTimeoutMs,
  });
  const permissionPolicy = createPermissionService({ database, now, generateId });
  let executors = [];
  let started = false;
  const activeRuns = new Map();
  const activeTurns = new Map();
  const closingPermissionTurnIds = new Set();
  const permissionControllers = new Map();
  const activeRunSettlements = new Set();
  const interactionDeliverySettlements = new Set();
  const replyMappingRecoverySettlements = new Set();
  const endedResidentFences = new Map();
  const pendingInteractionRecoveries = new Map();
  const pendingRecoveryIsolations = new Map();
  const orphanedWorkspaceRecoveryTurnIds = new Set();
  const recoveringOwnershipReleases = new Map();
  const releasedRecoveringOwnershipTurnIds = new Set();
  const timedOutLeaseReleases = new Map();
  const stopSettlements = new Map();
  const steerSettlements = new Map();
  let lifecycle = 'open';
  let closePromise = null;
  let residentHeartbeat = null;
  let workspaceHeartbeat = null;
  let turnLeaseRenewal = null;
  let nonterminalSweep = null;
  let permissionSweep = null;
  let residentHeartbeatFailure = null;
  let workspaceHeartbeatFailure = null;
  let workspaceRecoveryFlight = null;
  const observabilityPublisher = createRuntimeSnapshotPublisher({
    database,
    serviceInstanceId,
    hostId,
    startedAt: serviceStartedAt ?? now(),
    now,
    generateId,
    getServiceState: () => ({
      degraded: residentHeartbeatFailure !== null
        || workspaceHeartbeatFailure !== null
        || workspaceRecoveryFlight !== null
        || lifecycle === 'close_failed',
      offline: !started || lifecycle === 'closed',
      maintenance: false,
      draining: lifecycle === 'closing',
      reconciling: workspaceRecoveryFlight !== null,
    }),
  });
  const operationsControl = operationsPolicy === null ? null : createOperationsControlService({
    database,
    serviceInstanceId,
    deploymentPolicy: operationsPolicy,
    runtimeStore: store,
    serviceNamespace: operationsServiceNamespace,
    now,
    generateId,
  });

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
    refresh();
    return {
      service_instance_id: serviceInstanceId,
      executors: executors.map((executor) => ({
        ...executor,
        queued_turn_ids: [...executor.queued_turn_ids],
      })),
      workspace_leases: store.listWorkspaceLeaseObservability(),
    };
  }

  function start() {
    if (lifecycle !== 'open') {
      throw new Error(`Executor service is ${lifecycle}; it cannot be started.`);
    }
    if (started) return snapshot();
    permissionPolicy.expireDue();
    if (permissionSweep === null) {
      permissionSweep = schedulePermissionSweep(() => {
        try {
          permissionPolicy.expireDue();
        } catch (error) {
          onDeadlineError(error, Object.freeze({
            kind: 'permission_expiry_sweep',
            expires_at: now(),
          }));
        }
      }, permissionSweepIntervalMs);
      permissionSweep?.unref?.();
    }
    store.reconcileExpiredResidents();
    adoptOrphanedWorkspaceRecoveries();
    store.reconcileExpiredStartedInteractionHandoffs();
    store.reconcileNonterminalTurns([], 'startup_reconciliation');
    observabilityPublisher.recordReconciliation();
    if (nonterminalSweep === null) {
      nonterminalSweep = scheduleNonterminalSweep(() => {
        try {
          store.reconcileNonterminalTurns(
            [...activeRuns.values()].map(({ turnContext }) => turnContext),
            'sweep_reconciliation',
          );
          observabilityPublisher.recordReconciliation();
          refresh();
          reschedulePendingInteractionDeadlines();
        } catch (error) {
          onDeadlineError(error, Object.freeze({
            kind: 'executor_nonterminal_reconciliation',
            expires_at: now(),
          }));
        }
      }, nonterminalSweepIntervalMs);
      nonterminalSweep?.unref?.();
    }
    if (turnLeaseRenewal === null) {
      turnLeaseRenewal = scheduleTurnLeaseRenewal(() => {
        try {
          store.renewOwnedTurnLeases(
            [...activeRuns.values()].map(({ turnContext }) => turnContext),
          );
        } catch (error) {
          onDeadlineError(error, Object.freeze({
            kind: 'executor_turn_lease_renewal',
            expires_at: now(),
          }));
        }
      }, turnLeaseRenewalIntervalMs);
      turnLeaseRenewal?.unref?.();
    }
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
    if (workspaceHeartbeat === null) {
      workspaceHeartbeat = scheduleWorkspaceHeartbeat(async () => {
        if (lifecycle !== 'open') return;
        try {
          store.heartbeatOwnedWorkspaceLeases();
          workspaceHeartbeatFailure = null;
        } catch (error) {
          try {
            await recoverExpiredWorkspaceRuns(error);
          } catch (recoveryError) {
            workspaceHeartbeatFailure = recoveryError;
            return;
          }
        }
        try {
          adoptOrphanedWorkspaceRecoveries();
          await reconcileWorkspaceRecoveries();
          workspaceHeartbeatFailure = null;
        } catch (error) {
          workspaceHeartbeatFailure = error;
        }
      }, workspaceHeartbeatIntervalMs);
      workspaceHeartbeat?.unref?.();
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

  function committedControlStatus(activeRun) {
    if (activeRun.steerCommitted) return 'interrupted';
    if (activeRun.stopCommitted) return 'stopped';
    if (activeRun.recoveryCommitted) return 'recovering';
    return null;
  }

  function cleanupActiveRun(activeRun) {
    const { turnContext } = activeRun;
    if (activeRuns.get(turnContext.turn_id) === activeRun) {
      activeRuns.delete(turnContext.turn_id);
    }
    if (activeTurns.get(turnContext.conversation_id) === turnContext) {
      activeTurns.delete(turnContext.conversation_id);
    }
    const controllers = permissionControllers.get(turnContext.turn_id);
    if (!controllers || controllers.size === 0) {
      permissionControllers.delete(turnContext.turn_id);
      closingPermissionTurnIds.delete(turnContext.turn_id);
    }
    if (!activeRun.settled) {
      activeRun.settled = true;
      activeRunSettlements.delete(activeRun.settlement);
      activeRun.resolveSettlement(activeRun.providerTerminalObserved);
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
    if (releasedRecoveringOwnershipTurnIds.has(turnContext.turn_id)) {
      return { lease_released: true, already_released: true };
    }
    recoveringOwnershipReleases.set(turnContext.turn_id, turnContext);
    const result = store.releaseRecoveringExecutorOwnership(turnContext);
    releasedRecoveringOwnershipTurnIds.add(turnContext.turn_id);
    recoveringOwnershipReleases.delete(turnContext.turn_id);
    pendingRecoveryIsolations.delete(turnContext.turn_id);
    orphanedWorkspaceRecoveryTurnIds.delete(turnContext.turn_id);
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

  async function recoverUnknownInteractionDelivery(
    activeRun,
    delivery,
    cause,
    providerError = null,
  ) {
    const recovery = {
      activeRun,
      delivery,
      providerError,
      deliveryUnknown: null,
      isolationProven: false,
      marked: false,
    };
    pendingInteractionRecoveries.set(delivery.request.turn_id, recovery);
    try {
      recovery.deliveryUnknown = persist(
        () => store.markInteractionHandoffDeliveryUnknown(delivery, providerError),
      );
      recovery.marked = true;
    } catch (markFailure) {
      lifecycle = 'close_failed';
      cause.markFailure = markFailure;
    }
    if (activeRun && !activeRun.durableSettled) {
      try {
        recovery.isolationProven = await isolateInteractionRecovery(activeRun);
      } catch (isolationFailure) {
        lifecycle = 'close_failed';
        cause.isolationFailure = isolationFailure;
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
        cause.ownershipFailure = ownershipFailure;
      }
      pendingInteractionRecoveries.delete(delivery.request.turn_id);
    }
    reschedulePendingInteractionDeadlines();
    if (recovery.deliveryUnknown) cause.deliveryUnknown = recovery.deliveryUnknown;
    return recovery;
  }
  function transitionToRecovery(
    activeRun,
    reasonCode = 'provider_execution_uncertain',
    error = null,
    { allowUncertainWorkspace = false } = {},
  ) {
    const { state } = allowUncertainWorkspace
      ? store.assertRecoverableFence(activeRun.turnContext)
      : store.assertCurrentFence(activeRun.turnContext);
    persist(() => store.transitionTurn(activeRun.turnContext, state, 'recovering', {
      allowUncertainWorkspace,
      error: null,
      reasonCode,
      recovery: error === null ? null : {
        error,
        waitForDecision: error.side_effect_status === 'unknown',
      },
    }));
    activeRun.durableSettled = true;
    activeRun.forcedResult = resultFor(activeRun, 'recovering');
    refresh();
    cleanupActiveRun(activeRun);
    pendingRecoveryIsolations.set(activeRun.turnContext.turn_id, activeRun.turnContext);
    return activeRun.forcedResult;
  }

  async function recoverExpiredWorkspaceRuns(heartbeatError) {
    const affectedRuns = [...activeRuns.values()].filter(
      (activeRun) => !activeRun.durableSettled
        && store.isWorkspaceRecoveryRequired(activeRun.turnContext),
    );
    if (affectedRuns.length === 0) {
      if (heartbeatError?.code === 'stale_workspace_lease') return;
      throw heartbeatError;
    }
    for (const activeRun of affectedRuns) {
      const { turnContext } = activeRun;
      try {
        transitionToRecovery(
          activeRun,
          'workspace_lease_expired',
          {
            code: 'workspace_lease_expired',
            category: 'conflict',
            retryable: false,
            side_effect_status: 'unknown',
            user_message: 'Workspace ownership expired while provider work was still active.',
          },
          { allowUncertainWorkspace: true },
        );
      } catch (recoveryFailure) {
        workspaceHeartbeatFailure = recoveryFailure;
        continue;
      }
    }
    if (workspaceHeartbeatFailure === heartbeatError) workspaceHeartbeatFailure = null;
  }

  function adoptOrphanedWorkspaceRecoveries() {
    for (const turnContext of store.claimOrphanedWorkspaceRecoveries()) {
      pendingRecoveryIsolations.set(turnContext.turn_id, turnContext);
      orphanedWorkspaceRecoveryTurnIds.add(turnContext.turn_id);
    }
  }

  async function performWorkspaceRecoveryReconciliation() {
    adoptOrphanedWorkspaceRecoveries();
    const result = { isolated: [], notification_pending: [], isolation_pending: [] };
    for (const [turnId, turnContext] of pendingRecoveryIsolations) {
      if (!turnContext.workspace) continue;
      if (!store.isRecoveryNotificationDelivered(turnContext)) {
        result.notification_pending.push(turnId);
        continue;
      }
      const orphaned = orphanedWorkspaceRecoveryTurnIds.has(turnId);
      if (orphaned) {
        if (isolateOrphanedWorkspace === null) {
          result.isolation_pending.push(turnId);
          continue;
        }
        const isolation = await isolateOrphanedWorkspace(Object.freeze({
          conversation_id: turnContext.conversation_id,
          turn_id: turnContext.turn_id,
          workspace: Object.freeze({ ...turnContext.workspace }),
          attempt: Object.freeze({ ...turnContext.attempt }),
        }));
        if (isolation !== true && isolation?.isolated !== true) {
          result.isolation_pending.push(turnId);
          continue;
        }
      } else {
        if (typeof adapter.abort !== 'function') {
          result.isolation_pending.push(turnId);
          continue;
        }
        await adapter.abort(turnContext);
      }
      releaseRecoveringOwnership(turnContext);
      result.isolated.push(turnId);
    }
    refresh();
    return result;
  }

  function reconcileWorkspaceRecoveries() {
    if (workspaceRecoveryFlight) return workspaceRecoveryFlight;
    if (lifecycle !== 'open') {
      return Promise.reject(new Error(
        `Executor service is ${lifecycle}; it cannot reconcile workspace recovery.`,
      ));
    }
    const flight = performWorkspaceRecoveryReconciliation();
    workspaceRecoveryFlight = flight;
    const clearFlight = () => {
      if (workspaceRecoveryFlight === flight) workspaceRecoveryFlight = null;
    };
    flight.then(clearFlight, clearFlight);
    return flight;
  }

  function beginUnknownBackgroundRecovery(activeRun) {
    return transitionToRecovery(
      activeRun,
      'provider_background_work_unknown',
      {
        code: 'provider_background_work_unknown',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'Background workspace work ended without a confirmed outcome.',
      },
    );
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
    const controlStatus = committedControlStatus(activeRun);
    if (controlStatus !== null) {
      activeRun.durableSettled = true;
      cleanupActiveRun(activeRun);
      refresh();
      return resultFor(activeRun, controlStatus);
    }
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
      if (activeRun.forcedResult) return activeRun.forcedResult;
      throw error;
    }
    if (error.persistenceFailure) {
      try {
        store.listActiveWorkspaceLeases();
        if (store.isWorkspaceRecoveryRequired(turnContext)) {
          return transitionToRecovery(
            activeRun,
            'workspace_lease_expired',
            {
              code: 'workspace_lease_expired',
              category: 'conflict',
              retryable: false,
              side_effect_status: 'unknown',
              user_message: 'Workspace ownership expired while provider work was still active.',
            },
            { allowUncertainWorkspace: true },
          );
        }
      } catch (recoveryFailure) {
        error.recoveryFailure = recoveryFailure;
      }
      if (typeof adapter.abort === 'function') {
        await adapter.abort(turnContext);
      }
      cleanupActiveRun(activeRun);
      throw error;
    }
    closingPermissionTurnIds.add(turnContext.turn_id);
    for (const controller of permissionControllers.get(turnContext.turn_id) ?? []) {
      controller.abort();
    }
    if (activeRun.backgroundUncertain) {
      return beginUnknownBackgroundRecovery(activeRun);
    }
    if (isExplicitProviderError(error)) {
      const normalizedError = normalizeProviderError(error, now());
      const attemptNo = turnContext.attempt.attempt_no;
      if (isSafeProviderRetry(normalizedError) && attemptNo <= 3) {
        const baseBackoff = providerRetryBaseDelayMs * (2 ** (attemptNo - 1));
        const jitterFactor = 1 + ((providerRetryRandom() * 2) - 1) * providerRetryJitterRatio;
        const backoffMs = Math.max(0, Math.round(baseBackoff * jitterFactor));
        const retry = persist(
          () => store.scheduleProviderRetry(turnContext, normalizedError, backoffMs),
        );
        activeRun.durableSettled = true;
        refresh();
        cleanupActiveRun(activeRun);
        reschedulePendingInteractionDeadlines();
        return resultFor(activeRun, 'retry_scheduled', { retry });
      }
      if (isSafeProviderRetry(normalizedError) && attemptNo === 4) {
        persist(() => store.exhaustProviderRetries(turnContext, normalizedError));
        activeRun.durableSettled = true;
        refresh();
        cleanupActiveRun(activeRun);
        releaseAbsentResident(turnContext);
        reschedulePendingInteractionDeadlines();
        return resultFor(activeRun, 'failed');
      }
      if (normalizedError.side_effect_status === 'unknown') {
        const recovery = persist(() => store.markProviderFailure(turnContext, normalizedError));
        activeRun.durableSettled = true;
        refresh();
        cleanupActiveRun(activeRun);
        reschedulePendingInteractionDeadlines();
        return resultFor(activeRun, recovery.status, { recovery });
      }
      const { state } = store.assertCurrentFence(turnContext);
      persist(() => store.transitionTurn(turnContext, state, 'failed', {
        error: normalizedError,
        reasonCode: 'executor_failed',
        providerEventObserved: true,
      }));
      activeRun.durableSettled = true;
      refresh();
      cleanupActiveRun(activeRun);
      releaseAbsentResident(turnContext);
      reschedulePendingInteractionDeadlines();
      return resultFor(activeRun, 'failed');
    }
    const { state } = store.assertCurrentFence(turnContext);
    store.transitionTurn(turnContext, state, 'failed', {
      error: {
        code: 'provider_stream_failed',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The provider stream ended before the turn completed.',
      },
      providerEventObserved: true,
    });
    activeRun.durableSettled = true;
    refresh();
    cleanupActiveRun(activeRun);
    releaseAbsentResident(turnContext);
    reschedulePendingInteractionDeadlines();
    return resultFor(activeRun, 'failed');
  }

  async function finishRun(activeRun) {
    if (activeRun.durableSettled && activeRun.forcedResult) {
      return activeRun.forcedResult;
    }
    const controlStatus = committedControlStatus(activeRun);
    if (controlStatus !== null) {
      const recoveringIsolationProven = controlStatus === 'recovering'
        && activeRun.providerTerminalObserved
        && pendingRecoveryIsolations.has(activeRun.turnContext.turn_id);
      activeRun.durableSettled = true;
      cleanupActiveRun(activeRun);
      if (recoveringIsolationProven) {
        try {
          releaseRecoveringOwnership(activeRun.turnContext);
        } catch {
          if (lifecycle !== 'closed') lifecycle = 'close_failed';
        }
      }
      refresh();
      return resultFor(activeRun, controlStatus);
    }
    activeRun.ensureProviderStarted();
    if (activeRun.backgroundUncertain) {
      return beginUnknownBackgroundRecovery(activeRun);
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
        providerEventObserved: true,
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
        if (committedControlStatus(activeRun) !== null) {
          if (next.done) return await finishRun(activeRun);
          persist(() => store.recordProviderEventDiagnostic(
            activeRun.turnContext,
            next.value,
          ));
          if (isProviderTerminalRecord(next.value)) {
            activeRun.providerTerminalObserved = true;
          }
          if (
            next.value?.type === 'provider_native_id'
            && typeof next.value.acknowledge === 'function'
          ) {
            next.value.acknowledge(new Error(
              'Provider identity arrived after the durable control fence.',
            ));
          }
          continue;
        }
        if (activeRun.providerFailureOutcome?.status === 'recovering') {
          return retainRecoveringRun(activeRun);
        }
        if (next.done) return await finishRun(activeRun);
        const record = next.value;
        if (record?.type === 'interaction_persisted') {
          reschedulePendingInteractionDeadlines();
          activeRun.pauseKind = 'interaction';
          refresh();
          return resultFor(activeRun, 'waiting_user', { request: record.request });
        }
        if (record?.type === 'background_work_started') {
          activeRun.usesManagedRecords = true;
          persist(() => store.startWorkspaceBackgroundWork(
            activeRun.turnContext,
            record.provider_task_id,
          ));
          continue;
        }
        if (record?.type === 'background_work_finished') {
          activeRun.usesManagedRecords = true;
          persist(() => store.finishWorkspaceBackgroundWork(
            activeRun.turnContext,
            record.provider_task_id,
            record.outcome,
          ));
          if (record.outcome === 'unknown') activeRun.backgroundUncertain = true;
          else if (record.outcome !== 'completed') activeRun.outcome = 'failed';
          continue;
        }
        if (record?.kind === 'interaction_requested') {
          activeRun.ensureProviderStarted();
          const request = persist(() => store.requestInteraction(
            activeRun.turnContext,
            record.payload,
          ));
          reschedulePendingInteractionDeadlines();
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
          activeRun.providerTerminalObserved = isProviderTerminalRecord(record);
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
    function authorizeProtectedAction(request) {
      const actionRef = typeof request?.action_ref === 'string'
        ? request.action_ref
        : generateId('protected-action');
      const actionKind = typeof request?.action_kind === 'string'
        ? request.action_kind
        : (typeof request?.tool_name === 'string' ? request.tool_name : 'provider_action');
      return persist(() => permissionPolicy.authorizeProtectedAction({
        turn_id: turnContext.turn_id,
        action_ref: actionRef,
        action_kind: actionKind,
      }));
    }
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
      assertWorkspaceWrite(approvalFence = undefined) {
        if (
          approvalFence !== undefined
          && (
            committedControlStatus(activeRun) !== null
            || activeRun.durableSettled
            || activeRuns.get(turnContext.turn_id) !== activeRun
            || approvalFence.provider_thread_id !== activeRun.currentProviderNativeId
          )
        ) {
          const error = new Error('The Codex approval no longer matches the active provider run.');
          error.code = 'stale_attempt';
          throw error;
        }
        return persist(() => store.assertWorkspaceWritable(turnContext, approvalFence));
      },
      authorizeProtectedAction(request) {
        return authorizeProtectedAction(request);
      },
      async requestPermission(request, { signal } = {}) {
        const policyDecision = authorizeProtectedAction(request);
        if (policyDecision.trusted) {
          return Object.freeze({
            behavior: 'allow',
            permission_basis: policyDecision.basis_kind,
          });
        }
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
            if (!closingPermissionTurnIds.has(turnContext.turn_id)) {
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

  function reserveWithWorkspace({ markCapacityWait = true } = {}) {
    const workspaceAccessByConversation = new Map(
      store.listWorkspaceReservationCandidates().map((candidate) => [
        candidate.conversation_id,
        resolveProviderWorkspaceAccess(adapter, {
          conversation_id: candidate.conversation_id,
          turn_id: candidate.turn_id,
          provider,
        }, { defaultRoot: workspaceRoot }),
      ]),
    );
    return {
      reservation: store.reserveNextExecutor({
        maxResidentExecutorsPerBot,
        markCapacityWait,
        workspaceAccessByConversation,
      }),
      workspaceAccessByConversation,
    };
  }

  async function runNext() {
    if (lifecycle !== 'open') {
      throw new Error(`Executor service is ${lifecycle}; it cannot claim another turn.`);
    }
    if (!started) start();
    if (residentHeartbeatFailure) throw persistenceFailure(residentHeartbeatFailure);
    if (workspaceHeartbeatFailure) throw persistenceFailure(workspaceHeartbeatFailure);
    const replyMappingRecovery = store.claimNextReplyMappingRecoveryNotice();
    if (replyMappingRecovery !== null) {
      if (!replyMappingRecovery.notice_delivered) {
        refresh();
        return replyMappingRecovery;
      }
      const recoveryClaim = persist(() => store.beginReplyMappingRecovery(
        replyMappingRecovery.turn_id,
        { allowNativeRecovery: typeof adapter.recoverLineage === 'function' },
      ));
      if (recoveryClaim.status === 'waiting_decision') {
        refresh();
        return recoveryClaim;
      }
      if (recoveryClaim.status === 'native_recovery_in_flight') {
        refresh();
        return replyMappingRecovery;
      }
      let nativeResult = {
        status: 'not_applicable',
        recovery_id: recoveryClaim.recovery_id,
        side_effect_status: 'none',
      };
      let finishRecoverySettlement = null;
      if (recoveryClaim.status === 'native_recovery_claimed') {
        const recoverySettlement = new Promise((resolve) => {
          finishRecoverySettlement = resolve;
        });
        replyMappingRecoverySettlements.add(recoverySettlement);
        recoverySettlement.finally(() => {
          replyMappingRecoverySettlements.delete(recoverySettlement);
        }).catch(() => {});
      }
      let recoveryCompletion;
      try {
        if (recoveryClaim.status === 'native_recovery_claimed') {
          try {
            nativeResult = await adapter.recoverLineage(deepFreeze({
              recovery_id: recoveryClaim.recovery_id,
              turn_id: recoveryClaim.turn_id,
              reason: recoveryClaim.reason,
              candidate: Object.freeze({ ...recoveryClaim.candidate }),
              native_recovery_attempt_id: recoveryClaim.native_recovery_attempt_id,
              native_recovery_attempt_no: recoveryClaim.native_recovery_attempt_no,
            }));
          } catch (error) {
            nativeResult = {
              status: 'failed',
              recovery_id: recoveryClaim.recovery_id,
              native_recovery_attempt_id: recoveryClaim.native_recovery_attempt_id,
              native_recovery_attempt_no: recoveryClaim.native_recovery_attempt_no,
              side_effect_status: error?.providerError?.side_effect_status ?? 'unknown',
            };
          }
        } else if (recoveryClaim.status === 'native_recovery_lost') {
          nativeResult = {
            status: 'lost',
            recovery_id: recoveryClaim.recovery_id,
            native_recovery_attempt_id: recoveryClaim.native_recovery_attempt_id,
            native_recovery_attempt_no: recoveryClaim.native_recovery_attempt_no,
            side_effect_status: 'unknown',
          };
        }
        recoveryCompletion = persist(
          () => store.completeReplyMappingRecovery(recoveryClaim, nativeResult),
        );
        refresh();
      } finally {
        finishRecoverySettlement?.();
      }
      if (recoveryCompletion.status === 'stopped') return recoveryCompletion;
      if (lifecycle !== 'open') {
        return {
          status: 'service_closing',
          turn_id: recoveryCompletion.turn_id,
          recovery_status: recoveryCompletion.status,
        };
      }
    }
    let reserved = reserveWithWorkspace({ markCapacityWait: false });
    let { reservation } = reserved;
    if (reservation.status === 'idle') return reservation;
    if (reservation.status === 'workspace_wait') {
      refresh();
      return reservation;
    }
    if (reservation.status === 'capacity_wait') {
      await evictIdleExecutors();
      if (lifecycle !== 'open') {
        throw new Error(`Executor service is ${lifecycle}; it cannot claim another turn.`);
      }
      reserved = reserveWithWorkspace();
      ({ reservation } = reserved);
    }
    if (reservation.status === 'workspace_wait') {
      refresh();
      return reservation;
    }
    if (reservation.status === 'capacity_wait') {
      refresh();
      return reservation;
    }
    let turnContext;
    try {
      turnContext = store.claimNextQueuedTurn({
        conversationId: reservation.conversation_id,
        requireResident: provider === 'claude',
        workspaceAccess: reserved.workspaceAccessByConversation.get(
          reservation.conversation_id,
        ) ?? resolveProviderWorkspaceAccess(adapter, {
          conversation_id: reservation.conversation_id,
          provider,
        }, { defaultRoot: workspaceRoot }),
        workspaceLease: reservation.workspace ?? null,
      });
    } catch (error) {
      if (reservation.workspace) {
        try {
          store.releaseWorkspaceReservation(reservation.workspace);
        } catch {
          // The original claim failure remains authoritative; expiry reconciles a lost fence.
        }
      }
      throw error;
    }
    if (turnContext?.status === 'workspace_wait') {
      if (reservation.workspace) store.releaseWorkspaceReservation(reservation.workspace);
      refresh();
      return turnContext;
    }
    if (!turnContext) {
      if (reservation.workspace) store.releaseWorkspaceReservation(reservation.workspace);
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
      backgroundUncertain: false,
      currentProviderNativeId: turnContext.provider_native_id,
      durableSettled: false,
      ensureProviderStarted: null,
      forcedResult: null,
      iterator: null,
      outcome: null,
      pauseKind: null,
      providerFailureOutcome: null,
      providerFailurePersistenceError: null,
      providerStarted: false,
      providerTerminalObserved: false,
      resolveSettlement,
      settled: false,
      settlement,
      timedOutExpiration: null,
      turnContext,
      usesManagedRecords: false,
      stopCommitted: false,
      steerCommitted: false,
      recoveryCommitted: false,
    };
    activeRunSettlements.add(settlement);
    activeRuns.set(turnContext.turn_id, activeRun);
    activeTurns.set(turnContext.conversation_id, turnContext);
    activeRun.ensureProviderStarted = () => {
      if (activeRun.providerStarted) return { status: 'already_started' };
      persist(() => store.transitionTurn(turnContext, 'starting', 'running', {
        reasonCode: 'provider_started',
        providerEventObserved: true,
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
        executor_instance_id: turnContext.executor_instance_id,
        workspace: turnContext.workspace === null
          ? null
          : Object.freeze({ ...turnContext.workspace }),
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
          if (
            committedControlStatus(activeRun) !== null
            || activeRun.durableSettled
            || activeRuns.get(turnContext.turn_id) !== activeRun
          ) {
            persist(() => store.recordProviderEventDiagnostic(
              turnContext,
              {
                kind: 'provider_native_id',
                payload: { provider_native_id: providerNativeId },
              },
            ));
            return { status: 'stale_attempt' };
          }
          const binding = persist(() => store.bindProviderNativeId(
            turnContext,
            providerNativeId,
          ));
          activeRun.currentProviderNativeId = providerNativeId;
          return binding;
        },
        reportProviderState(providerState) {
          if (
            committedControlStatus(activeRun) !== null
            || activeRun.durableSettled
            || activeRuns.get(turnContext.turn_id) !== activeRun
          ) {
            persist(() => store.recordProviderEventDiagnostic(
              turnContext,
              {
                kind: 'provider_state',
                payload: { state: providerState?.state ?? 'unknown' },
              },
            ));
            return { status: 'stale_attempt' };
          }
          if (
            !providerState
            || providerState.state !== 'started'
            || !Object.hasOwn(providerState, 'provider_native_id')
            || providerState.provider_native_id !== activeRun.currentProviderNativeId
          ) {
            throw new TypeError('provider state must match the current started lineage');
          }
          if (activeRun.providerStarted) {
            persist(() => store.recordProviderEventActivity(turnContext));
            return { status: 'already_started' };
          }
          return activeRun.ensureProviderStarted();
        },
        reportRuntimeEvidence(runtimeEvidence) {
          if (
            committedControlStatus(activeRun) !== null
            || activeRun.durableSettled
            || activeRuns.get(turnContext.turn_id) !== activeRun
          ) {
            persist(() => store.recordProviderEventDiagnostic(
              turnContext,
              {
                kind: 'runtime_evidence',
                payload: { status: 'arrived_after_control' },
              },
            ));
            return { status: 'stale_attempt' };
          }
          return persist(() => store.recordProviderRuntimeEvidence(
            turnContext,
            runtimeEvidence,
          ));
        },
        reportProviderFailure(providerFailure) {
          if (
            committedControlStatus(activeRun) !== null
            || activeRun.durableSettled
            || activeRuns.get(turnContext.turn_id) !== activeRun
          ) {
            persist(() => store.recordProviderEventDiagnostic(
              turnContext,
              {
                kind: 'provider_failure',
                payload: { status: 'arrived_after_control' },
              },
            ));
            return { status: 'stale_attempt' };
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

  async function performStop(request) {
    // Commit the provider-neutral terminal winner before asking an adapter to
    // terminate private provider work; subsequent provider output is fenced.
    const result = persist(() => store.stopConversation(request));
    const activeRun = result.active_turn
      ? activeRuns.get(result.active_turn.turn_id)
      : null;
    const turnContext = result.active_turn ? (activeRun?.turnContext ?? {
      conversation_id: result.conversation_id,
      turn_id: result.active_turn.turn_id,
      attempt: result.active_turn.attempt,
    }) : null;
    if (activeRun) {
      activeRun.stopCommitted = true;
      if (activeRun.recoveryCommitted) {
        pendingRecoveryIsolations.delete(activeRun.turnContext.turn_id);
        recoveringOwnershipReleases.delete(activeRun.turnContext.turn_id);
      }
    }
    if (turnContext) {
      closingPermissionTurnIds.add(turnContext.turn_id);
      for (const controller of permissionControllers.get(turnContext.turn_id) ?? []) {
        controller.abort();
      }
    }
    // These are rebuildable local projections. A failure must not reopen the
    // durable stop fence or prevent provider isolation.
    try { refresh(); } catch {}
    try { reschedulePendingInteractionDeadlines(); } catch {}
    const durableProviderStopStatus = result.provider_stop_status
      ?? (result.active_turn ? 'pending' : 'not_applicable');
    if (result.active_turn === null || durableProviderStopStatus !== 'pending') {
      if (activeRun?.recoveryCommitted && result.lease_released === true) {
        releasedRecoveringOwnershipTurnIds.add(activeRun.turnContext.turn_id);
      }
      return result;
    }

    async function requestProviderIsolation() {
      if (typeof adapter.abort !== 'function') return false;
      let isolationTimeoutHandle;
      const isolation = Promise.resolve()
        .then(() => adapter.abort(turnContext))
        .then(() => true, () => false);
      const isolated = await Promise.race([
        isolation,
        new Promise((resolve) => {
          isolationTimeoutHandle = setTimeoutFn(
            () => resolve(false),
            providerStopTimeoutMs,
          );
        }),
      ]);
      clearProviderStopTimeout(isolationTimeoutHandle);
      return isolated;
    }
    function clearProviderStopTimeout(timeoutHandle) {
      if (timeoutHandle === undefined) return;
      try { clearTimeoutFn(timeoutHandle); } catch {}
    }
    let providerStopStatus;
    let isolationProven = false;
    let cancellationTimeoutHandle;
    const cancellation = Promise.resolve().then(async () => {
      if (typeof adapter.cancel !== 'function') {
        throw new TypeError('adapter.cancel must be a function to stop an active turn');
      }
      await adapter.cancel(turnContext);
      return 'confirmed';
    }).catch(() => 'uncertain');
    providerStopStatus = await Promise.race([
      cancellation,
      new Promise((resolve) => {
        cancellationTimeoutHandle = setTimeoutFn(
          () => resolve('terminal_unconfirmed'),
          providerStopTimeoutMs,
        );
      }),
    ]);
    if (providerStopStatus !== 'terminal_unconfirmed') {
      clearProviderStopTimeout(cancellationTimeoutHandle);
    }
    if (providerStopStatus !== 'confirmed') {
      isolationProven = await requestProviderIsolation();
      if (isolationProven) {
        providerStopStatus = 'isolated';
      }
    }
    if (!isolationProven && providerStopStatus === 'confirmed') {
      if (activeRun?.pauseKind === 'interaction') {
        activeRun.pauseKind = null;
        advanceRun(activeRun).catch(() => {});
      }
      if (!activeRun) {
        providerStopStatus = 'terminal_unconfirmed';
        isolationProven = await requestProviderIsolation();
        if (isolationProven) providerStopStatus = 'isolated';
      } else {
        let timeoutHandle;
        const settled = await Promise.race([
          activeRun.settlement.then((terminalObserved) => terminalObserved === true),
          new Promise((resolve) => {
            timeoutHandle = setTimeoutFn(() => resolve(false), providerStopTimeoutMs);
          }),
        ]);
        clearProviderStopTimeout(timeoutHandle);
        isolationProven = settled;
        if (!settled) {
          providerStopStatus = 'terminal_unconfirmed';
          isolationProven = await requestProviderIsolation();
          if (isolationProven) {
            activeRun.durableSettled = true;
            cleanupActiveRun(activeRun);
            providerStopStatus = 'isolated';
          }
        }
      }
    }
    if (isolationProven && providerStopStatus === 'isolated' && activeRun && !activeRun.settled) {
      activeRun.durableSettled = true;
      cleanupActiveRun(activeRun);
    }
    if (!isolationProven) {
      const persistedIncident = persist(() => store.markProviderStopUnknown(
        {
          turn_id: result.active_turn.turn_id,
          attempt: result.active_turn.attempt,
        },
        providerStopStatus,
        result,
      ));
      const { stop_result: stoppedResult, ...incident } = persistedIncident;
      if (activeRun?.recoveryCommitted && stoppedResult.lease_released === true) {
        releasedRecoveringOwnershipTurnIds.add(activeRun.turnContext.turn_id);
      }
      try { refresh(); } catch {}
      return {
        ...stoppedResult,
        incident,
      };
    }
    const stoppedResult = persist(() => store.recordStopProviderOutcome(
      result,
      providerStopStatus,
    ));
    if (activeRun?.recoveryCommitted && stoppedResult.lease_released === true) {
      releasedRecoveringOwnershipTurnIds.add(activeRun.turnContext.turn_id);
    }
    if (activeRun) releaseAbsentResident(activeRun.turnContext);
    try { refresh(); } catch {}
    return stoppedResult;
  }

  function stop(request) {
    const conversationId = request?.conversation_id;
    const stopId = request?.stop_id;
    if (
      typeof conversationId !== 'string'
      || conversationId.length === 0
      || typeof stopId !== 'string'
      || stopId.length === 0
    ) {
      return performStop(request);
    }
    const settlementKey = `${stopId}\u0000${conversationId}`;
    const existing = stopSettlements.get(settlementKey);
    if (existing) {
      return existing.then((settledResult) => ({
        ...settledResult,
        deduplicated: true,
      }));
    }
    const settlement = performStop(request);
    stopSettlements.set(settlementKey, settlement);
    settlement.finally(() => {
      if (stopSettlements.get(settlementKey) === settlement) {
        stopSettlements.delete(settlementKey);
      }
    }).catch(() => {});
    return settlement;
  }

  async function executeOperationsControl(request, trustedTransportContext) {
    if (operationsControl === null) {
      throw new Error('Operations control is unavailable without a deployment policy.');
    }
    const result = await operationsControl.execute(request, trustedTransportContext);
    if (request?.action === 'stop_active_turn' && result.status === 'completed') {
      await performStop({
        conversation_id: request.target.conversation_id,
        stop_id: request.control_id,
        target_turn_id: request.target.turn_id,
        expected_turn_version: request.expected_version.version,
        clear_unstarted_queue: false,
      });
    } else if (request?.action === 'reconcile' && result.status === 'accepted') {
      try {
        store.reconcileExpiredResidents();
        adoptOrphanedWorkspaceRecoveries();
        store.reconcileExpiredStartedInteractionHandoffs();
        store.reconcileNonterminalTurns(
          [...activeRuns.values()].map(({ turnContext }) => turnContext),
          'sweep_reconciliation',
        );
        observabilityPublisher.recordReconciliation();
        const terminal = operationsControl.completeReconciliation(request);
        try { refresh(); } catch {}
        return terminal;
      } catch (error) {
        return operationsControl.completeReconciliation(request, error);
      }
    } else if (request?.action === 'evict_idle_executor' && result.status === 'accepted') {
      if (typeof adapter.evictIdle !== 'function') {
        return operationsControl.completeEviction(request, {
          code: 'unsupported_capability',
          category: 'internal',
          retryable: false,
          side_effect_status: 'none',
          user_message: 'The provider adapter does not support executor eviction.',
        });
      }
      try {
        const evicted = await adapter.evictIdle({
          canEvict: (conversationId) => (
            conversationId === request.target.conversation_id
            && store.isConversationEvictable(conversationId)
          ),
          maxCount: 1,
          targetConversationId: request.target.conversation_id,
        });
        if (!evicted.includes(request.target.conversation_id)) {
          throw new Error('The provider adapter did not evict the canonical executor target.');
        }
        endedResidentFences.delete(request.target.conversation_id);
        return operationsControl.completeEviction(request);
      } catch (error) {
        return operationsControl.completeEviction(request, error);
      }
    }
    return result;
  }

  async function performSteer(request) {
    // The durable running -> redirecting CAS selects the steer winner before
    // any provider-private cancellation is requested.
    const result = persist(() => store.beginSteer(request));
    if (result.reconciliation_required === true) {
      if (result.reconciliation) return result;
      const activeRun = activeRuns.get(result.old_turn?.turn_id);
      let providerIsolated = false;
      if (activeRun && !activeRun.durableSettled) {
        activeRun.recoveryCommitted = true;
        pendingRecoveryIsolations.set(
          activeRun.turnContext.turn_id,
          activeRun.turnContext,
        );
        try {
          providerIsolated = await isolateInteractionRecovery(activeRun);
        } catch {
          providerIsolated = false;
        }
        if (activeRun.stopCommitted) {
          try { refresh(); } catch {}
          return persist(() => store.recordSteerReconciliationOutcome(
            result,
            providerIsolated,
          ));
        }
        if (
          activeRun.providerTerminalObserved
          || releasedRecoveringOwnershipTurnIds.has(activeRun.turnContext.turn_id)
        ) {
          providerIsolated = true;
        }
        if (providerIsolated) {
          activeRun.durableSettled = true;
          cleanupActiveRun(activeRun);
          if (!releasedRecoveringOwnershipTurnIds.has(activeRun.turnContext.turn_id)) {
            try {
              releaseRecoveringOwnership(activeRun.turnContext);
            } catch {
              if (lifecycle !== 'closed') lifecycle = 'close_failed';
            }
          }
        } else {
          lifecycle = 'close_failed';
        }
      }
      try { refresh(); } catch {}
      return persist(() => store.recordSteerReconciliationOutcome(
        result,
        providerIsolated,
      ));
    }
    if (result.status !== 'redirecting') return result;
    const activeRun = activeRuns.get(result.old_turn.turn_id);
    const turnContext = activeRun?.turnContext ?? {
      conversation_id: result.conversation_id,
      turn_id: result.old_turn.turn_id,
      attempt: result.old_turn.attempt,
    };
    if (activeRun) activeRun.steerCommitted = true;
    closingPermissionTurnIds.add(turnContext.turn_id);
    for (const controller of permissionControllers.get(turnContext.turn_id) ?? []) {
      controller.abort();
    }
    try { refresh(); } catch {}
    try { reschedulePendingInteractionDeadlines(); } catch {}

    function clearProviderStopTimeout(timeoutHandle) {
      if (timeoutHandle === undefined) return;
      try { clearTimeoutFn(timeoutHandle); } catch {}
    }
    async function requestProviderIsolation() {
      if (typeof adapter.abort !== 'function') return false;
      let timeoutHandle;
      const isolated = await Promise.race([
        Promise.resolve().then(() => adapter.abort(turnContext)).then(() => true, () => false),
        new Promise((resolve) => {
          timeoutHandle = setTimeoutFn(() => resolve(false), providerStopTimeoutMs);
        }),
      ]);
      clearProviderStopTimeout(timeoutHandle);
      return isolated;
    }

    let cancellationTimeoutHandle;
    let providerStopStatus = await Promise.race([
      Promise.resolve().then(async () => {
        if (typeof adapter.cancel !== 'function') {
          throw new TypeError('adapter.cancel must be a function to steer an active turn');
        }
        await adapter.cancel(turnContext, { reason: 'steer' });
        return 'confirmed';
      }).catch(() => 'uncertain'),
      new Promise((resolve) => {
        cancellationTimeoutHandle = setTimeoutFn(
          () => resolve('terminal_unconfirmed'),
          providerStopTimeoutMs,
        );
      }),
    ]);
    if (providerStopStatus !== 'terminal_unconfirmed') {
      clearProviderStopTimeout(cancellationTimeoutHandle);
    }

    let isolationProven = false;
    if (providerStopStatus === 'confirmed' && activeRun) {
      let settlementTimeoutHandle;
      isolationProven = await Promise.race([
        activeRun.settlement.then((terminalObserved) => terminalObserved === true),
        new Promise((resolve) => {
          settlementTimeoutHandle = setTimeoutFn(() => resolve(false), providerStopTimeoutMs);
        }),
      ]);
      clearProviderStopTimeout(settlementTimeoutHandle);
      if (!isolationProven) providerStopStatus = 'terminal_unconfirmed';
    }
    if (!isolationProven) {
      isolationProven = await requestProviderIsolation();
      if (isolationProven) providerStopStatus = 'isolated';
    }
    if (isolationProven && activeRun && !activeRun.settled) {
      activeRun.durableSettled = true;
      cleanupActiveRun(activeRun);
    }
    if (!isolationProven) {
      const failed = persist(() => store.failSteer(result, providerStopStatus));
      try { refresh(); } catch {}
      return failed;
    }

    const completed = persist(() => store.completeSteer(result, providerStopStatus));
    try { refresh(); } catch {}
    return completed;
  }

  function steer(request) {
    const conversationId = request?.conversation_id;
    const steerId = request?.steer_id;
    if (
      typeof conversationId !== 'string'
      || conversationId.length === 0
      || typeof steerId !== 'string'
      || steerId.length === 0
    ) {
      return performSteer(request);
    }
    const settlementKey = `${steerId}\u0000${conversationId}`;
    const requestIdentity = canonicalizeJson(request);
    const existing = steerSettlements.get(settlementKey);
    if (existing && existing.requestIdentity === requestIdentity) {
      return existing.settlement.then((settledResult) => ({
        ...settledResult,
        deduplicated: true,
      }));
    }
    const settlement = performSteer(request);
    if (existing) return settlement;
    const entry = { requestIdentity, settlement };
    steerSettlements.set(settlementKey, entry);
    settlement.finally(() => {
      if (steerSettlements.get(settlementKey) === entry) {
        steerSettlements.delete(settlementKey);
      }
    }).catch(() => {});
    return settlement;
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
    if (permissionPolicy.handlesInteraction(answer?.interaction_id)) {
      const result = permissionPolicy.submitConfirmation(answer);
      if (
        ['accepted', 'duplicate'].includes(result.status)
        || (result.interaction_state !== null && result.interaction_state !== 'pending')
      ) {
        clearInteractionDeadline(result.interaction_id);
      }
      reschedulePendingInteractionDeadlines();
      return result;
    }
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
    if (permissionPolicy.handlesInteraction(expiration?.interaction_id)) {
      const result = permissionPolicy.expireDue();
      clearInteractionDeadline(expiration.interaction_id);
      reschedulePendingInteractionDeadlines();
      return {
        status: result.confirmations_expired > 0 ? 'expired' : 'not_pending',
        interaction_id: expiration.interaction_id,
      };
    }
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
      store.markProviderStopUnknown({
        turn_id: result.turn_id,
        attempt: result.attempt,
      }, 'not_current');
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
        store.markProviderStopUnknown({
          turn_id: result.turn_id,
          attempt: result.attempt,
        }, 'uncertain');
        reschedulePendingInteractionDeadlines();
        return { ...result, lease_released: false, provider_stop_status: 'uncertain' };
      }
      if (interruption?.status !== 'provider_stopped') {
        store.markProviderStopUnknown({
          turn_id: result.turn_id,
          attempt: result.attempt,
        }, interruption?.status ?? 'uncertain');
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

  function recordLateInteractionAcknowledgement(activeRun, delivery, status) {
    persist(() => store.recordProviderEventDiagnostic(
      activeRun.turnContext,
      {
        kind: 'interaction_handoff_acknowledgement',
        payload: {
          handoff_id: delivery.handoff.handoff_id,
          status,
        },
      },
    ));
    return {
      status: 'stale_acknowledgement',
      turn_id: delivery.request.turn_id,
      turn_state: 'stopped',
    };
  }

  async function executeInteractionDelivery(handoffId) {
    const beginning = persist(() => store.beginInteractionHandoff(handoffId));
    if (beginning.acknowledgement !== null) {
      reschedulePendingInteractionDeadlines();
      refresh();
      return {
        acknowledgement: beginning.acknowledgement,
        execution: null,
        recovery_resolution: beginning.recovery_resolution ?? null,
      };
    }
    if (typeof adapter.prepareInteractionAnswer !== 'function') {
      throw new TypeError('adapter.prepareInteractionAnswer must be a function');
    }
    const { delivery } = beginning;
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
      if (activeRun && committedControlStatus(activeRun) !== null) {
        return recordLateInteractionAcknowledgement(
          activeRun,
          delivery,
          'handler_failed_after_stop',
        );
      }
      if (provider === 'codex' && isExplicitProviderError(error)) {
        const providerError = normalizeProviderError(error, now());
        if (providerError.side_effect_status === 'unknown') {
          const recovery = await recoverUnknownInteractionDelivery(
            activeRun,
            sendingDelivery,
            error,
            providerError,
          );
          if (recovery.deliveryUnknown) return recovery.deliveryUnknown;
        }
      }
      await recoverUnknownInteractionDelivery(activeRun, sendingDelivery, error);
      throw error;
    }
    if (activeRun && committedControlStatus(activeRun) !== null) {
      return recordLateInteractionAcknowledgement(
        activeRun,
        delivery,
        handlerAcknowledgement?.status ?? 'unknown',
      );
    }
    const managedPermissions = permissionControllers.get(delivery.request.turn_id)?.size ?? 0;
    let acknowledgement;
    try {
      acknowledgement = store.acknowledgeInteractionHandoff(handlerAcknowledgement, {
        holdForPermission: managedPermissions > 0
          || handlerAcknowledgement.blocking_interactions_remaining === true,
      });
    } catch (error) {
      await recoverUnknownInteractionDelivery(activeRun, sendingDelivery, error);
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
    store.assertInteractionHandoffRecoveryNoticeDelivered(delivery);
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
    store.assertInteractionHandoffRecoveryNoticeDelivered(delivery);
    const decision = Object.freeze({
      capability: 'interaction.handoff.resolve',
      scope: Object.freeze({
        conversation_id: delivery.request.conversation_id,
        turn_id: delivery.request.turn_id,
        handoff_id: delivery.handoff.handoff_id,
        action: disposition?.action,
        replacement_interaction: disposition?.replacement_interaction,
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
        if (workspaceRecoveryFlight) {
          try {
            await workspaceRecoveryFlight;
          } catch (error) {
            shutdownFailures.push(error);
          }
        }
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
        await Promise.allSettled([...replyMappingRecoverySettlements]);
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
                recovery.providerError,
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
          if (pendingRecoveryIsolations.has(activeRun.turnContext.turn_id)) continue;
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
          const activeRun = activeRuns.get(turnId);
          let isolationProven = closedConversationIdSet.has(turnContext.conversation_id);
          if (!isolationProven && activeRun) {
            try {
              isolationProven = await isolateInteractionRecovery(activeRun);
            } catch (error) {
              shutdownFailures.push(error);
            }
          }
          if (isolationProven) {
            if (activeRun && !activeRun.durableSettled) {
              activeRun.durableSettled = true;
              cleanupActiveRun(activeRun);
            }
            recordRecoveringIsolation(turnContext);
          } else {
            if (activeRun && !activeRun.settled) {
              skippedSettlements.add(activeRun.settlement);
            }
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
        if (lifecycle === 'closed' && workspaceHeartbeat !== null) {
          cancelWorkspaceHeartbeat(workspaceHeartbeat);
          workspaceHeartbeat = null;
        }
        if (permissionSweep !== null) {
          cancelPermissionSweep(permissionSweep);
          permissionSweep = null;
        }
        if (turnLeaseRenewal !== null) {
          cancelTurnLeaseRenewal(turnLeaseRenewal);
          turnLeaseRenewal = null;
        }
        if (nonterminalSweep !== null) {
          cancelNonterminalSweep(nonterminalSweep);
          nonterminalSweep = null;
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
    close,
    deliverInteractionAnswer,
    evictIdleExecutors,
    executeOperationsControl,
    expireInteraction,
    publishObservabilitySnapshot: observabilityPublisher.publish,
    reconcileWorkspaceRecoveries,
    reconcileInteractionHandoff,
    resolveInteractionHandoff,
    runNext,
    snapshot,
    start,
    steer,
    stop,
    submitInteractionAnswer,
  });
}
