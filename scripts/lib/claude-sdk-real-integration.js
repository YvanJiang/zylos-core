export const CLAUDE_SDK_TARGET = Object.freeze({
  package: '@anthropic-ai/claude-agent-sdk',
  sdk_version: '0.3.215',
  bundled_cli_version: '2.1.215',
  provider_transport: 'claude_agent_sdk',
});

function evidence(file, testName) {
  return Object.freeze({ file, test_name: testName });
}

function acceptanceCase(caseId, lane, references) {
  return Object.freeze({
    case_id: caseId,
    lane,
    evidence: Object.freeze(references),
  });
}

const LIVE_RUNNER = 'scripts/integration/claude-agent-sdk/live-runner.js';

export const CLAUDE_SDK_ACCEPTANCE_MATRIX = Object.freeze([
  acceptanceCase('long_lived_async_multi_turn', 'real_provider', [
    evidence(LIVE_RUNNER, "'long_lived_async_multi_turn'"),
    evidence(
      'test/runtime-claude-conversation-executor.test.js',
      'keeps one SDK query across turns and binds its first session ID before output',
    ),
  ]),
  acceptanceCase('durable_session_id', 'real_provider', [
    evidence(LIVE_RUNNER, "'durable_session_id'"),
    evidence(
      'test/runtime-claude-conversation-executor.test.js',
      'keeps one SDK query across turns and binds its first session ID before output',
    ),
  ]),
  acceptanceCase('cancel', 'real_provider', [
    evidence(LIVE_RUNNER, "'cancel'"),
    evidence(
      'test/runtime-claude-conversation-executor.test.js',
      'stops the current fenced turn and cancels queued work through the cutoff',
    ),
  ]),
  acceptanceCase('permission_interaction', 'real_provider', [
    evidence(LIVE_RUNNER, "'permission_interaction'"),
    evidence(
      'test/runtime-claude-conversation-executor.test.js',
      'routes permission callbacks through the durable provider-neutral turn fence',
    ),
  ]),
  acceptanceCase('idle_eviction_rebuild', 'real_provider', [
    evidence(LIVE_RUNNER, "'idle_eviction_rebuild'"),
    evidence(
      'test/runtime-claude-conversation-executor.test.js',
      'evicts only truly idle executors and resumes the durable session afterward',
    ),
  ]),
  acceptanceCase('service_restart_resume', 'real_provider', [
    evidence(LIVE_RUNNER, "'service_restart_resume'"),
    evidence(
      'test/runtime-claude-conversation-executor.test.js',
      'rebuilds a resident query from durable lineage after service restart',
    ),
  ]),
  acceptanceCase('provider_transient', 'deterministic', [
    evidence(
      'test/runtime-executor-service.test.js',
      'safely retries one turn at most three times with new attempts and lease epochs',
    ),
  ]),
  acceptanceCase('provider_auth', 'deterministic', [
    evidence(
      'test/runtime-executor-service.test.js',
      'never retries %s/%s provider failures',
    ),
  ]),
  acceptanceCase('provider_context', 'deterministic', [
    evidence(
      'test/runtime-executor-service.test.js',
      'never retries %s/%s provider failures',
    ),
  ]),
  acceptanceCase('stale_attempt', 'deterministic', [
    evidence(
      'test/runtime-executor-service.test.js',
      'records callbacks from a superseded provider attempt as diagnostics only',
    ),
  ]),
  acceptanceCase('mapping_recovery', 'deterministic', [
    evidence(
      'test/runtime-reply-mapping-recovery.test.js',
      'claims a pending lineage only to publish its notice and never calls the provider before delivery',
    ),
    evidence(
      'test/runtime-reply-mapping-recovery.test.js',
      'attempts one unique native candidate only after notice delivery and binds before provider execution',
    ),
  ]),
  acceptanceCase('answer_send_before_ack', 'deterministic', [
    evidence(
      'test/runtime-interaction-happy-path.test.js',
      'claims a handoff without send evidence and fences the later send start',
    ),
    evidence(
      'test/runtime-claude-conversation-executor.test.js',
      'persists delivery_unknown when provider ack cannot be durably committed',
    ),
  ]),
  acceptanceCase('side_effect_unknown_notify_first', 'deterministic', [
    evidence(
      'test/runtime-executor-service.test.js',
      'normalizes uncertain provider failure into fenced recovery and a durable decision',
    ),
  ]),
  acceptanceCase('no_blind_replay', 'deterministic', [
    evidence(
      'test/runtime-interaction-happy-path.test.js',
      'keeps delivery_unknown without resending when the provider query cannot prove acceptance',
    ),
    evidence(
      'test/runtime-reply-mapping-recovery.test.js',
      'does not guess without a unique candidate and creates an explicit recovery lineage',
    ),
  ]),
]);

const CREDENTIAL_NAMES = Object.freeze([
  'ANTHROPIC_API_KEY',
  'ANTHROPIC_AUTH_TOKEN',
  'CLAUDE_CODE_OAUTH_TOKEN',
]);

export function detectClaudeSdkPrerequisites({
  environment,
  nativeAuth,
  networkAvailable,
}) {
  const credentialNames = CREDENTIAL_NAMES.filter(
    (name) => typeof environment?.[name] === 'string' && environment[name].length > 0,
  );
  const nativeLoggedIn = nativeAuth?.loggedIn === true;
  const credentialSource = credentialNames.length > 0
    ? 'environment'
    : (nativeLoggedIn ? 'native_login' : null);
  const missing = [];
  if (credentialSource === null) missing.push('credential');
  if (networkAvailable !== true) missing.push('network');
  return Object.freeze({
    available: missing.length === 0,
    credential_source: credentialSource,
    credential_names: Object.freeze(credentialNames),
    native_auth_method: nativeLoggedIn && typeof nativeAuth.authMethod === 'string'
      ? nativeAuth.authMethod
      : null,
    native_auth_original_logged_in: nativeAuth?.originalLoggedIn === true,
    native_auth_isolated_logged_in: nativeLoggedIn,
    network_available: networkAvailable === true,
    missing: Object.freeze(missing),
  });
}

export function validateClaudeSdkTarget({
  sdkVersion,
  bundledCliVersion,
  querySurface,
  capabilities,
}) {
  const requiredSurface = ['async_iterator', 'interrupt', 'cancel_async_message', 'close'];
  const failures = [];
  if (sdkVersion !== CLAUDE_SDK_TARGET.sdk_version) {
    failures.push(`SDK ${sdkVersion ?? 'unavailable'}`);
  }
  if (bundledCliVersion !== CLAUDE_SDK_TARGET.bundled_cli_version) {
    failures.push(`bundled CLI ${bundledCliVersion ?? 'unavailable'}`);
  }
  for (const method of requiredSurface) {
    if (querySurface?.[method] !== true) failures.push(`query surface ${method}`);
  }
  if (!capabilities?.includes('interrupt_receipt_v1')) {
    failures.push('capability interrupt_receipt_v1');
  }
  if (failures.length > 0) {
    throw new Error(`Claude Agent SDK target validation failed: ${failures.join(', ')}`);
  }
  return Object.freeze({
    status: 'passed',
    ...CLAUDE_SDK_TARGET,
    capabilities: Object.freeze([...capabilities]),
    query_surface: Object.freeze({ ...querySurface }),
  });
}

export function buildClaudeSdkEvidence({ deterministic, nativeTarget, realProvider }) {
  const requiredRealCases = CLAUDE_SDK_ACCEPTANCE_MATRIX
    .filter(({ lane }) => lane === 'real_provider')
    .map(({ case_id: caseId }) => caseId);
  const passedRealCases = new Set(
    (realProvider?.cases ?? [])
      .filter(({ status }) => status === 'passed')
      .map(({ case_id: caseId }) => caseId),
  );
  const unrunCases = requiredRealCases.filter((caseId) => !passedRealCases.has(caseId));
  const deterministicPassed = deterministic?.status === 'passed';
  const nativePassed = nativeTarget?.status === 'passed';
  const realPassed = realProvider?.status === 'passed' && unrunCases.length === 0;
  const hardFailure = [deterministic, nativeTarget, realProvider]
    .some(({ status } = {}) => status === 'failed');
  const releaseReady = deterministicPassed && nativePassed && realPassed;
  return Object.freeze({
    schema_version: 1,
    status: releaseReady ? 'passed' : (hardFailure ? 'failed' : 'partial'),
    release_ready: releaseReady,
    target: CLAUDE_SDK_TARGET,
    acceptance_matrix: CLAUDE_SDK_ACCEPTANCE_MATRIX,
    lanes: Object.freeze({
      deterministic,
      native_target: nativeTarget,
      real_provider: realProvider,
    }),
    unrun_cases: Object.freeze(unrunCases),
  });
}
