import path from 'node:path';

const TARGET_CODEX_VERSION = '0.144.5';
const TARGET_PLATFORM = 'linux/arm64';
const CONTAINERFILE_NAME = 'Containerfile.codex-app-server-acceptance';
const CONTAINER_CODEX_HOME = '/acceptance/codex-home';
const CONTAINER_EVIDENCE_FILE = `${CONTAINER_CODEX_HOME}/global43/real-evidence.json`;

function requireResourceName(value, label) {
  if (typeof value !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._:/-]*$/.test(value)) {
    throw new TypeError(`${label} must be a non-empty container resource name.`);
  }
  return value;
}

function requireCandidateSha(candidateSha) {
  if (typeof candidateSha !== 'string' || !/^[0-9a-f]{40}$/.test(candidateSha)) {
    throw new TypeError('candidateSha must be a lowercase 40-character Git SHA.');
  }
  return candidateSha;
}

function redactValue(value) {
  if (Array.isArray(value)) return value.map(redactValue);
  if (value === null || typeof value !== 'object') return value;
  return Object.fromEntries(Object.entries(value).map(([key, nested]) => [
    key,
    /credential|token|api[_-]?key|secret|authorization/i.test(key)
      ? '[REDACTED]'
      : redactValue(nested),
  ]));
}

export function redactCodexAppServerContainerEvidence(evidence) {
  return redactValue(structuredClone(evidence));
}

export function buildCodexAppServerContainerAcceptancePlan({
  candidateSha,
  worktreePath,
  imageTag,
  volumeName,
}) {
  const revision = requireCandidateSha(candidateSha);
  if (typeof worktreePath !== 'string' || !path.isAbsolute(worktreePath)) {
    throw new TypeError('worktreePath must be absolute.');
  }
  const image = requireResourceName(imageTag, 'imageTag');
  const stateVolume = requireResourceName(volumeName, 'volumeName');
  const containerfile = path.join(worktreePath, CONTAINERFILE_NAME);
  const stateMount = `type=volume,source=${stateVolume},target=${CONTAINER_CODEX_HOME}`;
  const interactionCommand = (scenario) => Object.freeze([
    'container', 'run',
    '--remove',
    '--interactive',
    '--init',
    '--name', `zylos-global43-codex-${scenario.replaceAll('_', '-')}`,
    '--platform', TARGET_PLATFORM,
    '--mount', stateMount,
    '--env', `CODEX_HOME=${CONTAINER_CODEX_HOME}`,
    '--env', 'HOME=/acceptance/home',
    '--env', `ZYLOS_CODEX_INTERACTION_SCENARIO=${scenario}`,
    '--workdir', '/opt/zylos',
    image,
    'node', 'scripts/e2e/codex-app-server-interaction-probe.js',
  ]);

  return Object.freeze({
    evidence_schema_version: 1,
    candidate_sha: revision,
    target: Object.freeze({
      package: '@openai/codex',
      cli_version: TARGET_CODEX_VERSION,
      platform: TARGET_PLATFORM,
      transport: 'stdio',
      app_server_argv: Object.freeze(['codex', 'app-server', '--stdio']),
    }),
    isolation: Object.freeze({
      codex_home: CONTAINER_CODEX_HOME,
      state_volume: stateVolume,
      host_codex_home_reused: false,
      source_context: worktreePath,
    }),
    commands: Object.freeze({
      build: Object.freeze([
        'container', 'build',
        '--platform', TARGET_PLATFORM,
        '--build-arg', `CODEX_VERSION=${TARGET_CODEX_VERSION}`,
        '--build-arg', `CANDIDATE_SHA=${revision}`,
        '--tag', image,
        '--file', containerfile,
        worktreePath,
      ]),
      acceptance: Object.freeze([
        'container', 'run',
        '--remove',
        '--interactive',
        '--init',
        '--name', 'zylos-global43-codex-acceptance',
        '--platform', TARGET_PLATFORM,
        '--mount', stateMount,
        '--env', `CODEX_HOME=${CONTAINER_CODEX_HOME}`,
        '--env', 'HOME=/acceptance/home',
        '--env', `ZYLOS_CODEX_APP_SERVER_EVIDENCE_FILE=${CONTAINER_EVIDENCE_FILE}`,
        '--workdir', '/opt/zylos',
        image,
        'node', 'scripts/e2e/codex-app-server-real-integration.js',
      ]),
      restart_resume: Object.freeze([
        'container', 'run',
        '--remove',
        '--interactive',
        '--init',
        '--name', 'zylos-global43-codex-restart-resume',
        '--platform', TARGET_PLATFORM,
        '--mount', stateMount,
        '--env', `CODEX_HOME=${CONTAINER_CODEX_HOME}`,
        '--env', 'HOME=/acceptance/home',
        '--env', `ZYLOS_CODEX_APP_SERVER_EVIDENCE_FILE=${CONTAINER_EVIDENCE_FILE}`,
        '--workdir', '/opt/zylos',
        image,
        'node', 'scripts/e2e/codex-app-server-restart-resume.js',
      ]),
      interactions: Object.freeze({
        command_approval: interactionCommand('command_approval'),
        file_approval: interactionCommand('file_approval'),
        request_user_input: interactionCommand('request_user_input'),
      }),
    }),
  });
}

export const CODEX_APP_SERVER_CONTAINER_TARGET = Object.freeze({
  cliVersion: TARGET_CODEX_VERSION,
  platform: TARGET_PLATFORM,
  containerfileName: CONTAINERFILE_NAME,
  codexHome: CONTAINER_CODEX_HOME,
});
