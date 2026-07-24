import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

import { initializeRuntimePersistence } from '../persistence/schema.js';

export const CONVERSATION_WORKSPACE_STATES = Object.freeze([
  'requested',
  'provisioning',
  'ready',
  'quarantined',
  'failed',
  'retired',
]);

const WAIT_REASON_BY_STATE = Object.freeze({
  requested: 'workspace_provisioning',
  provisioning: 'workspace_provisioning',
  ready: null,
  quarantined: 'workspace_quarantined',
  failed: 'workspace_failed',
  retired: 'workspace_retired',
});

const WORKSPACE_MARKER = '.zylos-conversation-workspace.json';
const DEFAULT_PROVISIONING_LEASE_MS = 60_000;

const EXCLUDED_DIRECTORY_NAMES = new Set([
  '.aws',
  '.cache',
  '.claude',
  '.codex',
  '.config',
  '.docker',
  '.git',
  '.gnupg',
  '.hg',
  '.kube',
  '.local',
  '.npm',
  '.pm2',
  '.pnpm-store',
  '.ssh',
  '.svn',
  '.terraform',
  '.tmp',
  '.venv',
  '.yarn',
  '.zylos',
  '__pycache__',
  'credentials',
  'logs',
  'node_modules',
  'pm2',
  'secrets',
  'tmp',
  'venv',
]);

const EXCLUDED_FILE_NAMES = new Set([
  '.DS_Store',
  '.git-credentials',
  '.gitconfig',
  '.netrc',
  '.npmrc',
  'application_default_credentials.json',
  'auth.json',
  'credentials',
  'credentials.json',
  'dump.pm2',
  'id_ed25519',
  'id_rsa',
  'secret',
  'secrets',
  'service-account.json',
  'token.json',
  'tokens.json',
]);

export class ConversationWorkspaceError extends Error {
  constructor(code, message, { terminal = false, cause } = {}) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = 'ConversationWorkspaceError';
    this.code = code;
    this.terminal = terminal;
  }
}

function requireDatabase(database) {
  if (!database || typeof database.prepare !== 'function') {
    throw new TypeError('Conversation workspaces require a Core SQLite database.');
  }
}

function requireText(name, value) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string.`);
  }
  return value;
}

function requirePositiveSafeInteger(name, value) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer.`);
  }
  return value;
}

function requireTimestamp(name, value) {
  requireText(name, value);
  if (!Number.isFinite(Date.parse(value))) {
    throw new TypeError(`${name} must be a valid timestamp.`);
  }
  return value;
}

function defaultGenerateId(kind) {
  return `${kind}-${crypto.randomUUID()}`;
}

function lstatDirectory(name, directory) {
  requireText(name, directory);
  if (!path.isAbsolute(directory)) {
    throw new TypeError(`${name} must be an absolute directory.`);
  }
  const normalized = path.resolve(directory);
  if (normalized === path.parse(normalized).root) {
    throw new TypeError(`${name} must not be the filesystem root.`);
  }
  let stat;
  try {
    stat = fs.lstatSync(normalized);
  } catch (error) {
    throw new TypeError(`${name} must be an existing directory.`, { cause: error });
  }
  if (stat.isSymbolicLink() || !stat.isDirectory()) {
    throw new TypeError(`${name} must be a non-symlink directory.`);
  }
  return fs.realpathSync.native(normalized);
}

function containsPath(root, candidate) {
  const relative = path.relative(root, candidate);
  return relative !== '' && relative !== '..' && !relative.startsWith(`..${path.sep}`)
    && !path.isAbsolute(relative);
}

function assertContainedPath(root, candidate, name) {
  if (!path.isAbsolute(candidate) || !containsPath(root, path.resolve(candidate))) {
    throw new ConversationWorkspaceError(
      'workspace_path_escape',
      `${name} escaped the controlled conversation workspace store.`,
      { terminal: true },
    );
  }
}

function assertControlledDirectoryIdentity(directory, name) {
  let stat;
  try {
    stat = fs.lstatSync(directory);
  } catch (error) {
    throw new ConversationWorkspaceError(
      'workspace_control_root_missing',
      `${name} is no longer available.`,
      { terminal: true, cause: error },
    );
  }
  if (
    stat.isSymbolicLink()
    || !stat.isDirectory()
    || fs.realpathSync.native(directory) !== directory
  ) {
    throw new ConversationWorkspaceError(
      'workspace_control_root_changed',
      `${name} changed identity.`,
      { terminal: true },
    );
  }
}

function rootsOverlap(first, second) {
  return first === second || containsPath(first, second) || containsPath(second, first);
}

function serializeError(error, occurredAt, fallbackCode = 'workspace_provisioning_failed') {
  return JSON.stringify({
    code: typeof error?.code === 'string' ? error.code : fallbackCode,
    message: typeof error?.message === 'string'
      ? error.message
      : 'Conversation workspace provisioning failed.',
    terminal: error?.terminal === true,
    occurred_at: occurredAt,
  });
}

function loadBindingRow(database, conversationId) {
  return database.prepare(`
    SELECT *
    FROM runtime_conversation_workspaces
    WHERE conversation_id = ?
  `).get(conversationId);
}

function projectLineageBinding(row) {
  if (!row) return null;
  return Object.freeze({
    lineage_id: row.lineage_id,
    workspace_id: row.workspace_id,
    conversation_id: row.conversation_id,
    workspace_root: row.workspace_root,
    generation: row.generation,
    bound_at: row.bound_at,
  });
}

function runImmediate(database, work) {
  const transaction = database.transaction(work);
  return database.inTransaction ? transaction() : transaction.immediate();
}

function projectBinding(row) {
  if (!row) return null;
  const waitReason = WAIT_REASON_BY_STATE[row.state];
  if (waitReason === undefined) {
    throw new Error(`Unsupported conversation workspace state ${row.state}.`);
  }
  return Object.freeze({
    workspace_id: row.workspace_id,
    conversation_id: row.conversation_id,
    workspace_root: row.workspace_root,
    base_snapshot_root: row.base_snapshot_root,
    base_snapshot_ref: row.base_snapshot_ref,
    generation: row.generation,
    state: row.state,
    wait_reason: waitReason,
    claimable: row.state === 'ready',
    requested_at: row.requested_at,
    provisioning_started_at: row.provisioning_started_at,
    ready_at: row.ready_at,
    quarantined_at: row.quarantined_at,
    failed_at: row.failed_at,
    retired_at: row.retired_at,
    updated_at: row.updated_at,
    error: row.last_error_json === null ? null : JSON.parse(row.last_error_json),
    retention_reason: row.retention_reason,
    retain_until: row.retain_until,
  });
}

function workspacePaths(workspaceStoreRoot, row) {
  const suffix = crypto.createHash('sha256')
    .update(row.workspace_id)
    .update('\0')
    .update(row.conversation_id)
    .update('\0')
    .update(String(row.generation))
    .digest('hex')
    .slice(0, 32);
  const name = `workspace-${suffix}-g${row.generation}`;
  const workspaceRoot = path.join(workspaceStoreRoot, name);
  const stagingRoot = path.join(workspaceStoreRoot, `.${name}.provisioning`);
  assertContainedPath(workspaceStoreRoot, workspaceRoot, 'workspace root');
  assertContainedPath(workspaceStoreRoot, stagingRoot, 'workspace staging root');
  return { workspaceRoot, stagingRoot };
}

function shouldExclude(relativePath, directory) {
  const name = path.basename(relativePath);
  const lower = name.toLowerCase();
  if (directory && EXCLUDED_DIRECTORY_NAMES.has(lower)) return true;
  if (!directory && EXCLUDED_FILE_NAMES.has(lower)) return true;
  if (!directory && (lower === '.env' || lower.startsWith('.env.'))) return true;
  return !directory && (
    lower.endsWith('.db')
    || lower.endsWith('.sqlite')
    || lower.endsWith('.sqlite3')
    || lower.endsWith('-journal')
    || lower.endsWith('-shm')
    || lower.endsWith('-wal')
    || lower.endsWith('.key')
    || lower.endsWith('.p12')
    || lower.endsWith('.pfx')
    || lower.endsWith('.pem')
    || lower.endsWith('.sock')
  );
}

function normalizeSnapshotFiles(snapshotFiles) {
  if (!Array.isArray(snapshotFiles)) {
    throw new TypeError('snapshotFiles must be an array of relative file paths.');
  }
  const normalized = [];
  const seen = new Set();
  for (const entry of snapshotFiles) {
    requireText('snapshotFiles entry', entry);
    if (
      entry.includes('\0')
      || path.isAbsolute(entry)
      || path.normalize(entry) !== entry
      || entry === '.'
      || entry === '..'
      || entry.startsWith(`..${path.sep}`)
    ) {
      throw new TypeError(`snapshotFiles entry must be a canonical relative path: ${entry}.`);
    }
    const parts = entry.split(path.sep);
    if (
      shouldExclude(entry, false)
      || parts.slice(0, -1).some((_, index) => (
        shouldExclude(parts.slice(0, index + 1).join(path.sep), true)
      ))
    ) {
      throw new TypeError(`snapshotFiles entry is excluded from task workspaces: ${entry}.`);
    }
    if (seen.has(entry)) {
      throw new TypeError(`snapshotFiles contains a duplicate path: ${entry}.`);
    }
    seen.add(entry);
    normalized.push(entry);
  }
  return Object.freeze(normalized.sort());
}

function parseDurableSnapshotFiles(serialized) {
  let files;
  try {
    files = JSON.parse(serialized);
  } catch (error) {
    throw new ConversationWorkspaceError(
      'base_snapshot_manifest_invalid',
      'Durable base snapshot manifest is invalid.',
      { terminal: true, cause: error },
    );
  }
  try {
    return normalizeSnapshotFiles(files);
  } catch (error) {
    throw new ConversationWorkspaceError(
      'base_snapshot_manifest_invalid',
      error.message,
      { terminal: true, cause: error },
    );
  }
}

function copyRegularFile(source, destination, mode) {
  const sourceFlags = fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0);
  let sourceDescriptor;
  let destinationDescriptor;
  try {
    sourceDescriptor = fs.openSync(source, sourceFlags);
    if (!fs.fstatSync(sourceDescriptor).isFile()) {
      throw new ConversationWorkspaceError(
        'base_snapshot_special_file_rejected',
        `Base snapshot entry is no longer a regular file: ${source}.`,
        { terminal: true },
      );
    }
    destinationDescriptor = fs.openSync(
      destination,
      fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL,
      mode,
    );
    const buffer = Buffer.allocUnsafe(64 * 1024);
    let bytesRead;
    do {
      bytesRead = fs.readSync(sourceDescriptor, buffer, 0, buffer.length, null);
      let written = 0;
      while (written < bytesRead) {
        written += fs.writeSync(
          destinationDescriptor,
          buffer,
          written,
          bytesRead - written,
          null,
        );
      }
    } while (bytesRead > 0);
    fs.fsyncSync(destinationDescriptor);
  } finally {
    if (destinationDescriptor !== undefined) fs.closeSync(destinationDescriptor);
    if (sourceDescriptor !== undefined) fs.closeSync(sourceDescriptor);
  }
  fs.chmodSync(destination, mode);
}

function copySnapshotFiles(
  sourceRoot,
  destinationRoot,
  snapshotFiles,
  heartbeat = () => {},
) {
  for (const relativePath of parseDurableSnapshotFiles(
    JSON.stringify(snapshotFiles),
  )) {
    heartbeat();
    const source = path.join(sourceRoot, relativePath);
    let resolvedSource;
    try {
      resolvedSource = fs.realpathSync.native(source);
    } catch (error) {
      throw new ConversationWorkspaceError(
        'base_snapshot_entry_missing',
        `Base snapshot manifest entry is unavailable: ${relativePath}.`,
        { terminal: true, cause: error },
      );
    }
    if (resolvedSource !== source) {
      const escaped = !containsPath(sourceRoot, resolvedSource);
      throw new ConversationWorkspaceError(
        escaped ? 'base_snapshot_path_escape' : 'base_snapshot_symlink_rejected',
        escaped
          ? `Base snapshot entry escaped its root: ${relativePath}.`
          : `Base snapshot symlink is not allowed: ${relativePath}.`,
        { terminal: true },
      );
    }
    const stat = fs.lstatSync(source);
    if (stat.isSymbolicLink()) {
      throw new ConversationWorkspaceError(
        'base_snapshot_symlink_rejected',
        `Base snapshot symlink is not allowed: ${relativePath}.`,
        { terminal: true },
      );
    }
    if (!stat.isFile()) {
      throw new ConversationWorkspaceError(
        'base_snapshot_special_file_rejected',
        `Base snapshot manifest entry is not a regular file: ${relativePath}.`,
        { terminal: true },
      );
    }
    const destination = path.join(destinationRoot, relativePath);
    assertContainedPath(destinationRoot, destination, 'snapshot destination');
    const destinationParent = path.dirname(destination);
    fs.mkdirSync(destinationParent, { recursive: true, mode: 0o700 });
    let parent = destinationParent;
    while (parent !== destinationRoot) {
      fs.chmodSync(parent, 0o700);
      parent = path.dirname(parent);
    }
    copyRegularFile(source, destination, (stat.mode & 0o111) === 0 ? 0o600 : 0o700);
    heartbeat();
  }
}

function hardenPopulatedTree(root, relativePath = '') {
  const candidate = relativePath === '' ? root : path.join(root, relativePath);
  if (relativePath !== '') assertContainedPath(root, candidate, 'populated workspace entry');
  const stat = fs.lstatSync(candidate);
  if (stat.isSymbolicLink()) {
    throw new ConversationWorkspaceError(
      'workspace_materializer_symlink_rejected',
      `Workspace materializer created a symlink: ${relativePath || '.'}.`,
      { terminal: true },
    );
  }
  if (relativePath !== '' && shouldExclude(relativePath, stat.isDirectory())) {
    throw new ConversationWorkspaceError(
      'workspace_sensitive_material_rejected',
      `Workspace materializer produced excluded state: ${relativePath}.`,
      { terminal: true },
    );
  }
  if (stat.isDirectory()) {
    for (const name of fs.readdirSync(candidate).sort()) {
      hardenPopulatedTree(root, relativePath === '' ? name : path.join(relativePath, name));
    }
    fs.chmodSync(candidate, 0o700);
    return;
  }
  if (!stat.isFile()) {
    throw new ConversationWorkspaceError(
      'workspace_materializer_special_file_rejected',
      `Workspace materializer created a special file: ${relativePath}.`,
      { terminal: true },
    );
  }
  fs.chmodSync(candidate, (stat.mode & 0o111) === 0 ? 0o600 : 0o700);
}

function markerDocument(row) {
  return {
    contract: 'zylos.conversation-workspace',
    contract_version: '1.0',
    workspace_id: row.workspace_id,
    conversation_id: row.conversation_id,
    generation: row.generation,
    base_snapshot_ref: row.base_snapshot_ref,
  };
}

function writeMarker(stagingRoot, row) {
  fs.writeFileSync(
    path.join(stagingRoot, WORKSPACE_MARKER),
    `${JSON.stringify(markerDocument(row))}\n`,
    { encoding: 'utf8', mode: 0o600, flag: 'wx' },
  );
}

function validateActivatedWorkspace(workspaceStoreRoot, row) {
  assertContainedPath(workspaceStoreRoot, row.workspace_root, 'durable workspace root');
  let stat;
  try {
    stat = fs.lstatSync(row.workspace_root);
  } catch (error) {
    throw new ConversationWorkspaceError(
      'workspace_root_missing',
      'The durable conversation workspace root is missing.',
      { terminal: true, cause: error },
    );
  }
  if (stat.isSymbolicLink() || !stat.isDirectory()) {
    throw new ConversationWorkspaceError(
      'workspace_root_invalid',
      'The durable conversation workspace root is not a private directory.',
      { terminal: true },
    );
  }
  const realRoot = fs.realpathSync.native(row.workspace_root);
  if (realRoot !== row.workspace_root) {
    throw new ConversationWorkspaceError(
      'workspace_root_alias_rejected',
      'The durable conversation workspace root changed identity.',
      { terminal: true },
    );
  }
  const markerPath = path.join(realRoot, WORKSPACE_MARKER);
  let marker;
  try {
    const markerStat = fs.lstatSync(markerPath);
    if (!markerStat.isFile() || markerStat.isSymbolicLink()) {
      throw new Error('workspace marker is not a regular file');
    }
    marker = JSON.parse(fs.readFileSync(markerPath, 'utf8'));
  } catch (error) {
    throw new ConversationWorkspaceError(
      'workspace_marker_invalid',
      'The activated conversation workspace lacks valid Core ownership evidence.',
      { terminal: true, cause: error },
    );
  }
  if (JSON.stringify(marker) !== JSON.stringify(markerDocument(row))) {
    throw new ConversationWorkspaceError(
      'workspace_marker_mismatch',
      'The activated conversation workspace ownership evidence does not match SQLite.',
      { terminal: true },
    );
  }
  fs.chmodSync(realRoot, 0o700);
}

function loadUnsafeRuntimeEvidence(database, conversationId) {
  return database.prepare(`
    SELECT reason FROM (
      SELECT 'background_side_effect_unknown' AS reason
      FROM runtime_background_tasks
      WHERE execution_conversation_id = ? AND side_effect_status = 'unknown'
      UNION ALL
      SELECT 'background_recovering' AS reason
      FROM runtime_background_tasks
      WHERE execution_conversation_id = ? AND state = 'recovering'
      UNION ALL
      SELECT 'turn_recovering' AS reason
      FROM runtime_turns
      WHERE conversation_id = ? AND state = 'recovering'
      UNION ALL
      SELECT 'workspace_lease_uncertain' AS reason
      FROM runtime_workspace_leases
      WHERE holder_conversation_id = ? AND state = 'uncertain'
      UNION ALL
      SELECT 'workspace_background_unknown' AS reason
      FROM runtime_workspace_background_work AS background
      JOIN runtime_workspace_leases AS lease
        ON lease.workspace_lease_id = background.workspace_lease_id
      WHERE lease.holder_conversation_id = ? AND background.state = 'unknown'
    )
    LIMIT 1
  `).get(
    conversationId,
    conversationId,
    conversationId,
    conversationId,
    conversationId,
  ) ?? null;
}

export function requestConversationWorkspaceInTransaction(database, {
  workspaceId,
  conversationId,
  requestedAt,
}) {
  requireDatabase(database);
  requireText('workspaceId', workspaceId);
  requireText('conversationId', conversationId);
  requireText('requestedAt', requestedAt);
  if (!database.inTransaction) {
    throw new Error('Conversation workspace requests must join the inbound SQLite transaction.');
  }
  const detached = database.prepare(`
    SELECT 1
    FROM runtime_background_tasks
    WHERE execution_conversation_id = ?
  `).get(conversationId);
  if (!detached) {
    throw new ConversationWorkspaceError(
      'workspace_binding_not_detached',
      'Only a detached execution conversation can request an independent workspace.',
      { terminal: true },
    );
  }
  database.prepare(`
    INSERT INTO runtime_conversation_workspaces (
      workspace_id, conversation_id, workspace_root, staging_root,
      base_snapshot_root, base_snapshot_ref, generation, state,
      provisioning_owner, provisioning_expires_at, requested_at,
      provisioning_started_at, ready_at, quarantined_at, failed_at,
      retired_at, updated_at, last_error_json, retention_reason, retain_until
    ) VALUES (
      ?, ?, NULL, NULL, NULL, NULL, 1, 'requested',
      NULL, NULL, ?, NULL, NULL, NULL, NULL, NULL, ?, NULL, NULL, NULL
    )
  `).run(workspaceId, conversationId, requestedAt, requestedAt);
  return getConversationWorkspaceBinding(database, conversationId);
}

export function getConversationWorkspaceBinding(database, conversationId) {
  requireDatabase(database);
  requireText('conversationId', conversationId);
  return projectBinding(database.prepare(`
    SELECT *
    FROM runtime_conversation_workspaces
    WHERE conversation_id = ?
  `).get(conversationId));
}

export function bindLineageWorkspaceInTransaction(database, {
  lineageId,
  conversationId,
  boundAt,
}) {
  requireDatabase(database);
  requireText('lineageId', lineageId);
  requireText('conversationId', conversationId);
  requireTimestamp('boundAt', boundAt);
  if (!database.inTransaction) {
    throw new Error('Lineage workspace binding must join the executor claim transaction.');
  }
  const workspace = database.prepare(`
    SELECT workspace_id, conversation_id, workspace_root, generation
    FROM runtime_conversation_workspaces
    WHERE conversation_id = ? AND state = 'ready'
  `).get(conversationId);
  if (!workspace) {
    throw new ConversationWorkspaceError(
      'workspace_not_ready',
      'A lineage can bind only after its conversation workspace is ready.',
      { terminal: true },
    );
  }
  const lineage = database.prepare(`
    SELECT conversation_id
    FROM runtime_lineages
    WHERE lineage_id = ?
  `).get(lineageId);
  if (!lineage || lineage.conversation_id !== conversationId) {
    throw new ConversationWorkspaceError(
      'lineage_workspace_conversation_mismatch',
      'Lineage and conversation workspace identities do not match.',
      { terminal: true },
    );
  }
  database.prepare(`
    INSERT INTO runtime_lineage_workspace_bindings (
      lineage_id, workspace_id, conversation_id, workspace_root, generation, bound_at
    ) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(lineage_id) DO NOTHING
  `).run(
    lineageId,
    workspace.workspace_id,
    conversationId,
    workspace.workspace_root,
    workspace.generation,
    boundAt,
  );
  const binding = database.prepare(`
    SELECT *
    FROM runtime_lineage_workspace_bindings
    WHERE lineage_id = ?
  `).get(lineageId);
  if (
    !binding
    || binding.workspace_id !== workspace.workspace_id
    || binding.conversation_id !== conversationId
    || binding.workspace_root !== workspace.workspace_root
    || binding.generation !== workspace.generation
  ) {
    throw new ConversationWorkspaceError(
      'lineage_workspace_binding_conflict',
      'The lineage is already bound to a different workspace identity.',
      { terminal: true },
    );
  }
  return projectLineageBinding(binding);
}

export function getLineageWorkspaceBinding(database, lineageId) {
  requireDatabase(database);
  requireText('lineageId', lineageId);
  return projectLineageBinding(database.prepare(`
    SELECT *
    FROM runtime_lineage_workspace_bindings
    WHERE lineage_id = ?
  `).get(lineageId));
}

export function createConversationWorkspaceProvisioner({
  database,
  workspaceStoreRoot,
  baseSnapshotRoot,
  baseSnapshotRef,
  snapshotFiles,
  now = () => new Date().toISOString(),
  generateId = defaultGenerateId,
  provisioningLeaseMs = DEFAULT_PROVISIONING_LEASE_MS,
  provisioningFault,
}) {
  requireDatabase(database);
  if (typeof now !== 'function' || typeof generateId !== 'function') {
    throw new TypeError('Conversation workspace provisioning requires now and generateId functions.');
  }
  if (provisioningFault !== undefined && typeof provisioningFault !== 'function') {
    throw new TypeError('provisioningFault must be a function.');
  }
  requirePositiveSafeInteger('provisioningLeaseMs', provisioningLeaseMs);
  requireText('baseSnapshotRef', baseSnapshotRef);
  const configuredSnapshotFiles = normalizeSnapshotFiles(snapshotFiles);
  const configuredSnapshotManifest = JSON.stringify(configuredSnapshotFiles);
  const controlledStoreRoot = lstatDirectory('workspaceStoreRoot', workspaceStoreRoot);
  const configuredSnapshotRoot = lstatDirectory('baseSnapshotRoot', baseSnapshotRoot);
  if (rootsOverlap(controlledStoreRoot, configuredSnapshotRoot)) {
    throw new TypeError('workspaceStoreRoot and baseSnapshotRoot must not overlap.');
  }
  fs.chmodSync(controlledStoreRoot, 0o700);
  initializeRuntimePersistence(database);
  const provisionerOwner = generateId('workspace-provisioner');

  function timestamp() {
    return requireTimestamp('now()', now());
  }

  function quarantineRow(row, {
    reasonCode,
    message,
    occurredAt = timestamp(),
  }) {
    const error = new ConversationWorkspaceError(
      requireText('reasonCode', reasonCode),
      requireText('message', message),
      { terminal: true },
    );
    runImmediate(database, () => {
      database.prepare(`
        UPDATE runtime_conversation_workspaces
        SET state = 'quarantined', provisioning_owner = NULL,
          provisioning_expires_at = NULL, quarantined_at = ?,
          updated_at = ?, last_error_json = ?
        WHERE conversation_id = ?
          AND state IN ('requested', 'provisioning', 'ready')
      `).run(
        occurredAt,
        occurredAt,
        serializeError(error, occurredAt, reasonCode),
        row.conversation_id,
      );
    });
    return loadBindingRow(database, row.conversation_id);
  }

  function quarantineUnsafeRuntime(row) {
    if (!['requested', 'provisioning', 'ready'].includes(row.state)) return row;
    const evidence = loadUnsafeRuntimeEvidence(database, row.conversation_id);
    if (!evidence) return row;
    return quarantineRow(row, {
      reasonCode: 'workspace_runtime_uncertain',
      message: `Conversation workspace quarantined because ${evidence.reason}.`,
    });
  }

  function validateDurableProvisioningPaths(row) {
    assertControlledDirectoryIdentity(
      controlledStoreRoot,
      'Conversation workspace store root',
    );
    assertContainedPath(
      controlledStoreRoot,
      row.workspace_root,
      'durable conversation workspace root',
    );
    const expected = workspacePaths(controlledStoreRoot, row);
    if (row.workspace_root !== expected.workspaceRoot) {
      throw new ConversationWorkspaceError(
        'workspace_path_identity_mismatch',
        'Durable conversation workspace root does not match its Core identity.',
        { terminal: true },
      );
    }
    if (row.state === 'provisioning') {
      assertContainedPath(
        controlledStoreRoot,
        row.staging_root,
        'durable conversation workspace staging root',
      );
      if (row.staging_root !== expected.stagingRoot) {
        throw new ConversationWorkspaceError(
          'workspace_path_identity_mismatch',
          'Durable conversation workspace staging root does not match its Core identity.',
          { terminal: true },
        );
      }
      const durableSnapshotRoot = lstatDirectory(
        'durable baseSnapshotRoot',
        row.base_snapshot_root,
      );
      if (durableSnapshotRoot !== row.base_snapshot_root) {
        throw new ConversationWorkspaceError(
          'base_snapshot_identity_changed',
          'Durable base snapshot root changed identity.',
          { terminal: true },
        );
      }
      parseDurableSnapshotFiles(row.base_snapshot_manifest_json);
    }
  }

  function markReady(row, readyAt, { requireOwner = false } = {}) {
    runImmediate(database, () => {
      const ownerFence = requireOwner
        ? `AND provisioning_owner = ?
          AND julianday(provisioning_expires_at) > julianday(?)`
        : '';
      const values = [
        readyAt,
        readyAt,
        row.conversation_id,
        row.workspace_root,
        row.generation,
      ];
      if (requireOwner) values.push(provisionerOwner, readyAt);
      const updated = database.prepare(`
        UPDATE runtime_conversation_workspaces
        SET state = 'ready', staging_root = NULL, provisioning_owner = NULL,
          provisioning_expires_at = NULL, ready_at = ?, updated_at = ?,
          last_error_json = NULL
        WHERE conversation_id = ? AND state = 'provisioning'
          AND workspace_root = ? AND generation = ?
          ${ownerFence}
      `).run(...values);
      if (updated.changes !== 1) {
        throw new ConversationWorkspaceError(
          'workspace_state_conflict',
          'Conversation workspace lost its provisioning state before activation committed.',
        );
      }
    });
    return loadBindingRow(database, row.conversation_id);
  }

  function resetExpiredProvisioning(row, resetAt) {
    runImmediate(database, () => {
      const fenced = database.prepare(`
        UPDATE runtime_conversation_workspaces
        SET provisioning_owner = ?, updated_at = ?
        WHERE conversation_id = ? AND state = 'provisioning'
          AND workspace_root = ? AND staging_root = ? AND generation = ?
          AND julianday(provisioning_expires_at) <= julianday(?)
      `).run(
        provisionerOwner,
        resetAt,
        row.conversation_id,
        row.workspace_root,
        row.staging_root,
        row.generation,
        resetAt,
      );
      if (fenced.changes !== 1) {
        throw new ConversationWorkspaceError(
          'workspace_provisioning_still_owned',
          'Conversation workspace provisioning is still owned by another live lease.',
        );
      }
      // This provisioning-only recovery transaction intentionally spans the
      // filesystem cleanup. It prevents a new owner or an uncertainty trigger
      // from observing `requested` until the old deterministic staging path is
      // gone. A crash rolls SQLite back to the expired provisioning row; the
      // partially/missing staging tree is then safe to clean again on reopen.
      if (row.staging_root !== null && fs.existsSync(row.staging_root)) {
        assertContainedPath(controlledStoreRoot, row.staging_root, 'expired staging root');
        fs.rmSync(row.staging_root, { recursive: true, force: true });
      }
      const reset = database.prepare(`
        UPDATE runtime_conversation_workspaces
        SET state = 'requested', provisioning_owner = NULL, provisioning_expires_at = NULL,
          provisioning_started_at = NULL, updated_at = ?
        WHERE conversation_id = ? AND state = 'provisioning'
          AND provisioning_owner = ? AND workspace_root = ?
          AND staging_root = ? AND generation = ?
      `).run(
        resetAt,
        row.conversation_id,
        provisionerOwner,
        row.workspace_root,
        row.staging_root,
        row.generation,
      );
      if (reset.changes !== 1) {
        throw new ConversationWorkspaceError(
          'workspace_provisioning_fence_lost',
          'Conversation workspace recovery lost its cleanup fence.',
        );
      }
    });
    return loadBindingRow(database, row.conversation_id);
  }

  function reconcileRow(row) {
    let current = quarantineUnsafeRuntime(row);
    if (!['provisioning', 'ready'].includes(current.state)) return current;
    try {
      validateDurableProvisioningPaths(current);
      if (current.state === 'ready') {
        validateActivatedWorkspace(controlledStoreRoot, current);
        return current;
      }
      if (fs.existsSync(current.workspace_root)) {
        validateActivatedWorkspace(controlledStoreRoot, current);
        if (current.staging_root !== null && fs.existsSync(current.staging_root)) {
          fs.rmSync(current.staging_root, { recursive: true, force: true });
        }
        return markReady(current, timestamp());
      }
      const currentAt = timestamp();
      if (
        current.provisioning_owner !== provisionerOwner
        && Date.parse(current.provisioning_expires_at) > Date.parse(currentAt)
      ) {
        return current;
      }
      return resetExpiredProvisioning(current, currentAt);
    } catch (error) {
      if (
        error instanceof ConversationWorkspaceError
        && error.code === 'workspace_provisioning_still_owned'
      ) {
        return loadBindingRow(database, current.conversation_id);
      }
      return quarantineRow(current, {
        reasonCode: error?.code ?? 'workspace_reconciliation_failed',
        message: error?.message ?? 'Conversation workspace reconciliation failed.',
      });
    }
  }

  function beginProvisioning(row) {
    const startedAt = timestamp();
    const { workspaceRoot, stagingRoot } = workspacePaths(controlledStoreRoot, row);
    const snapshotRoot = row.base_snapshot_root ?? configuredSnapshotRoot;
    const snapshotRef = row.base_snapshot_ref ?? baseSnapshotRef;
    const snapshotManifest = row.base_snapshot_manifest_json
      ?? configuredSnapshotManifest;
    const expiresAt = new Date(
      Date.parse(startedAt) + provisioningLeaseMs,
    ).toISOString();
    runImmediate(database, () => {
      const updated = database.prepare(`
        UPDATE runtime_conversation_workspaces
        SET state = 'provisioning', workspace_root = ?, staging_root = ?,
          base_snapshot_root = ?, base_snapshot_ref = ?,
          base_snapshot_manifest_json = ?,
          provisioning_owner = ?, provisioning_expires_at = ?,
          provisioning_started_at = ?, updated_at = ?, last_error_json = NULL
        WHERE conversation_id = ? AND state = 'requested' AND generation = ?
      `).run(
        workspaceRoot,
        stagingRoot,
        snapshotRoot,
        snapshotRef,
        snapshotManifest,
        provisionerOwner,
        expiresAt,
        startedAt,
        startedAt,
        row.conversation_id,
        row.generation,
      );
      if (updated.changes !== 1) {
        throw new ConversationWorkspaceError(
          'workspace_state_conflict',
          'Conversation workspace was claimed by another provisioner.',
        );
      }
    });
    return loadBindingRow(database, row.conversation_id);
  }

  function persistProvisioningFailure(row, error, failedAt) {
    if (error?.terminal === true) {
      let changed = false;
      runImmediate(database, () => {
        const failed = database.prepare(`
          UPDATE runtime_conversation_workspaces
          SET state = 'failed', provisioning_owner = NULL,
            provisioning_expires_at = NULL, failed_at = ?, updated_at = ?,
            last_error_json = ?
          WHERE conversation_id = ? AND state = 'provisioning'
            AND provisioning_owner = ? AND workspace_root = ? AND generation = ?
        `).run(
          failedAt,
          failedAt,
          serializeError(error, failedAt),
          row.conversation_id,
          provisionerOwner,
          row.workspace_root,
          row.generation,
        );
        changed = failed.changes === 1;
      });
      if (changed && row.staging_root !== null && fs.existsSync(row.staging_root)) {
        assertContainedPath(controlledStoreRoot, row.staging_root, 'failed staging root');
        fs.rmSync(row.staging_root, { recursive: true, force: true });
      }
      return;
    }
    runImmediate(database, () => {
      database.prepare(`
        UPDATE runtime_conversation_workspaces
        SET updated_at = ?, last_error_json = ?
        WHERE conversation_id = ? AND state = 'provisioning'
          AND provisioning_owner = ? AND workspace_root = ? AND generation = ?
      `).run(
        failedAt,
        serializeError(error, failedAt),
        row.conversation_id,
        provisionerOwner,
        row.workspace_root,
        row.generation,
      );
    });
  }

  function heartbeatProvisioning(row) {
    const renewedAt = timestamp();
    const expiresAt = new Date(
      Date.parse(renewedAt) + provisioningLeaseMs,
    ).toISOString();
    runImmediate(database, () => {
      const renewed = database.prepare(`
        UPDATE runtime_conversation_workspaces
        SET provisioning_expires_at = ?, updated_at = ?
        WHERE conversation_id = ? AND state = 'provisioning'
          AND provisioning_owner = ? AND workspace_root = ? AND generation = ?
          AND julianday(provisioning_expires_at) > julianday(?)
      `).run(
        expiresAt,
        renewedAt,
        row.conversation_id,
        provisionerOwner,
        row.workspace_root,
        row.generation,
        renewedAt,
      );
      if (renewed.changes !== 1) {
        throw new ConversationWorkspaceError(
          'workspace_provisioning_fence_lost',
          'Conversation workspace provisioning ownership was lost before activation.',
        );
      }
    });
  }

  function materialize(row) {
    try {
      validateDurableProvisioningPaths(row);
      heartbeatProvisioning(row);
      if (fs.existsSync(row.workspace_root)) {
        validateActivatedWorkspace(controlledStoreRoot, row);
        return markReady(row, timestamp(), { requireOwner: true });
      }
      if (fs.existsSync(row.staging_root)) {
        fs.rmSync(row.staging_root, { recursive: true, force: true });
      }
      fs.mkdirSync(row.staging_root, { mode: 0o700 });
      const heartbeat = () => heartbeatProvisioning(row);
      provisioningFault?.('after_staging_created', Object.freeze({
        source_root: row.base_snapshot_root,
        staging_root: row.staging_root,
        workspace_root: row.workspace_root,
      }));
      copySnapshotFiles(
        row.base_snapshot_root,
        row.staging_root,
        parseDurableSnapshotFiles(row.base_snapshot_manifest_json),
        heartbeat,
      );
      provisioningFault?.('after_snapshot_copy', Object.freeze({
        staging_root: row.staging_root,
        workspace_root: row.workspace_root,
      }));
      heartbeat();
      hardenPopulatedTree(row.staging_root);
      heartbeat();
      writeMarker(row.staging_root, row);
      heartbeat();
      provisioningFault?.('before_activation', Object.freeze({
        staging_root: row.staging_root,
        workspace_root: row.workspace_root,
      }));
      fs.renameSync(row.staging_root, row.workspace_root);
      provisioningFault?.('after_activation', Object.freeze({
        workspace_root: row.workspace_root,
      }));
      validateActivatedWorkspace(controlledStoreRoot, row);
      return markReady(row, timestamp(), { requireOwner: true });
    } catch (error) {
      const failedAt = timestamp();
      try {
        persistProvisioningFailure(row, error, failedAt);
      } catch {
        // The original error remains authoritative; the provisioning row and
        // activated root are intentionally left for reopen reconciliation.
      }
      throw error instanceof ConversationWorkspaceError
        ? error
        : new ConversationWorkspaceError(
          'workspace_provisioning_failed',
          error?.message ?? 'Conversation workspace provisioning failed.',
          { cause: error },
        );
    }
  }

  function ensure(conversationId) {
    requireText('conversationId', conversationId);
    let row = loadBindingRow(database, conversationId);
    if (!row) {
      throw new ConversationWorkspaceError(
        'workspace_binding_missing',
        'Detached execution conversation has no durable workspace binding.',
        { terminal: true },
      );
    }
    row = reconcileRow(row);
    if (row.state !== 'requested') return projectBinding(row);
    row = beginProvisioning(row);
    return projectBinding(materialize(row));
  }

  function reconcile() {
    return Object.freeze(database.prepare(`
      SELECT *
      FROM runtime_conversation_workspaces
      ORDER BY requested_at, workspace_id
    `).all().map((row) => projectBinding(reconcileRow(row))));
  }

  function quarantine({
    conversationId,
    reasonCode,
    message = 'Conversation workspace requires manual recovery.',
  }) {
    requireText('conversationId', conversationId);
    const row = loadBindingRow(database, conversationId);
    if (!row) {
      throw new ConversationWorkspaceError(
        'workspace_binding_missing',
        'Conversation workspace binding does not exist.',
        { terminal: true },
      );
    }
    if (['quarantined', 'failed', 'retired'].includes(row.state)) {
      return projectBinding(row);
    }
    return projectBinding(quarantineRow(row, { reasonCode, message }));
  }

  function retire({ conversationId, reason, retainUntil = null }) {
    requireText('conversationId', conversationId);
    requireText('reason', reason);
    if (retainUntil !== null) requireTimestamp('retainUntil', retainUntil);
    let row = loadBindingRow(database, conversationId);
    if (!row) {
      throw new ConversationWorkspaceError(
        'workspace_binding_missing',
        'Conversation workspace binding does not exist.',
        { terminal: true },
      );
    }
    row = reconcileRow(row);
    if (row.state === 'retired') return projectBinding(row);
    if (row.state !== 'ready') {
      throw new ConversationWorkspaceError(
        'workspace_retire_precondition_failed',
        `Only a ready conversation workspace can retire; found ${row.state}.`,
        { terminal: true },
      );
    }
    const retiredAt = timestamp();
    runImmediate(database, () => {
      const blocker = database.prepare(`
        SELECT reason FROM (
          SELECT 'nonterminal_turn' AS reason
          FROM runtime_turns
          WHERE conversation_id = ?
            AND state IN (
              'received', 'queued', 'starting', 'running', 'waiting_user',
              'redirecting', 'recovering'
            )
          UNION ALL
          SELECT 'workspace_lease' AS reason
          FROM runtime_workspace_leases
          WHERE holder_conversation_id = ? AND state IN ('active', 'uncertain')
          UNION ALL
          SELECT 'workspace_background_work' AS reason
          FROM runtime_workspace_background_work AS background
          JOIN runtime_workspace_leases AS lease
            ON lease.workspace_lease_id = background.workspace_lease_id
          WHERE lease.holder_conversation_id = ?
            AND background.state IN ('active', 'unknown')
        )
        LIMIT 1
      `).get(conversationId, conversationId, conversationId);
      if (blocker) {
        throw new ConversationWorkspaceError(
          'workspace_retire_blocked',
          `Conversation workspace cannot retire while ${blocker.reason} remains.`,
          { terminal: true },
        );
      }
      const retired = database.prepare(`
        UPDATE runtime_conversation_workspaces
        SET state = 'retired', retired_at = ?, updated_at = ?,
          retention_reason = ?, retain_until = ?
        WHERE conversation_id = ? AND state = 'ready'
      `).run(retiredAt, retiredAt, reason, retainUntil, conversationId);
      if (retired.changes !== 1) {
        throw new ConversationWorkspaceError(
          'workspace_state_conflict',
          'Conversation workspace changed before retirement committed.',
        );
      }
    });
    return projectBinding(loadBindingRow(database, conversationId));
  }

  return Object.freeze({
    ensure,
    get(conversationId) {
      return getConversationWorkspaceBinding(database, conversationId);
    },
    quarantine,
    reconcile,
    retire,
  });
}
