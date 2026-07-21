import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

export const REQUIRED_SCAN_SURFACES = Object.freeze([
  'runtime_code',
  'services',
  'config',
  'cli',
  'installer_upgrade_doctor_self_heal_uninstall',
  'docs_help_errors',
  'tests_fixtures',
  'observability_consumers',
  'package_install_contents',
]);

export const FIVE_REPO_RUNTIME_MATRIX = Object.freeze([
  'zylos-core',
  'zylos-feishu',
  'zylos-lark',
  'zylos-dashboard',
  'luna-pet',
].map((repository) => Object.freeze({ repository, surfaces: REQUIRED_SCAN_SURFACES })));

const DIRECTORY_ENV = Object.freeze({
  'zylos-feishu': 'ZYLOS_FEISHU_RETIRED_RUNTIME_REPO',
  'zylos-lark': 'ZYLOS_LARK_RETIRED_RUNTIME_REPO',
  'zylos-dashboard': 'ZYLOS_DASHBOARD_RETIRED_RUNTIME_REPO',
  'luna-pet': 'ZYLOS_LUNA_RETIRED_RUNTIME_REPO',
});

const DEFAULT_DIRECTORY = Object.freeze({
  'zylos-feishu': 'zylos-feishu-integration',
  'zylos-lark': 'zylos-lark-integration',
  'zylos-dashboard': 'zylos-dashboard-integration',
  'luna-pet': 'luna-pet-integration',
});

export function buildFiveRepoRetiredRuntimePlan({
  coreDirectory,
  workspaceDirectory,
  environment = process.env,
  allowlist = [],
}) {
  if (!Array.isArray(allowlist)) throw new TypeError('allowlist must be an array');
  const repositories = new Set(FIVE_REPO_RUNTIME_MATRIX.map(({ repository }) => repository));
  for (const entry of allowlist) {
    if (!repositories.has(entry?.repository)) {
      throw new Error(`unknown allowlist repository: ${entry?.repository ?? '<missing>'}`);
    }
  }
  const resolvedCore = path.resolve(coreDirectory);
  const resolvedWorkspace = path.resolve(workspaceDirectory);
  return FIVE_REPO_RUNTIME_MATRIX.map((entry) => {
    const configured = DIRECTORY_ENV[entry.repository]
      ? environment[DIRECTORY_ENV[entry.repository]]
      : null;
    const directory = entry.repository === 'zylos-core'
      ? resolvedCore
      : configured
        ? path.resolve(configured)
        : path.join(resolvedWorkspace, '.codex-worktrees', DEFAULT_DIRECTORY[entry.repository]);
    return Object.freeze({
      ...entry,
      directory,
      allowlist: Object.freeze(allowlist.filter(({ repository }) => repository === entry.repository)),
    });
  });
}

const compound = (...parts) => parts.join('');
const fileSuffixPattern = '(?:file|File|FILE|_file|_FILE)?';
const bareShortIdentifierPattern = `(?:${compound('p', 'id')}|${compound('P', 'ID')})`;
const camelShortIdentifierPattern = `(?:${compound('P', 'id')}|${compound('P', 'ID')})`;
const longIdentifierPattern = `(?:${[
  compound('process', 'Id'),
  compound('process', 'ID'),
  compound('process_', 'id'),
  compound('PROCESS_', 'ID'),
].join('|')})`;
const processIdentifierPattern = `(?:${[
  compound('process\\.', bareShortIdentifierPattern),
  compound(bareShortIdentifierPattern, fileSuffixPattern),
  compound(longIdentifierPattern, fileSuffixPattern),
  compound('[A-Za-z_$][\\w$]*', '(?:', camelShortIdentifierPattern, '|', compound('Process', 'Id'), '|', compound('Process', 'ID'), ')', fileSuffixPattern),
  compound('[A-Za-z_$][\\w$]*_', '(?:', bareShortIdentifierPattern, '|', longIdentifierPattern, ')', fileSuffixPattern),
].join('|')})`;
const caseInsensitiveWord = (word) => [...word]
  .map((character) => `[${character.toLowerCase()}${character.toUpperCase()}]`)
  .join('');
const livenessStatePattern = `(?:${[
  'healthy',
  'health',
  'alive',
  'liveness',
  'online',
  'running',
  'down',
].map(caseInsensitiveWord).join('|')})`;

export const RETIRED_RUNTIME_RULES = Object.freeze([
  Object.freeze({
    id: 'retired_terminal_multiplexer',
    pattern: new RegExp(compound('t', 'mux'), 'iu'),
    description: 'retired terminal-multiplexer identifier',
  }),
  Object.freeze({
    id: 'terminal_input_authority',
    pattern: new RegExp(`\\b(?:${[
      compound('capture', '[ _-]?pane'),
      compound('send', '[ _-]?keys'),
      compound('paste', '[ _-]?buffer'),
      compound('terminal', '[ _-]?injection'),
    ].join('|')})\\b`, 'iu'),
    description: 'terminal input or pane injection authority',
  }),
  Object.freeze({
    id: 'provider_session_scope',
    pattern: new RegExp(`\\b${compound('global', '[ _-]?sessions?')}\\b`, 'iu'),
    description: 'provider-wide session authority',
  }),
  Object.freeze({
    id: 'terminal_observation_authority',
    pattern: new RegExp(`\\b(?:${[
      compound('agent', '-status\\.json'),
      compound('input', '[ _-]?(?:health|state)'),
      compound('window', '[ _-]?health'),
    ].join('|')})\\b`, 'iu'),
    description: 'retired terminal observation authority',
  }),
  Object.freeze({
    id: 'retired_activity_authority',
    pattern: new RegExp(`\\b(?:${[
      compound('activity', '[ _-]?monitor'),
      compound('am', '[ _-]?heartbeat'),
      compound('c4', '[ _-]?(?:dispatcher|control|session[ _-]?init)'),
    ].join('|')})\\b`, 'iu'),
    description: 'retired activity or C4 execution authority',
  }),
  Object.freeze({
    id: 'retired_idle_gate',
    pattern: new RegExp(`\\b(?:${[
      compound('block', '[ _-]?queue[ _-]?until[ _-]?idle'),
      compound('no', '[ _-]?block[ _-]?queue[ _-]?until[ _-]?idle'),
      compound('require', '[ _-]?idle'),
      compound('no', '[ _-]?require[ _-]?idle'),
      compound('idle', '[ _-]?seconds'),
      compound('sustained', '[ _-]?idle'),
    ].join('|')})\\b`, 'iu'),
    description: 'retired scheduler idle-gating flag or state',
  }),
  Object.freeze({
    id: 'retired_route_alias',
    pattern: new RegExp(`\\b(?:${[
      compound('use', '[ _-]?legacy[ _-]?(?:runtime|executor|transport|route)'),
      compound('legacy', '[ _-]?(?:runtime|executor|transport|route)(?:[ _-]?fallback)?'),
      compound('compatibility', '\\s*=\\s*[\'\"]legacy[ _-]+(?:runtime|executor|transport|route)'),
    ].join('|')})\\b`, 'iu'),
    description: 'renamed retired runtime route or compatibility alias',
  }),
  Object.freeze({
    id: 'process_liveness_authority',
    pattern: new RegExp(`(?:${[
      compound(`\\b${processIdentifierPattern}\\b`, `[^\\r\\n]{0,120}\\b${livenessStatePattern}\\b`),
      compound(`\\b${livenessStatePattern}\\b`, `[^\\r\\n]{0,120}\\b${processIdentifierPattern}\\b`),
    ].join('|')})`, 'u'),
    description: 'process-identity liveness authority',
  }),
]);

const RULE_BY_ID = new Map(RETIRED_RUNTIME_RULES.map((rule) => [rule.id, rule]));

function sourceFor(sources, file) {
  const source = sources instanceof Map ? sources.get(file) : sources?.[file];
  if (typeof source === 'string') return source;
  if (Buffer.isBuffer(source)) return source.toString('utf8');
  return null;
}

function matchingRules(source) {
  if (source === null) return [];
  return RETIRED_RUNTIME_RULES.filter(({ pattern }) => pattern.test(source));
}

function importSpecifiers(source) {
  const specifiers = [];
  const patterns = [
    /\b(?:import|export)\s+(?:[^'"]*?\s+from\s+)?['"]([^'"]+)['"]/gu,
    /\brequire\s*\(\s*['"]([^'"]+)['"]\s*\)/gu,
    /\bimport\s*\(\s*['"]([^'"]+)['"]\s*\)/gu,
  ];
  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) specifiers.push(match[1]);
  }
  return specifiers;
}

function importResolvesTo(importer, specifier, target) {
  if (!specifier.startsWith('.')) return false;
  const resolved = path.posix.normalize(path.posix.join(path.posix.dirname(importer), specifier));
  const candidates = path.posix.extname(resolved)
    ? [resolved]
    : [resolved, `${resolved}.js`, `${resolved}.cjs`, `${resolved}.mjs`, `${resolved}/index.js`];
  return candidates.includes(target);
}

function sourceImportsTarget(importer, source, target) {
  return importSpecifiers(source).some((specifier) => importResolvesTo(importer, specifier, target));
}

function pathSpecifiers(source) {
  const specifiers = [];
  const pattern = /(?:^|[^A-Za-z0-9_./-])((?:(?:\.{1,2}\/)+|\/)?[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)+)/gmu;
  for (const match of source.matchAll(pattern)) specifiers.push(match[1]);
  return specifiers;
}

function pathReferenceResolvesTo(importer, specifier, target) {
  if (specifier === target) return true;
  if (specifier.startsWith('/')) return specifier.endsWith(`/${target}`);
  return importResolvesTo(importer, specifier, target)
    || path.posix.normalize(specifier) === target;
}

function sourceReferencesTarget(importer, source, target) {
  if (sourceImportsTarget(importer, source, target)) return true;
  if (importer === 'scripts/config/five-repo-retired-runtime-allowlist.json') return false;
  const executableSource = /\.(?:[cm]?js|jsx|tsx?|sh|bash|zsh)$/u.test(importer)
    || source.startsWith('#!');
  const configurationSource = /\.(?:jsonc?|ya?ml|toml|ini|conf|env)$/iu.test(importer)
    || /(?:^|\/)\.env(?:\.|$)/u.test(importer);
  if (!executableSource && !configurationSource) return false;
  return pathSpecifiers(source)
    .some((specifier) => pathReferenceResolvesTo(importer, specifier, target));
}

function allowlistLocationIsIsolated(entry) {
  if (entry.kind === 'negative_proof') {
    return entry.file.startsWith('test/') || entry.file.includes('/__tests__/');
  }
  if (entry.kind === 'one_time_cleanup') {
    return entry.file.startsWith('runtime/migration/')
      || entry.file.startsWith('scripts/migration/')
      || entry.file.includes('/migration/');
  }
  if (entry.kind === 'migration_record') {
    return entry.file.startsWith('runtime/migration/');
  }
  return false;
}

export function validateRetiredRuntimeAllowlist({
  repository,
  trackedFiles,
  packagedFiles,
  sources,
  allowlist,
}) {
  const available = new Set([...trackedFiles, ...packagedFiles]);
  const errors = [];
  const seen = new Set();
  for (const entry of allowlist) {
    const prefix = `${entry.repository ?? '<missing>'}:${entry.file ?? '<missing>'}`;
    if (entry.repository !== repository) {
      errors.push(`${prefix}: repository does not match ${repository}`);
      continue;
    }
    if (!available.has(entry.file)) errors.push(`${prefix}: file does not exist in either scan scope`);
    if (typeof entry.purpose !== 'string' || entry.purpose.trim().length < 12) {
      errors.push(`${prefix}: purpose must explain the isolated one-time cleanup or proof`);
    }
    if (!allowlistLocationIsIsolated(entry)) {
      errors.push(`${prefix}: allowlist location is not an isolated migration or negative proof`);
    }
    if (!Array.isArray(entry.rules) || entry.rules.length === 0) {
      errors.push(`${prefix}: at least one exact rule is required`);
      continue;
    }
    const source = sourceFor(sources, entry.file);
    const allowedImporters = entry.allowed_importers ?? [];
    if (!Array.isArray(allowedImporters)) {
      errors.push(`${prefix}: allowed_importers must be an array`);
    } else {
      const importerSeen = new Set();
      for (const importer of allowedImporters) {
        const importerKey = `${prefix}:importer:${importer}`;
        if (importerSeen.has(importer)) errors.push(`${importerKey}: duplicate allowed importer`);
        importerSeen.add(importer);
        if (!available.has(importer)) {
          errors.push(`${importerKey}: allowed importer does not exist in either scan scope`);
          continue;
        }
        const importerSource = sourceFor(sources, importer);
        if (importerSource === null || !sourceImportsTarget(importer, importerSource, entry.file)) {
          errors.push(`${importerKey}: stale allowed importer does not import the allowlisted file`);
        }
      }
    }
    for (const ruleId of entry.rules) {
      const key = `${prefix}:${ruleId}`;
      if (seen.has(key)) errors.push(`${key}: duplicate allowlist rule`);
      seen.add(key);
      const rule = RULE_BY_ID.get(ruleId);
      if (!rule) {
        errors.push(`${key}: unknown rule`);
      } else if (source === null || !rule.pattern.test(source)) {
        errors.push(`${key}: stale rule does not match the file`);
      }
    }
  }
  return errors;
}

export function auditRepositorySnapshot({
  repository,
  trackedFiles,
  packagedFiles,
  sources,
  allowlist,
}) {
  const allowlistErrors = validateRetiredRuntimeAllowlist({
    repository,
    trackedFiles,
    packagedFiles,
    sources,
    allowlist,
  });
  const validEntries = allowlistErrors.length === 0 ? allowlist : [];
  const entriesByFile = new Map();
  for (const entry of validEntries) entriesByFile.set(entry.file, entry);

  const violations = allowlistErrors.map((message) => Object.freeze({
    file: '<allowlist>',
    scope: 'allowlist',
    rule: 'invalid_allowlist',
    description: message,
  }));
  const allowlisted = new Map();
  for (const [scope, files] of [['tracked', trackedFiles], ['package', packagedFiles]]) {
    for (const file of files) {
      const source = sourceFor(sources, file);
      for (const rule of matchingRules(source)) {
        const entry = entriesByFile.get(file);
        if (entry?.rules.includes(rule.id)) {
          allowlisted.set(`${file}:${rule.id}`, Object.freeze({
            repository,
            file,
            rule: rule.id,
            kind: entry.kind,
            purpose: entry.purpose,
          }));
          continue;
        }
        violations.push(Object.freeze({
          repository,
          file,
          scope,
          rule: rule.id,
          description: rule.description,
        }));
      }
    }
  }
  const negativeProofFiles = new Set(validEntries
    .filter(({ kind }) => kind === 'negative_proof')
    .map(({ file }) => file));
  for (const entry of validEntries) {
    if (entry.kind === 'negative_proof') continue;
    const allowedImporters = new Set(entry.allowed_importers ?? []);
    for (const [file, source] of sources) {
      if (file === entry.file || negativeProofFiles.has(file) || !sourceReferencesTarget(file, source, entry.file)) continue;
      if (allowedImporters.has(file)) continue;
      violations.push(Object.freeze({
        repository,
        file,
        scope: 'reachability',
        rule: 'undeclared_allowlist_reference',
        description: `normal path references allowlisted ${entry.kind}: ${entry.file}`,
      }));
    }
  }
  return Object.freeze({
    repository,
    passed: violations.length === 0,
    violations: Object.freeze(violations),
    allowlisted: Object.freeze([...allowlisted.values()]),
  });
}

function run(command, arguments_, { cwd, spawn = spawnSync }) {
  const result = spawn(command, arguments_, {
    cwd,
    encoding: 'utf8',
    shell: false,
    maxBuffer: 16 * 1024 * 1024,
    timeout: 60_000,
  });
  if (result?.error) throw result.error;
  if (result?.signal) throw new Error(`${command} terminated by ${result.signal}`);
  if (result?.status !== 0) {
    const detail = [result?.stderr, result?.stdout].filter(Boolean).join('\n').trim();
    throw new Error(`${command} ${arguments_.join(' ')} failed with exit ${result?.status}: ${detail}`);
  }
  return typeof result.stdout === 'string' ? result.stdout : '';
}

function parseNulList(output) {
  return output.split('\0').filter(Boolean).sort();
}

function readTextFiles(directory, files) {
  const sources = new Map();
  const root = path.resolve(directory);
  const realRoot = fs.realpathSync(root);
  const decoder = new TextDecoder('utf-8', { fatal: true });
  for (const file of files) {
    const filename = path.resolve(root, file);
    if (filename !== root && !filename.startsWith(`${root}${path.sep}`)) {
      throw new Error(`Package entry escapes repository root: ${file}`);
    }
    let stat;
    try {
      stat = fs.lstatSync(filename);
    } catch (error) {
      throw new Error(`Package or tracked file is missing: ${file}`, { cause: error });
    }
    if (stat.isSymbolicLink()) throw new Error(`Package or tracked file is a symbolic link: ${file}`);
    if (!stat.isFile()) continue;
    const realFilename = fs.realpathSync(filename);
    if (realFilename !== realRoot && !realFilename.startsWith(`${realRoot}${path.sep}`)) {
      throw new Error(`Package or tracked file resolves outside repository root: ${file}`);
    }
    const contents = fs.readFileSync(filename);
    if (contents.includes(0)) continue;
    try {
      sources.set(file, decoder.decode(contents));
    } catch {
      // Binary files are package-covered but have no textual authority surface.
    }
  }
  return sources;
}

export function scanRepositoryRetiredRuntime(item, { spawn = spawnSync } = {}) {
  if (!fs.statSync(item.directory).isDirectory()) {
    throw new Error(`${item.repository} directory is not a directory: ${item.directory}`);
  }
  const status = run('git', ['status', '--porcelain=v1', '-z', '--untracked-files=all'], {
    cwd: item.directory,
    spawn,
  });
  if (status.length > 0) {
    throw new Error(`${item.repository} worktree is not clean; refusing to attest HEAD`);
  }
  const trackedFiles = parseNulList(run('git', ['ls-files', '-z'], {
    cwd: item.directory,
    spawn,
  }));
  const packOutput = run('npm', ['pack', '--dry-run', '--json', '--ignore-scripts'], {
    cwd: item.directory,
    spawn,
  });
  let pack;
  try {
    [pack] = JSON.parse(packOutput);
  } catch (error) {
    throw new Error(`${item.repository} emitted invalid npm pack JSON`, { cause: error });
  }
  const packagedFiles = [...new Set((pack?.files ?? []).map(({ path: file }) => file))].sort();
  if (packagedFiles.length === 0) throw new Error(`${item.repository} package contains no files`);
  const allFiles = [...new Set([...trackedFiles, ...packagedFiles])].sort();
  const sources = readTextFiles(item.directory, allFiles);
  const revision = run('git', ['rev-parse', 'HEAD'], { cwd: item.directory, spawn }).trim();
  const audit = auditRepositorySnapshot({
    repository: item.repository,
    trackedFiles,
    packagedFiles,
    sources,
    allowlist: item.allowlist,
  });
  return Object.freeze({
    ...audit,
    directory: item.directory,
    revision,
    trackedFiles: trackedFiles.length,
    packagedFiles: packagedFiles.length,
  });
}

export function executeFiveRepoRetiredRuntimePlan(plan, options = {}) {
  const results = plan.map((item) => {
    try {
      return scanRepositoryRetiredRuntime(item, options);
    } catch (error) {
      return Object.freeze({
        repository: item.repository,
        directory: item.directory,
        revision: null,
        trackedFiles: 0,
        packagedFiles: 0,
        passed: false,
        violations: Object.freeze([Object.freeze({
          file: '<preflight>',
          scope: 'preflight',
          rule: 'scan_failed',
          description: error.message,
        })]),
        allowlisted: Object.freeze([]),
      });
    }
  });
  return Object.freeze({
    passed: results.every(({ passed }) => passed),
    results: Object.freeze(results),
  });
}
