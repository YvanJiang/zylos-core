import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

function requireZylosDir(zylosDir) {
  if (typeof zylosDir !== 'string' || !path.isAbsolute(zylosDir)) {
    throw new TypeError('zylosDir must be an absolute path');
  }
  if (path.parse(zylosDir).root === zylosDir) {
    throw new TypeError('zylosDir must not be a filesystem root');
  }
  return path.resolve(zylosDir);
}

const RETIRED_RUNTIME_SKILLS = new Set(['activity-monitor']);

const OBSOLETE_HOOK_BASE_KEYS = Object.freeze([
  'skills/activity-monitor/scripts/context-monitor.js',
  'skills/activity-monitor/scripts/hook-activity.js',
  'skills/activity-monitor/scripts/hook-auth-prompt.js',
  'skills/activity-monitor/scripts/session-start-orchestrator.js',
  'skills/zylos-memory/scripts/session-start-inject.js',
  'skills/comm-bridge/scripts/c4-session-init.js',
  'skills/activity-monitor/scripts/session-foreground.js',
  'skills/activity-monitor/scripts/session-start-prompt.js',
]);

export function obsoleteHookBaseKeys() {
  return new Set(OBSOLETE_HOOK_BASE_KEYS);
}

export function isObsoleteProviderSessionHook(command, zylosDir) {
  if (typeof command !== 'string' || command.length === 0) return false;
  const installedPath = path.join(
    requireZylosDir(zylosDir), '.claude', 'skills', 'activity-monitor',
    'scripts', 'session-start-orchestrator.js',
  );
  return command.includes(installedPath) || command.includes('session-start-orchestrator.js');
}

export function isRetiredRuntimeSkill(skillName) {
  return RETIRED_RUNTIME_SKILLS.has(skillName);
}

export function cleanupRetiredRuntimeSkillArtifacts({ skillsDir }) {
  const root = requireZylosDir(skillsDir);
  const artifacts = [
    path.join(root, 'comm-bridge', 'scripts', 'c4-dispatcher.js'),
    path.join(root, 'comm-bridge', 'scripts', 'tmux-input-state.js'),
    path.join(root, 'comm-bridge', 'scripts', 'c4-control.js'),
  ];
  const removed = [];
  for (const artifact of artifacts) {
    try {
      fs.unlinkSync(artifact);
      removed.push(artifact);
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
    }
  }
  return Object.freeze({ removed: Object.freeze(removed) });
}

// These exact identifiers exist only for one-time post-commit cleanup. Nothing in
// the normal runtime imports, dispatches, executes, or selects these artifacts.
export function legacyLifecycleArtifactPaths(zylosDir) {
  const root = requireZylosDir(zylosDir);
  return Object.freeze([
    path.join(root, 'pm2', 'tmux-runtime.config.cjs'),
    path.join(root, '.zylos', 'tmux-runtime.json'),
    path.join(root, 'bin', 'start-tmux-runtime.sh'),
    path.join(root, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-dispatcher.js'),
    path.join(root, '.claude', 'skills', 'comm-bridge', 'scripts', 'tmux-input-state.js'),
    path.join(root, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-control.js'),
    path.join(root, '.claude', 'skills', 'activity-monitor'),
    path.join(root, '.codex', 'skills', 'activity-monitor'),
  ]);
}

function obsoleteInstalledHookPaths(root, homeDir) {
  const normalizedRoot = root.replaceAll('\\', '/');
  const paths = ['.claude', '.codex'].flatMap((directory) => OBSOLETE_HOOK_BASE_KEYS.map(
    (key) => path.join(normalizedRoot, directory, key).replaceAll('\\', '/'),
  ));
  const relativeToHome = path.relative(homeDir, root).replaceAll('\\', '/');
  if (relativeToHome !== '..' && !relativeToHome.startsWith('../') && !path.isAbsolute(relativeToHome)) {
    const homeRelativeRoot = relativeToHome.length === 0 ? '~' : `~/${relativeToHome}`;
    const environmentRelativeRoot = relativeToHome.length === 0 ? '$HOME' : `$HOME/${relativeToHome}`;
    const braceEnvironmentRelativeRoot = relativeToHome.length === 0 ? '${HOME}' : `\${HOME}/${relativeToHome}`;
    for (const directory of ['.claude', '.codex']) {
      for (const key of OBSOLETE_HOOK_BASE_KEYS) {
        paths.push(`${homeRelativeRoot}/${directory}/${key}`);
        paths.push(`${environmentRelativeRoot}/${directory}/${key}`);
        paths.push(`${braceEnvironmentRelativeRoot}/${directory}/${key}`);
      }
    }
  }
  return paths;
}

function isObsoleteInstalledHook(command, root, homeDir) {
  if (typeof command !== 'string') return false;
  const normalized = command.replaceAll('\\', '/');
  return obsoleteInstalledHookPaths(root, homeDir).some((ownedPath) => {
    let offset = normalized.indexOf(ownedPath);
    while (offset !== -1) {
      const before = normalized[offset - 1] ?? '';
      const after = normalized[offset + ownedPath.length] ?? '';
      const beginsToken = offset === 0 || /[\s'"`([{:;,|&]/.test(before);
      const endsToken = after === '' || /[\s'"`)]\},:;|&]/.test(after);
      if (beginsToken && endsToken) return true;
      offset = normalized.indexOf(ownedPath, offset + ownedPath.length);
    }
    return false;
  });
}

function isContainedPath(root, candidate) {
  const relative = path.relative(root, candidate);
  return relative === '' || (!relative.startsWith(`..${path.sep}`)
    && relative !== '..' && !path.isAbsolute(relative));
}

function writeHookConfigurationAtomically({ file, root, parent, sourceStat, content }) {
  const currentParent = fs.realpathSync(path.dirname(file));
  if (currentParent !== parent || !isContainedPath(root, currentParent)) {
    throw new Error(`Hook configuration parent changed before cleanup: ${file}`);
  }
  const target = path.join(parent, path.basename(file));
  const current = fs.lstatSync(target);
  if (!current.isFile() || current.nlink !== 1
    || current.dev !== sourceStat.dev || current.ino !== sourceStat.ino) {
    throw new Error(`Hook configuration changed before cleanup: ${file}`);
  }
  const temporary = path.join(parent, `.${path.basename(file)}.${process.pid}.${crypto.randomUUID()}.partial`);
  let descriptor = null;
  try {
    descriptor = fs.openSync(
      temporary,
      fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW,
      0o600,
    );
    fs.writeFileSync(descriptor, content, 'utf8');
    fs.fsyncSync(descriptor);
    fs.closeSync(descriptor);
    descriptor = null;
    if (fs.realpathSync(path.dirname(file)) !== parent) {
      throw new Error(`Hook configuration parent changed before cleanup: ${file}`);
    }
    const beforeReplace = fs.lstatSync(target);
    if (!beforeReplace.isFile() || beforeReplace.nlink !== 1
      || beforeReplace.dev !== sourceStat.dev || beforeReplace.ino !== sourceStat.ino) {
      throw new Error(`Hook configuration changed before cleanup: ${file}`);
    }
    fs.renameSync(temporary, target);
  } finally {
    if (descriptor !== null) fs.closeSync(descriptor);
    try { fs.unlinkSync(temporary); } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
    }
  }
}

function cleanupObsoleteHooksInFile(file, root, homeDir, removed) {
  try {
    const realRoot = fs.realpathSync(root);
    const realParent = fs.realpathSync(path.dirname(file));
    if (!isContainedPath(realRoot, realParent)) {
      throw new Error(`Refusing to rewrite hook configuration outside installation root: ${file}`);
    }
    const descriptor = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    let sourceStat;
    let document;
    try {
      sourceStat = fs.fstatSync(descriptor);
      if (!sourceStat.isFile() || sourceStat.nlink !== 1) {
        throw new Error(`Refusing to rewrite unexpected hook configuration type: ${file}`);
      }
      document = JSON.parse(fs.readFileSync(descriptor, 'utf8'));
    } finally {
      fs.closeSync(descriptor);
    }
    if (Array.isArray(document)) {
      const retained = document.filter((hook) => !isObsoleteInstalledHook(hook?.command, root, homeDir));
      if (retained.length !== document.length) {
        writeHookConfigurationAtomically({
          file, root: realRoot, parent: realParent, sourceStat,
          content: `${JSON.stringify(retained, null, 2)}\n`,
        });
        removed.push(file);
      }
      return;
    }
    if (!document || typeof document !== 'object' || !document.hooks
      || typeof document.hooks !== 'object' || Array.isArray(document.hooks)) {
      return;
    }
    let changed = false;
    for (const [event, groups] of Object.entries(document.hooks)) {
      if (!Array.isArray(groups)) continue;
      const retainedGroups = groups.map((group) => {
        if (!Array.isArray(group?.hooks)) return group;
        const hooks = group.hooks.filter(
          (hook) => !isObsoleteInstalledHook(hook?.command, root, homeDir),
        );
        if (hooks.length !== group.hooks.length) changed = true;
        return { ...group, hooks };
      }).filter((group) => !Array.isArray(group.hooks) || group.hooks.length > 0);
      if (retainedGroups.length > 0) document.hooks[event] = retainedGroups;
      else if (retainedGroups.length !== groups.length) {
        delete document.hooks[event];
        changed = true;
      }
    }
    if (changed) {
      writeHookConfigurationAtomically({
        file, root: realRoot, parent: realParent, sourceStat,
        content: `${JSON.stringify(document, null, 2)}\n`,
      });
      removed.push(file);
    }
  } catch (error) {
    if (error?.code === 'ELOOP') {
      throw new Error(`Refusing to follow symlinked hook configuration: ${file}`);
    }
    if (error?.code !== 'ENOENT') throw error;
  }
}

export function cleanupObsoleteLifecycleArtifacts({
  zylosDir,
  upgradeState,
  homeDir = os.homedir(),
}) {
  if (upgradeState !== 'committed') {
    throw new Error('Obsolete lifecycle artifacts may be removed only after the upgrade is durably committed.');
  }
  const root = requireZylosDir(zylosDir);
  const normalizedHome = requireZylosDir(homeDir);
  const removed = [];
  cleanupObsoleteHooksInFile(path.join(root, '.codex', 'hooks.json'), root, normalizedHome, removed);
  cleanupObsoleteHooksInFile(path.join(root, '.claude', 'settings.json'), root, normalizedHome, removed);
  for (const artifact of legacyLifecycleArtifactPaths(zylosDir)) {
    let stat;
    try {
      stat = fs.lstatSync(artifact);
    } catch (error) {
      if (error?.code === 'ENOENT') continue;
      throw error;
    }
    if (!stat.isFile() && !stat.isSymbolicLink() && !stat.isDirectory()) {
      throw new Error(`Refusing to remove unexpected lifecycle artifact type: ${artifact}`);
    }
    if (stat.isDirectory()) fs.rmSync(artifact, { recursive: true });
    else fs.unlinkSync(artifact);
    removed.push(artifact);
  }
  return Object.freeze({ removed: Object.freeze(removed) });
}
