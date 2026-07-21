import fs from 'node:fs';
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

function isObsoleteInstalledHook(command, root) {
  if (typeof command !== 'string') return false;
  const normalized = command.replaceAll('\\', '/');
  return OBSOLETE_HOOK_BASE_KEYS.some((key) => normalized.includes(
    path.join(root, '.claude', key).replaceAll('\\', '/'),
  ));
}

function cleanupObsoleteHooksInFile(file, root, removed) {
  try {
    const document = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (!document || typeof document !== 'object' || Array.isArray(document)
      || !document.hooks || typeof document.hooks !== 'object' || Array.isArray(document.hooks)) {
      return;
    }
    let changed = false;
    for (const [event, groups] of Object.entries(document.hooks)) {
      if (!Array.isArray(groups)) continue;
      const retainedGroups = groups.map((group) => {
        if (!Array.isArray(group?.hooks)) return group;
        const hooks = group.hooks.filter(
          (hook) => !isObsoleteInstalledHook(hook?.command, root),
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
      fs.writeFileSync(file, `${JSON.stringify(document, null, 2)}\n`, { mode: 0o600 });
      removed.push(file);
    }
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
  }
}

export function cleanupObsoleteLifecycleArtifacts({ zylosDir, upgradeState }) {
  if (upgradeState !== 'committed') {
    throw new Error('Obsolete lifecycle artifacts may be removed only after the upgrade is durably committed.');
  }
  const root = requireZylosDir(zylosDir);
  const removed = [];
  cleanupObsoleteHooksInFile(path.join(root, '.codex', 'hooks.json'), root, removed);
  cleanupObsoleteHooksInFile(path.join(root, '.claude', 'settings.json'), root, removed);
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
