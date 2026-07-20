/** Isolated one-time exact-base provider suspension. Normal runtime never imports this module. */

function sessionFor(provider) {
  if (provider === 'claude') return 'claude-main';
  if (provider === 'codex') return 'codex-main';
  throw new TypeError('provider must be claude or codex');
}

function run(execFileSyncFn, file, args, options = {}) {
  return execFileSyncFn(file, args, {
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: 30_000, ...options,
  });
}

export function createLegacyProviderQuiescence({
  provider,
  execFileSyncFn,
  tmuxArgsPrefix = [],
  signalProcess = (pid, signal) => process.kill(pid, signal),
}) {
  if (typeof execFileSyncFn !== 'function') throw new TypeError('execFileSyncFn is required');
  if (typeof signalProcess !== 'function') throw new TypeError('signalProcess must be a function');
  if (!Array.isArray(tmuxArgsPrefix) || tmuxArgsPrefix.some((value) => typeof value !== 'string')) {
    throw new TypeError('tmuxArgsPrefix must be string arguments');
  }
  const session = sessionFor(provider);
  const tmux = (args, options) => run(execFileSyncFn, 'tmux', [...tmuxArgsPrefix, ...args], options);

  function inspect() {
    try {
      tmux(['has-session', '-t', `=${session}`]);
    } catch (error) {
      if (error?.status === 1) return Object.freeze({ active: false, session });
      throw error;
    }
    const sessions = String(tmux(['list-sessions', '-F', '#{session_name}']))
      .trim().split('\n').filter(Boolean);
    if (sessions.length !== 1 || sessions[0] !== session) {
      throw new Error('Refusing to suspend a shared or ambiguous legacy tmux server.');
    }
    const serverPid = Number(String(tmux(['display-message', '-p', '#{pid}'])).trim());
    if (!Number.isSafeInteger(serverPid) || serverPid <= 1) {
      throw new Error('Legacy provider tmux server identity is invalid.');
    }
    const panes = String(tmux([
      'list-panes', '-t', `=${session}`, '-F', '#{session_name}\t#{pane_pid}',
    ])).trim().split('\n').filter(Boolean);
    if (panes.length !== 1) throw new Error(`Legacy provider session ${session} is not single-pane owned.`);
    const [actualSession, pidText] = panes[0].split('\t');
    const panePid = Number(pidText);
    if (actualSession !== session || !Number.isSafeInteger(panePid) || panePid <= 1) {
      throw new Error(`Legacy provider session ${session} has invalid ownership facts.`);
    }
    const rows = String(run(execFileSyncFn, 'ps', ['-axo', 'pid=,ppid='])).trim()
      .split('\n').map((line) => line.trim().split(/\s+/).map(Number));
    const processIds = [panePid];
    for (let index = 0; index < processIds.length; index += 1) {
      for (const [pid, parentPid] of rows) {
        if (parentPid === processIds[index] && !processIds.includes(pid)) processIds.push(pid);
      }
    }
    return Object.freeze({
      active: true, session, server_pid: serverPid,
      pane_pid: panePid, process_ids: Object.freeze(processIds),
    });
  }

  function suspend(record = null) {
    const observed = record ?? inspect();
    if (!observed.active) return observed;
    signalProcess(observed.server_pid, 'SIGSTOP');
    for (const pid of [...observed.process_ids].reverse()) {
      signalProcess(pid, 'SIGSTOP');
    }
    return Object.freeze({ ...observed, suspended: true });
  }

  function resume(record) {
    if (record?.active !== true) return Object.freeze({ resumed: false, session });
    if (record.session !== session || !Number.isSafeInteger(record.server_pid)
      || !Number.isSafeInteger(record.pane_pid)
      || !Array.isArray(record.process_ids)
      || record.process_ids.some((pid) => !Number.isSafeInteger(pid) || pid <= 1)) {
      throw new Error('Legacy provider resume ownership record is invalid.');
    }
    signalProcess(record.server_pid, 'SIGCONT');
    for (const pid of record.process_ids) {
      try { signalProcess(pid, 'SIGCONT'); } catch (error) {
        if (error?.code !== 'ESRCH') throw error;
      }
    }
    return Object.freeze({
      resumed: true, session, pane_pid: record.pane_pid,
      process_ids: Object.freeze([...record.process_ids]),
    });
  }

  function commit(record) {
    if (record?.active !== true) return Object.freeze({ removed: false, session });
    if (record.session !== session || !Number.isSafeInteger(record.server_pid)) {
      throw new Error('Legacy provider commit ownership record is invalid.');
    }
    try {
      signalProcess(record.server_pid, 'SIGCONT');
    } catch (error) {
      if (error?.code === 'ESRCH') {
        return Object.freeze({ removed: true, session, already_removed: true });
      }
      throw error;
    }
    try {
      tmux(['kill-session', '-t', `=${session}`]);
    } catch (error) {
      if (error?.status !== 1) throw error;
    }
    return Object.freeze({ removed: true, session });
  }

  return Object.freeze({ inspect, suspend, resume, commit });
}
