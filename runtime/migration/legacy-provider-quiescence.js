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

function processRows(execFileSyncFn) {
  const output = String(run(execFileSyncFn, 'ps', [
    '-axo', 'pid=,ppid=,pgid=,sess=,state=',
  ])).trim();
  if (output.length === 0) return [];
  return output.split('\n').map((line) => {
    const fields = line.trim().split(/\s+/);
    return {
      pid: Number(fields[0]), ppid: Number(fields[1]), pgid: Number(fields[2]),
      sid: Number(fields[3]), state: fields[4],
    };
  }).filter(({ pid, ppid, pgid, sid, state }) => (
    Number.isSafeInteger(pid) && pid > 0 && Number.isSafeInteger(ppid)
    && Number.isSafeInteger(pgid) && pgid > 0 && Number.isSafeInteger(sid)
    && typeof state === 'string' && state.length > 0
  ));
}

function descendants(rows, rootPid) {
  const byParent = new Map();
  for (const row of rows) {
    const children = byParent.get(row.ppid) ?? [];
    children.push(row);
    byParent.set(row.ppid, children);
  }
  const result = [];
  const pending = [rootPid];
  const seen = new Set();
  while (pending.length > 0) {
    const pid = pending.shift();
    if (seen.has(pid)) continue;
    seen.add(pid);
    const row = rows.find((candidate) => candidate.pid === pid);
    if (row) result.push(row);
    for (const child of byParent.get(pid) ?? []) pending.push(child.pid);
  }
  return result;
}

function requireCompleteProcessGroup(rows, members, processGroupId, operation) {
  const currentGroup = rows.filter(({ pgid }) => pgid === processGroupId);
  if (currentGroup.length !== members.length
    || currentGroup.some(({ pid }) => !members.some((member) => member.pid === pid))) {
    throw new Error(`Legacy provider process group has unowned members before ${operation}.`);
  }
}

function birthIdentity(execFileSyncFn, pid) {
  let value;
  try {
    value = String(run(execFileSyncFn, 'ps', ['-o', 'lstart=', '-p', String(pid)])).trim();
  } catch (error) {
    if (error?.status === 1 || error?.code === 'ESRCH') {
      throw Object.assign(new Error(`Process ${pid} disappeared.`), { code: 'ESRCH' });
    }
    throw error;
  }
  if (value.length === 0) throw Object.assign(new Error(`Process ${pid} disappeared.`), { code: 'ESRCH' });
  return value;
}

function ownedIdentity(execFileSyncFn, row) {
  return Object.freeze({ ...row, birth_identity: birthIdentity(execFileSyncFn, row.pid) });
}

function validateRecord(record, session) {
  if (record?.active !== true || record.session !== session
    || !record.server || !record.pane || !Array.isArray(record.members)
    || !Number.isSafeInteger(record.process_group_id) || record.process_group_id <= 1) {
    throw new Error('Legacy provider ownership record is invalid.');
  }
  for (const identity of [record.server, record.pane, ...record.members]) {
    if (!Number.isSafeInteger(identity?.pid) || identity.pid <= 1
      || !Number.isSafeInteger(identity.ppid) || !Number.isSafeInteger(identity.pgid)
      || !Number.isSafeInteger(identity.sid)
      || typeof identity.birth_identity !== 'string' || identity.birth_identity.length === 0) {
      throw new Error('Legacy provider process identity is invalid.');
    }
  }
}

export function createLegacyProviderQuiescence({
  provider,
  execFileSyncFn,
  tmuxArgsPrefix = [],
  signalProcess = (pid, signal) => process.kill(pid, signal),
  wait = (milliseconds) => Atomics.wait(
    new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds,
  ),
}) {
  if (typeof execFileSyncFn !== 'function') throw new TypeError('execFileSyncFn is required');
  if (typeof signalProcess !== 'function') throw new TypeError('signalProcess must be a function');
  if (typeof wait !== 'function') throw new TypeError('wait must be a function');
  if (!Array.isArray(tmuxArgsPrefix) || tmuxArgsPrefix.some((value) => typeof value !== 'string')) {
    throw new TypeError('tmuxArgsPrefix must be string arguments');
  }
  const session = sessionFor(provider);
  const tmux = (args, options) => run(execFileSyncFn, 'tmux', [...tmuxArgsPrefix, ...args], options);

  function currentIdentity(expected, { mismatchIsMissing = false } = {}) {
    const row = processRows(execFileSyncFn).find(({ pid }) => pid === expected.pid);
    if (!row) return null;
    let birth;
    try { birth = birthIdentity(execFileSyncFn, expected.pid); } catch (error) {
      if (error?.code === 'ESRCH') return null;
      throw error;
    }
    if (row.ppid !== expected.ppid || row.pgid !== expected.pgid || row.sid !== expected.sid
      || birth !== expected.birth_identity) {
      if (mismatchIsMissing) return null;
      throw new Error(`Legacy provider PID ${expected.pid} was reused or changed identity.`);
    }
    return { ...row, birth_identity: birth };
  }

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
    const panes = String(tmux([
      'list-panes', '-t', `=${session}`, '-F', '#{session_name}\t#{pane_pid}',
    ])).trim().split('\n').filter(Boolean);
    if (panes.length !== 1) throw new Error(`Legacy provider session ${session} is not single-pane owned.`);
    const [actualSession, pidText] = panes[0].split('\t');
    const panePid = Number(pidText);
    if (actualSession !== session || !Number.isSafeInteger(serverPid) || serverPid <= 1
      || !Number.isSafeInteger(panePid) || panePid <= 1) {
      throw new Error(`Legacy provider session ${session} has invalid ownership facts.`);
    }
    const rows = processRows(execFileSyncFn);
    const serverRow = rows.find(({ pid }) => pid === serverPid);
    const paneRow = rows.find(({ pid }) => pid === panePid);
    if (!serverRow || !paneRow || paneRow.ppid !== serverPid || paneRow.pgid <= 1) {
      throw new Error(`Legacy provider session ${session} process ownership changed.`);
    }
    const tree = descendants(rows, panePid);
    if (tree.length === 0 || tree.some(({ pgid }) => pgid !== paneRow.pgid)) {
      throw new Error('Legacy provider process tree must occupy one dedicated process group.');
    }
    requireCompleteProcessGroup(rows, tree, paneRow.pgid, 'inspection');
    const members = tree.map((row) => ownedIdentity(execFileSyncFn, row));
    return Object.freeze({
      active: true, session, process_group_id: paneRow.pgid,
      server: ownedIdentity(execFileSyncFn, serverRow),
      pane: members.find(({ pid }) => pid === panePid),
      members: Object.freeze(members),
    });
  }

  function hasExactSession() {
    try {
      tmux(['has-session', '-t', `=${session}`], { timeout: 1_000 });
      return true;
    } catch (error) {
      if (error?.status === 1) return false;
      throw error;
    }
  }

  function stableStoppedTree(record) {
    let previous = null;
    let identities = record.members.map((member) => ({ ...member }));
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const rows = processRows(execFileSyncFn);
      const tree = descendants(rows, record.pane.pid);
      if (tree.length === 0) throw new Error('Legacy provider pane disappeared while suspending.');
      requireCompleteProcessGroup(rows, tree, record.process_group_id, 'suspension');
      for (const row of tree) {
        const existing = identities.find(({ pid }) => pid === row.pid);
        if (existing) currentIdentity(existing);
        else identities.push(ownedIdentity(execFileSyncFn, row));
      }
      for (const row of tree.filter(({ state }) => !state.startsWith('T'))) {
        const identity = identities.find(({ pid }) => pid === row.pid);
        currentIdentity(identity);
        signalProcess(row.pid, 'SIGSTOP');
      }
      const signature = tree.map(({ pid, pgid, state }) => `${pid}:${pgid}:${state[0]}`).sort().join(',');
      if (tree.every(({ state }) => state.startsWith('T')) && signature === previous) {
        const currentPids = new Set(tree.map(({ pid }) => pid));
        return Object.freeze(identities
          .filter(({ pid }) => currentPids.has(pid))
          .map((identity) => Object.freeze({ ...identity })));
      }
      previous = signature;
      wait(10);
    }
    throw new Error('Legacy provider process tree did not reach a stable stopped state.');
  }

  function suspend(record = null, { onPhase = () => {} } = {}) {
    const observed = record ?? inspect();
    if (!observed.active) return observed;
    validateRecord(observed, session);
    onPhase('suspending', observed);
    if (currentIdentity(observed.server) === null || currentIdentity(observed.pane) === null) {
      throw new Error('Legacy provider server or pane disappeared before suspension.');
    }
    signalProcess(observed.server.pid, 'SIGSTOP');
    for (const member of observed.members) {
      currentIdentity(member);
      signalProcess(member.pid, 'SIGSTOP');
    }
    const members = stableStoppedTree(observed);
    const suspended = Object.freeze({ ...observed, members, suspended: true });
    onPhase('suspended', suspended);
    return suspended;
  }

  function resume(record, { phase = 'suspended', onPhase = () => {} } = {}) {
    if (record?.active !== true) return Object.freeze({ resumed: false, session });
    validateRecord(record, session);
    if (!['suspended', 'resuming', 'resumed'].includes(phase)) {
      throw new Error('Legacy provider resume phase is invalid.');
    }
    const currentServer = currentIdentity(record.server);
    const currentPane = currentIdentity(record.pane);
    if (currentServer === null || currentPane === null) {
      throw new Error('Legacy provider resume requires its exact server and pane identities.');
    }
    const rows = processRows(execFileSyncFn);
    const currentTree = descendants(rows, record.pane.pid);
    const expectedMembers = phase === 'suspended' ? record.members : currentTree;
    requireCompleteProcessGroup(rows, expectedMembers, record.process_group_id, 'resume');
    const currentMembers = [];
    for (const member of expectedMembers) {
      const recorded = record.members.find(({ pid }) => pid === member.pid);
      const current = recorded === undefined
        ? ownedIdentity(execFileSyncFn, member) : currentIdentity(recorded);
      if (current === null) {
        throw new Error('Legacy provider resume requires every recorded process identity.');
      }
      currentMembers.push(current);
    }
    const resumedRecord = Object.freeze({
      ...record,
      members: Object.freeze(currentMembers.map((member) => Object.freeze({ ...member }))),
    });
    const allStopped = currentServer.state.startsWith('T')
      && currentPane.state.startsWith('T')
      && currentMembers.every(({ state }) => state.startsWith('T'));
    const allRunning = !currentServer.state.startsWith('T')
      && !currentPane.state.startsWith('T')
      && currentMembers.every(({ state }) => !state.startsWith('T'));
    if (phase === 'suspended' && !allStopped) {
      throw new Error('Legacy provider suspended phase requires every exact process to remain stopped.');
    }
    if (phase === 'resumed') {
      if (!allRunning) {
        throw new Error('Legacy provider resumed phase requires every exact process to be running.');
      }
      const alreadyResumed = inspect();
      if (!alreadyResumed.active
        || alreadyResumed.server.birth_identity !== record.server.birth_identity
        || alreadyResumed.pane.birth_identity !== record.pane.birth_identity) {
        throw new Error('Legacy provider resumed phase lost its exact recorded session.');
      }
      return Object.freeze({
        resumed: true, session, process_group_id: record.process_group_id,
        already_resumed: true,
      });
    }
    if (phase !== 'resuming') onPhase('resuming', resumedRecord);
    for (const member of currentMembers.filter(({ state }) => state.startsWith('T'))) {
      currentIdentity(member);
      signalProcess(member.pid, 'SIGCONT');
    }
    if (currentServer.state.startsWith('T')) {
      currentIdentity(record.server);
      signalProcess(record.server.pid, 'SIGCONT');
    }
    const observed = inspect();
    if (!observed.active || observed.server.birth_identity !== record.server.birth_identity
      || observed.pane.birth_identity !== record.pane.birth_identity) {
      throw new Error('Legacy provider did not resume its exact recorded session.');
    }
    const result = Object.freeze({ resumed: true, session, process_group_id: record.process_group_id });
    onPhase('resumed', resumedRecord);
    return result;
  }

  function exactSurvivors(record, options) {
    return record.members.filter((member) => currentIdentity(member, options) !== null);
  }

  function waitForExit(record, timeoutMs, identityOptions) {
    const deadline = Date.now() + timeoutMs;
    let survivors = exactSurvivors(record, identityOptions);
    while (survivors.length > 0 && Date.now() < deadline) {
      wait(10);
      survivors = exactSurvivors(record, identityOptions);
    }
    return survivors;
  }

  function waitForIdentityExit(identity, timeoutMs, identityOptions) {
    const deadline = Date.now() + timeoutMs;
    let survivor = currentIdentity(identity, identityOptions);
    while (survivor !== null && Date.now() < deadline) {
      wait(10);
      survivor = currentIdentity(identity, identityOptions);
    }
    return survivor;
  }

  function commit(record, { phase = 'suspended', onPhase = () => {} } = {}) {
    if (record?.active !== true) return Object.freeze({ removed: false, session });
    validateRecord(record, session);
    const reconcilingCommit = ['committing', 'removed'].includes(phase);
    let server = currentIdentity(record.server, { mismatchIsMissing: reconcilingCommit });
    // A suspended exact server cannot answer a client query. Its validated
    // process identity is sufficient until it is continued below.
    const sessionActive = server !== null || hasExactSession();
    const identityOptions = { mismatchIsMissing: reconcilingCommit && !sessionActive };
    if (server === null && sessionActive) server = currentIdentity(record.server, identityOptions);
    const initialSurvivors = exactSurvivors(record, identityOptions);
    if (phase === 'removed' && !sessionActive && server === null
      && initialSurvivors.length === 0) {
      return Object.freeze({ removed: true, session, already_removed: true });
    }
    if (!reconcilingCommit
      && (server === null || initialSurvivors.length !== record.members.length)) {
      throw new Error('Legacy provider ownership became partial before commit.');
    }
    if (phase !== 'removed') onPhase('committing', record);
    if (server !== null) {
      signalProcess(record.server.pid, 'SIGCONT');
      try {
        tmux(['kill-session', '-t', `=${session}`]);
      } catch (error) {
        if (error?.status !== 1) throw error;
      }
    }
    let survivors = exactSurvivors(record, identityOptions);
    for (const member of survivors) signalProcess(member.pid, 'SIGTERM');
    for (const member of survivors) {
      try { signalProcess(member.pid, 'SIGCONT'); } catch (error) {
        if (error?.code !== 'ESRCH') throw error;
      }
    }
    survivors = waitForExit(record, 1_000, identityOptions);
    for (const member of survivors) {
      currentIdentity(member, identityOptions);
      signalProcess(member.pid, 'SIGKILL');
    }
    survivors = waitForExit(record, 1_000, identityOptions);
    let serverSurvivor = currentIdentity(record.server, identityOptions);
    if (serverSurvivor !== null) {
      signalProcess(serverSurvivor.pid, 'SIGTERM');
      serverSurvivor = waitForIdentityExit(record.server, 500, identityOptions);
    }
    if (serverSurvivor !== null) {
      signalProcess(serverSurvivor.pid, 'SIGKILL');
      serverSurvivor = waitForIdentityExit(record.server, 500, identityOptions);
    }
    if (survivors.length > 0 || serverSurvivor !== null) {
      throw new Error('Legacy provider processes remained after commit cleanup.');
    }
    try {
      tmux(['has-session', '-t', `=${session}`]);
      throw new Error('Legacy provider session remained after commit cleanup.');
    } catch (error) {
      if (error?.status !== 1) throw error;
    }
    onPhase('removed', record);
    return Object.freeze({ removed: true, session });
  }

  return Object.freeze({ inspect, suspend, resume, commit });
}
