import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';

function closeChild(child, graceMs) {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve();
  return new Promise((resolve) => {
    let settled = false;
    let timer = null;
    const finish = () => {
      if (settled) return;
      settled = true;
      if (timer !== null) clearTimeout(timer);
      resolve();
    };
    child.once('close', finish);
    child.kill('SIGTERM');
    timer = setTimeout(() => {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    }, graceMs);
    timer.unref?.();
  });
}

export function createExecutorPrerequisiteOwner({
  zylosDir,
  releasePath,
  spawnFn = spawn,
  existsSync = fs.existsSync,
  closeGraceMs = 5_000,
} = {}) {
  if (typeof zylosDir !== 'string' || !path.isAbsolute(zylosDir)
    || path.parse(zylosDir).root === zylosDir) {
    throw new TypeError('zylosDir must be an explicit absolute non-root path');
  }
  if (typeof releasePath !== 'string' || !path.isAbsolute(releasePath)
    || path.parse(releasePath).root === releasePath) {
    throw new TypeError('releasePath must be an explicit absolute non-root path');
  }
  const releaseRoot = path.resolve(releasePath);
  const children = new Map();
  let started = false;
  let closing = false;
  let failure = null;

  function descriptors() {
    const skills = path.join(releaseRoot, 'skills');
    const values = [
      {
        name: 'scheduler',
        command: process.execPath,
        args: [path.join(skills, 'scheduler', 'scripts', 'daemon.js')],
        cwd: zylosDir,
      },
      {
        name: 'web-console',
        command: process.execPath,
        args: [path.join(skills, 'web-console', 'scripts', 'server.js')],
        cwd: zylosDir,
      },
    ];
    const caddy = path.join(zylosDir, 'bin', 'caddy');
    const caddyfile = path.join(zylosDir, 'http', 'Caddyfile');
    if (existsSync(caddy) && existsSync(caddyfile)) {
      values.push({
        name: 'caddy', command: caddy,
        args: ['run', '--config', caddyfile, '--adapter', 'caddyfile'],
        cwd: zylosDir,
      });
    }
    return values.filter(({ command, args }) => (
      command === process.execPath ? existsSync(args[0]) : existsSync(command)
    ));
  }

  async function start() {
    if (started) return health();
    for (const descriptor of descriptors()) {
      const child = spawnFn(descriptor.command, descriptor.args, {
        cwd: descriptor.cwd,
        env: { ...process.env, ZYLOS_DIR: zylosDir, NODE_ENV: 'production' },
        stdio: 'inherit',
      });
      await new Promise((resolve, reject) => {
        child.once('spawn', resolve);
        child.once('error', reject);
      });
      children.set(descriptor.name, child);
      child.once('exit', (code, signal) => {
        if (!closing) {
          failure = new Error(
            `Executor prerequisite ${descriptor.name} exited ${code ?? signal}.`,
          );
        }
      });
    }
    started = true;
    return health();
  }

  function health() {
    return Object.freeze({
      ok: failure === null,
      error: failure?.message ?? null,
      services: Object.freeze([...children.keys()]),
    });
  }

  async function close() {
    if (closing) return;
    closing = true;
    await Promise.all([...children.values()].map((child) => closeChild(child, closeGraceMs)));
    children.clear();
  }

  return Object.freeze({ close, health, start });
}
