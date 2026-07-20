import { spawn } from 'node:child_process';

export function runForwardedNode(entry, args = [], options = {}) {
  const child = spawn(process.execPath, [entry, ...args], {
    ...options,
    stdio: 'inherit',
  });
  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.once(signal, () => {
      if (!child.killed) child.kill(signal);
    });
  }
  child.once('error', (error) => {
    console.error(error);
    process.exitCode = 1;
  });
  child.once('exit', (code, signal) => {
    process.exitCode = Number.isInteger(code) ? code : (signal ? 1 : 0);
  });
  return child;
}
