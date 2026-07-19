import { ContractKernelError, createContractError } from './errors.js';

function reject(message) {
  throw new ContractKernelError(createContractError({
    code: 'validation_error',
    userMessage: message,
  }));
}

function assertWellFormedUnicode(value) {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!Number.isInteger(next) || next < 0xdc00 || next > 0xdfff) {
        reject('JCS input contains invalid Unicode.');
      }
      index += 1;
    } else if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) {
      reject('JCS input contains invalid Unicode.');
    }
  }
}

function serializePrimitive(value) {
  if (typeof value === 'string') {
    assertWellFormedUnicode(value);
    return JSON.stringify(value);
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) reject('JCS numbers must be finite.');
    return JSON.stringify(value);
  }
  if (value === null || typeof value === 'boolean') return JSON.stringify(value);
  reject(`JCS input contains unsupported value type ${typeof value}.`);
}

function assertPlainJsonObject(value) {
  const prototype = Object.getPrototypeOf(value);
  const constructor = prototype === null
    ? null
    : Object.getOwnPropertyDescriptor(prototype, 'constructor')?.value;
  const isCrossRealmPlainObject = prototype !== null
    && Object.getPrototypeOf(prototype) === null
    && typeof constructor === 'function'
    && constructor.name === 'Object';
  if (prototype !== Object.prototype && prototype !== null && !isCrossRealmPlainObject) {
    reject('JCS objects must be plain JSON objects.');
  }
  if (Object.getOwnPropertySymbols(value).length > 0) {
    reject('JCS objects must not contain symbol properties.');
  }
}

export function canonicalizeJson(value) {
  const ancestors = new Set();

  function serialize(current) {
    if (current === null || typeof current !== 'object') return serializePrimitive(current);
    if (ancestors.has(current)) reject('JCS input must not contain cycles.');
    ancestors.add(current);

    let result;
    if (Array.isArray(current)) {
      for (let index = 0; index < current.length; index += 1) {
        if (!Object.hasOwn(current, index)) reject('JCS arrays must not be sparse.');
      }
      result = `[${current.map((entry) => serialize(entry)).join(',')}]`;
    } else {
      assertPlainJsonObject(current);
      const entries = [];
      for (const key of Object.keys(current).sort()) {
        assertWellFormedUnicode(key);
        const descriptor = Object.getOwnPropertyDescriptor(current, key);
        if (!descriptor?.enumerable || descriptor.get || descriptor.set) {
          reject('JCS objects must contain enumerable data properties only.');
        }
        entries.push(`${JSON.stringify(key)}:${serialize(descriptor.value)}`);
      }
      result = `{${entries.join(',')}}`;
    }

    ancestors.delete(current);
    return result;
  }

  return serialize(value);
}

export function canonicalizeJsonBytes(value) {
  return Buffer.from(canonicalizeJson(value), 'utf8');
}
