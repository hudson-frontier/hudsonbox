import { inspect } from 'node:util';
import { schema } from './schema.js';
export type JsonValue = null | boolean | string | number | bigint | JsonValue[] | { [key: string]: JsonValue };
export interface Profile { kind: string; inner?: Profile; name?: string; wide?: boolean; min?: string; max?: string }
export interface Rule { type: Profile; required: boolean; nullable: boolean; omit_false: boolean; default?: JsonValue; reject_null?: boolean }
export interface Schema { models: Record<string, { fields: Record<string, Rule>; closed: boolean }>; codes: string[]; version: string }
export type ErrorKind = 'configuration' | 'request' | 'transport' | 'protocol' | 'http' | 'wait_timeout' | 'file' | 'integrity';
const messages: Record<ErrorKind, string> = {
  configuration: 'invalid private client configuration or credential file', request: 'invalid or oversized request',
  transport: 'transport failed; a sent mutation may have been admitted; retain its key and payload',
  protocol: 'invalid, oversized or inconsistent server response', http: 'HTTP request failed',
  wait_timeout: 'wait deadline reached; poll the same operation', file: 'local file operation failed; destination must not already exist',
  integrity: 'download integrity verification failed',
};
export class ClientError extends Error {
  readonly code?: string;
  readonly operationId?: string;
  constructor(readonly kind: ErrorKind, readonly status?: number, readonly rawCode?: string, operationId?: string) {
    super(messages[kind] + (kind === 'http' ? ` with status ${status}` : ''));
    this.name = 'ClientError';
    this.code = rawCode === undefined ? undefined : schema.codes.includes(rawCode) ? rawCode : 'unrecognized_problem_code';
    this.operationId = operationId && /^op_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(operationId) ? operationId : undefined;
  }
  [inspect.custom](): string { return `ClientError(${this.kind}${this.status === undefined ? '' : `, ${this.status}`})`; }
}
export function fail(kind: ErrorKind): never { throw new ClientError(kind); }
function object(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === 'object' && !Array.isArray(value); }
function text(value: unknown, kind: ErrorKind): string { if (typeof value !== 'string' || !value.isWellFormed()) fail(kind); return value; }
export function integer(value: unknown, kind: ErrorKind = 'request'): bigint {
  if (typeof value === 'bigint') return value;
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) fail(kind);
  return BigInt(value);
}
export function uint(value: string): bigint {
  if (!/^[0-9]{1,20}$/.test(value)) fail('protocol');
  const n = BigInt(value); if (n > 18446744073709551615n) fail('protocol'); return n;
}
// Keep lexical floating-point values distinct until typed model normalization.
class JsonFloat { constructor(readonly value: number) {} }
// Parse number tokens ourselves: JSON.parse would round u64 before a reviver sees it.
export function parseJson(input: Uint8Array | string): JsonValue {
  try {
    const source = typeof input === 'string' ? input : new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(input);
    let at = 0;
    const ws = () => { while (' \t\r\n'.includes(source[at] ?? '\0')) at++; };
    function string(): string {
      const start = at++;
      while (at < source.length) {
        const ch = source[at++];
        if (ch === '"') return text(JSON.parse(source.slice(start, at)), 'protocol');
        if (ch === '\\') at++;
      }
      return fail('protocol');
    }
    function value(depth: number): JsonValue {
      if (depth > 128) fail('protocol'); ws(); const ch = source[at];
      if (ch === '"') return string();
      if (ch === '{') {
        at++; ws(); const result: Record<string, JsonValue> = Object.create(null);
        if (source[at] === '}') { at++; return result; }
        while (true) {
          ws(); if (source[at] !== '"') fail('protocol'); const key = string();
          if (Object.hasOwn(result, key)) fail('protocol'); ws(); if (source[at++] !== ':') fail('protocol');
          result[key] = value(depth + 1); ws(); const delimiter = source[at++];
          if (delimiter === '}') return result; if (delimiter !== ',') fail('protocol');
        }
      }
      if (ch === '[') {
        at++; ws(); const result: JsonValue[] = [];
        if (source[at] === ']') { at++; return result; }
        while (true) {
          result.push(value(depth + 1)); ws(); const delimiter = source[at++];
          if (delimiter === ']') return result; if (delimiter !== ',') fail('protocol');
        }
      }
      for (const [literal, result] of [['true', true], ['false', false], ['null', null]] as const) {
        if (source.startsWith(literal, at)) { at += literal.length; return result; }
      }
      const token = /^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/.exec(source.slice(at))?.[0];
      if (!token) fail('protocol'); at += token.length;
      if (!/[.eE]/.test(token)) {
        if (token.replace('-', '').length > 20) fail('protocol');
        const n = BigInt(token); return n >= BigInt(Number.MIN_SAFE_INTEGER) && n <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(n) : n;
      }
      const n = Number(token);
      if (!Number.isFinite(n) || (Number.isInteger(n) && !Number.isSafeInteger(n))) fail('protocol');
      return new JsonFloat(n) as unknown as number;
    }
    const result = value(0); ws(); if (at !== source.length) fail('protocol'); return result;
  } catch { return fail('protocol'); }
}
export function stringify(value: unknown): string {
  function encode(v: unknown, depth: number): string {
    if (depth > 128) fail('request');
    if (v instanceof JsonFloat) return String(v.value);
    if (v === null) return 'null';
    if (typeof v === 'string') return JSON.stringify(text(v, 'request'));
    if (typeof v === 'boolean' || typeof v === 'bigint') return String(v);
    if (typeof v === 'number') {
      if (!Number.isFinite(v) || (Number.isInteger(v) && !Number.isSafeInteger(v))) fail('request'); return String(v);
    }
    if (Array.isArray(v)) return '[' + v.map(x => encode(x, depth + 1)).join(',') + ']';
    if (!object(v)) fail('request');
    return '{' + Object.keys(v).map(k => JSON.stringify(text(k, 'request')) + ':' + encode(v[k], depth + 1)).join(',') + '}';
  }
  return encode(value, 0);
}
export function model(name: string, input: unknown, decode = true, depth = 0): Record<string, JsonValue> {
  const kind = decode ? 'protocol' : 'request'; if (depth > 128 || !object(input)) fail(kind);
  const spec = schema.models[name]; if (!spec) fail(kind);
  if (spec.closed && Object.keys(input).some(k => !Object.hasOwn(spec.fields, k))) fail(kind);
  const result: Record<string, JsonValue> = Object.create(null);
  for (const [key, rule] of Object.entries(spec.fields)) {
    const present = Object.hasOwn(input, key) && input[key] !== undefined;
    if (!present && rule.required) fail(kind);
    const item = present ? input[key] : Object.hasOwn(rule, 'default') ? rule.default : null;
    if (present && item === null && rule.reject_null) fail(kind);
    if (item === null && !rule.required && !Object.hasOwn(rule, 'default')) {
      if (rule.nullable) result[key] = null;
      continue;
    }
    const converted = normalize(rule.type, item, decode, depth + 1);
    if (converted !== false || !rule.omit_false) result[key] = converted;
  }
  return result;
}
function normalize(p: Profile, v: unknown, decode: boolean, depth: number): JsonValue {
  const kind = decode ? 'protocol' : 'request'; if (depth > 128) fail(kind);
  switch (p.kind) {
    case 'nullable': return v === null ? null : normalize(p.inner!, v, decode, depth + 1);
    case 'ref': return model(p.name!, v, decode, depth + 1);
    case 'string': return text(v, kind);
    case 'boolean': if (typeof v !== 'boolean') fail(kind); return v;
    case 'integer': {
      const n = integer(v, kind); if (n < BigInt(p.min!) || n > BigInt(p.max!)) fail(kind); return p.wide ? n : Number(n);
    }
    case 'array': if (!Array.isArray(v)) fail(kind); return v.map(x => normalize(p.inner!, x, decode, depth + 1));
    case 'map': {
      if (!object(v)) fail(kind); const result: Record<string, JsonValue> = Object.create(null);
      for (const k of Object.keys(v).sort()) result[text(k, kind)] = normalize(p.inner!, v[k], decode, depth + 1);
      return result;
    }
    case 'json': {
      if (v instanceof JsonFloat) return v.value;
      if (v === null || typeof v === 'boolean') return v;
      if (typeof v === 'string') return text(v, kind);
      if (typeof v === 'number' && !Number.isInteger(v)) { if (!Number.isFinite(v)) fail(kind); return v; }
      if (typeof v === 'number' || typeof v === 'bigint') {
        const n = integer(v, kind); if (n < -9223372036854775808n || n > 18446744073709551615n) fail(kind); return v;
      }
      if (Array.isArray(v)) return v.map(x => normalize(p, x, decode, depth + 1));
      if (!object(v)) fail(kind); const result: Record<string, JsonValue> = Object.create(null);
      for (const k of Object.keys(v)) result[text(k, kind)] = normalize(p, v[k], decode, depth + 1);
      return result;
    }
    default: return fail(kind);
  }
}
export type Headers = Record<string, string[]>;
export function one(headers: Headers, name: string): string {
  const values = headers[name.toLowerCase()]; if (!values || values.length !== 1) fail('protocol'); return values[0]!;
}
function boolean(s: string): boolean { if (s !== 'true' && s !== 'false') fail('protocol'); return s === 'true'; }
export class RangeChunk {
  seen?: bigint; truncated?: boolean; sha256?: string; guest_reported?: boolean;
  constructor(readonly data: Uint8Array, readonly offset: bigint, readonly next_offset: bigint, readonly size: bigint, readonly eof: boolean, readonly simulated: boolean) {}
  [inspect.custom](): string { return `RangeChunk(bytes=${this.data.byteLength}, offset=${this.offset})`; }
}
export function rangeChunk(headers: Headers, data: Uint8Array, prefix: string, offset: bigint, limit: bigint): RangeChunk {
  const read = (field: string) => one(headers, `x-${prefix}-${field}`);
  const chunk = new RangeChunk(data, uint(read('offset')), uint(read('next-offset')), uint(read('size')), boolean(read('eof')), boolean(read('simulated')));
  if (prefix === 'output') {
    chunk.seen = uint(read('seen')); chunk.truncated = boolean(read('truncated'));
    if (chunk.size > 10485760n || chunk.seen < chunk.size || chunk.truncated !== (chunk.seen > chunk.size)) fail('protocol');
  } else {
    chunk.sha256 = read('sha256'); chunk.guest_reported = boolean(read('guest-reported'));
    if (chunk.size > 8388608n || !/^[0-9a-f]{64}$/.test(chunk.sha256) || !chunk.guest_reported) fail('protocol');
  }
  if (chunk.offset !== offset || chunk.next_offset !== offset + BigInt(data.byteLength) || chunk.next_offset > chunk.size || chunk.eof !== (chunk.next_offset === chunk.size) || BigInt(data.byteLength) > limit || (!chunk.eof && !data.byteLength)) fail('protocol');
  return chunk;
}
