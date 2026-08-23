import { bridgeResponse } from './bridge';
import { decodeSecret, deriveCapability, randomToken, validateHostname, validToken } from './capability';
import { FrameType, encodeFrame, parseHello } from './frame';
import { DEFAULT_LIMITS } from './limits';
import { publicResponse } from './public-site';
import { RelayCore, type TelegramConnectorLike } from './relay-core';
import { TelegramConnector } from './mtproxy';
import { CloudflareTelegramDialer } from './tcp';
import { WebSocketBatcher } from './ws-batcher';
import { IdleLiveness, SerializedInboundQueue, readBoundedBody } from './session-guards';

interface BootstrapEntry {
  expiresAt: number;
  issuanceIp: string;
  used: boolean;
  bodyDigest?: string;
  sessionToken?: string;
}

interface SessionConfig {
  token: string;
  clientIp: string;
  expiresAt: number;
}

const INTERNAL_AUTH = 'X-Tproxy-Internal-Token';
const BOOTSTRAP_TTL_MS = 2 * 60 * 1000;
const SESSION_TTL_MS = 6 * 60 * 60 * 1000;
const CREATE_BODY_LIMIT = 64;
const CREATE_BODY_DEADLINE_MS = 10_000;
const MAX_BOOTSTRAPS = 512;
const BOOTSTRAP_RATE_WINDOW_MS = 60_000;
const BOOTSTRAP_RATE_BURST = 256;
const IDLE_PERIOD_MS = 75_000;

type ConnectorFactory = (secret: Uint8Array) => TelegramConnectorLike;
const connectorFactories = new WeakMap<RelaySession, ConnectorFactory>();

/** Internal test seam: the factory is attached to an in-process DO instance, never to a request. */
export function installRelaySessionTestFactory(instance: RelaySession, factory: ConnectorFactory): void {
  connectorFactories.set(instance, factory);
}

function title(env: Env): string { return env.PUBLIC_SITE_TITLE || 'Public Site'; }
function hidden(env: Env): Response { return publicResponse(title(env), 404); }

function bearer(request: Request): string | null {
  const value = request.headers.get('Authorization');
  if (!value?.startsWith('Bearer ')) return null;
  const token = value.slice(7);
  return validToken(token) ? token : null;
}

function internalRequest(path: string, token: string, init: RequestInit = {}): Request {
  const headers = new Headers(init.headers);
  headers.set(INTERNAL_AUTH, token);
  return new Request(`https://internal${path}`, { ...init, headers });
}

async function sha256Base64(bytes: Uint8Array): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', Uint8Array.from(bytes).buffer));
  let binary = '';
  for (const byte of digest) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function constantTimeEqual(left: string, right: string): boolean {
  const a = new TextEncoder().encode(left);
  const b = new TextEncoder().encode(right);
  let difference = a.byteLength ^ b.byteLength;
  const length = Math.max(a.byteLength, b.byteLength);
  for (let index = 0; index < length; index++) difference |= (a[index] ?? 0) ^ (b[index] ?? 0);
  return difference === 0;
}

export class BootstrapRegistry {
  readonly #state: DurableObjectState;
  readonly #env: Env;
  readonly #createTails = new Map<string, Promise<void>>();
  #issueTail: Promise<void> = Promise.resolve();

  constructor(state: DurableObjectState, env: Env) { this.#state = state; this.#env = env; }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const token = request.headers.get(INTERNAL_AUTH) || '';
    if (!validToken(token)) return new Response(null, { status: 404 });
    if (url.pathname === '/reset' && request.method === 'POST') {
      await this.#state.storage.deleteAll();
      return new Response(null, { status: 204 });
    }
    if (url.pathname === '/issue' && request.method === 'POST') {
      return this.#serializeIssue(() => this.#issue(token, request.headers.get('X-Client-IP') || ''));
    }
    const key = `bootstrap:${token}`;
    const entry = await this.#state.storage.get<BootstrapEntry>(key);
    if (!entry || Date.now() > entry.expiresAt) {
      if (entry) await this.#state.storage.delete(key);
      return new Response(null, { status: 404 });
    }
    if (url.pathname === '/lookup' && request.method === 'POST') return new Response(null, { status: 204 });
    if (url.pathname === '/create' && request.method === 'POST') return this.#serializeCreate(token, () => this.#create(token, key, request));
    return new Response(null, { status: 404 });
  }

  #serializeCreate(token: string, task: () => Promise<Response>): Promise<Response> {
    const previous = this.#createTails.get(token) ?? Promise.resolve();
    const result = previous.then(task, task);
    const tail = result.then(() => undefined, () => undefined);
    this.#createTails.set(token, tail);
    void tail.finally(() => { if (this.#createTails.get(token) === tail) this.#createTails.delete(token); });
    return result;
  }

  #serializeIssue(task: () => Promise<Response>): Promise<Response> {
    const result = this.#issueTail.then(task, task);
    this.#issueTail = result.then(() => undefined, () => undefined);
    return result;
  }

  async #issue(token: string, issuanceIp: string): Promise<Response> {
    const now = Date.now();
    const rate = await this.#state.storage.get<{ start: number; count: number }>('bootstrap-rate') ?? { start: now, count: 0 };
    if (now - rate.start >= BOOTSTRAP_RATE_WINDOW_MS) { rate.start = now; rate.count = 0; }
    const active = await this.#activeBootstraps(now);
    if (active.length >= MAX_BOOTSTRAPS || rate.count >= BOOTSTRAP_RATE_BURST) return new Response(null, { status: 429, headers: { 'Retry-After': '1' } });
    const entry: BootstrapEntry = { expiresAt: now + BOOTSTRAP_TTL_MS, issuanceIp, used: false };
    rate.count++;
    await this.#state.storage.put({ [`bootstrap:${token}`]: entry, 'bootstrap-rate': rate });
    await this.#scheduleAlarm(active.map(([, value]) => value.expiresAt).concat(entry.expiresAt));
    return new Response(null, { status: 204 });
  }

  async #activeBootstraps(now: number): Promise<Array<[string, BootstrapEntry]>> {
    const listed = await this.#state.storage.list<BootstrapEntry>({ prefix: 'bootstrap:' });
    const active: Array<[string, BootstrapEntry]> = [];
    const expired: string[] = [];
    for (const [key, value] of listed) {
      if (value.expiresAt <= now) expired.push(key);
      else active.push([key, value]);
    }
    if (expired.length) await this.#state.storage.delete(expired);
    return active;
  }

  async #scheduleAlarm(expiries: number[]): Promise<void> {
    if (expiries.length) await this.#state.storage.setAlarm(Math.min(...expiries));
  }

  async alarm(): Promise<void> {
    const active = await this.#activeBootstraps(Date.now());
    if (active.length) await this.#scheduleAlarm(active.map(([, value]) => value.expiresAt));
  }

  async #create(token: string, key: string, request: Request): Promise<Response> {
      const entry = await this.#state.storage.get<BootstrapEntry>(key);
      if (!entry || Date.now() > entry.expiresAt) {
        if (entry) await this.#state.storage.delete(key);
        return new Response(null, { status: 404 });
      }
      const body = await readBoundedBody(request.body, CREATE_BODY_LIMIT, CREATE_BODY_DEADLINE_MS);
      let digest: string;
      try { parseHello(body); digest = await sha256Base64(body); }
      catch { return new Response(null, { status: 400 }); }
      if (entry.used) {
        if (entry.bodyDigest !== digest || !entry.sessionToken) return new Response(null, { status: 404 });
        return this.#created(entry.sessionToken);
      }
      const sessionToken = randomToken();
      const session = this.#env.SESSIONS.get(this.#env.SESSIONS.idFromName(sessionToken));
      const initialized = await session.fetch(internalRequest('/init', sessionToken, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: sessionToken, clientIp: request.headers.get('X-Client-IP') || '', expiresAt: Date.now() + SESSION_TTL_MS })
      }));
      if (!initialized.ok) return new Response(null, { status: 503, headers: { 'Retry-After': '1' } });
      entry.used = true;
      entry.bodyDigest = digest;
      entry.sessionToken = sessionToken;
      await this.#state.storage.put(key, entry);
      return this.#created(sessionToken);
  }

  #created(sessionToken: string): Response {
    return new Response(Uint8Array.from(encodeFrame(FrameType.Welcome, 0)).buffer, {
      status: 200,
      headers: {
        'Content-Type': 'application/octet-stream', 'Cache-Control': 'no-store',
        'X-Session-Token': sessionToken, 'X-Down-Cursor': '0', 'X-Carrier-Mode': 'websocket'
      }
    });
  }
}

export class RelaySession {
  readonly #state: DurableObjectState;
  readonly #env: Env;
  #config: SessionConfig | undefined;
  #socket: WebSocket | undefined;
  #core: RelayCore | undefined;
  #batcher: WebSocketBatcher | undefined;
  #queue: SerializedInboundQueue<Uint8Array> | undefined;
  #liveness: IdleLiveness | undefined;
  #closing: Promise<void> | undefined;

  constructor(state: DurableObjectState, env: Env) {
    this.#state = state;
    this.#env = env;
    state.blockConcurrencyWhile(async () => {
      this.#config = await state.storage.get<SessionConfig>('config');
      if (this.#config && this.#config.expiresAt <= Date.now()) { this.#config = undefined; await state.storage.deleteAll(); }
    });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const token = request.headers.get(INTERNAL_AUTH) || '';
    if (url.pathname === '/init' && request.method === 'POST') {
      if (!validToken(token)) return new Response(null, { status: 404 });
      const incoming = await request.json<SessionConfig>();
      if (incoming.token !== token || !Number.isFinite(incoming.expiresAt) || incoming.expiresAt <= Date.now()) return new Response(null, { status: 400 });
      if (this.#config && this.#config.token !== token) return new Response(null, { status: 409 });
      this.#config = incoming;
      await this.#state.storage.put('config', this.#config);
      await this.#state.storage.setAlarm(this.#config.expiresAt);
      return new Response(null, { status: 204 });
    }
    if (!this.#config || token !== this.#config.token || this.#config.expiresAt <= Date.now()) return new Response(null, { status: 404 });
    if (url.pathname === '/close' && request.method === 'DELETE') {
      await this.#close();
      return new Response(null, { status: 204 });
    }
    if (url.pathname === '/ws' && request.method === 'GET') {
      if (this.#socket) return new Response(null, { status: 409 });
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair) as [WebSocket, WebSocket];
      server.accept({ allowHalfOpen: true });
      server.binaryType = 'arraybuffer';
      this.#socket = server;
      this.#batcher = new WebSocketBatcher((value) => server.send(value), {
        packBytes: 32 * 1024, directBytes: 32 * 1024, delayMs: 1,
        maxPendingBytes: DEFAULT_LIMITS.maxPendingBytes, maxPendingItems: DEFAULT_LIMITS.maxPendingItems
      });
      const secret = decodeSecret(this.#env.WEB_SECRET);
      try {
        const factory = connectorFactories.get(this);
        this.#core = new RelayCore({
          limits: DEFAULT_LIMITS, connector: factory ? factory(secret) : new TelegramConnector(secret, new CloudflareTelegramDialer()),
          send: (batch) => { try { this.#batcher?.send(batch); } catch { void this.#close(); } },
          closeCarrier: () => { void this.#close(); }
        });
      } finally { secret.fill(0); }
      this.#queue = new SerializedInboundQueue({
        maxBytes: DEFAULT_LIMITS.maxPendingBytes,
        maxItems: DEFAULT_LIMITS.maxPendingItems,
        size: (value) => value.byteLength + 256,
        handle: async (value) => {
          try { await this.#core?.receive(value); }
          catch (error) { void this.#close(1002, 'protocol error'); throw error; }
        }
      });
      this.#liveness = new IdleLiveness(IDLE_PERIOD_MS, () => {
        try { this.#batcher?.send(encodeFrame(FrameType.Ping, 0, crypto.getRandomValues(new Uint8Array(8)))); }
        catch { void this.#close(); }
      }, () => { void this.#close(1001, 'idle timeout'); });
      this.#liveness.start();
      server.addEventListener('message', (event) => {
        this.#liveness?.touch();
        if (typeof event.data === 'string') { void this.#close(1003, 'binary messages required'); return; }
        const bytes = event.data instanceof ArrayBuffer
          ? new Uint8Array(event.data)
          : ArrayBuffer.isView(event.data) ? new Uint8Array(event.data.buffer, event.data.byteOffset, event.data.byteLength) : null;
        if (!bytes || bytes.byteLength === 0 || bytes.byteLength > DEFAULT_LIMITS.maxCarrierBatchBytes) { void this.#close(1009, 'message limit'); return; }
        const copy = bytes.slice();
        if (!this.#queue?.push(copy)) void this.#close(1009, 'message queue limit');
      });
      server.addEventListener('close', () => { void this.#close(); });
      server.addEventListener('error', () => { void this.#close(); });
      return new Response(null, { status: 101, webSocket: client, headers: { 'Sec-WebSocket-Extensions': '' } });
    }
    return new Response(null, { status: 404 });
  }

  async #close(code = 1000, reason = ''): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#closing = (async () => {
      const socket = this.#socket;
      this.#socket = undefined;
      this.#liveness?.stop();
      this.#liveness = undefined;
      this.#queue?.clear();
      this.#queue = undefined;
      try { this.#core?.close(); } catch { /* shutdown must continue */ }
      try { this.#batcher?.close(); } catch { /* shutdown must continue */ }
      this.#core = undefined;
      this.#batcher = undefined;
      try { socket?.close(code, reason); } catch { /* already closed */ }
      this.#config = undefined;
      try { await this.#state.storage.deleteAll(); } finally { try { await this.#state.storage.deleteAlarm(); } catch { /* no alarm */ } }
    })();
    return this.#closing;
  }

  async alarm(): Promise<void> { await this.#close(1001, 'session expired'); }
}

async function issueBridge(request: Request, env: Env): Promise<Response> {
  const token = randomToken();
  const registry = env.BOOTSTRAPS.get(env.BOOTSTRAPS.idFromName('global'));
  const issued = await registry.fetch(internalRequest('/issue', token, { method: 'POST', headers: { 'X-Client-IP': request.headers.get('CF-Connecting-IP') || '' } }));
  if (!issued.ok) return hidden(env);
  return bridgeResponse(env.PUBLIC_HOSTNAME, token);
}

async function createSession(request: Request, env: Env): Promise<Response> {
  if (request.headers.has('Cookie') || request.headers.get('Content-Type') !== 'application/octet-stream') return hidden(env);
  const token = bearer(request);
  if (!token) return hidden(env);
  const registry = env.BOOTSTRAPS.get(env.BOOTSTRAPS.idFromName('global'));
  const lookup = await registry.fetch(internalRequest('/lookup', token, { method: 'POST' }));
  if (!lookup.ok) return hidden(env);
  const contentLength = request.headers.get('Content-Length');
  if (contentLength && (!/^(0|[1-9][0-9]*)$/.test(contentLength) || Number(contentLength) > CREATE_BODY_LIMIT)) return hidden(env);
  let body: Uint8Array;
  try { body = await readBoundedBody(request.body, CREATE_BODY_LIMIT, CREATE_BODY_DEADLINE_MS); }
  catch { return hidden(env); }
  const created = await registry.fetch(internalRequest('/create', token, {
    method: 'POST', body: Uint8Array.from(body).buffer,
    headers: { 'Content-Type': 'application/octet-stream', 'X-Client-IP': request.headers.get('CF-Connecting-IP') || '' }
  }));
  if (created.status === 404 || created.status === 400) return hidden(env);
  return created;
}

async function deleteSession(request: Request, env: Env): Promise<Response> {
  if (request.headers.has('Cookie')) return hidden(env);
  const token = bearer(request);
  if (!token) return hidden(env);
  const session = env.SESSIONS.get(env.SESSIONS.idFromName(token));
  const response = await session.fetch(internalRequest('/close', token, { method: 'DELETE' }));
  return response.status === 204 ? response : hidden(env);
}

async function websocket(request: Request, env: Env): Promise<Response> {
  if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') return hidden(env);
  const protocol = request.headers.get('Sec-WebSocket-Protocol') || '';
  const match = /^tproxy-v1\.([A-Za-z0-9_-]{43})$/.exec(protocol);
  if (!match) return hidden(env);
  const token = match[1]!;
  const session = env.SESSIONS.get(env.SESSIONS.idFromName(token));
  const response = await session.fetch(internalRequest('/ws', token, { headers: { Upgrade: 'websocket' } }));
  if (response.status !== 101) return hidden(env);
  const headers = new Headers(response.headers);
  headers.set('Sec-WebSocket-Protocol', protocol);
  headers.set('Sec-WebSocket-Extensions', '');
  return new Response(null, { status: 101, webSocket: response.webSocket, headers });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === 'GET' && url.pathname === '/') {
      const match = /^\?bridge=([A-Za-z0-9_-]{43})$/.exec(url.search);
      const bridge = match?.[1];
      if (bridge) {
        let secret: Uint8Array | undefined;
        try {
          validateHostname(env.PUBLIC_HOSTNAME);
          secret = decodeSecret(env.WEB_SECRET);
          const expected = await deriveCapability(env.PUBLIC_HOSTNAME, secret);
          if (constantTimeEqual(bridge, expected)) return issueBridge(request, env);
        } catch { return hidden(env); }
        finally { secret?.fill(0); }
      }
    }
    if (url.pathname === '/api/v1/session' && request.method === 'POST') return createSession(request, env);
    if (url.pathname === '/api/v1/session' && request.method === 'DELETE') return deleteSession(request, env);
    if (url.pathname === '/api/v1/ws' && request.method === 'GET') return websocket(request, env);
    if (request.method === 'GET' && url.pathname === '/') return publicResponse(title(env));
    return hidden(env);
  }
};
