import { bridgeResponse } from './bridge';
import { decodeSecret, deriveCapability, randomToken, validateHostname, validToken } from './capability';
import { FrameType, encodeFrame, parseHello } from './frame';
import { DEFAULT_LIMITS } from './limits';
import { publicResponse } from './public-site';
import { RelayCore } from './relay-core';
import { CloudflareSocketConnector } from './tcp';
import { WebSocketBatcher } from './ws-batcher';

interface BootstrapEntry {
  expiresAt: number;
  issuanceIp: string;
  used: boolean;
  bodyDigest?: string;
  sessionToken?: string;
}

interface SessionConfig {
  token: string;
  backendHost: string;
  backendPort: number;
  clientIp: string;
  closed: boolean;
}

const INTERNAL_AUTH = 'X-Tproxy-Internal-Token';
const BOOTSTRAP_TTL_MS = 2 * 60 * 1000;
const CREATE_BODY_LIMIT = 64;

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

export class BootstrapRegistry {
  readonly #state: DurableObjectState;
  readonly #env: Env;
  #createTail: Promise<void> = Promise.resolve();

  constructor(state: DurableObjectState, env: Env) { this.#state = state; this.#env = env; }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === '/reset' && request.method === 'POST') {
      await this.#state.storage.deleteAll();
      return new Response(null, { status: 204 });
    }
    const token = request.headers.get(INTERNAL_AUTH) || '';
    if (!validToken(token)) return new Response(null, { status: 404 });
    if (url.pathname === '/issue' && request.method === 'POST') {
      const entry: BootstrapEntry = { expiresAt: Date.now() + BOOTSTRAP_TTL_MS, issuanceIp: request.headers.get('X-Client-IP') || '', used: false };
      await this.#state.storage.put(`bootstrap:${token}`, entry);
      return new Response(null, { status: 204 });
    }
    const key = `bootstrap:${token}`;
    const entry = await this.#state.storage.get<BootstrapEntry>(key);
    if (!entry || Date.now() > entry.expiresAt) {
      if (entry) await this.#state.storage.delete(key);
      return new Response(null, { status: 404 });
    }
    if (url.pathname === '/lookup' && request.method === 'POST') return new Response(null, { status: 204 });
    if (url.pathname === '/create' && request.method === 'POST') return this.#serializeCreate(() => this.#create(token, key, request));
    return new Response(null, { status: 404 });
  }

  #serializeCreate(task: () => Promise<Response>): Promise<Response> {
    const result = this.#createTail.then(task, task);
    this.#createTail = result.then(() => undefined, () => undefined);
    return result;
  }

  async #create(token: string, key: string, request: Request): Promise<Response> {
      const entry = await this.#state.storage.get<BootstrapEntry>(key);
      if (!entry || Date.now() > entry.expiresAt) {
        if (entry) await this.#state.storage.delete(key);
        return new Response(null, { status: 404 });
      }
      const body = new Uint8Array(await request.arrayBuffer());
      let digest: string;
      try { parseHello(body); digest = await sha256Base64(body); }
      catch { return new Response(null, { status: 400 }); }
      if (entry.used) {
        if (entry.bodyDigest !== digest || !entry.sessionToken) return new Response(null, { status: 404 });
        return this.#created(entry.sessionToken);
      }
      const sessionToken = randomToken();
      const backendPort = Number(this.#env.BACKEND_PORT);
      if (!Number.isInteger(backendPort) || backendPort < 1 || backendPort > 65535 || !this.#env.BACKEND_HOST) return new Response(null, { status: 500 });
      const session = this.#env.SESSIONS.get(this.#env.SESSIONS.idFromName(sessionToken));
      const initialized = await session.fetch(internalRequest('/init', sessionToken, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: sessionToken, backendHost: this.#env.BACKEND_HOST, backendPort, clientIp: request.headers.get('X-Client-IP') || '' })
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
  #config: SessionConfig | undefined;
  #socket: WebSocket | undefined;
  #core: RelayCore | undefined;
  #batcher: WebSocketBatcher | undefined;
  #receiveChain: Promise<void> = Promise.resolve();

  constructor(state: DurableObjectState) {
    this.#state = state;
    state.blockConcurrencyWhile(async () => { this.#config = await state.storage.get<SessionConfig>('config'); });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const token = request.headers.get(INTERNAL_AUTH) || '';
    if (url.pathname === '/init' && request.method === 'POST') {
      if (!validToken(token)) return new Response(null, { status: 404 });
      const incoming = await request.json<SessionConfig>();
      if (incoming.token !== token || !incoming.backendHost || !Number.isInteger(incoming.backendPort)) return new Response(null, { status: 400 });
      if (this.#config && this.#config.token !== token) return new Response(null, { status: 409 });
      this.#config = { ...incoming, closed: false };
      await this.#state.storage.put('config', this.#config);
      return new Response(null, { status: 204 });
    }
    if (!this.#config || this.#config.closed || token !== this.#config.token) return new Response(null, { status: 404 });
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
      this.#core = new RelayCore({
        backendHost: this.#config.backendHost, backendPort: this.#config.backendPort,
        limits: DEFAULT_LIMITS, connector: new CloudflareSocketConnector(),
        send: (batch) => { try { this.#batcher?.send(batch); } catch { void this.#close(); } },
        closeCarrier: () => { void this.#close(); }
      });
      server.addEventListener('message', (event) => {
        if (typeof event.data === 'string') { void this.#close(1003, 'binary messages required'); return; }
        const bytes = event.data instanceof ArrayBuffer
          ? new Uint8Array(event.data)
          : ArrayBuffer.isView(event.data) ? new Uint8Array(event.data.buffer, event.data.byteOffset, event.data.byteLength) : null;
        if (!bytes || bytes.byteLength === 0 || bytes.byteLength > DEFAULT_LIMITS.maxCarrierBatchBytes) { void this.#close(1009, 'message limit'); return; }
        this.#receiveChain = this.#receiveChain.then(async () => {
          await this.#core?.receive(bytes);
        }).catch(async () => { await this.#close(1002, 'protocol error'); });
      });
      server.addEventListener('close', () => { void this.#close(); });
      server.addEventListener('error', () => { void this.#close(); });
      return new Response(null, { status: 101, webSocket: client, headers: { 'Sec-WebSocket-Extensions': '' } });
    }
    return new Response(null, { status: 404 });
  }

  async #close(code = 1000, reason = ''): Promise<void> {
    if (!this.#config || this.#config.closed) return;
    this.#config.closed = true;
    this.#core?.close();
    this.#batcher?.close();
    const socket = this.#socket;
    this.#socket = undefined;
    this.#core = undefined;
    this.#batcher = undefined;
    try { socket?.close(code, reason); } catch { /* already closed */ }
    await this.#state.storage.put('config', this.#config);
  }
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
  const declared = Number(request.headers.get('Content-Length') || 0);
  if (declared > CREATE_BODY_LIMIT) return hidden(env);
  const body = new Uint8Array(await request.arrayBuffer());
  if (body.byteLength > CREATE_BODY_LIMIT) return hidden(env);
  const created = await registry.fetch(internalRequest('/create', token, {
    method: 'POST', body,
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
    if (request.method === 'GET' && url.pathname === '/' && url.searchParams.size === 1) {
      const bridge = url.searchParams.get('bridge');
      if (bridge && bridge.length === 43) {
        try {
          validateHostname(env.PUBLIC_HOSTNAME);
          const expected = await deriveCapability(env.PUBLIC_HOSTNAME, decodeSecret(env.WEB_SECRET));
          if (bridge === expected) return issueBridge(request, env);
        } catch { return hidden(env); }
      }
    }
    if (url.pathname === '/api/v1/session' && request.method === 'POST') return createSession(request, env);
    if (url.pathname === '/api/v1/session' && request.method === 'DELETE') return deleteSession(request, env);
    if (url.pathname === '/api/v1/ws' && request.method === 'GET') return websocket(request, env);
    if (request.method === 'GET' && url.pathname === '/') return publicResponse(title(env));
    return hidden(env);
  }
};
