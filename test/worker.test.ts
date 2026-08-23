import { env, runDurableObjectAlarm, runInDurableObject, SELF } from 'cloudflare:test';
import { beforeEach, describe, expect, it } from 'vitest';
import { deriveCapability, decodeSecret } from '../src/capability';
import { FrameType, encodeFrame, parseRelayBatch } from '../src/frame';
import { TelegramConnector, type DirectTelegramConnection, type TelegramDialer } from '../src/mtproxy';
import { installRelaySessionTestFactory, type RelaySession } from '../src/index';
import { concatenate, makeObfuscated2Vector } from './obfuscated2-vectors';

class FakeDc implements DirectTelegramConnection {
  writes: Uint8Array[] = [];
  closed = false;
  #reads: Uint8Array[] = [];
  #wake: (() => void) | undefined;
  async write(value: Uint8Array): Promise<void> { this.writes.push(value.slice()); }
  async *read(): AsyncIterable<Uint8Array> {
    while (!this.closed) {
      if (this.#reads.length) { yield this.#reads.shift()!; continue; }
      await new Promise<void>((resolve) => { this.#wake = resolve; });
    }
  }
  emit(value: Uint8Array): void { this.#reads.push(value.slice()); this.#wake?.(); this.#wake = undefined; }
  close(): void { this.closed = true; this.#wake?.(); }
}

async function waitFor(check: () => boolean): Promise<void> {
  for (let attempts = 0; attempts !== 100; attempts++) {
    if (check()) return;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error('condition not reached');
}

async function bridge(): Promise<{ response: Response; bootstrap: string }> {
  const capability = await deriveCapability('proxy.example.com', decodeSecret(env.WEB_SECRET));
  const response = await SELF.fetch(`https://proxy.example.com/?bridge=${capability}`);
  const html = await response.clone().text();
  const bootstrap = /const bootstrap=("[A-Za-z0-9_-]{43}")/.exec(html)?.[1];
  if (!bootstrap) throw new Error('bridge bootstrap not found');
  return { response, bootstrap: JSON.parse(bootstrap) as string };
}

async function create(bootstrap: string, body = encodeFrame(FrameType.Hello, 0, new Uint8Array([1]))): Promise<Response> {
  return SELF.fetch('https://proxy.example.com/api/v1/session', {
    method: 'POST',
    headers: { Authorization: `Bearer ${bootstrap}`, 'Content-Type': 'application/octet-stream' },
    body: Uint8Array.from(body).buffer
  });
}

describe('Worker and Durable Object integration', () => {
  beforeEach(async () => {
    await env.BOOTSTRAPS.get(env.BOOTSTRAPS.idFromName('global')).fetch('https://internal/reset', { method: 'POST', headers: { 'X-Tproxy-Internal-Token': 'A'.repeat(43) } });
  });

  it('serves an ordinary public website for unauthenticated paths and bad bridge queries', async () => {
    for (const path of ['/', '/api/v1/session', '/?bridge=wrong', '/?bridge=MHLEY5PmW1GWqJkSrlmJpvJUiLhBH_QKy6yKg8a0JPk&x=1']) {
      const response = await SELF.fetch(`https://proxy.example.com${path}`);
      expect(response.status).toBe(path === '/' ? 200 : path.startsWith('/?') ? 200 : 404);
      expect(response.headers.get('Content-Type')).toContain('text/html');
      expect(await response.text()).not.toMatch(/tproxy|session|credential|api\/v1/i);
    }
  });

  it('uses the stock nginx welcome page as the public camouflage', async () => {
    const response = await SELF.fetch('https://proxy.example.com/');
    expect(response.status).toBe(200);
    expect(response.headers.get('Server')).toBe('nginx');
    const html = await response.text();
    expect(html).toContain('<title>Welcome to nginx!</title>');
    expect(html).toContain('<h1>Welcome to nginx!</h1>');
    expect(html).toContain('Thank you for using nginx.');
    expect(html).not.toContain('Public Site');
  });

  it('renders only the exact capability bridge as dynamic no-store websocket page', async () => {
    const { response } = await bridge();
    expect(response.status).toBe(200);
    expect(response.headers.get('Cache-Control')).toBe('no-store');
    expect(response.headers.get('Content-Security-Policy')).toContain("connect-src 'self' wss://proxy.example.com");
    expect(await response.text()).toContain('carrierMode="websocket"');
  });

  it('the bridge answers relay PING frames with an exact PONG before native delivery', async () => {
    const { response } = await bridge();
    const html = await response.text();
    expect(html).toContain('type===5&&streamId===0');
    expect(html).toContain('new Uint8Array(value.slice(offset,end))');
    expect(html).toContain('pong[0]=6;send(pong.buffer)');
    const ping = encodeFrame(FrameType.Ping, 0, Uint8Array.of(1, 2, 3));
    const pong = new Uint8Array(ping.buffer.slice(0));
    pong[0] = FrameType.Pong;
    expect(parseRelayBatch(encodeFrame(FrameType.Ping, 0, Uint8Array.of(1, 2, 3)))[0]?.type).toBe(FrameType.Ping);
    expect(pong[0]).toBe(FrameType.Pong);
    expect(new Uint8Array(pong.buffer)).toEqual(encodeFrame(FrameType.Pong, 0, Uint8Array.of(1, 2, 3)));
  });

  it('atomically exchanges a bootstrap and returns an idempotent creation result', async () => {
    const { bootstrap } = await bridge();
    const first = await create(bootstrap);
    const firstToken = first.headers.get('X-Session-Token');
    expect(first.status).toBe(200);
    expect(first.headers.get('X-Carrier-Mode')).toBe('websocket');
    expect(first.headers.get('X-Down-Cursor')).toBe('0');
    expect(firstToken).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(parseRelayBatch(new Uint8Array(await first.arrayBuffer()))).toEqual([
      { type: FrameType.Welcome, streamId: 0, payload: new Uint8Array() }
    ]);
    const retry = await create(bootstrap);
    expect(retry.status).toBe(200);
    expect(retry.headers.get('X-Session-Token')).toBe(firstToken);
    expect((await create(bootstrap, encodeFrame(FrameType.Hello, 0, new Uint8Array([2])))).status).toBe(404);
  });

  it('returns one session token for concurrent byte-identical creation requests', async () => {
    const { bootstrap } = await bridge();
    const [left, right] = await Promise.all([create(bootstrap), create(bootstrap)]);
    expect(left.status).toBe(200);
    expect(right.status).toBe(200);
    expect(left.headers.get('X-Session-Token')).toBe(right.headers.get('X-Session-Token'));
  });

  it('serializes concurrent bootstrap issuance so the configured burst cannot be raced', async () => {
    const registry = env.BOOTSTRAPS.get(env.BOOTSTRAPS.idFromName('global'));
    const responses = await Promise.all(Array.from({ length: 257 }, (_, index) => {
      const token = index.toString(36).padStart(43, 'A');
      return registry.fetch('https://internal/issue', {
        method: 'POST', headers: { 'X-Tproxy-Internal-Token': token, 'X-Client-IP': '192.0.2.1' }
      });
    }));
    expect(responses.filter((response) => response.status === 204)).toHaveLength(256);
    expect(responses.filter((response) => response.status === 429)).toHaveLength(1);
  });

  it('hides unknown credentials behind the same public 404', async () => {
    const unknown = 'A'.repeat(43);
    const response = await create(unknown);
    expect(response.status).toBe(404);
    expect(response.headers.get('Content-Type')).toContain('text/html');
    expect(await response.text()).not.toMatch(/token|auth|session|tproxy/i);
  });

  it('requires and echoes the exact websocket subprotocol', async () => {
    const { bootstrap } = await bridge();
    const created = await create(bootstrap);
    const token = created.headers.get('X-Session-Token')!;
    const wrong = await SELF.fetch('https://proxy.example.com/api/v1/ws', {
      headers: { Upgrade: 'websocket', 'Sec-WebSocket-Protocol': `wrong.${token}` }
    });
    expect(wrong.status).toBe(404);
    const correct = await SELF.fetch('https://proxy.example.com/api/v1/ws', {
      headers: { Upgrade: 'websocket', 'Sec-WebSocket-Protocol': `tproxy-v1.${token}` }
    });
    expect(correct.status).toBe(101);
    expect(correct.headers.get('Sec-WebSocket-Protocol')).toBe(`tproxy-v1.${token}`);
    correct.webSocket?.accept();
    correct.webSocket?.close(1000, 'test complete');
  });

  it('expires a RelaySession deterministically through its Durable Object alarm', async () => {
    const token = 'Z'.repeat(43);
    const stub = env.SESSIONS.get(env.SESSIONS.idFromName(token));
    const initialized = await stub.fetch('https://internal/init', {
      method: 'POST',
      headers: { 'X-Tproxy-Internal-Token': token, 'Content-Type': 'application/json' },
      body: JSON.stringify({ token, clientIp: '192.0.2.1', expiresAt: Date.now() + 60_000 })
    });
    expect(initialized.status).toBe(204);
    expect(await runDurableObjectAlarm(stub)).toBe(true);
    expect((await stub.fetch('https://internal/ws', {
      headers: { 'X-Tproxy-Internal-Token': token, Upgrade: 'websocket' }
    })).status).toBe(404);
  });

  it('carries OPEN plus a fragmented obfuscated2 handshake and encrypted req_pq through the real RelaySession data plane', async () => {
    const vector = await makeObfuscated2Vector({
      dc: 2,
      clearPayload: Uint8Array.from([40, 0, 0, 0, ...new Uint8Array(40).map((_, index) => index + 1)]),
      clearResponse: Uint8Array.from({ length: 37 }, (_, index) => 200 - index)
    });
    const { bootstrap } = await bridge();
    const token = (await create(bootstrap)).headers.get('X-Session-Token')!;
    const dc = new FakeDc();
    const dialer: TelegramDialer = { connect: async () => dc };
    const stub = env.SESSIONS.get(env.SESSIONS.idFromName(token));
    await runInDurableObject(stub, (instance) => {
      installRelaySessionTestFactory(instance as unknown as RelaySession, (secret) => new TelegramConnector(secret, dialer));
    });
    const response = await SELF.fetch('https://proxy.example.com/api/v1/ws', {
      headers: { Upgrade: 'websocket', 'Sec-WebSocket-Protocol': `tproxy-v1.${token}` }
    });
    expect(response.status).toBe(101);
    const socket = response.webSocket!;
    socket.binaryType = 'arraybuffer';
    socket.accept();
    const incoming: Uint8Array[] = [];
    socket.addEventListener('message', (event) => {
      const value = event.data;
      const bytes = value instanceof ArrayBuffer
        ? new Uint8Array(value)
        : ArrayBuffer.isView(value) ? new Uint8Array(value.buffer, value.byteOffset, value.byteLength) : new Uint8Array();
      if (bytes.byteLength) incoming.push(bytes.slice());
    });
    socket.send(encodeFrame(FrameType.Open, 1));
    const wire = concatenate(vector.transformedHeader, vector.transformedPayload);
    for (const [start, end] of [[0, 5], [5, 31], [31, 63], [63, 68], [68, wire.length]] as const) {
      socket.send(encodeFrame(FrameType.Data, 1, wire.slice(start, end)));
    }
    const expectedDcWrite = concatenate(Uint8Array.of(0xee, 0xee, 0xee, 0xee), vector.clearPayload);
    await waitFor(() => dc.writes.reduce((sum, value) => sum + value.byteLength, 0) === expectedDcWrite.byteLength);
    expect(dc.writes[0]).toEqual(Uint8Array.of(0xee, 0xee, 0xee, 0xee));
    expect(concatenate(...dc.writes)).toEqual(expectedDcWrite);
    dc.emit(vector.clearResponse);
    await waitFor(() => incoming.flatMap((batch) => parseRelayBatch(batch)).some((frame) => frame.type === FrameType.Data));
    const encrypted = incoming.flatMap((batch) => parseRelayBatch(batch)).filter((frame) => frame.type === FrameType.Data).map((frame) => frame.payload);
    expect(concatenate(...encrypted)).toEqual(vector.transformedResponse);
    socket.close(1000, 'done');
  });

  it('closes a session via authenticated DELETE and then hides the bearer', async () => {
    const { bootstrap } = await bridge();
    const token = (await create(bootstrap)).headers.get('X-Session-Token')!;
    const closed = await SELF.fetch('https://proxy.example.com/api/v1/session', {
      method: 'DELETE', headers: { Authorization: `Bearer ${token}` }
    });
    expect(closed.status).toBe(204);
    const again = await SELF.fetch('https://proxy.example.com/api/v1/session', {
      method: 'DELETE', headers: { Authorization: `Bearer ${token}` }
    });
    expect(again.status).toBe(404);
  });
});
