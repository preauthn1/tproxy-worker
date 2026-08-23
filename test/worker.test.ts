import { env, SELF } from 'cloudflare:test';
import { beforeEach, describe, expect, it } from 'vitest';
import { deriveCapability, decodeSecret } from '../src/capability';
import { FrameType, encodeFrame, parseRelayBatch } from '../src/frame';

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
    await env.BOOTSTRAPS.get(env.BOOTSTRAPS.idFromName('global')).fetch('https://internal/reset', { method: 'POST' });
  });

  it('serves an ordinary public website for unauthenticated paths and bad bridge queries', async () => {
    for (const path of ['/', '/api/v1/session', '/?bridge=wrong', '/?bridge=MHLEY5PmW1GWqJkSrlmJpvJUiLhBH_QKy6yKg8a0JPk&x=1']) {
      const response = await SELF.fetch(`https://proxy.example.com${path}`);
      expect(response.status).toBe(path === '/' ? 200 : path.startsWith('/?') ? 200 : 404);
      expect(response.headers.get('Content-Type')).toContain('text/html');
      expect(await response.text()).not.toMatch(/tproxy|session|credential|api\/v1/i);
    }
  });

  it('renders only the exact capability bridge as dynamic no-store websocket page', async () => {
    const { response } = await bridge();
    expect(response.status).toBe(200);
    expect(response.headers.get('Cache-Control')).toBe('no-store');
    expect(response.headers.get('Content-Security-Policy')).toContain("connect-src 'self' wss://proxy.example.com");
    expect(await response.text()).toContain('carrierMode="websocket"');
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
