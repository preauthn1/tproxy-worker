import { decodeSecret, deriveCapability, validateHostname } from '../src/capability';
import { FrameType, encodeFrame, parseRelayBatch } from '../src/frame';

const [rawOrigin, secret] = process.argv.slice(2);
if (!rawOrigin || !secret) {
  console.error('usage: npm run smoke -- <https-origin> <secret>');
  process.exit(2);
}
const origin = new URL(rawOrigin);
if (origin.protocol !== 'https:' || origin.pathname !== '/' || origin.search || origin.hash || origin.port) throw new Error('origin must be https://hostname');
validateHostname(origin.hostname);
const capability = await deriveCapability(origin.hostname, decodeSecret(secret));
const bridge = await fetch(new URL(`/?bridge=${capability}`, origin), { redirect: 'error' });
if (!bridge.ok || bridge.headers.get('Cache-Control') !== 'no-store') throw new Error(`bridge failed: ${bridge.status}`);
const html = await bridge.text();
const literal = /const bootstrap=("[A-Za-z0-9_-]{43}")/.exec(html)?.[1];
if (!literal) throw new Error('bridge did not contain a bootstrap token');
const bootstrap = JSON.parse(literal) as string;
const created = await fetch(new URL('/api/v1/session', origin), {
  method: 'POST',
  headers: { Authorization: `Bearer ${bootstrap}`, 'Content-Type': 'application/octet-stream' },
  body: Uint8Array.from(encodeFrame(FrameType.Hello, 0, new Uint8Array([1]))).buffer
});
if (created.status !== 200) throw new Error(`session create failed: ${created.status}`);
const token = created.headers.get('X-Session-Token');
const frames = parseRelayBatch(new Uint8Array(await created.arrayBuffer()));
if (!token || frames.length !== 1 || frames[0]?.type !== FrameType.Welcome) throw new Error('invalid creation response');
const closed = await fetch(new URL('/api/v1/session', origin), { method: 'DELETE', headers: { Authorization: `Bearer ${token}` } });
if (closed.status !== 204) throw new Error(`session delete failed: ${closed.status}`);
console.log('public bridge, session creation, WELCOME, and authenticated DELETE: OK');
