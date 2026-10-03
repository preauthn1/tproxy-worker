// Behavioural test of the bridge uplink batcher using fake WebSocket / MessagePort / fetch.
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const script = readFileSync('/tmp/bridge-script.js', 'utf8');
const frame = (type, id, payload) => { const b = new Uint8Array(8 + payload.length); b[0] = type; b[1] = id >> 16; b[2] = id >> 8; b[3] = id; new DataView(b.buffer).setUint32(4, payload.length); b.set(payload, 8); return b.buffer; };

let socket, buffered = 0, fetches = 0;
class FakeWS {
  static OPEN = 1;
  constructor() { this.readyState = 0; this.sent = []; socket = this; setTimeout(() => { this.readyState = 1; this.onopen?.(); }, 0); }
  get bufferedAmount() { return buffered; }
  send(v) { this.sent.push(new Uint8Array(v.slice ? v.slice(0) : v)); }
  close() { this.readyState = 3; }
}
const portMsgs = [];
const port = { onmessage: null, start() {}, close() {}, postMessage(v) { portMsgs.push(v); } };
const listeners = {};
const ctx = {
  WebSocket: FakeWS, setTimeout, clearTimeout, queueMicrotask, URL, DataView, Uint8Array, ArrayBuffer, JSON, Error, Promise,
  location: { hash: '', pathname: '/' }, history: { replaceState() {} },
  fetch: async () => { fetches++; return { status: 200, headers: { get: (k) => ({ 'X-Carrier-Mode': 'websocket', 'X-Session-Token': 'B'.repeat(43) })[k] ?? null }, arrayBuffer: async () => frame(0x11, 0, new Uint8Array()) }; },
  addEventListener: (t, f) => { listeners[t] = f; }, parent: {}
};
ctx.globalThis = ctx;
vm.runInNewContext(script, ctx);
listeners.message({ source: ctx.parent, origin: 'http://127.0.0.1:1234', data: { t: 'tproxy-init', v: 1 }, ports: [port] });
const tick = (ms = 0) => new Promise((r) => setTimeout(r, ms));

// First message triggers session create; queue many small frames before socket opens.
port.onmessage({ data: frame(0x10, 0, new Uint8Array([1])) });
const expected = [];
for (let i = 0; i < 300; i++) { const f = frame(0x02, 1 + (i % 3), new Uint8Array(100).fill(i & 0xff)); expected.push(new Uint8Array(f)); port.onmessage({ data: f }); }
await tick(20);
const concat = (arrs) => { const n = arrs.reduce((s, a) => s + a.length, 0); const o = new Uint8Array(n); let k = 0; for (const a of arrs) { o.set(a, k); k += a.length; } return o; };
const want = concat(expected);
let got = concat(socket.sent);
console.log('pre-open: sent messages', socket.sent.length, 'bytes', got.length, 'exact', got.length === want.length && got.every((v, i) => v === want[i]));
if (socket.sent.length >= 300 || !got.every((v, i) => v === want[i])) throw new Error('pre-open batching failed');

// Live traffic: burst within one task should coalesce into few messages.
socket.sent.length = 0; const burst = [];
for (let i = 0; i < 200; i++) { const f = frame(0x02, 2, new Uint8Array(1000).fill(i & 0xff)); burst.push(new Uint8Array(f)); port.onmessage({ data: f }); }
await tick(5);
got = concat(socket.sent); const wantB = concat(burst);
console.log('live burst: 200 frames ->', socket.sent.length, 'messages, exact', got.length === wantB.length && got.every((v, i) => v === wantB[i]), 'max msg', Math.max(...socket.sent.map((m) => m.length)));
if (!got.every((v, i) => v === wantB[i]) || socket.sent.some((m) => m.length > 524288)) throw new Error('burst failed');

// Back-pressure: when browser buffer is high, hold then drain later in order.
socket.sent.length = 0; buffered = 300000;
const held = frame(0x02, 3, new Uint8Array(500).fill(7)); port.onmessage({ data: held });
await tick(2);
console.log('back-pressure held:', socket.sent.length === 0);
if (socket.sent.length !== 0) throw new Error('did not hold under back-pressure');
buffered = 0; await tick(30);
console.log('drained after pressure:', socket.sent.length === 1 && socket.sent[0].length === 508);
if (!(socket.sent.length === 1)) throw new Error('did not drain');

// Overflow: exceeding 32 MiB queue must fail the carrier.
buffered = 33554000; const before = portMsgs.length;
port.onmessage({ data: frame(0x02, 3, new Uint8Array(1000)) });
const failed = portMsgs.slice(before).some((m) => m && m.t === 'close');
console.log('overflow fails carrier:', failed);
if (!failed) throw new Error('overflow not enforced');
console.log('ALL BRIDGE CHECKS PASSED');
