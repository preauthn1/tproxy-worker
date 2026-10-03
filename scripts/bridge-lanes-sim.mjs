// Behavioural test of the bridge page in websocket-lanes mode (fake WebSocket/port/fetch).
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const script = readFileSync('/tmp/bridge-websocket-lanes.js', 'utf8');
const frame = (type, id, payload = new Uint8Array()) => { const b = new Uint8Array(8 + payload.length); b[0] = type; b[1] = id >> 16; b[2] = id >> 8; b[3] = id; new DataView(b.buffer).setUint32(4, payload.length); b.set(payload, 8); return b.buffer; };
const TOKEN = 'B'.repeat(43);
const sockets = [];
class FakeWS {
  static OPEN = 1; static CONNECTING = 0;
  constructor(url, protocol) { this.url = url; this.protocol = protocol; this.readyState = 0; this.sent = []; this.bufferedAmount = 0; sockets.push(this); setTimeout(() => { if (this.readyState === 0) { this.readyState = 1; this.onopen?.(); } }, 0); }
  send(v) { this.sent.push(new Uint8Array(v.slice(0))); }
  close() { if (this.readyState === 3) return; this.readyState = 3; setTimeout(() => this.onclose?.(), 0); }
  serverPush(buf) { this.onmessage?.({ data: buf }); }
}
const portMsgs = [];
const port = { onmessage: null, start() {}, close() {}, postMessage(v) { portMsgs.push(v instanceof ArrayBuffer ? new Uint8Array(v.slice(0)) : v); } };
const listeners = {};
let mode = 'websocket-lanes';
const ctx = {
  WebSocket: FakeWS, setTimeout, clearTimeout, queueMicrotask, URL, DataView, Uint8Array, ArrayBuffer, JSON, Error, Promise, Map, Set,
  location: { hash: '', pathname: '/' }, history: { replaceState() {} },
  fetch: async () => ({ status: 200, headers: { get: (k) => ({ 'X-Carrier-Mode': mode, 'X-Session-Token': TOKEN })[k] ?? null }, arrayBuffer: async () => frame(0x11, 0) }),
  addEventListener: (t, f) => { listeners[t] = f; }, parent: {}
};
ctx.globalThis = ctx;
vm.runInNewContext(script, ctx);
listeners.message({ source: ctx.parent, origin: 'http://127.0.0.1:1234', data: { t: 'tproxy-init', v: 1 }, ports: [port] });
const tick = (ms = 0) => new Promise((r) => setTimeout(r, ms));
const assert = (c, m) => { if (!c) throw new Error('FAIL: ' + m); console.log('ok -', m); };
const closeMsg = () => portMsgs.some((m) => m && m.t === 'close');

// HELLO starts the session; OPEN+DATA for streams 1 and 2 queued before session exists.
port.onmessage({ data: frame(0x10, 0, new Uint8Array([1])) });
port.onmessage({ data: frame(0x01, 1) });
port.onmessage({ data: frame(0x02, 1, new Uint8Array([9, 9])) });
port.onmessage({ data: frame(0x01, 2) });
await tick(20);
assert(portMsgs.some((m) => m instanceof Uint8Array && m[0] === 0x11), 'WELCOME delivered to app');
assert(sockets.length === 2, 'one WebSocket per stream (2 lanes)');
assert(sockets[0].protocol === `tproxy-lane-v1.${TOKEN}.1` && sockets[1].protocol === `tproxy-lane-v1.${TOKEN}.2`, 'lane subprotocols carry token and stream id');
assert(sockets.every((s) => s.url === 'wss://proxy.example.com/api/v1/ws'), 'lane URL');
assert(sockets[0].sent.length === 1 && sockets[0].sent[0].length === 8 + 10 && sockets[0].sent[0][0] === 1 && sockets[0].sent[0][8] === 2, 'lane 1 got OPEN+DATA batched in order');
assert(sockets.every((s) => s.sent.every((m) => { for (let o = 0; o < m.length;) { const id = (m[o + 1] << 16) | (m[o + 2] << 8) | m[o + 3]; if (id !== Number(s.protocol.split('.').pop())) return false; o += 8 + new DataView(m.buffer, o + 4, 4).getUint32(0); } return true; })), 'no cross-lane frames on uplink');

// Downlink isolation: server data on lane 2 reaches the app; cross-lane frame fails carrier.
portMsgs.length = 0;
sockets[1].serverPush(frame(0x02, 2, new Uint8Array([5])));
assert(portMsgs.some((m) => m instanceof Uint8Array && m[3] === 2 && m[8] === 5), 'lane 2 downlink delivered');

// Backpressure on lane 1 must not hold lane 2.
sockets[0].bufferedAmount = 600000;
port.onmessage({ data: frame(0x02, 1, new Uint8Array(100)) });
port.onmessage({ data: frame(0x02, 2, new Uint8Array(100)) });
await tick(2);
assert(sockets[0].sent.length === 1, 'lane 1 held under its own backpressure');
assert(sockets[1].sent.length === 2, 'lane 2 still sends while lane 1 is backed up');
sockets[0].bufferedAmount = 0; await tick(30);
assert(sockets[0].sent.length === 2, 'lane 1 drains after backpressure clears');

// Remote CLOSE then socket close: lane finished without synthetic CLOSE to app.
portMsgs.length = 0;
sockets[1].serverPush(frame(0x03, 2)); sockets[1].close(); await tick(5);
const closes2 = portMsgs.filter((m) => m instanceof Uint8Array && m[0] === 3 && m[3] === 2).length;
assert(closes2 === 1, 'remote CLOSE delivered once (no duplicate synthetic CLOSE)');
assert(!closeMsg(), 'carrier still alive after one lane closed');

// Abrupt lane drop (no CLOSE seen) -> synthetic CLOSE for that stream only.
portMsgs.length = 0;
port.onmessage({ data: frame(0x01, 3) }); await tick(5);
const lane3 = sockets[sockets.length - 1];
assert(lane3.protocol.endsWith('.3'), 'stream 3 opens its own lane');
lane3.close(); await tick(5);
assert(portMsgs.some((m) => m instanceof Uint8Array && m[0] === 3 && m[3] === 3), 'abrupt lane drop -> synthetic CLOSE to app');
assert(!closeMsg(), 'other lanes unaffected by lane 3 drop');

// Reusing a closed stream id is a protocol violation.
port.onmessage({ data: frame(0x01, 2) });
assert(closeMsg(), 'reusing a closed stream id fails the carrier');
console.log('ALL LANE BRIDGE CHECKS PASSED');
