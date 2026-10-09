// Simulate Telegram Desktop's built-in WebView bridge (window.TelegramWebProxy, #android=nonce)
// against a bridge page script. usage: node native-sim.mjs <script.js> <mode>
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const [file, mode] = process.argv.slice(2);
const script = readFileSync(file, 'utf8');
const frame = (type, id, payload = new Uint8Array()) => { const b = new Uint8Array(8 + payload.length); b[0] = type; b[1] = id >> 16; b[2] = id >> 8; b[3] = id; new DataView(b.buffer).setUint32(4, payload.length); b.set(payload, 8); return b.buffer; };
const TOKEN = 'B'.repeat(43);
const NONCE = 'N'.repeat(43);
const sockets = [];
class FakeWS {
  static OPEN = 1; static CONNECTING = 0;
  constructor(url, protocol) { this.protocol = protocol; this.readyState = 0; this.sent = []; this.bufferedAmount = 0; sockets.push(this); setTimeout(() => { if (this.readyState === 0) { this.readyState = 1; this.onopen?.(); } }, 0); }
  send(v) { this.sent.push(v); }
  close() { this.readyState = 3; }
}
// Mirrors tdesktop BridgeScript(): ArrayBuffer -> 'b', string -> 'c', anything else -> 'f' (carrier failure).
const nativeOut = [];
const native = {
  onmessage: null,
  postMessage(value) {
    if (value instanceof ArrayBuffer) nativeOut.push('b');
    else if (typeof value === 'string') nativeOut.push('c' + value);
    else nativeOut.push('f:' + Object.prototype.toString.call(value) + ' ' + (value && value.constructor && value.constructor.name) + ' ' + JSON.stringify(value).slice(0,80));
  }
};
const ctx = {
  WebSocket: FakeWS, setTimeout, clearTimeout, queueMicrotask, URL, DataView, Uint8Array, ArrayBuffer, JSON, Error, Promise, Map, Set, Object,
  location: { hash: '#android=' + NONCE, pathname: '/' }, history: { replaceState() {} },
  fetch: async () => ({ status: 200, headers: { get: (k) => ({ 'X-Carrier-Mode': mode, 'X-Session-Token': TOKEN })[k] ?? null }, arrayBuffer: async () => frame(0x11, 0) }),
  addEventListener() {}, parent: {}, TelegramWebProxy: native
};
ctx.globalThis = ctx;
vm.runInNewContext(script, ctx);
const tick = (ms = 0) => new Promise((r) => setTimeout(r, ms));
// Native side sends HELLO, OPEN+DATA.
native.onmessage({ data: frame(0x10, 0, new Uint8Array([1])) });
await tick(20);
native.onmessage({ data: frame(0x01, 1) });
await tick(20);
const fails = nativeOut.filter((m) => m.startsWith('f'));
const binaries = nativeOut.filter((m) => m === 'b').length;
console.log(mode, 'native messages:', nativeOut.map((m) => m.slice(0, 40)).join(' | '));
console.log(mode, fails.length ? `NATIVE BRIDGE FAILURE (${fails[0]})` : `ok: ${binaries} binary frame(s) delivered, ${sockets.length} socket(s)`);
process.exit(fails.length ? 1 : 0);
