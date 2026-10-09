// End-to-end check: bridge -> session -> carrier (websocket or websocket-lanes) ->
// obfuscated2 -> real Telegram DC -> req_pq_multi -> resPQ. Prints timings.
// usage: npx tsx scripts/e2e.mts <https-or-http-origin> <secret-hex> [streams]
import { webcrypto } from 'node:crypto';
import { decodeSecret, deriveCapability } from '../src/capability';
import { FrameType, encodeFrame, parseRelayBatch } from '../src/frame';

const subtle = webcrypto.subtle;
const [rawOrigin, secretText, streamsText] = process.argv.slice(2);
if (!rawOrigin || !secretText) { console.error('usage: e2e.mts <origin> <secret> [streams]'); process.exit(2); }
const origin = new URL(rawOrigin);
const streams = Number(streamsText || 3);
const DC = Number(process.env.DC || 2);
const HOLD_MS = Number(process.env.HOLD_MS || 0); // keep lanes open and re-probe periodically
const secret = decodeSecret(secretText);
const mtSecret = secret.byteLength === 17 ? secret.slice(1) : secret.slice(0, 16);

const t0 = performance.now();
const ms = () => (performance.now() - t0).toFixed(0) + 'ms';
const concat = (...a: Uint8Array[]) => { const o = new Uint8Array(a.reduce((s, x) => s + x.length, 0)); let k = 0; for (const x of a) { o.set(x, k); k += x.length; } return o; };
const sha256 = async (...a: Uint8Array[]) => new Uint8Array(await subtle.digest('SHA-256', concat(...a)));

class Ctr {
  #key: Promise<CryptoKey>; #iv: Uint8Array; #offset = 0;
  constructor(key: Uint8Array, iv: Uint8Array) { this.#key = subtle.importKey('raw', key, 'AES-CTR', false, ['encrypt']); this.#iv = iv.slice(); }
  async apply(data: Uint8Array): Promise<Uint8Array> {
    const block = Math.floor(this.#offset / 16), skip = this.#offset % 16;
    const counter = this.#iv.slice(); let carry = block;
    for (let i = 15; i >= 0 && carry > 0; i--) { const v = counter[i]! + (carry & 0xff); counter[i] = v & 0xff; carry = Math.floor(carry / 256) + (v >> 8); }
    const padded = concat(new Uint8Array(skip), data);
    const out = new Uint8Array(await subtle.encrypt({ name: 'AES-CTR', counter, length: 128 }, await this.#key, padded)).slice(skip);
    this.#offset += data.byteLength; return out;
  }
}

// One obfuscated2 client toward DC (env DC, default 2) using intermediate (0xeeeeeeee) transport.
async function makeClient() {
  let header: Uint8Array;
  for (;;) {
    header = webcrypto.getRandomValues(new Uint8Array(64));
    const v = new DataView(header.buffer);
    const first = v.getUint32(0, true), second = v.getUint32(4, true);
    if (header[0] === 0xef || [0x44414548, 0x54534f50, 0x20544547, 0x4954504f, 0xeeeeeeee, 0xdddddddd, 0x02010316].includes(first) || second === 0) continue;
    v.setUint32(56, 0xeeeeeeee, true); v.setInt16(60, DC, true); break;
  }
  const enc = new Ctr(await sha256(header.slice(8, 40), mtSecret), header.slice(40, 56));
  const rev = Uint8Array.from(header.slice(8, 56)).reverse();
  const dec = new Ctr(await sha256(rev.slice(0, 32), mtSecret), rev.slice(32, 48));
  const encHeader = await enc.apply(header);
  const wire = header.slice(); wire.set(encHeader.slice(56, 64), 56);
  // req_pq_multi with unencrypted MTProto envelope.
  const nonce = webcrypto.getRandomValues(new Uint8Array(16));
  const body = new Uint8Array(20); new DataView(body.buffer).setUint32(0, 0xbe7e8ef1, true); body.set(nonce, 4);
  const msg = new Uint8Array(20 + body.length); const mv = new DataView(msg.buffer);
  mv.setBigUint64(8, BigInt(Math.floor(Date.now() / 1000)) << 32n, true); mv.setUint32(16, body.length, true); msg.set(body, 20);
  const packet = new Uint8Array(4 + msg.length); new DataView(packet.buffer).setUint32(0, msg.length, true); packet.set(msg, 4);
  return { first: concat(wire, await enc.apply(packet)), enc, dec, nonce, packet: async () => {
    const n = webcrypto.getRandomValues(new Uint8Array(16));
    const b = new Uint8Array(20); new DataView(b.buffer).setUint32(0, 0xbe7e8ef1, true); b.set(n, 4);
    const m = new Uint8Array(40); const v2 = new DataView(m.buffer);
    v2.setBigUint64(8, BigInt(Math.floor(Date.now() / 1000)) << 32n, true); v2.setUint32(16, 20, true); m.set(b, 20);
    const p = new Uint8Array(44); new DataView(p.buffer).setUint32(0, 40, true); p.set(m, 4);
    return enc.apply(p);
  } };
}

const capability = await deriveCapability(origin.hostname, secret);
const bridge = await fetch(new URL(`/?bridge=${capability}`, origin));
const html = await bridge.text();
const bootstrap = JSON.parse(/const bootstrap=("[A-Za-z0-9_-]{43}")/.exec(html)![1]!) as string;
const pageMode = JSON.parse(/const carrierMode=("[a-z-]+")/.exec(html)![1]!) as string;
console.log(ms(), 'bridge', bridge.status, 'mode', pageMode);
const created = await fetch(new URL('/api/v1/session', origin), {
  method: 'POST', headers: { Authorization: `Bearer ${bootstrap}`, 'Content-Type': 'application/octet-stream' },
  body: Uint8Array.from(encodeFrame(FrameType.Hello, 0, new Uint8Array([1]))).buffer
});
const token = created.headers.get('X-Session-Token')!, mode = created.headers.get('X-Carrier-Mode');
const welcome = parseRelayBatch(new Uint8Array(await created.arrayBuffer()));
if (created.status !== 200 || welcome[0]?.type !== FrameType.Welcome || mode !== pageMode) throw new Error(`create failed ${created.status} ${mode}`);
console.log(ms(), 'session created, carrier', mode);
const wsOrigin = origin.href.replace(/^http/, 'ws').replace(/\/$/, '') + '/api/v1/ws';

function openSocket(protocol: string): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsOrigin, protocol); ws.binaryType = 'arraybuffer';
    ws.onopen = () => resolve(ws); ws.onerror = () => reject(new Error('ws error ' + protocol.slice(0, 16)));
  });
}

const holds: Promise<void>[] = [];
async function runStream(id: number, ws: WebSocket, shared: boolean) {
  const c = await makeClient();
  let probes = 0; let want = 1; let onProbe: (() => void) | undefined;
  const start = performance.now();
  const done = new Promise<number>((resolve, reject) => {
    let buf = new Uint8Array();
    const timer = setTimeout(() => reject(new Error(`stream ${id} timeout`)), 20000);
    ws.addEventListener('close', (event) => { if (HOLD_MS > 0) console.log(ms(), `stream ${id} socket closed code=${(event as CloseEvent).code} after ${probes} probes`); });
    ws.addEventListener('message', async (event) => {
      for (const f of parseRelayBatch(new Uint8Array(event.data as ArrayBuffer))) {
        if (f.streamId !== id) { if (!shared) reject(new Error('cross-lane frame')); continue; }
        if (f.type === FrameType.Close) { clearTimeout(timer); if (probes > 0) console.log(ms(), `stream ${id} CLOSE from relay after ${probes} probes`); reject(new Error(`stream ${id} closed by relay`)); return; }
        if (f.type !== FrameType.Data) continue;
        buf = concat(buf, await c.dec.apply(f.payload));
        while (buf.length >= 4) {
          const len = new DataView(buf.buffer, buf.byteOffset).getUint32(0, true);
          if (buf.length < 4 + len) break;
          const ctor = new DataView(buf.buffer, buf.byteOffset).getUint32(4 + 20, true);
          buf = buf.slice(4 + len);
          if (ctor !== 0x05162463) { reject(new Error(`stream ${id} unexpected ctor ${ctor.toString(16)}`)); return; }
          probes++;
          if (probes === 1) { clearTimeout(timer); resolve(performance.now() - start); }
          else onProbe?.();
        }
      }
    });
  });
  ws.send(concat(encodeFrame(FrameType.Open, id), encodeFrame(FrameType.Data, id, c.first)));
  if (HOLD_MS > 0) holds.push(done.then(async () => {
    const until = Date.now() + HOLD_MS;
    while (Date.now() < until) {
      await new Promise((r) => setTimeout(r, Math.min(30_000, until - Date.now())));
      if (Date.now() >= until) break;
      want++;
      const got = new Promise<void>((resolve, reject) => { const t = setTimeout(() => reject(new Error(`stream ${id} re-probe ${want} timeout`)), 20_000); onProbe = () => { if (probes >= want) { clearTimeout(t); resolve(); } }; });
      ws.send(encodeFrame(FrameType.Data, id, await c.packet()));
      await got;
    }
  }));
  return done;
}

const results: number[] = [];
if (mode === 'websocket-lanes') {
  const sockets = await Promise.all(Array.from({ length: streams }, (_, i) => openSocket(`tproxy-lane-v1.${token}.${i + 1}`)));
  console.log(ms(), streams, 'lane sockets open');
  results.push(...await Promise.all(sockets.map((ws, i) => runStream(i + 1, ws, false))));
  if (HOLD_MS > 0) {
    console.log(ms(), `holding ${streams} lanes for ${HOLD_MS}ms with 30s re-probes`);
    await Promise.all(holds);
    // a brand-new lane after the hold proves the session token outlives the peer's 5-minute cliff
    const late = await openSocket(`tproxy-lane-v1.${token}.${streams + 1}`);
    console.log(ms(), 'late lane after hold: resPQ in', (await runStream(streams + 1, late, false)).toFixed(0), 'ms');
    late.close();
  }
  for (const ws of sockets) ws.close();
} else {
  const ws = await openSocket(`tproxy-v1.${token}`);
  console.log(ms(), 'shared socket open');
  results.push(...await Promise.all(Array.from({ length: streams }, (_, i) => runStream(i + 1, ws, true))));
  ws.close();
}
console.log(ms(), `resPQ from Telegram DC${DC} on`, results.length, 'streams; RTT ms:', results.map((x) => x.toFixed(0)).join(', '));
await fetch(new URL('/api/v1/session', origin), { method: 'DELETE', headers: { Authorization: `Bearer ${token}` } });
console.log('E2E OK');
process.exit(0);
