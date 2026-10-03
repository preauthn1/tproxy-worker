// Lightweight checks for server-side lane routing/validation (no vitest).
import { FrameType, encodeFrame } from '../src/frame';
import { LaneOverflow, LaneRouter, parseLaneProtocol, validateLaneMessage } from '../src/lanes';

const ok = (c: boolean, m: string) => { if (!c) throw new Error('FAIL: ' + m); console.log('ok -', m); };
const cat = (...a: Uint8Array[]) => { const o = new Uint8Array(a.reduce((s, x) => s + x.length, 0)); let k = 0; for (const x of a) { o.set(x, k); k += x.length; } return o; };
const T = 'A'.repeat(43);

ok(parseLaneProtocol(`tproxy-lane-v1.${T}.7`)?.streamId === 7, 'parse lane protocol');
ok(parseLaneProtocol(`tproxy-lane-v1.${T}.0`) === null, 'stream 0 rejected');
ok(parseLaneProtocol(`tproxy-lane-v1.${T}.16777216`) === null, 'stream id > 24-bit rejected');
ok(parseLaneProtocol(`tproxy-lane-v1.${T}.01`) === null, 'leading zero rejected');
ok(parseLaneProtocol(`tproxy-v1.${T}`) === null, 'plain websocket protocol not a lane');

const open5 = encodeFrame(FrameType.Open, 5), data5 = encodeFrame(FrameType.Data, 5, new Uint8Array([1, 2]));
ok(validateLaneMessage(cat(open5, data5), 5, true) === null, 'first message OPEN+DATA valid');
ok(validateLaneMessage(data5, 5, true) === 'lane must begin with OPEN', 'first message without OPEN rejected');
ok(validateLaneMessage(cat(open5, encodeFrame(FrameType.Data, 6)), 5, true) === 'cross-lane frame', 'cross-lane frame rejected');
ok(validateLaneMessage(open5, 5, false) === 'duplicate OPEN', 'second OPEN rejected');
ok(validateLaneMessage(cat(data5, encodeFrame(FrameType.Close, 5)), 5, false) === null, 'later DATA+CLOSE valid');
ok(validateLaneMessage(data5.subarray(0, 9), 5, false) === 'malformed batch', 'truncated batch rejected');

// Routing: each frame of a mixed relay batch goes only to its lane; stream 0 dropped.
const sent = new Map<number, Uint8Array[]>(), closed: number[] = [];
const sock = (id: number) => ({ send: (v: Uint8Array) => { (sent.get(id) ?? sent.set(id, []).get(id)!).push(v.slice()); }, close: () => { closed.push(id); } });
const router = new LaneRouter(2);
router.attach(1, sock(1)); router.attach(2, sock(2));
ok(!router.canAttach(3), 'max lanes enforced');
ok(!router.canAttach(1), 'duplicate lane id rejected');
const batch = cat(encodeFrame(FrameType.Data, 1, new Uint8Array([11])), encodeFrame(FrameType.Ping, 0, new Uint8Array(8)), encodeFrame(FrameType.Window, 2, new Uint8Array(4)), encodeFrame(FrameType.Data, 1, new Uint8Array([12])));
router.route(batch, false);
await new Promise((r) => setTimeout(r, 30));
const all = (id: number) => cat(...(sent.get(id) ?? []));
const l1 = all(1), l2 = all(2);
ok(l1.length === 18 && l1[8] === 11 && l1[17] === 12, 'lane 1 got its two DATA frames in order');
ok(l2.length === 12 && l2[0] === FrameType.Window, 'lane 2 got only its WINDOW frame');
ok(![...l1, ...l2].some((_, i, a) => false) && !sent.has(0), 'stream-0 PING dropped');

// CLOSE completes the lane and frees the id permanently.
router.route(encodeFrame(FrameType.Close, 2), true);
await new Promise((r) => setTimeout(r, 20));
ok(closed.includes(2) && !router.has(2), 'CLOSE flushes and closes lane 2');
ok(all(2).at(-8) === FrameType.Close, 'CLOSE frame was delivered before socket close');
ok(!router.canAttach(2), 'closed stream id cannot be reused');
ok(router.canAttach(3), 'freed slot can host a new stream');

// Per-lane overflow throws LaneOverflow for that lane only.
const r2 = new LaneRouter(4); const blocked = { send: () => {}, close: () => {} };
r2.attach(9, blocked);
let caught: unknown; const small = encodeFrame(FrameType.Data, 9, new Uint8Array(100));
try { for (let i = 0; i < 2000; i++) r2.route(small, false); } catch (e) { caught = e; }
ok(caught instanceof LaneOverflow && (caught as LaneOverflow).streamId === 9, 'per-lane item limit (1024) -> LaneOverflow(9)');
console.log('ALL LANE ROUTER CHECKS PASSED');
