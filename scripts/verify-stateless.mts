// Lightweight checks (no vitest/pytest) for stateless tokens, host policy and WSS host order.
import { mintToken, tokenKey, verifyToken } from '../src/tokens';
import { sameOrigin, servedHost } from '../src/hosts';
import { telegramWssHosts } from '../src/wss';
import { carrierModeOf, statelessLanes } from '../src/carrier';

const ok = (c: boolean, m: string) => { if (!c) throw new Error('FAIL: ' + m); console.log('ok -', m); };
const env = { WEB_SECRET: '0123456789abcdef0123456789abcdef' };
const key = await tokenKey(env);
const boot = await mintToken(key, 'bootstrap', 'a.example.com', 120_000);
ok(/^[A-Za-z0-9_-]{43}$/.test(boot), 'token is 43-char base64url (bridge contract)');
ok((await verifyToken(key, 'bootstrap', 'a.example.com', boot)) !== null, 'bootstrap verifies');
ok((await verifyToken(key, 'session', 'a.example.com', boot)) === null, 'bootstrap is not a session token');
ok((await verifyToken(key, 'bootstrap', 'b.example.com', boot)) === null, 'token bound to host');
const other = await tokenKey({ WEB_SECRET: 'ffffffffffffffffffffffffffffffff' });
ok((await verifyToken(other, 'bootstrap', 'a.example.com', boot)) === null, 'token bound to secret');
const old = await mintToken(key, 'session', 'a.example.com', 1000, Date.now() - 10_000);
ok((await verifyToken(key, 'session', 'a.example.com', old)) === null, 'expired token rejected');
const sess = await mintToken(key, 'session', 'a.example.com', 24 * 3600_000);
const exp = await verifyToken(key, 'session', 'a.example.com', sess);
ok(exp !== null && exp - Date.now() > 23 * 3600_000, 'session lives ~24h (no 5-minute cliff)');
const flipped = sess.slice(0, 42) + (sess[42] === 'A' ? 'B' : 'A');
ok((await verifyToken(key, 'session', 'a.example.com', flipped)) === null, 'tampered MAC rejected');
const tk = await tokenKey({ ...env, TOKEN_SECRET: 'x'.repeat(32) });
ok((await verifyToken(tk, 'session', 'a.example.com', sess)) === null, 'TOKEN_SECRET rotates tokens independently');

const req = (u: string, origin?: string) => new Request(u, origin ? { headers: { Origin: origin } } : {});
ok(servedHost(req('https://a.example.com/'), {}) === 'a.example.com', 'no host config -> request host');
ok(servedHost(req('https://a.example.com/'), { PUBLIC_HOSTNAME: 'b.example.com' }) === null, 'unlisted host hidden');
ok(servedHost(req('https://c.example.com/'), { PUBLIC_HOSTNAME: 'b.example.com', PUBLIC_HOSTNAMES: 'c.example.com, d.example.com' }) === 'c.example.com', 'PUBLIC_HOSTNAMES list');
ok(servedHost(req('https://z.example.com/'), { PUBLIC_HOSTNAME: 'b.example.com', ALLOW_ANY_HOST: '1' }) === 'z.example.com', 'ALLOW_ANY_HOST');
ok(servedHost(req('https://127.0.0.1/'), {}) === null, 'IP literal host rejected');
ok(sameOrigin(req('https://a.example.com/api/v1/ws'), 'a.example.com'), 'absent Origin allowed (native adapter)');
ok(sameOrigin(req('https://a.example.com/api/v1/ws', 'https://a.example.com'), 'a.example.com'), 'same Origin allowed');
ok(!sameOrigin(req('https://a.example.com/api/v1/ws', 'https://evil.example'), 'a.example.com'), 'foreign Origin rejected');

ok(JSON.stringify(telegramWssHosts(2)) === JSON.stringify(['kws2.web.telegram.org', 'venus.web.telegram.org', 'kws2-1.web.telegram.org']), 'WSS hosts DC2');
ok(JSON.stringify(telegramWssHosts(-4)) === JSON.stringify(['kws4-1.web.telegram.org', 'vesta-1.web.telegram.org', 'kws4.web.telegram.org']), 'WSS hosts media DC4');

const e = (v: Record<string, string>) => ({ ...v });
ok(carrierModeOf(e({})) === 'websocket-lanes' && !statelessLanes(e({})), 'default = websocket-lanes on Durable Object backend');
ok(statelessLanes(e({ LANES_BACKEND: 'stateless' })), 'LANES_BACKEND=stateless opts into DO-free lanes');
ok(carrierModeOf(e({ CARRIER_MODE: 'websocket' })) === 'websocket' && !statelessLanes(e({ CARRIER_MODE: 'websocket' })), 'CARRIER_MODE=websocket fallback');
ok(!statelessLanes(e({ CARRIER_MODE: 'websocket', LANES_BACKEND: 'stateless' })), 'stateless only applies to lanes');
console.log('ALL STATELESS CHECKS PASSED');
