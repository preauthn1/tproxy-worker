// Lightweight byte-exactness check for StreamingAes256Ctr prefetch (Node webcrypto reference).
import { webcrypto } from 'node:crypto';
import { StreamingAes256Ctr, KEYSTREAM_PREFETCH_BYTES } from '../src/aes-ctr';
import { REGIONS, regionFor, regionalToken, tokenRegion } from '../src/region';
import { validToken } from '../src/capability';

async function reference(key: Uint8Array, counter: Uint8Array, input: Uint8Array): Promise<Uint8Array> {
  const k = await webcrypto.subtle.importKey('raw', key, 'AES-CTR', false, ['encrypt']);
  return new Uint8Array(await webcrypto.subtle.encrypt({ name: 'AES-CTR', counter, length: 128 }, k, input));
}
const eq = (a: Uint8Array, b: Uint8Array) => a.byteLength === b.byteLength && a.every((v, i) => v === b[i]);
let rng = 12345;
const rand = (n: number) => { rng = (rng * 1103515245 + 12345) >>> 0; return rng % n; };

let cases = 0;
for (const counterFill of [0x00, 0xff, 0xfe]) {
  for (const total of [1, 17, KEYSTREAM_PREFETCH_BYTES - 1, KEYSTREAM_PREFETCH_BYTES, KEYSTREAM_PREFETCH_BYTES * 3 + 7, 300_000]) {
    const key = Uint8Array.from({ length: 32 }, (_, i) => (i * 7 + counterFill) & 0xff);
    const counter = new Uint8Array(16).fill(counterFill); // 0xff.. exercises full 128-bit carry wrap
    const input = Uint8Array.from({ length: total }, (_, i) => (i * 31 + 5) & 0xff);
    const expected = await reference(key, counter, input);
    const c = await StreamingAes256Ctr.create(key, counter);
    const out = new Uint8Array(total);
    let off = 0;
    while (off < total) {
      const size = Math.min(total - off, [1, 15, 16, 33, 1500, 70_000][rand(6)]!);
      // unaligned input view
      const buf = new Uint8Array(size + 3); buf.set(input.subarray(off, off + size), 3);
      out.set(await c.transform(buf.subarray(3)), off);
      off += size;
    }
    if (!eq(out, expected)) throw new Error(`mismatch fill=${counterFill} total=${total}`);
    cases++;
  }
}
for (const r of REGIONS) {
  const t = regionalToken(r);
  if (!validToken(t) || tokenRegion(t) !== r) throw new Error('region token ' + r);
}
const g = regionalToken(undefined);
if (!validToken(g) || tokenRegion(g) !== undefined) throw new Error('global token');
const checks: Array<[object, string | undefined]> = [
  [{ continent: 'AS', country: 'JP' }, 'apac'], [{ continent: 'AS', country: 'HK' }, 'apac'],
  [{ continent: 'AS', country: 'AE' }, 'me'], [{ continent: 'EU', country: 'DE' }, 'weur'],
  [{ continent: 'EU', country: 'PL' }, 'eeur'], [{ continent: 'NA', longitude: '-122.4' }, 'wnam'],
  [{ continent: 'NA', longitude: '-77' }, 'enam'], [{}, undefined]
];
for (const [cf, want] of checks) if (regionFor(cf) !== want) throw new Error('regionFor ' + JSON.stringify(cf));
console.log(`aes-ctr byte-exact cases: ${cases}; region checks OK`);
