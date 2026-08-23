/* SPDX-License-Identifier: GPL-3.0-only */
import { webcrypto } from 'node:crypto';

const subtle = webcrypto.subtle;

export interface Obfuscated2Vector {
  secret: Uint8Array;
  normalizedSecret: Uint8Array;
  clearHeader: Uint8Array;
  transformedHeader: Uint8Array;
  clearPayload: Uint8Array;
  transformedPayload: Uint8Array;
  clearResponse: Uint8Array;
  transformedResponse: Uint8Array;
}

function concat(...values: Uint8Array[]): Uint8Array {
  const output = new Uint8Array(values.reduce((sum, value) => sum + value.byteLength, 0));
  let offset = 0;
  for (const value of values) { output.set(value, offset); offset += value.byteLength; }
  return output;
}

function reverse(value: Uint8Array): Uint8Array {
  return Uint8Array.from(value).reverse();
}

async function sha256(...values: Uint8Array[]): Promise<Uint8Array> {
  return new Uint8Array(await subtle.digest('SHA-256', concat(...values)));
}

async function aesCtr(key: Uint8Array, counter: Uint8Array, value: Uint8Array): Promise<Uint8Array> {
  const imported = await subtle.importKey('raw', Uint8Array.from(key), 'AES-CTR', false, ['encrypt']);
  return new Uint8Array(await subtle.encrypt({ name: 'AES-CTR', counter: Uint8Array.from(counter), length: 128 }, imported, Uint8Array.from(value)));
}

export async function makeObfuscated2Vector(options: {
  dd?: boolean;
  tag?: number;
  dc?: number;
  clearPayload?: Uint8Array;
  clearResponse?: Uint8Array;
  reserved?: [number, number];
} = {}): Promise<Obfuscated2Vector> {
  const normalizedSecret = Uint8Array.from({ length: 16 }, (_, index) => index);
  const secret = options.dd ? concat(new Uint8Array([0xdd]), normalizedSecret) : normalizedSecret.slice();
  const clearHeader = Uint8Array.from({ length: 64 }, (_, index) => (index * 29 + 17) & 0xff);
  const view = new DataView(clearHeader.buffer);
  view.setUint32(56, options.tag ?? 0xeeeeeeee, true);
  view.setInt16(60, options.dc ?? 2, true);
  clearHeader[62] = options.reserved?.[0] ?? 0xa5;
  clearHeader[63] = options.reserved?.[1] ?? 0x5a;

  const inboundKey = await sha256(clearHeader.slice(8, 40), normalizedSecret);
  const inboundIv = clearHeader.slice(40, 56);
  const clearPayload = options.clearPayload?.slice() ?? Uint8Array.from({ length: 73 }, (_, index) => (index * 7 + 3) & 0xff);
  const inboundCiphertext = await aesCtr(inboundKey, inboundIv, concat(clearHeader, clearPayload));
  const transformedHeader = clearHeader.slice();
  transformedHeader.set(inboundCiphertext.slice(56, 64), 56);
  const transformedPayload = inboundCiphertext.slice(64);

  const reversed = reverse(clearHeader.slice(8, 56));
  const outboundKey = await sha256(reversed.slice(0, 32), normalizedSecret);
  const outboundIv = reversed.slice(32, 48);
  const clearResponse = options.clearResponse?.slice() ?? Uint8Array.from({ length: 51 }, (_, index) => (index * 11 + 9) & 0xff);
  const transformedResponse = await aesCtr(outboundKey, outboundIv, clearResponse);
  return { secret, normalizedSecret, clearHeader, transformedHeader, clearPayload, transformedPayload, clearResponse, transformedResponse };
}

export function concatenate(...values: Uint8Array[]): Uint8Array { return concat(...values); }
