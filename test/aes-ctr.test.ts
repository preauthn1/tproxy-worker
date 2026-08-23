/* SPDX-License-Identifier: GPL-3.0-only */
import { webcrypto } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { StreamingAes256Ctr } from '../src/aes-ctr';
import { concatenate } from './obfuscated2-vectors';

async function reference(key: Uint8Array, counter: Uint8Array, input: Uint8Array): Promise<Uint8Array> {
  const imported = await webcrypto.subtle.importKey('raw', key, 'AES-CTR', false, ['encrypt']);
  return new Uint8Array(await webcrypto.subtle.encrypt({ name: 'AES-CTR', counter, length: 128 }, imported, input));
}

describe('StreamingAes256Ctr', () => {
  it('is byte-exact across arbitrary partial-block chunk boundaries', async () => {
    const key = Uint8Array.from({ length: 32 }, (_, index) => index * 3);
    const counter = Uint8Array.from({ length: 16 }, (_, index) => 255 - index * 5);
    const input = Uint8Array.from({ length: 513 }, (_, index) => (index * 17 + 41) & 0xff);
    const expected = await reference(key, counter, input);
    const cipher = await StreamingAes256Ctr.create(key, counter);
    const sizes = [1, 15, 2, 31, 16, 7, 64, 3, 129, 5, 240];
    const output: Uint8Array[] = [];
    let offset = 0;
    for (const size of sizes) {
      const end = Math.min(input.byteLength, offset + size);
      if (end > offset) output.push(await cipher.transform(input.slice(offset, end)));
      offset = end;
    }
    if (offset < input.byteLength) output.push(await cipher.transform(input.slice(offset)));
    expect(concatenate(...output)).toEqual(expected);
  });

  it('does not restart the counter for consecutive sub-16-byte chunks', async () => {
    const key = new Uint8Array(32).fill(0x42);
    const counter = new Uint8Array(16).fill(0x24);
    const cipher = await StreamingAes256Ctr.create(key, counter);
    const output = concatenate(await cipher.transform(new Uint8Array([1])), await cipher.transform(new Uint8Array(15).fill(2)), await cipher.transform(new Uint8Array([3])));
    expect(output).toEqual(await reference(key, counter, concatenate(new Uint8Array([1]), new Uint8Array(15).fill(2), new Uint8Array([3]))));
  });
});
