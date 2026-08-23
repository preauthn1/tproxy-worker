/* SPDX-License-Identifier: GPL-3.0-only */
import { describe, expect, it } from 'vitest';
import { decodeWebConnectionSecret, encodeWebConnectionSecret } from '../src/web-secret';

const mtSecret = Uint8Array.from({ length: 17 }, (_, index) => index === 0 ? 0xdd : index - 1);

describe('WEB connection secret envelope', () => {
  it('round-trips MT secret, canonical SNI host, and preferred edge authority', () => {
    const encoded = encodeWebConnectionSecret({
      mtSecret,
      serverName: 'proxy.example.com',
      edgeAddress: 'speed.cloudflare.example'
    });
    expect(encoded.startsWith('web1.')).toBe(true);
    expect(decodeWebConnectionSecret(encoded)).toEqual({
      version: 1,
      mtSecret,
      serverName: 'proxy.example.com',
      edgeAddress: 'speed.cloudflare.example'
    });
  });

  it('supports an IPv4 preferred edge while preserving SNI/Host', () => {
    const encoded = encodeWebConnectionSecret({ mtSecret, serverName: 'proxy.example.com', edgeAddress: '1.1.1.1' });
    expect(decodeWebConnectionSecret(encoded).edgeAddress).toBe('1.1.1.1');
    expect(decodeWebConnectionSecret(encoded).serverName).toBe('proxy.example.com');
  });

  it.each([
    { mtSecret: new Uint8Array(15), serverName: 'proxy.example.com', edgeAddress: '1.1.1.1' },
    { mtSecret, serverName: 'Proxy.Example.com', edgeAddress: '1.1.1.1' },
    { mtSecret, serverName: 'proxy.example.com', edgeAddress: 'https://1.1.1.1/' },
    { mtSecret, serverName: 'proxy.example.com', edgeAddress: '127.0.0.1' }
  ])('rejects invalid or unsafe envelopes', (value) => {
    expect(() => encodeWebConnectionSecret(value)).toThrow();
  });

  it('rejects tampered and noncanonical envelopes', () => {
    const encoded = encodeWebConnectionSecret({ mtSecret, serverName: 'proxy.example.com', edgeAddress: '1.1.1.1' });
    expect(() => decodeWebConnectionSecret(encoded.slice(0, -1) + (encoded.endsWith('A') ? 'B' : 'A'))).toThrow();
    expect(() => decodeWebConnectionSecret('web1.not_base64!')).toThrow();
  });
});
