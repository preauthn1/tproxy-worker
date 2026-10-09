/* SPDX-License-Identifier: GPL-3.0-only */
// Stateless HMAC bootstrap/session tokens for the Durable-Object-free lanes
// backend (idea from ToiCF/CF-Workers-TGProxy). Unlike that project the
// session lifetime is long (no 5-minute expiry), the MAC binds the public
// hostname, and the key is derived from WEB_SECRET unless TOKEN_SECRET is set.
import { base64Url, decodeSecret, validToken } from './capability';

export type TokenKind = 'bootstrap' | 'session';

export const STATELESS_BOOTSTRAP_TTL_MS = 2 * 60 * 1000;
export const STATELESS_SESSION_TTL_MS = 24 * 60 * 60 * 1000;

const BODY_BYTES = 16; // 4-byte big-endian expiry (seconds) + 12-byte nonce
const MAC_BYTES = 16;
const encoder = new TextEncoder();
const keyCache = new Map<string, Promise<CryptoKey>>();

function unBase64Url(value: string): Uint8Array {
  const canonical = value.replace(/-/g, '+').replace(/_/g, '/');
  const padded = canonical + '='.repeat((4 - canonical.length % 4) % 4);
  return Uint8Array.from(atob(padded), (character) => character.charCodeAt(0));
}

async function hmac(key: CryptoKey, message: Uint8Array): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.sign('HMAC', key, Uint8Array.from(message).buffer));
}

/** Token MAC key: TOKEN_SECRET if configured, otherwise HMAC(WEB_SECRET, label). */
export function tokenKey(env: { WEB_SECRET: string; TOKEN_SECRET?: string | undefined }): Promise<CryptoKey> {
  const material = env.TOKEN_SECRET?.trim() ? `t:${env.TOKEN_SECRET.trim()}` : `w:${env.WEB_SECRET.trim().toLowerCase()}`;
  let cached = keyCache.get(material);
  if (!cached) {
    cached = (async () => {
      let raw: Uint8Array;
      if (env.TOKEN_SECRET?.trim()) raw = encoder.encode(env.TOKEN_SECRET.trim());
      else {
        const secret = decodeSecret(env.WEB_SECRET);
        try {
          const base = await crypto.subtle.importKey('raw', Uint8Array.from(secret).buffer, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
          raw = await hmac(base, encoder.encode('tproxy-stateless-token-key-v1'));
        } finally { secret.fill(0); }
      }
      try { return await crypto.subtle.importKey('raw', Uint8Array.from(raw).buffer, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']); }
      finally { raw.fill(0); }
    })();
    cached.catch(() => keyCache.delete(material));
    if (keyCache.size > 16) keyCache.clear();
    keyCache.set(material, cached);
  }
  return cached;
}

async function mac(key: CryptoKey, kind: TokenKind, host: string, body: Uint8Array): Promise<Uint8Array> {
  const prefix = encoder.encode(`tproxy-token-v1\n${kind}\n${host}\n`);
  const message = new Uint8Array(prefix.byteLength + body.byteLength);
  message.set(prefix);
  message.set(body, prefix.byteLength);
  return (await hmac(key, message)).slice(0, MAC_BYTES);
}

export async function mintToken(key: CryptoKey, kind: TokenKind, host: string, ttlMs: number, now = Date.now()): Promise<string> {
  const body = new Uint8Array(BODY_BYTES);
  new DataView(body.buffer).setUint32(0, Math.ceil((now + ttlMs) / 1000));
  crypto.getRandomValues(body.subarray(4));
  const token = new Uint8Array(BODY_BYTES + MAC_BYTES);
  token.set(body);
  token.set(await mac(key, kind, host, body), BODY_BYTES);
  return base64Url(token);
}

/** Returns the token expiry (ms since epoch) when valid, otherwise null. Constant-time MAC compare. */
export async function verifyToken(key: CryptoKey, kind: TokenKind, host: string, value: string, now = Date.now()): Promise<number | null> {
  if (!validToken(value)) return null;
  let token: Uint8Array;
  try { token = unBase64Url(value); } catch { return null; }
  if (token.byteLength !== BODY_BYTES + MAC_BYTES || base64Url(token) !== value) return null;
  const body = token.subarray(0, BODY_BYTES);
  const expiresAt = new DataView(token.buffer, token.byteOffset, 4).getUint32(0) * 1000;
  if (expiresAt <= now) return null;
  const want = await mac(key, kind, host, body);
  const got = token.subarray(BODY_BYTES);
  let difference = 0;
  for (let index = 0; index < MAC_BYTES; index++) difference |= want[index]! ^ got[index]!;
  return difference === 0 ? expiresAt : null;
}
