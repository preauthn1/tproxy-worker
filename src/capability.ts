const CONTEXT = 'tdesktop-web-proxy-bridge-v1\n';

function fromHex(value: string): Uint8Array {
  if (!/^[0-9a-fA-F]+$/.test(value) || value.length % 2 !== 0) throw new Error('invalid hexadecimal secret');
  const result = new Uint8Array(value.length / 2);
  for (let index = 0; index < result.length; index++) result[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  return result;
}

export function decodeSecret(value: string): Uint8Array {
  const trimmed = value.trim();
  let decoded: Uint8Array;
  if (trimmed.length === 32 || trimmed.length === 34) decoded = fromHex(trimmed);
  else {
    const canonical = trimmed.replace(/-/g, '+').replace(/_/g, '/');
    const padded = canonical + '='.repeat((4 - canonical.length % 4) % 4);
    try {
      const binary = atob(padded);
      decoded = Uint8Array.from(binary, (character) => character.charCodeAt(0));
    } catch { throw new Error('secret must be hex or base64url'); }
  }
  if (decoded.byteLength !== 16 && decoded.byteLength !== 17) throw new Error('secret must decode to 16 bytes, optionally prefixed with dd');
  if (decoded.byteLength === 17 && decoded[0] !== 0xdd) throw new Error('17-byte secret must use the dd prefix');
  return decoded;
}

export function base64Url(bytes: Uint8Array): string {
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

export async function deriveCapability(hostname: string, secret: Uint8Array): Promise<string> {
  const keyBytes = Uint8Array.from(secret);
  const key = await crypto.subtle.importKey('raw', keyBytes.buffer, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const signature = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(CONTEXT + hostname));
  return base64Url(new Uint8Array(signature));
}

export function randomToken(): string {
  return base64Url(crypto.getRandomValues(new Uint8Array(32)));
}

export function validToken(value: string): boolean {
  return /^[A-Za-z0-9_-]{43}$/.test(value);
}

export function validateHostname(hostname: string): void {
  if (hostname.length > 253 || hostname !== hostname.toLowerCase() || hostname.endsWith('.') || !hostname.includes('.') || /[:/@?#[\]]/.test(hostname)) {
    throw new Error('PUBLIC_HOSTNAME must be a canonical lowercase ASCII hostname');
  }
  for (const label of hostname.split('.')) {
    if (!/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label)) throw new Error('invalid PUBLIC_HOSTNAME label');
  }
}
