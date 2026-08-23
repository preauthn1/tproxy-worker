/* SPDX-License-Identifier: GPL-3.0-only */
import { base64Url, validateHostname } from './capability';

export interface WebConnectionSecretInput {
  mtSecret: Uint8Array;
  serverName: string;
  edgeAddress: string;
}

export interface WebConnectionSecret extends WebConnectionSecretInput {
  version: 1;
}

interface WireEnvelope {
  v: 1;
  s: string;
  h: string;
  e: string;
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('');
}

function fromHex(value: string): Uint8Array {
  if (!/^[0-9a-f]+$/.test(value) || value.length % 2 !== 0) throw new Error('invalid MTProxy secret hex');
  return Uint8Array.from({ length: value.length / 2 }, (_, index) => Number.parseInt(value.slice(index * 2, index * 2 + 2), 16));
}

function validateMtSecret(secret: Uint8Array): void {
  if (secret.byteLength === 16) return;
  if (secret.byteLength === 17 && secret[0] === 0xdd) return;
  throw new Error('MTProxy secret must be 16 bytes, optionally prefixed with dd');
}

function publicIpv4(value: string): boolean {
  const parts = value.split('.');
  if (parts.length !== 4 || parts.some((part) => !/^(0|[1-9][0-9]{0,2})$/.test(part) || Number(part) > 255)) return false;
  const numbers = parts.map(Number);
  const a = numbers[0]!;
  const b = numbers[1]!;
  if (a === 0 || a === 10 || a === 127 || a >= 224) return false;
  if (a === 100 && b >= 64 && b <= 127) return false;
  if (a === 169 && b === 254) return false;
  if (a === 172 && b >= 16 && b <= 31) return false;
  if (a === 192 && (b === 0 || b === 168)) return false;
  if (a === 198 && (b === 18 || b === 19)) return false;
  return true;
}

function validateEdgeAddress(value: string): void {
  if (/^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$/.test(value)) {
    if (!publicIpv4(value)) throw new Error('preferred edge IP must be public');
    return;
  }
  validateHostname(value);
}

function canonicalWire(input: WebConnectionSecretInput): WireEnvelope {
  validateMtSecret(input.mtSecret);
  validateHostname(input.serverName);
  validateEdgeAddress(input.edgeAddress);
  return { v: 1, s: hex(input.mtSecret), h: input.serverName, e: input.edgeAddress };
}

export function encodeWebConnectionSecret(input: WebConnectionSecretInput): string {
  const wire = canonicalWire(input);
  return `web1.${base64Url(new TextEncoder().encode(JSON.stringify(wire)))}`;
}

export function decodeWebConnectionSecret(value: string): WebConnectionSecret {
  if (!value.startsWith('web1.')) throw new Error('unsupported WEB secret version');
  const payload = value.slice(5);
  if (!/^[A-Za-z0-9_-]+$/.test(payload)) throw new Error('invalid WEB secret encoding');
  const canonical = payload.replace(/-/g, '+').replace(/_/g, '/');
  const padded = canonical + '='.repeat((4 - canonical.length % 4) % 4);
  let bytes: Uint8Array;
  try { bytes = Uint8Array.from(atob(padded), (character) => character.charCodeAt(0)); }
  catch { throw new Error('invalid WEB secret encoding'); }
  if (base64Url(bytes) !== payload) throw new Error('noncanonical WEB secret encoding');
  let parsed: unknown;
  try { parsed = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); }
  catch { throw new Error('invalid WEB secret payload'); }
  if (!parsed || typeof parsed !== 'object') throw new Error('invalid WEB secret payload');
  const wire = parsed as Partial<WireEnvelope>;
  if (wire.v !== 1 || typeof wire.s !== 'string' || typeof wire.h !== 'string' || typeof wire.e !== 'string') {
    throw new Error('invalid WEB secret payload');
  }
  const mtSecret = fromHex(wire.s);
  const canonicalObject = canonicalWire({ mtSecret, serverName: wire.h, edgeAddress: wire.e });
  if (JSON.stringify(wire) !== JSON.stringify(canonicalObject)) throw new Error('noncanonical WEB secret payload');
  return { version: 1, mtSecret, serverName: wire.h, edgeAddress: wire.e };
}

/**
 * Native adapter rule:
 * 1. Resolve/connect `edgeAddress` on TCP 443.
 * 2. Perform TLS with SNI=`serverName` and validate that hostname normally.
 * 3. Send HTTP Host/`:authority`=`serverName` and open the bridge/WSS paths there.
 * Browser-only fetch/WebSocket cannot apply this address/SNI split.
 */
export function webConnectionRouting(secret: WebConnectionSecret): {
  connectAddress: string;
  port: 443;
  serverName: string;
  authority: string;
} {
  return { connectAddress: secret.edgeAddress, port: 443, serverName: secret.serverName, authority: secret.serverName };
}
