/* SPDX-License-Identifier: GPL-3.0-only */
// Obfuscated2 derivation is based on TelegramMessenger/MTProxy commit
// f36d8af769ffaeac36978d38c2c0f6d1104c2137 (LGPL-2.0-or-later).
import { StreamingAes256Ctr } from './aes-ctr';
import { telegramDcCandidates, type TelegramEndpoint } from './telegram-dc';
import type { DialLimiter } from './dial-limiter';
import { diag, errorKind } from './diag';

const HEADER_BYTES = 64;
const DEFAULT_HANDSHAKE_TIMEOUT_MS = 30_000;
const VALID_TAGS = new Set([0xeeeeeeee, 0xdddddddd, 0xefefefef]);
/** Outbound fallback dialer (e.g. Telegram web WSS); receives the validated signed DC and transport tag. */
export type TelegramFallbackDialer = (signedDc: number, tag: number, signal: AbortSignal) => Promise<DirectTelegramConnection>;

export interface MtProxyOptions {
  handshakeTimeoutMs?: number;
  dialTimeoutMs?: number;
  limiter?: DialLimiter;
  fallback?: TelegramFallbackDialer | undefined;
  /** Skip TCP and use only the fallback dialer. */
  fallbackOnly?: boolean;
  streamId?: number;
}

function transportMarker(tag: number): Uint8Array {
  if (tag === 0xeeeeeeee) return Uint8Array.of(0xee, 0xee, 0xee, 0xee);
  if (tag === 0xdddddddd) return Uint8Array.of(0xdd, 0xdd, 0xdd, 0xdd);
  if (tag === 0xefefefef) return Uint8Array.of(0xef);
  throw new Error('invalid MTProxy transport tag');
}

const DEFAULT_DIAL_TIMEOUT_MS = 5_000;

function withTimeout<T>(promise: Promise<T>, ms: number, onLate: (value: T) => void): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => { settled = true; reject(new Error('dial timeout')); }, ms);
    promise.then((value) => {
      if (settled) { onLate(value); return; }
      settled = true; clearTimeout(timer); resolve(value);
    }, (error: unknown) => {
      if (settled) return;
      settled = true; clearTimeout(timer);
      reject(error instanceof Error ? error : new Error('dial failed'));
    });
  });
}

export interface DirectTelegramConnection {
  write(data: Uint8Array): Promise<void>;
  read(): AsyncIterable<Uint8Array>;
  close(): void;
}

export interface TelegramDialer { connect(endpoint: TelegramEndpoint, signal?: AbortSignal): Promise<DirectTelegramConnection> }

function concatenate(...values: Uint8Array[]): Uint8Array {
  const output = new Uint8Array(values.reduce((sum, value) => sum + value.byteLength, 0));
  let offset = 0;
  for (const value of values) { output.set(value, offset); offset += value.byteLength; }
  return output;
}

async function sha256(source: Uint8Array, secret: Uint8Array): Promise<Uint8Array> {
  const material = concatenate(source, secret);
  try { return new Uint8Array(await crypto.subtle.digest('SHA-256', Uint8Array.from(material))); }
  finally { material.fill(0); }
}

export function normalizeMtProxySecret(secret: Uint8Array): Uint8Array {
  if (secret.byteLength === 16) return secret.slice();
  if (secret.byteLength === 17 && secret[0] === 0xdd) return secret.slice(1);
  throw new Error('MTProxy secret must be 16 bytes, optionally prefixed with dd');
}

export class MtProxyTerminator implements DirectTelegramConnection {
  readonly #secret: Uint8Array;
  readonly #dialer: TelegramDialer;
  readonly #header = new Uint8Array(HEADER_BYTES);
  #headerBytes = 0;
  #inbound: StreamingAes256Ctr | undefined;
  #outbound: StreamingAes256Ctr | undefined;
  #connection: DirectTelegramConnection | undefined;
  #closed = false;
  #readySettled = false;
  readonly #ready: Promise<void>;
  readonly #resolveReady: () => void;
  readonly #rejectReady: (error: Error) => void;
  readonly #handshakeTimer: ReturnType<typeof setTimeout>;
  readonly #options: MtProxyOptions;
  readonly #abort = new AbortController();

  constructor(secret: Uint8Array, dialer: TelegramDialer, options: MtProxyOptions | number = {}) {
    this.#options = typeof options === 'number' ? { handshakeTimeoutMs: options } : options;
    const handshakeTimeoutMs = this.#options.handshakeTimeoutMs ?? DEFAULT_HANDSHAKE_TIMEOUT_MS;
    this.#secret = normalizeMtProxySecret(secret);
    this.#dialer = dialer;
    let resolveReady!: () => void;
    let rejectReady!: (error: Error) => void;
    this.#ready = new Promise<void>((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
    // A read may not have attached yet when this fires; mark the promise handled
    // while preserving its rejection for the eventual reader.
    void this.#ready.catch(() => undefined);
    this.#resolveReady = resolveReady;
    this.#rejectReady = rejectReady;
    this.#handshakeTimer = setTimeout(() => {
      const error = new Error('MTProxy handshake deadline exceeded');
      this.#settleReady(error);
      this.close(error);
    }, handshakeTimeoutMs);
  }

  async write(data: Uint8Array): Promise<void> {
    if (this.#closed) throw new Error('MTProxy terminator closed');
    let offset = 0;
    if (this.#headerBytes < HEADER_BYTES) {
      const length = Math.min(HEADER_BYTES - this.#headerBytes, data.byteLength);
      this.#header.set(data.subarray(0, length), this.#headerBytes);
      this.#headerBytes += length;
      offset = length;
      if (this.#headerBytes < HEADER_BYTES) return;
      await this.#initialize();
    }
    if (offset < data.byteLength) {
      const clear = await this.#inbound!.transform(data.subarray(offset));
      try { await this.#connection!.write(clear); }
      finally { clear.fill(0); }
    }
  }

  async *read(): AsyncIterable<Uint8Array> {
    await this.#ready;
    if (this.#closed || !this.#connection || !this.#outbound) return;
    for await (const clear of this.#connection.read()) {
      if (this.#closed) return;
      yield await this.#outbound.transform(clear);
    }
  }

  close(error = new Error('MTProxy terminator closed')): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#abort.abort();
    clearTimeout(this.#handshakeTimer);
    this.#settleReady(error);
    this.#connection?.close();
    this.#inbound?.close();
    this.#outbound?.close();
    this.#secret.fill(0);
    this.#header.fill(0);
  }

  async #initialize(): Promise<void> {
    const source = this.#header.slice(8, 40);
    const iv = this.#header.slice(40, 56);
    const reversed = Uint8Array.from(this.#header.slice(8, 56)).reverse();
    const outboundSource = reversed.slice(0, 32);
    const outboundIv = reversed.slice(32, 48);
    let inboundKey: Uint8Array<ArrayBufferLike> = new Uint8Array();
    let outboundKey: Uint8Array<ArrayBufferLike> = new Uint8Array();
    let decrypted: Uint8Array<ArrayBufferLike> = new Uint8Array();
    try {
      inboundKey = await sha256(source, this.#secret);
      outboundKey = await sha256(outboundSource, this.#secret);
      this.#inbound = await StreamingAes256Ctr.create(inboundKey, iv);
      this.#outbound = await StreamingAes256Ctr.create(outboundKey, outboundIv);
      decrypted = await this.#inbound.transform(this.#header.slice());
      const view = new DataView(decrypted.buffer, decrypted.byteOffset, decrypted.byteLength);
      const tag = view.getUint32(56, true);
      if (!VALID_TAGS.has(tag)) throw new Error('invalid MTProxy transport tag');
      const marker = transportMarker(tag);
      const dc = view.getInt16(60, true);
      const candidates = telegramDcCandidates(dc);
      const streamId = this.#options.streamId;
      const started = Date.now();
      let release: (() => void) | undefined;
      try {
        if (this.#options.limiter) {
          const queuedAt = Date.now();
          release = await this.#options.limiter.acquire(this.#abort.signal);
          const waited = Date.now() - queuedAt;
          if (waited > 50) diag('dial_queued', { stream: streamId, dc, waitMs: waited });
        }
        let lastError: unknown;
        for (const endpoint of this.#options.fallbackOnly ? [] : candidates) {
          if (this.#closed) throw new Error('MTProxy terminator closed');
          const attemptAt = Date.now();
          let connection: DirectTelegramConnection | undefined;
          try {
            connection = await withTimeout(
              this.#dialer.connect(endpoint, this.#abort.signal),
              this.#options.dialTimeoutMs ?? DEFAULT_DIAL_TIMEOUT_MS,
              (late) => late.close()
            );
            if (this.#closed) { connection.close(); throw new Error('MTProxy terminator closed'); }
            await connection.write(marker);
            if (this.#closed) { connection.close(); throw new Error('MTProxy terminator closed'); }
            this.#adopt(connection);
            diag('dc_connected', { stream: streamId, dc, via: 'tcp', ipv6: endpoint.hostname.includes(':'), connectMs: Date.now() - attemptAt, totalMs: Date.now() - started });
            return;
          } catch (error) {
            connection?.close();
            lastError = error;
            if (this.#closed) throw error;
            diag('dc_dial_failed', { stream: streamId, dc, via: 'tcp', ipv6: endpoint.hostname.includes(':'), ms: Date.now() - attemptAt, error: errorKind(error) });
          }
        }
        const fallback = this.#options.fallback;
        if (fallback && !this.#closed) {
          const attemptAt = Date.now();
          try {
            const connection = await fallback(dc, tag, this.#abort.signal);
            if (this.#closed) { connection.close(); throw new Error('MTProxy terminator closed'); }
            this.#adopt(connection);
            diag('dc_connected', { stream: streamId, dc, via: 'wss', connectMs: Date.now() - attemptAt, totalMs: Date.now() - started });
            return;
          } catch (error) {
            lastError = error;
            diag('dc_dial_failed', { stream: streamId, dc, via: 'wss', ms: Date.now() - attemptAt, error: errorKind(error) });
          }
        }
        throw lastError instanceof Error ? lastError : new Error('Telegram DC dial failed');
      } finally { release?.(); }
    } catch (error) {
      this.close(error instanceof Error ? error : new Error('MTProxy initialization failed'));
      throw error;
    } finally {
      source.fill(0); iv.fill(0); reversed.fill(0); outboundSource.fill(0); outboundIv.fill(0);
      inboundKey.fill(0); outboundKey.fill(0); decrypted.fill(0);
    }
  }

  #adopt(connection: DirectTelegramConnection): void {
    this.#connection = connection;
    clearTimeout(this.#handshakeTimer);
    this.#settleReady();
  }

  #settleReady(error?: Error): void {
    if (this.#readySettled) return;
    this.#readySettled = true;
    if (error) this.#rejectReady(error);
    else this.#resolveReady();
  }
}

export class TelegramConnector {
  readonly #secret: Uint8Array;
  readonly #dialer: TelegramDialer;
  readonly #options: MtProxyOptions;
  #closed = false;
  constructor(secret: Uint8Array, dialer: TelegramDialer, options: MtProxyOptions = {}) {
    this.#secret = normalizeMtProxySecret(secret);
    this.#dialer = dialer;
    this.#options = options;
  }
  open(streamId?: number): MtProxyTerminator {
    if (this.#closed) throw new Error('Telegram connector closed');
    return new MtProxyTerminator(this.#secret, this.#dialer, streamId === undefined ? this.#options : { ...this.#options, streamId });
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#options.limiter?.close();
    this.#secret.fill(0);
  }
}
