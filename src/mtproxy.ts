/* SPDX-License-Identifier: GPL-3.0-only */
// Obfuscated2 derivation is based on TelegramMessenger/MTProxy commit
// f36d8af769ffaeac36978d38c2c0f6d1104c2137 (LGPL-2.0-or-later).
import { StreamingAes256Ctr } from './aes-ctr';
import { telegramDcCandidates, type TelegramEndpoint } from './telegram-dc';

const HEADER_BYTES = 64;
const DEFAULT_HANDSHAKE_TIMEOUT_MS = 30_000;
const VALID_TAGS = new Set([0xeeeeeeee, 0xdddddddd, 0xefefefef]);

function transportMarker(tag: number): Uint8Array {
  if (tag === 0xeeeeeeee) return Uint8Array.of(0xee, 0xee, 0xee, 0xee);
  if (tag === 0xdddddddd) return Uint8Array.of(0xdd, 0xdd, 0xdd, 0xdd);
  if (tag === 0xefefefef) return Uint8Array.of(0xef);
  throw new Error('invalid MTProxy transport tag');
}

export interface DirectTelegramConnection {
  write(data: Uint8Array): Promise<void>;
  read(): AsyncIterable<Uint8Array>;
  close(): void;
}

export interface TelegramDialer { connect(endpoint: TelegramEndpoint): Promise<DirectTelegramConnection> }

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

  constructor(secret: Uint8Array, dialer: TelegramDialer, handshakeTimeoutMs = DEFAULT_HANDSHAKE_TIMEOUT_MS) {
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
      let lastError: unknown;
      for (const endpoint of candidates) {
        if (this.#closed) throw new Error('MTProxy terminator closed');
        let connection: DirectTelegramConnection | undefined;
        try {
          connection = await this.#dialer.connect(endpoint);
          if (this.#closed) { connection.close(); throw new Error('MTProxy terminator closed'); }
          await connection.write(marker);
          if (this.#closed) { connection.close(); throw new Error('MTProxy terminator closed'); }
          this.#connection = connection;
          clearTimeout(this.#handshakeTimer);
          this.#settleReady();
          return;
        } catch (error) {
          connection?.close();
          lastError = error;
        }
      }
      throw lastError instanceof Error ? lastError : new Error('Telegram DC dial failed');
    } catch (error) {
      this.close(error instanceof Error ? error : new Error('MTProxy initialization failed'));
      throw error;
    } finally {
      source.fill(0); iv.fill(0); reversed.fill(0); outboundSource.fill(0); outboundIv.fill(0);
      inboundKey.fill(0); outboundKey.fill(0); decrypted.fill(0);
    }
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
  #closed = false;
  constructor(secret: Uint8Array, dialer: TelegramDialer) {
    this.#secret = normalizeMtProxySecret(secret);
    this.#dialer = dialer;
  }
  open(): MtProxyTerminator {
    if (this.#closed) throw new Error('Telegram connector closed');
    return new MtProxyTerminator(this.#secret, this.#dialer);
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#secret.fill(0);
  }
}
