/*
 * SPDX-License-Identifier: GPL-3.0-only
 * Incorporates adapted GrainTCP BYOB-first read behavior.
 * Modified by preauthn1 on 2026-08-23. See ../THIRD_PARTY_NOTICES.md.
 */
import { connect } from 'cloudflare:sockets';
import type { DirectTelegramConnection, TelegramDialer } from './mtproxy';
import { RELAY_DATA_CHUNK } from './frame';
import type { TelegramEndpoint } from './telegram-dc';

class CloudflareTelegramConnection implements DirectTelegramConnection {
  readonly #socket: ReturnType<typeof connect>;
  readonly #writer: WritableStreamDefaultWriter<Uint8Array>;
  #closed = false;

  constructor(socket: ReturnType<typeof connect>) {
    this.#socket = socket;
    this.#writer = socket.writable.getWriter();
  }

  async write(data: Uint8Array): Promise<void> { await this.#writer.write(data); }

  async *read(): AsyncIterable<Uint8Array> {
    let byob: ReadableStreamBYOBReader | undefined;
    try {
      byob = this.#socket.readable.getReader({ mode: 'byob' });
      let buffer = new ArrayBuffer(RELAY_DATA_CHUNK);
      while (!this.#closed) {
        const { done, value } = await byob.read(new Uint8Array(buffer));
        if (done) return;
        if (!value?.byteLength) continue;
        const output = value.byteLength >= RELAY_DATA_CHUNK / 2 ? value : value.slice();
        yield output;
        buffer = value.buffer instanceof ArrayBuffer && value.buffer.byteLength >= RELAY_DATA_CHUNK ? value.buffer : new ArrayBuffer(RELAY_DATA_CHUNK);
      }
    } catch (error) {
      if (byob || this.#closed) throw error;
      const reader = this.#socket.readable.getReader();
      try {
        while (!this.#closed) {
          const { done, value } = await reader.read();
          if (done) return;
          if (value?.byteLength) yield value;
        }
      } finally { reader.releaseLock(); }
    } finally { try { byob?.releaseLock(); } catch { /* already released */ } }
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    try { this.#writer.releaseLock(); } catch { /* already released */ }
    try { this.#socket.close(); } catch { /* already closed */ }
  }
}

export class CloudflareTelegramDialer implements TelegramDialer {
  async connect(endpoint: TelegramEndpoint, signal?: AbortSignal): Promise<DirectTelegramConnection> {
    const socket = connect(endpoint, { allowHalfOpen: false });
    const abort = () => { try { socket.close(); } catch { /* already closed */ } };
    signal?.addEventListener('abort', abort, { once: true });
    try {
      if (signal?.aborted) throw new Error('dial cancelled');
      await socket.opened;
      if (signal?.aborted) throw new Error('dial cancelled');
    } catch (error) {
      abort();
      throw error;
    } finally { signal?.removeEventListener('abort', abort); }
    return new CloudflareTelegramConnection(socket);
  }
}
