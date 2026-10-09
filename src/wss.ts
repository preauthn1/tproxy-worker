/* SPDX-License-Identifier: GPL-3.0-only */
import { StreamingAes256Ctr } from './aes-ctr';
import type { DirectTelegramConnection } from './mtproxy';

const DC_NAMES = ['pluto', 'venus', 'aurora', 'vesta', 'flora'] as const;
const MAX_PENDING_DOWNLINK_BYTES = 8 * 1024 * 1024;

export function telegramWssHost(signedDc: number): string {
  const name = DC_NAMES[Math.abs(signedDc) - 1];
  if (!Number.isInteger(signedDc) || signedDc === 0 || !name) throw new Error('unknown Telegram DC id');
  return `${name}${signedDc < 0 ? '-1' : ''}.web.telegram.org`;
}

/**
 * Ordered Telegram Web WSS front doors for a signed DC: the Web K `kws{N}[-1]`
 * names (as used by ToiCF/CF-Workers-TGProxy), the legacy planet names, then the
 * opposite media variant of `kws{N}`.
 */
export function telegramWssHosts(signedDc: number): string[] {
  const legacy = telegramWssHost(signedDc);
  const dc = Math.abs(signedDc);
  const media = signedDc < 0;
  return [`kws${dc}${media ? '-1' : ''}.web.telegram.org`, legacy, `kws${dc}${media ? '' : '-1'}.web.telegram.org`];
}

function validInit(init: Uint8Array): boolean {
  if (init[0] === 0xef) return false;
  const view = new DataView(init.buffer, init.byteOffset, init.byteLength);
  const first = view.getUint32(0, true);
  const second = view.getUint32(4, true);
  if (second === 0) return false;
  return ![0x44414548, 0x54534f50, 0x20544547, 0x4954504f, 0x02010316, 0xdddddddd, 0xeeeeeeee].includes(first);
}

class WssTelegramConnection implements DirectTelegramConnection {
  readonly #socket: WebSocket;
  readonly #tx: StreamingAes256Ctr;
  readonly #rx: StreamingAes256Ctr;
  readonly #queue: Uint8Array[] = [];
  #queuedBytes = 0;
  #waiter: (() => void) | undefined;
  #ended = false;
  #error: Error | undefined;
  #closed = false;
  #writeChain: Promise<void> = Promise.resolve();

  constructor(socket: WebSocket, tx: StreamingAes256Ctr, rx: StreamingAes256Ctr) {
    this.#socket = socket;
    this.#tx = tx;
    this.#rx = rx;
    socket.addEventListener('message', (event) => {
      const data = event.data as unknown;
      if (typeof data === 'string') { this.#fail(new Error('non-binary Telegram WSS message')); return; }
      const bytes = data instanceof ArrayBuffer ? new Uint8Array(data)
        : ArrayBuffer.isView(data) ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice() : null;
      if (!bytes) { this.#fail(new Error('unsupported Telegram WSS message')); return; }
      if (!bytes.byteLength) return;
      this.#queuedBytes += bytes.byteLength;
      if (this.#queuedBytes > MAX_PENDING_DOWNLINK_BYTES) { this.#fail(new Error('Telegram WSS downlink overflow')); return; }
      this.#queue.push(bytes);
      this.#wake();
    });
    socket.addEventListener('close', () => { this.#ended = true; this.#wake(); });
    socket.addEventListener('error', () => { this.#fail(new Error('Telegram WSS error')); });
  }

  /** Sends the obfuscated init (first 56 bytes clear + encrypted tail). */
  async start(init: Uint8Array): Promise<void> {
    const encrypted = await this.#tx.transform(init);
    const wire = init.slice();
    wire.set(encrypted.subarray(56, 64), 56);
    encrypted.fill(0);
    this.#socket.send(wire);
  }

  write(data: Uint8Array): Promise<void> {
    const next = this.#writeChain.then(async () => {
      if (this.#closed || this.#error) throw this.#error ?? new Error('Telegram WSS closed');
      const encrypted = await this.#tx.transform(data);
      this.#socket.send(encrypted);
    });
    this.#writeChain = next.catch(() => undefined);
    return next;
  }

  async *read(): AsyncIterable<Uint8Array> {
    for (;;) {
      if (this.#closed) return;
      const value = this.#queue.shift();
      if (value) {
        this.#queuedBytes -= value.byteLength;
        yield await this.#rx.transform(value);
        continue;
      }
      if (this.#error) throw this.#error;
      if (this.#ended) return;
      await new Promise<void>((resolve) => { this.#waiter = resolve; });
    }
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#queue.length = 0;
    this.#queuedBytes = 0;
    try { this.#socket.close(1000, 'closed'); } catch { /* already closed */ }
    this.#tx.close();
    this.#rx.close();
    this.#wake();
  }

  #fail(error: Error): void {
    if (!this.#error) this.#error = error;
    try { this.#socket.close(1011, 'error'); } catch { /* already closed */ }
    this.#wake();
  }

  #wake(): void {
    const waiter = this.#waiter;
    this.#waiter = undefined;
    waiter?.();
  }
}

export interface WssDialOptions {
  /** Per-host upgrade timeout. */
  timeoutMs: number;
  signal?: AbortSignal | undefined;
  fetchImpl?: typeof fetch;
  hosts?: readonly string[];
}

export async function dialTelegramWss(signedDc: number, tag: number, options: WssDialOptions): Promise<DirectTelegramConnection> {
  const hosts = options.hosts ?? telegramWssHosts(signedDc);
  let lastError: unknown;
  for (const host of hosts) {
    if (options.signal?.aborted) throw new Error('Telegram WSS dial cancelled');
    try { return await dialTelegramWssHost(host, signedDc, tag, options); }
    catch (error) { lastError = error; }
  }
  throw lastError instanceof Error ? lastError : new Error('Telegram WSS dial failed');
}

async function dialTelegramWssHost(host: string, signedDc: number, tag: number, options: WssDialOptions): Promise<DirectTelegramConnection> {
  const controller = new AbortController();
  const onAbort = () => controller.abort();
  options.signal?.addEventListener('abort', onAbort, { once: true });
  if (options.signal?.aborted) controller.abort();
  const timer = setTimeout(() => controller.abort(), options.timeoutMs);
  let socket: WebSocket | null = null;
  try {
    const response = await (options.fetchImpl ?? fetch)(`https://${host}/apiws`, {
      headers: { Upgrade: 'websocket', 'Sec-WebSocket-Protocol': 'binary' },
      signal: controller.signal
    });
    socket = response.webSocket;
    if (controller.signal.aborted) throw new Error('Telegram WSS dial cancelled');
    if (response.status !== 101 || !socket) throw new Error(`Telegram WSS handshake failed: ${response.status}`);
    socket.binaryType = 'arraybuffer';
    socket.accept();
  } catch (error) {
    try { socket?.close(1000, 'dial failed'); } catch { /* ignore */ }
    throw error instanceof Error ? error : new Error('Telegram WSS dial failed');
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', onAbort);
  }
  let init = new Uint8Array(64);
  for (let attempt = 0; ; attempt++) {
    crypto.getRandomValues(init);
    const view = new DataView(init.buffer);
    view.setUint32(56, tag, true);
    view.setInt16(60, signedDc, true);
    if (validInit(init)) break;
    if (attempt > 128) { socket.close(1011, 'init'); throw new Error('cannot generate Telegram init'); }
    init = new Uint8Array(64);
  }
  const reversed = Uint8Array.from(init.subarray(8, 56)).reverse();
  const tx = await StreamingAes256Ctr.create(init.subarray(8, 40), init.subarray(40, 56));
  const rx = await StreamingAes256Ctr.create(reversed.subarray(0, 32), reversed.subarray(32, 48));
  reversed.fill(0);
  const connection = new WssTelegramConnection(socket, tx, rx);
  try { await connection.start(init); }
  catch (error) { connection.close(); throw error; }
  finally { init.fill(0); }
  return connection;
}
