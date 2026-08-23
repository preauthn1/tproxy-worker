/* SPDX-License-Identifier: GPL-3.0-only */

interface QueueOptions<T> {
  maxBytes: number;
  maxItems: number;
  size(value: T): number;
  handle(value: T): Promise<void>;
}

export class SerializedInboundQueue<T> {
  readonly #options: QueueOptions<T>;
  readonly #values: Array<{ value: T; bytes: number }> = [];
  #running = false;
  #pendingBytes = 0;
  #drainWaiters: Array<() => void> = [];
  #failed: unknown;

  constructor(options: QueueOptions<T>) { this.#options = options; }
  get pendingBytes(): number { return this.#pendingBytes; }
  get pendingItems(): number { return this.#values.length + (this.#running ? 1 : 0); }

  push(value: T): boolean {
    if (this.#failed) return false;
    const bytes = this.#options.size(value);
    if (bytes < 0 || this.#pendingBytes + bytes > this.#options.maxBytes || this.pendingItems >= this.#options.maxItems) return false;
    this.#pendingBytes += bytes;
    this.#values.push({ value, bytes });
    void this.#run();
    return true;
  }

  async drained(): Promise<void> {
    if (!this.#running && this.#values.length === 0) {
      if (this.#failed) throw this.#failed;
      return;
    }
    await new Promise<void>((resolve) => this.#drainWaiters.push(resolve));
    if (this.#failed) throw this.#failed;
  }

  clear(): void {
    for (const item of this.#values.splice(0)) this.#pendingBytes -= item.bytes;
    this.#finishDrain();
  }

  async #run(): Promise<void> {
    if (this.#running) return;
    this.#running = true;
    try {
      for (let item = this.#values.shift(); item; item = this.#values.shift()) {
        try { await this.#options.handle(item.value); }
        catch (error) { this.#failed = error; this.clear(); break; }
        finally { this.#pendingBytes -= item.bytes; }
      }
    } finally {
      this.#running = false;
      this.#finishDrain();
    }
  }

  #finishDrain(): void {
    if (this.#running || this.#values.length) return;
    for (const resolve of this.#drainWaiters.splice(0)) resolve();
  }
}

export async function readBoundedBody(body: ReadableStream<Uint8Array> | null, limit: number, deadlineMs: number): Promise<Uint8Array> {
  if (!body) return new Uint8Array();
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let bytes = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error('body read deadline exceeded')), deadlineMs);
  });
  try {
    while (true) {
      const { done, value } = await Promise.race([reader.read(), deadline]);
      if (done) break;
      if (!value?.byteLength) continue;
      const remaining = limit + 1 - bytes;
      const kept = value.subarray(0, Math.max(0, remaining)).slice();
      chunks.push(kept);
      bytes += kept.byteLength;
      if (bytes > limit) throw new Error('body limit exceeded');
    }
    const output = new Uint8Array(bytes);
    let offset = 0;
    for (const chunk of chunks) { output.set(chunk, offset); offset += chunk.byteLength; }
    return output;
  } catch (error) {
    try { await reader.cancel(error); } catch { /* cancellation is best effort */ }
    throw error;
  } finally {
    if (timer) clearTimeout(timer);
    try { reader.releaseLock(); } catch { /* already released */ }
  }
}

export class IdleLiveness {
  readonly #periodMs: number;
  readonly #ping: () => void;
  readonly #close: () => void;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #awaitingActivity = false;
  #stopped = true;

  constructor(periodMs: number, ping: () => void, close: () => void) {
    this.#periodMs = periodMs;
    this.#ping = ping;
    this.#close = close;
  }

  start(): void { this.#stopped = false; this.#schedule(); }
  touch(): void { this.#awaitingActivity = false; if (!this.#stopped) this.#schedule(); }
  stop(): void { this.#stopped = true; if (this.#timer) clearTimeout(this.#timer); this.#timer = undefined; }

  #schedule(): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      if (this.#stopped) return;
      if (this.#awaitingActivity) { this.#close(); this.stop(); return; }
      this.#awaitingActivity = true;
      this.#ping();
      this.#schedule();
    }, this.#periodMs);
  }
}
