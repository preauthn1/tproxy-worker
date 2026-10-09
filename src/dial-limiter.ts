/* SPDX-License-Identifier: GPL-3.0-only */
export class DialQueueFull extends Error {
  constructor() { super('dial queue full'); this.name = 'DialQueueFull'; }
}

interface Waiter { resolve(): void; reject(error: Error): void; signal: AbortSignal | undefined; onAbort?: () => void }

/** Bounds concurrent outbound Telegram dials per session; queued dials are abortable. */
export class DialLimiter {
  readonly #max: number;
  readonly #maxQueue: number;
  #active = 0;
  readonly #queue: Waiter[] = [];
  #closed = false;

  constructor(maxConcurrent = 4, maxQueue = 256) { this.#max = maxConcurrent; this.#maxQueue = maxQueue; }

  get active(): number { return this.#active; }
  get queued(): number { return this.#queue.length; }

  /** Resolves with a release function once a dial slot is available. */
  async acquire(signal?: AbortSignal): Promise<() => void> {
    if (this.#closed) throw new Error('dial limiter closed');
    if (signal?.aborted) throw new Error('dial cancelled');
    if (this.#active < this.#max) { this.#active++; return this.#releaser(); }
    if (this.#queue.length >= this.#maxQueue) throw new DialQueueFull();
    await new Promise<void>((resolve, reject) => {
      const waiter: Waiter = { resolve, reject, signal };
      if (signal) {
        waiter.onAbort = () => {
          const index = this.#queue.indexOf(waiter);
          if (index >= 0) this.#queue.splice(index, 1);
          reject(new Error('dial cancelled'));
        };
        signal.addEventListener('abort', waiter.onAbort, { once: true });
      }
      this.#queue.push(waiter);
    });
    return this.#releaser();
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const waiter of this.#queue.splice(0)) {
      if (waiter.onAbort) waiter.signal?.removeEventListener('abort', waiter.onAbort);
      waiter.reject(new Error('dial limiter closed'));
    }
  }

  #releaser(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = this.#queue.shift();
      if (next) {
        if (next.onAbort) next.signal?.removeEventListener('abort', next.onAbort);
        next.resolve();
      } else this.#active--;
    };
  }
}
