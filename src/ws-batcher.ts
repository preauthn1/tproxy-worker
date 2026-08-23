/*
 * SPDX-License-Identifier: GPL-3.0-only
 * Incorporates adapted GrainTCP batching behavior.
 * Modified by preauthn1 on 2026-08-23. See ../THIRD_PARTY_NOTICES.md.
 */
import { GrainCollector } from './grain';

interface BatcherOptions {
  packBytes: number;
  directBytes: number;
  delayMs: number;
  maxPendingBytes?: number;
  maxPendingItems?: number;
}

/** GrainTCP-derived small-downlink grain core with a short flush gate. */
export class WebSocketBatcher {
  readonly #emit: (value: Uint8Array) => void;
  readonly #collector: GrainCollector;
  readonly #options: Required<BatcherOptions>;
  #items = 0;
  #timer: ReturnType<typeof setTimeout> | undefined;

  constructor(emit: (value: Uint8Array) => void, options: BatcherOptions) {
    this.#emit = emit;
    this.#collector = new GrainCollector(options.packBytes);
    this.#options = {
      ...options,
      maxPendingBytes: options.maxPendingBytes ?? 12 * 1024 * 1024,
      maxPendingItems: options.maxPendingItems ?? 8192
    };
  }

  send(value: Uint8Array): void {
    if (value.byteLength === 0) return;
    if (value.byteLength >= this.#options.directBytes) {
      this.flush();
      this.#emit(value);
      return;
    }
    if (this.#collector.bytes + value.byteLength > this.#options.maxPendingBytes || this.#items >= this.#options.maxPendingItems) {
      throw new Error('downlink queue overflow');
    }
    this.#collector.push(value);
    this.#items++;
    if (this.#collector.bytes >= this.#options.packBytes) this.flush();
    else if (!this.#timer) this.#timer = setTimeout(() => this.flush(), this.#options.delayMs);
  }

  flush(): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    for (let value = this.#collector.take(); value; value = this.#collector.take()) this.#emit(value);
    this.#items = 0;
  }

  close(): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    while (this.#collector.take()) { /* discard during noexcept shutdown */ }
    this.#items = 0;
  }
}
