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
  controlReserveBytes?: number;
  controlReserveItems?: number;
}

export const DEFAULT_WEBSOCKET_BATCHER_OPTIONS: Readonly<BatcherOptions> = Object.freeze({
  packBytes: 512 * 1024,
  directBytes: 1024 * 1024,
  delayMs: 1
});

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
      maxPendingItems: options.maxPendingItems ?? 8192,
      controlReserveBytes: options.controlReserveBytes ?? 64 * 1024,
      controlReserveItems: options.controlReserveItems ?? 64
    };
  }

  send(value: Uint8Array, control = false): void {
    if (value.byteLength === 0) return;
    if (control) {
      this.flush();
      this.#emit(value);
      return;
    }
    if (value.byteLength >= this.#options.directBytes) {
      this.flush();
      this.#emit(value);
      return;
    }
    const bytesLimit = Math.max(0, this.#options.maxPendingBytes - this.#options.controlReserveBytes);
    const itemsLimit = Math.max(0, this.#options.maxPendingItems - this.#options.controlReserveItems);
    if (this.#collector.bytes + value.byteLength > bytesLimit || this.#items >= itemsLimit) {
      throw new Error('downlink queue overflow');
    }
    if (!this.#collector.empty && this.#collector.bytes + value.byteLength > this.#options.packBytes) this.flush();
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
