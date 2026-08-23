/*
 * SPDX-License-Identifier: GPL-3.0-only
 * Derived from ToiCF/GrainTCP commit 1d22628 (HiinEnkelte).
 * Modified by preauthn1 on 2026-08-23. See ../THIRD_PARTY_NOTICES.md.
 */
/** A bounded small-chunk collector adapted from GrainTCP's mkK/mkQ grain core. */
export class GrainCollector {
  readonly #capacity: number;
  #queue: Array<Uint8Array | undefined> = [];
  #head = 0;
  #bytes = 0;

  constructor(capacity: number) {
    if (!Number.isInteger(capacity) || capacity <= 0) throw new Error('capacity must be positive');
    this.#capacity = capacity;
  }

  get bytes(): number { return this.#bytes; }
  get empty(): boolean { return this.#head === this.#queue.length; }
  get retainedItems(): number { return this.#queue.length; }

  push(value: Uint8Array): void {
    if (value.byteLength === 0) return;
    this.#queue.push(value);
    this.#bytes += value.byteLength;
  }

  take(): Uint8Array | null {
    const first = this.#queue[this.#head];
    if (!first) return null;
    this.#queue[this.#head++] = undefined;
    this.#bytes -= first.byteLength;
    if (first.byteLength >= this.#capacity || this.#head === this.#queue.length) {
      this.#compact();
      return first;
    }
    let total = first.byteLength;
    let count = 0;
    while (this.#head + count < this.#queue.length && total + this.#queue[this.#head + count]!.byteLength <= this.#capacity) {
      total += this.#queue[this.#head + count]!.byteLength;
      count++;
    }
    if (count === 0) { this.#compact(); return first; }
    const result = new Uint8Array(total);
    result.set(first);
    let offset = first.byteLength;
    for (let index = 0; index < count; index++) {
      const value = this.#queue[this.#head]!;
      this.#queue[this.#head++] = undefined;
      result.set(value, offset);
      offset += value.byteLength;
      this.#bytes -= value.byteLength;
    }
    this.#compact();
    return result;
  }

  clear(): void { this.#queue = []; this.#head = 0; this.#bytes = 0; }

  #compact(): void {
    if (this.#head === this.#queue.length) {
      this.#queue = [];
      this.#head = 0;
    } else if (this.#head >= 1024 && this.#head * 2 >= this.#queue.length) {
      this.#queue = this.#queue.slice(this.#head);
      this.#head = 0;
    }
  }
}
