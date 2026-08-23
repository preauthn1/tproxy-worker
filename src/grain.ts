/** A bounded small-chunk collector adapted from GrainTCP's mkK/mkQ grain core. */
export class GrainCollector {
  readonly #capacity: number;
  #queue: Uint8Array[] = [];
  #bytes = 0;

  constructor(capacity: number) {
    if (!Number.isInteger(capacity) || capacity <= 0) throw new Error('capacity must be positive');
    this.#capacity = capacity;
  }

  get bytes(): number { return this.#bytes; }
  get empty(): boolean { return this.#queue.length === 0; }

  push(value: Uint8Array): void {
    if (value.byteLength === 0) return;
    this.#queue.push(value);
    this.#bytes += value.byteLength;
  }

  take(): Uint8Array | null {
    const first = this.#queue.shift();
    if (!first) return null;
    this.#bytes -= first.byteLength;
    if (first.byteLength >= this.#capacity || this.#queue.length === 0) return first;
    let total = first.byteLength;
    let count = 0;
    while (count < this.#queue.length && total + this.#queue[count]!.byteLength <= this.#capacity) {
      total += this.#queue[count]!.byteLength;
      count++;
    }
    if (count === 0) return first;
    const result = new Uint8Array(total);
    result.set(first);
    let offset = first.byteLength;
    for (const value of this.#queue.splice(0, count)) {
      result.set(value, offset);
      offset += value.byteLength;
      this.#bytes -= value.byteLength;
    }
    return result;
  }

  clear(): void { this.#queue = []; this.#bytes = 0; }
}
