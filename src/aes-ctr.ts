/* SPDX-License-Identifier: GPL-3.0-only */

/**
 * Streaming AES-256-CTR with explicit 128-bit counter and partial-block state.
 * WebCrypto is used only to generate whole AES-CTR keystream blocks; this class
 * owns counter advancement so calls of any size are byte-identical to one call.
 */
export class StreamingAes256Ctr {
  readonly #key: CryptoKey;
  readonly #counter: Uint8Array;
  #remaining = new Uint8Array();
  #closed = false;

  private constructor(key: CryptoKey, counter: Uint8Array) {
    this.#key = key;
    this.#counter = counter;
  }

  static async create(key: Uint8Array, counter: Uint8Array): Promise<StreamingAes256Ctr> {
    if (key.byteLength !== 32) throw new Error('AES-256 key must be 32 bytes');
    if (counter.byteLength !== 16) throw new Error('AES-CTR counter must be 16 bytes');
    const temporary = key.slice();
    try {
      const imported = await crypto.subtle.importKey('raw', temporary, 'AES-CTR', false, ['encrypt']);
      return new StreamingAes256Ctr(imported, counter.slice());
    } finally { temporary.fill(0); }
  }

  async transform(input: Uint8Array): Promise<Uint8Array> {
    if (this.#closed) throw new Error('AES-CTR state is closed');
    const output = new Uint8Array(input.byteLength);
    let offset = 0;
    if (this.#remaining.byteLength) {
      const length = Math.min(input.byteLength, this.#remaining.byteLength);
      for (let index = 0; index < length; index++) output[index] = input[index]! ^ this.#remaining[index]!;
      const previous = this.#remaining;
      this.#remaining = previous.slice(length);
      previous.fill(0);
      offset = length;
    }
    const needed = input.byteLength - offset;
    if (!needed) return output;
    const blocks = Math.ceil(needed / 16);
    const zeros = new Uint8Array(blocks * 16);
    const stream = new Uint8Array(await crypto.subtle.encrypt(
      { name: 'AES-CTR', counter: this.#counter.slice(), length: 128 },
      this.#key,
      zeros
    ));
    zeros.fill(0);
    this.#increment(blocks);
    for (let index = 0; index < needed; index++) output[offset + index] = input[offset + index]! ^ stream[index]!;
    if (stream.byteLength > needed) this.#remaining = stream.slice(needed);
    stream.fill(0);
    return output;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#counter.fill(0);
    this.#remaining.fill(0);
    this.#remaining = new Uint8Array();
  }

  #increment(blocks: number): void {
    for (let block = 0; block < blocks; block++) {
      for (let index = 15; index >= 0; index--) {
        this.#counter[index] = (this.#counter[index]! + 1) & 0xff;
        if (this.#counter[index] !== 0) break;
      }
    }
  }
}
