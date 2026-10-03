/* SPDX-License-Identifier: GPL-3.0-only */

/** Minimum keystream generated per WebCrypto call; amortizes per-call overhead for small chunks. */
export const KEYSTREAM_PREFETCH_BYTES = 16 * 1024;

/**
 * Streaming AES-256-CTR with explicit 128-bit counter and keystream state.
 * WebCrypto is used only to generate whole AES-CTR keystream blocks; this class
 * owns counter advancement so calls of any size are byte-identical to one call.
 * Keystream is generated ahead in at least KEYSTREAM_PREFETCH_BYTES so a stream
 * of small MTProto packets costs one WebCrypto round trip per prefetch window
 * instead of one per packet. The keystream depends only on key and position, so
 * prefetching does not change any output byte.
 */
export class StreamingAes256Ctr {
  readonly #key: CryptoKey;
  readonly #counter: Uint8Array;
  #keystream = new Uint8Array();
  #keystreamOffset = 0;
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
    while (offset < input.byteLength) {
      if (this.#keystreamOffset === this.#keystream.byteLength) {
        await this.#refill(input.byteLength - offset);
        if (this.#closed) throw new Error('AES-CTR state is closed');
      }
      const length = Math.min(input.byteLength - offset, this.#keystream.byteLength - this.#keystreamOffset);
      xorInto(output, offset, input, offset, this.#keystream, this.#keystreamOffset, length);
      this.#keystreamOffset += length;
      offset += length;
    }
    return output;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#counter.fill(0);
    this.#keystream.fill(0);
    this.#keystream = new Uint8Array();
    this.#keystreamOffset = 0;
  }

  async #refill(needed: number): Promise<void> {
    this.#keystream.fill(0);
    const blocks = Math.ceil(Math.max(needed, KEYSTREAM_PREFETCH_BYTES) / 16);
    const zeros = new Uint8Array(blocks * 16);
    const counter = this.#counter.slice();
    // Advance before awaiting so state stays consistent even if a caller races.
    this.#increment(blocks);
    this.#keystream = new Uint8Array(await crypto.subtle.encrypt(
      { name: 'AES-CTR', counter, length: 128 },
      this.#key,
      zeros
    ));
    this.#keystreamOffset = 0;
    counter.fill(0);
  }

  #increment(blocks: number): void {
    // Add `blocks` to the big-endian 128-bit counter in one pass.
    let carry = blocks;
    for (let index = 15; index >= 0 && carry > 0; index--) {
      const sum = this.#counter[index]! + (carry % 256);
      this.#counter[index] = sum & 0xff;
      carry = Math.floor(carry / 256) + (sum >> 8);
    }
  }
}

function xorInto(
  output: Uint8Array, outputOffset: number,
  input: Uint8Array, inputOffset: number,
  stream: Uint8Array, streamOffset: number,
  length: number
): void {
  let index = 0;
  const outAddress = output.byteOffset + outputOffset;
  const inAddress = input.byteOffset + inputOffset;
  const streamAddress = stream.byteOffset + streamOffset;
  // Word-wise XOR when all three views share 4-byte alignment.
  if (length >= 64 && (outAddress & 3) === 0 && (inAddress & 3) === 0 && (streamAddress & 3) === 0) {
    const words = length >>> 2;
    const out32 = new Uint32Array(output.buffer, outAddress, words);
    const in32 = new Uint32Array(input.buffer, inAddress, words);
    const ks32 = new Uint32Array(stream.buffer, streamAddress, words);
    for (let word = 0; word < words; word++) out32[word] = in32[word]! ^ ks32[word]!;
    index = words << 2;
  }
  for (; index < length; index++) {
    output[outputOffset + index] = input[inputOffset + index]! ^ stream[streamOffset + index]!;
  }
}
