/* SPDX-License-Identifier: GPL-3.0-only */
import { describe, expect, it, vi } from 'vitest';
import { MtProxyTerminator, TelegramConnector, type DirectTelegramConnection, type TelegramDialer } from '../src/mtproxy';
import { concatenate, makeObfuscated2Vector } from './obfuscated2-vectors';

class FakeDirectConnection implements DirectTelegramConnection {
  writes: Uint8Array[] = [];
  closed = false;
  #reads: Uint8Array[] = [];
  #wake: (() => void) | undefined;
  async write(data: Uint8Array): Promise<void> { this.writes.push(data.slice()); }
  async *read(): AsyncIterable<Uint8Array> {
    while (!this.closed) {
      if (this.#reads.length) { yield this.#reads.shift()!; continue; }
      await new Promise<void>((resolve) => { this.#wake = resolve; });
    }
  }
  emit(data: Uint8Array): void { this.#reads.push(data.slice()); this.#wake?.(); this.#wake = undefined; }
  close(): void { this.closed = true; this.#wake?.(); }
}

function fixture(secret: Uint8Array, handshakeTimeoutMs?: number) {
  const direct = new FakeDirectConnection();
  const dialer: TelegramDialer = { connect: vi.fn(async () => direct) };
  const terminator = new MtProxyTerminator(secret, dialer, handshakeTimeoutMs);
  return { direct, dialer, terminator };
}

async function collectOne(iterable: AsyncIterable<Uint8Array>): Promise<Uint8Array> {
  for await (const value of iterable) return value;
  throw new Error('stream ended');
}

describe('MTProxy obfuscated2 termination', () => {
  it.each([
    [0xeeeeeeee, Uint8Array.of(0xee, 0xee, 0xee, 0xee)],
    [0xdddddddd, Uint8Array.of(0xdd, 0xdd, 0xdd, 0xdd)],
    [0xefefefef, Uint8Array.of(0xef)]
  ])('consumes a fragmented header and writes the clear marker for tag %s before fragmented payload', async (tag, marker) => {
    const vector = await makeObfuscated2Vector({ tag });
    const { direct, dialer, terminator } = fixture(vector.secret);
    const wire = concatenate(vector.transformedHeader, vector.transformedPayload);
    for (const [start, end] of [[0, 3], [3, 17], [17, 55], [55, 64], [64, 65], [65, wire.length]] as const) {
      await terminator.write(wire.slice(start, end));
      if (end < 64) expect(dialer.connect).not.toHaveBeenCalled();
    }
    expect(dialer.connect).toHaveBeenCalledTimes(1);
    expect(direct.writes).toEqual([marker, vector.clearPayload.slice(0, 1), vector.clearPayload.slice(1)]);
    expect(concatenate(...direct.writes)).toEqual(concatenate(marker, vector.clearPayload));
  });

  it.each([false, true])('does not write payload before the %s-secret header is validated, the dial resolves, and the marker is written', async (dd) => {
    const vector = await makeObfuscated2Vector({ dd, tag: 0xdddddddd });
    const direct = new FakeDirectConnection();
    let resolve!: (value: DirectTelegramConnection) => void;
    const dialer: TelegramDialer = { connect: vi.fn(() => new Promise<DirectTelegramConnection>((done) => { resolve = done; })) };
    const terminator = new MtProxyTerminator(vector.secret, dialer);
    const write = terminator.write(concatenate(vector.transformedHeader, vector.transformedPayload));
    await vi.waitFor(() => expect(dialer.connect).toHaveBeenCalledTimes(1));
    expect(direct.writes).toEqual([]);
    resolve(direct);
    await write;
    expect(direct.writes[0]).toEqual(Uint8Array.of(0xdd, 0xdd, 0xdd, 0xdd));
    expect(concatenate(...direct.writes)).toEqual(concatenate(Uint8Array.of(0xdd, 0xdd, 0xdd, 0xdd), vector.clearPayload));
  });

  it('waits for the marker write itself before writing decrypted payload', async () => {
    const vector = await makeObfuscated2Vector({ tag: 0xefefefef });
    const direct = new FakeDirectConnection();
    let release!: () => void;
    direct.write = vi.fn(async (data: Uint8Array) => {
      direct.writes.push(data.slice());
      if (direct.writes.length === 1) await new Promise<void>((resolve) => { release = resolve; });
    });
    const terminator = new MtProxyTerminator(vector.secret, { connect: async () => direct });
    const write = terminator.write(concatenate(vector.transformedHeader, vector.transformedPayload));
    await vi.waitFor(() => expect(direct.writes).toEqual([Uint8Array.of(0xef)]));
    release();
    await write;
    expect(direct.writes).toEqual([Uint8Array.of(0xef), vector.clearPayload]);
  });

  it('uses outbound reverse key/IV state for fragmented DC responses', async () => {
    const vector = await makeObfuscated2Vector();
    const { direct, terminator } = fixture(vector.secret);
    await terminator.write(concatenate(vector.transformedHeader, vector.transformedPayload));
    const received = collectOne(terminator.read());
    direct.emit(vector.clearResponse.slice(0, 7));
    direct.emit(vector.clearResponse.slice(7));
    expect(await received).toEqual(vector.transformedResponse.slice(0, 7));
    expect(await collectOne(terminator.read())).toEqual(vector.transformedResponse.slice(7));
  });

  it('accepts Telegram-ignored reserved bytes 62..63', async () => {
    const vector = await makeObfuscated2Vector({ reserved: [0xff, 0x7e] });
    const { terminator } = fixture(vector.secret);
    await expect(terminator.write(vector.transformedHeader)).resolves.toBeUndefined();
  });

  it.each([
    ['wrong secret', async () => { const vector = await makeObfuscated2Vector(); return { vector, secret: new Uint8Array(16).fill(9) }; }],
    ['invalid tag', async () => { const vector = await makeObfuscated2Vector({ tag: 0xabababab }); return { vector, secret: vector.secret }; }],
    ['zero DC', async () => { const vector = await makeObfuscated2Vector({ dc: 0 }); return { vector, secret: vector.secret }; }],
    ['unknown DC', async () => { const vector = await makeObfuscated2Vector({ dc: 6 }); return { vector, secret: vector.secret }; }]
  ])('rejects %s before dialing', async (_name, build) => {
    const { vector, secret } = await build();
    const { dialer, terminator } = fixture(secret);
    await expect(terminator.write(vector.transformedHeader)).rejects.toThrow();
    expect(dialer.connect).not.toHaveBeenCalled();
  });

  it('fails over deterministically within the selected DC allowlist', async () => {
    const vector = await makeObfuscated2Vector({ dc: 1 });
    const direct = new FakeDirectConnection();
    const dialer: TelegramDialer = { connect: vi.fn().mockRejectedValueOnce(new Error('first failed')).mockResolvedValueOnce(direct) };
    const connector = new TelegramConnector(vector.secret, dialer);
    const stream = connector.open();
    await stream.write(concatenate(vector.transformedHeader, vector.transformedPayload));
    expect(dialer.connect).toHaveBeenCalledTimes(2);
    expect(concatenate(...direct.writes)).toEqual(concatenate(Uint8Array.of(0xee, 0xee, 0xee, 0xee), vector.clearPayload));
  });

  it('closes a candidate whose marker write fails before trying the next allowlisted endpoint', async () => {
    const vector = await makeObfuscated2Vector({ dc: 1, tag: 0xdddddddd });
    const failed = new FakeDirectConnection();
    failed.write = vi.fn(async () => { throw new Error('marker rejected'); });
    const direct = new FakeDirectConnection();
    const dialer: TelegramDialer = { connect: vi.fn().mockResolvedValueOnce(failed).mockResolvedValueOnce(direct) };
    const terminator = new MtProxyTerminator(vector.secret, dialer);
    await terminator.write(concatenate(vector.transformedHeader, vector.transformedPayload));
    expect(failed.closed).toBe(true);
    expect(dialer.connect).toHaveBeenCalledTimes(2);
    expect(direct.writes).toEqual([Uint8Array.of(0xdd, 0xdd, 0xdd, 0xdd), vector.clearPayload]);
  });

  it('closes a socket that arrives after the terminator was closed during dial', async () => {
    const vector = await makeObfuscated2Vector();
    const direct = new FakeDirectConnection();
    let resolve!: (value: DirectTelegramConnection) => void;
    const pending = new Promise<DirectTelegramConnection>((done) => { resolve = done; });
    const connect = vi.fn(() => pending);
    const terminator = new MtProxyTerminator(vector.secret, { connect });
    const write = terminator.write(vector.transformedHeader);
    await vi.waitFor(() => expect(connect).toHaveBeenCalled());
    terminator.close();
    resolve(direct);
    await expect(write).rejects.toThrow(/closed/i);
    expect(direct.closed).toBe(true);
  });

  it('waits event-wise for header completion and times out an idle OPEN', async () => {
    vi.useFakeTimers();
    try {
      const vector = await makeObfuscated2Vector();
      const { dialer, terminator } = fixture(vector.secret, 50);
      const read = collectOne(terminator.read());
      const rejected = expect(read).rejects.toThrow(/handshake deadline/i);
      await terminator.write(vector.transformedHeader.slice(0, 17));
      expect(dialer.connect).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(49);
      expect(dialer.connect).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      await rejected;
      await expect(terminator.write(vector.transformedHeader.slice(17))).rejects.toThrow(/closed/i);
    } finally { vi.useRealTimers(); }
  });

  it('zeroizes and disables a connector when the owning relay closes it', async () => {
    const vector = await makeObfuscated2Vector();
    const connector = new TelegramConnector(vector.secret, { connect: async () => new FakeDirectConnection() });
    connector.close();
    expect(() => connector.open()).toThrow(/closed/i);
  });
});
