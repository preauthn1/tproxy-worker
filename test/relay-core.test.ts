import { describe, expect, it, vi } from 'vitest';
import { FrameType, INITIAL_STREAM_CREDIT, encodeFrame, parseRelayBatch } from '../src/frame';
import { DEFAULT_LIMITS } from '../src/limits';
import { RelayCore, type TelegramConnection, type TelegramConnectorLike } from '../src/relay-core';

class MockConnection implements TelegramConnection {
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
  emit(data: Uint8Array): void { this.#reads.push(data); this.#wake?.(); this.#wake = undefined; }
  close(): void { this.closed = true; this.#wake?.(); }
}

async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

function fixture(limitOverrides: Partial<typeof DEFAULT_LIMITS> = {}) {
  const connection = new MockConnection();
  const connector: TelegramConnectorLike = { open: vi.fn(() => connection) };
  const sent: Uint8Array[] = [];
  const core = new RelayCore({
    limits: { ...DEFAULT_LIMITS, ...limitOverrides }, connector,
    send: (batch) => sent.push(batch.slice()), closeCarrier: vi.fn(), writeTimeoutMs: 30_000
  });
  return { core, connector, connection, sent };
}

describe('relay session state and flow control', () => {
  it('opens an in-memory Telegram terminator without backend destination arguments', async () => {
    const { core, connector } = fixture();
    await core.receive(encodeFrame(FrameType.Open, 42));
    expect(connector.open).toHaveBeenCalledWith();
  });

  it('rejects DATA beyond the initial 4 MiB credit', async () => {
    const { core } = fixture();
    await core.receive(encodeFrame(FrameType.Open, 1));
    const chunk = new Uint8Array(1024 * 1024);
    const overCredit = new Uint8Array([
      ...Array.from({ length: INITIAL_STREAM_CREDIT / chunk.byteLength }, () => Array.from(encodeFrame(FrameType.Data, 1, chunk))).flat(),
      ...encodeFrame(FrameType.Data, 1, new Uint8Array([1]))
    ]);
    await expect(core.receive(overCredit)).rejects.toThrow(/credit/i);
  });

  it('never permits stream id reuse after CLOSE', async () => {
    const { core } = fixture();
    await core.receive(new Uint8Array([...encodeFrame(FrameType.Open, 5), ...encodeFrame(FrameType.Close, 5)]));
    await expect(core.receive(encodeFrame(FrameType.Open, 5))).rejects.toThrow(/reuse/i);
  });

  it('never permits an old stream id after bounded tombstone eviction', async () => {
    const { core } = fixture({ maxClosedStreamIds: 2 });
    for (const id of [1, 2, 3]) await core.receive(new Uint8Array([...encodeFrame(FrameType.Open, id), ...encodeFrame(FrameType.Close, id)]));
    await expect(core.receive(encodeFrame(FrameType.Open, 1))).rejects.toThrow(/reuse/i);
  });

  it('requires OPEN ids to increase even within one carrier batch', async () => {
    const { core } = fixture();
    await expect(core.receive(new Uint8Array([...encodeFrame(FrameType.Open, 2), ...encodeFrame(FrameType.Open, 1)]))).rejects.toThrow(/reuse/i);
  });

  it('closes only an over-limit OPEN and preserves the session', async () => {
    const { core, sent } = fixture({ maxStreams: 1 });
    await core.receive(encodeFrame(FrameType.Open, 1));
    await core.receive(encodeFrame(FrameType.Open, 2));
    expect(parseRelayBatch(sent.at(-1)!)[0]).toMatchObject({ type: FrameType.Close, streamId: 2 });
    await expect(core.receive(encodeFrame(FrameType.Data, 1, new Uint8Array([9])))).resolves.toBeUndefined();
  });

  it('fails atomically when a batch would overflow the pending queue', async () => {
    const { core, connection } = fixture({ maxPendingBytes: 600, maxPendingItems: 2 });
    const batch = new Uint8Array([
      ...encodeFrame(FrameType.Open, 1),
      ...encodeFrame(FrameType.Data, 1, new Uint8Array(100)),
      ...encodeFrame(FrameType.Data, 1, new Uint8Array(100))
    ]);
    await expect(core.receive(batch)).rejects.toThrow(/queue/i);
    expect(connection.writes).toHaveLength(0);
  });

  it('returns WINDOW only after backend writes drain and batches small writes', async () => {
    const { core, connection, sent } = fixture();
    await core.receive(new Uint8Array([
      ...encodeFrame(FrameType.Open, 1),
      ...encodeFrame(FrameType.Data, 1, new Uint8Array([1, 2])),
      ...encodeFrame(FrameType.Data, 1, new Uint8Array([3, 4]))
    ]));
    expect(connection.writes.map((value) => Array.from(value))).toEqual([[1, 2, 3, 4]]);
    const windows = sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window);
    expect(windows).toHaveLength(1);
    expect(new DataView(windows[0]!.payload.buffer, windows[0]!.payload.byteOffset, 4).getUint32(0)).toBe(4);
  });

  it('does not return WINDOW before a deferred write resolves', async () => {
    const { core, connection, sent } = fixture();
    let release!: () => void;
    connection.write = vi.fn(async (data: Uint8Array) => {
      connection.writes.push(data.slice());
      await new Promise<void>((resolve) => { release = resolve; });
    });
    const receive = core.receive(new Uint8Array([...encodeFrame(FrameType.Open, 1), ...encodeFrame(FrameType.Data, 1, new Uint8Array([1, 2, 3]))]));
    await settle();
    expect(sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window)).toHaveLength(0);
    release();
    await receive;
    expect(sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window)).toHaveLength(1);
  });

  it('closes the carrier when a Telegram write exceeds the configured deadline', async () => {
    vi.useFakeTimers();
    try {
      const connection = new MockConnection();
      connection.write = vi.fn(() => new Promise<void>(() => undefined));
      const closeCarrier = vi.fn();
      const core = new RelayCore({
        limits: DEFAULT_LIMITS,
        connector: { open: () => connection },
        send: vi.fn(), closeCarrier, writeTimeoutMs: 50
      });
      void core.receive(new Uint8Array([...encodeFrame(FrameType.Open, 1), ...encodeFrame(FrameType.Data, 1, new Uint8Array([1]))]));
      await vi.advanceTimersByTimeAsync(49);
      expect(closeCarrier).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(closeCarrier).toHaveBeenCalledTimes(1);
      expect(connection.closed).toBe(true);
    } finally { vi.useRealTimers(); }
  });

  it('closes all backends when the session closes', async () => {
    const { core, connection } = fixture();
    await core.receive(encodeFrame(FrameType.Open, 1));
    core.close();
    expect(connection.closed).toBe(true);
  });

  it('caps unacknowledged downlink bytes across the session', async () => {
    const { core, connection, sent } = fixture({ maxPendingBytes: 4 });
    await core.receive(encodeFrame(FrameType.Open, 1));
    connection.emit(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]));
    await settle();
    expect(sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Data).reduce((sum, frame) => sum + frame.payload.byteLength, 0)).toBe(4);
    await core.receive(encodeFrame(FrameType.Window, 1, new Uint8Array([0, 0, 0, 4])));
    await settle();
    expect(sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Data).reduce((sum, frame) => sum + frame.payload.byteLength, 0)).toBe(8);
  });
});
