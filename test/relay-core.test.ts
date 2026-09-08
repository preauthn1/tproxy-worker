import { describe, expect, it, vi } from 'vitest';
import { FrameType, INITIAL_STREAM_CREDIT, encodeFrame, parseRelayBatch } from '../src/frame';
import { DEFAULT_LIMITS } from '../src/limits';
import { RelayCore, WINDOW_FLUSH_BYTES, WINDOW_FLUSH_DELAY_MS, type TelegramConnection, type TelegramConnectorLike } from '../src/relay-core';
import { SerializedInboundQueue } from '../src/session-guards';

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
  it('keeps room for Telegram Desktop concurrent streams within the session budget', () => {
    expect(DEFAULT_LIMITS.maxStreams).toBe(64);
  });

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

  it('permits stream id reuse after bounded tombstone eviction', async () => {
    const { core } = fixture({ maxClosedStreamIds: 2 });
    for (const id of [1, 2, 3]) await core.receive(new Uint8Array([...encodeFrame(FrameType.Open, id), ...encodeFrame(FrameType.Close, id)]));
    await expect(core.receive(encodeFrame(FrameType.Open, 1))).resolves.toBeUndefined();
  });

  it('accepts out-of-order fresh OPEN ids in one carrier batch', async () => {
    const { core, connector } = fixture();
    await expect(core.receive(new Uint8Array([...encodeFrame(FrameType.Open, 2), ...encodeFrame(FrameType.Open, 1)]))).resolves.toBeUndefined();
    expect(connector.open).toHaveBeenCalledTimes(2);
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
    vi.useFakeTimers();
    try {
      const { core, connection, sent } = fixture();
      await core.receive(new Uint8Array([
        ...encodeFrame(FrameType.Open, 1),
        ...encodeFrame(FrameType.Data, 1, new Uint8Array([1, 2])),
        ...encodeFrame(FrameType.Data, 1, new Uint8Array([3, 4]))
      ]));
      await vi.advanceTimersByTimeAsync(0);
      expect(connection.writes.map((value) => Array.from(value))).toEqual([[1, 2, 3, 4]]);
      expect(sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window)).toHaveLength(0);
      await vi.advanceTimersByTimeAsync(WINDOW_FLUSH_DELAY_MS);
      const windows = sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window);
      expect(windows).toHaveLength(1);
      expect(new DataView(windows[0]!.payload.buffer, windows[0]!.payload.byteOffset, 4).getUint32(0)).toBe(4);
    } finally { vi.useRealTimers(); }
  });

  it('drains independent stream uploads concurrently while preserving each stream order', async () => {
    const first = new MockConnection();
    const second = new MockConnection();
    const connections = [first, second];
    let releaseFirst!: () => void;
    let blocked = false;
    first.write = vi.fn(async (data: Uint8Array) => {
      first.writes.push(data.slice());
      if (!blocked) {
        blocked = true;
        await new Promise<void>((resolve) => { releaseFirst = resolve; });
      }
    });
    const core = new RelayCore({
      connector: { open: () => connections.shift()! }, send: vi.fn(), closeCarrier: vi.fn()
    });
    const receive = core.receive(new Uint8Array([
      ...encodeFrame(FrameType.Open, 1),
      ...encodeFrame(FrameType.Data, 1, new Uint8Array(40 * 1024).fill(1)),
      ...encodeFrame(FrameType.Data, 1, new Uint8Array(40 * 1024).fill(3)),
      ...encodeFrame(FrameType.Open, 2), ...encodeFrame(FrameType.Data, 2, Uint8Array.of(2))
    ]));
    await settle();
    expect(second.writes).toEqual([Uint8Array.of(2)]);
    releaseFirst();
    await receive;
    await settle();
    expect(first.writes).toHaveLength(2);
    expect(first.writes[0]?.every((value) => value === 1)).toBe(true);
    expect(first.writes[1]?.every((value) => value === 3)).toBe(true);
  });

  it('does not let a blocked write hold later carrier messages or another stream', async () => {
    const first = new MockConnection();
    const second = new MockConnection();
    const connections = [first, second];
    let releaseFirst!: () => void;
    first.write = vi.fn(async (data: Uint8Array) => {
      first.writes.push(data.slice());
      await new Promise<void>((resolve) => { releaseFirst = resolve; });
    });
    const core = new RelayCore({
      connector: { open: () => connections.shift()! }, send: vi.fn(), closeCarrier: vi.fn()
    });
    const handled: number[] = [];
    const queue = new SerializedInboundQueue<Uint8Array>({
      maxBytes: DEFAULT_LIMITS.maxPendingBytes,
      maxItems: DEFAULT_LIMITS.maxPendingItems,
      size: (value) => value.byteLength + 256,
      handle: async (value) => { await core.receive(value); handled.push(handled.length + 1); }
    });

    expect(queue.push(new Uint8Array([
      ...encodeFrame(FrameType.Open, 1),
      ...encodeFrame(FrameType.Data, 1, Uint8Array.of(1))
    ]))).toBe(true);
    expect(queue.push(new Uint8Array([
      ...encodeFrame(FrameType.Window, 1, new Uint8Array([0, 0, 0, 1])),
      ...encodeFrame(FrameType.Open, 2),
      ...encodeFrame(FrameType.Data, 2, Uint8Array.of(2)),
      ...encodeFrame(FrameType.Close, 1)
    ]))).toBe(true);

    await settle();
    expect(handled).toEqual([1, 2]);
    expect(second.writes).toEqual([Uint8Array.of(2)]);
    expect(first.closed).toBe(true);
    releaseFirst();
    await queue.drained();
  });

  it('preserves exact same-stream order across separate carrier messages', async () => {
    const { core, connection } = fixture();
    let releaseFirst!: () => void;
    connection.write = vi.fn(async (data: Uint8Array) => {
      connection.writes.push(data.slice());
      if (connection.writes.length === 1) await new Promise<void>((resolve) => { releaseFirst = resolve; });
    });
    await core.receive(new Uint8Array([
      ...encodeFrame(FrameType.Open, 1),
      ...encodeFrame(FrameType.Data, 1, Uint8Array.of(1))
    ]));
    await core.receive(encodeFrame(FrameType.Data, 1, Uint8Array.of(2)));
    await core.receive(encodeFrame(FrameType.Data, 1, Uint8Array.of(3)));
    await settle();
    expect(connection.writes).toEqual([Uint8Array.of(1)]);
    releaseFirst();
    await settle();
    expect(connection.writes).toEqual([Uint8Array.of(1), Uint8Array.of(2), Uint8Array.of(3)]);
  });

  it('keeps exactly one long-lived writer pump per open stream', async () => {
    const { core, connection } = fixture();
    await core.receive(encodeFrame(FrameType.Open, 1));
    await core.receive(encodeFrame(FrameType.Data, 1, Uint8Array.of(1)));
    await settle();
    await core.receive(encodeFrame(FrameType.Data, 1, Uint8Array.of(2)));
    await settle();
    expect(connection.writes).toEqual([Uint8Array.of(1), Uint8Array.of(2)]);
    core.close();
  });


  it('yields after a bounded writer turn when writes resolve immediately', async () => {
    const { core, connection } = fixture();
    await core.receive(encodeFrame(FrameType.Open, 1));
    const receives: Array<Promise<void>> = [];
    for (let index = 0; index < 257; index++) receives.push(core.receive(encodeFrame(FrameType.Data, 1, Uint8Array.of(index))));
    await Promise.all(receives);
    const beforeNextTurn = await new Promise<number>((resolve) => setTimeout(() => resolve(connection.writes.length), 0));
    expect(beforeNextTurn).toBe(256);
    await settle();
    expect(connection.writes).toHaveLength(257);
  });

  it('bounds each stream writer queue independently', async () => {
    const { core, connection } = fixture();
    connection.write = vi.fn(() => new Promise<void>(() => undefined));
    await core.receive(encodeFrame(FrameType.Open, 1));
    for (let index = 0; index < 1024; index++) await core.receive(encodeFrame(FrameType.Data, 1, Uint8Array.of(index)));
    await expect(core.receive(encodeFrame(FrameType.Data, 1, Uint8Array.of(1)))).rejects.toThrow(/stream.*queue/i);
    expect(core.pendingItems).toBe(1024);
    core.close();
    expect(core.pendingItems).toBe(0);
  });

  it('encodes downlink from the backend view without an intermediate slice', async () => {
    const connection = new MockConnection();
    const core = new RelayCore({ connector: { open: () => connection }, send: vi.fn(), closeCarrier: vi.fn() });
    await core.receive(encodeFrame(FrameType.Open, 1));
    const backend = new Uint8Array([1, 2, 3]);
    const slice = vi.spyOn(backend, 'slice');
    try {
      connection.emit(backend);
      await settle();
      expect(slice).not.toHaveBeenCalled();
    } finally { slice.mockRestore(); }
  });

  it('speculates validation state only for streams touched by a batch', async () => {
    const snapshots: number[] = [];
    const connection = new MockConnection();
    const core = new RelayCore({
      connector: { open: () => connection }, send: vi.fn(), closeCarrier: vi.fn(),
      onValidationSnapshotCount: (count) => snapshots.push(count)
    });
    for (let id = 1; id <= 16; id++) await core.receive(encodeFrame(FrameType.Open, id));
    snapshots.length = 0;
    await core.receive(encodeFrame(FrameType.Data, 8, Uint8Array.of(1)));
    expect(snapshots).toEqual([1]);
  });

  it('does not return WINDOW before a deferred write resolves', async () => {
    vi.useFakeTimers();
    try {
      const { core, connection, sent } = fixture();
      let release!: () => void;
      connection.write = vi.fn(async (data: Uint8Array) => {
        connection.writes.push(data.slice());
        await new Promise<void>((resolve) => { release = resolve; });
      });
      await core.receive(new Uint8Array([...encodeFrame(FrameType.Open, 1), ...encodeFrame(FrameType.Data, 1, new Uint8Array([1, 2, 3]))]));
      await vi.advanceTimersByTimeAsync(WINDOW_FLUSH_DELAY_MS);
      expect(sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window)).toHaveLength(0);
      release();
      await vi.advanceTimersByTimeAsync(WINDOW_FLUSH_DELAY_MS - 1);
      expect(sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window)).toHaveLength(0);
      await vi.advanceTimersByTimeAsync(1);
      expect(sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window)).toHaveLength(1);
    } finally { vi.useRealTimers(); }
  });

  it('flushes drained receive credit at 256 KiB or after 20 ms', async () => {
    vi.useFakeTimers();
    try {
      const { core, sent } = fixture();
      await core.receive(new Uint8Array([
        ...encodeFrame(FrameType.Open, 1),
        ...encodeFrame(FrameType.Data, 1, new Uint8Array(WINDOW_FLUSH_BYTES - 1))
      ]));
      await vi.advanceTimersByTimeAsync(WINDOW_FLUSH_DELAY_MS - 1);
      expect(sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window)).toHaveLength(0);
      await vi.advanceTimersByTimeAsync(1);
      let windows = sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window);
      expect(windows.map((frame) => new DataView(frame.payload.buffer, frame.payload.byteOffset, 4).getUint32(0))).toEqual([WINDOW_FLUSH_BYTES - 1]);

      await core.receive(encodeFrame(FrameType.Data, 1, new Uint8Array(WINDOW_FLUSH_BYTES)));
      await vi.advanceTimersByTimeAsync(0);
      windows = sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window);
      expect(windows.map((frame) => new DataView(frame.payload.buffer, frame.payload.byteOffset, 4).getUint32(0))).toEqual([
        WINDOW_FLUSH_BYTES - 1,
        WINDOW_FLUSH_BYTES
      ]);
    } finally { vi.useRealTimers(); }
  });

  it('releases queued accounting on CLOSE and ignores stale writer completion after id reuse', async () => {
    vi.useFakeTimers();
    try {
      const first = new MockConnection();
      const second = new MockConnection();
      let releaseFirst!: () => void;
      first.write = vi.fn(async (data: Uint8Array) => {
        first.writes.push(data.slice());
        await new Promise<void>((resolve) => { releaseFirst = resolve; });
      });
      const connections = [first, second];
      const sent: Uint8Array[] = [];
      const core = new RelayCore({
        limits: { ...DEFAULT_LIMITS, maxClosedStreamIds: 0 },
        connector: { open: () => connections.shift()! },
        send: (value) => sent.push(value.slice()), closeCarrier: vi.fn()
      });
      await core.receive(new Uint8Array([
        ...encodeFrame(FrameType.Open, 1),
        ...encodeFrame(FrameType.Data, 1, Uint8Array.of(1))
      ]));
      await core.receive(encodeFrame(FrameType.Data, 1, Uint8Array.of(2)));
      expect(core.pendingBytes).toBe(2 * (1 + 256));
      expect(core.pendingItems).toBe(2);

      await core.receive(encodeFrame(FrameType.Close, 1));
      expect(core.pendingBytes).toBe(0);
      expect(core.pendingItems).toBe(0);
      await core.receive(encodeFrame(FrameType.Open, 1));
      releaseFirst();
      await vi.advanceTimersByTimeAsync(WINDOW_FLUSH_DELAY_MS);
      expect(sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window)).toHaveLength(0);
      expect(core.pendingBytes).toBe(0);
      expect(core.pendingItems).toBe(0);

      await core.receive(encodeFrame(FrameType.Data, 1, Uint8Array.of(3)));
      await vi.advanceTimersByTimeAsync(WINDOW_FLUSH_DELAY_MS);
      const windows = sent.flatMap(parseRelayBatch).filter((frame) => frame.type === FrameType.Window);
      expect(windows).toHaveLength(1);
      expect(new DataView(windows[0]!.payload.buffer, windows[0]!.payload.byteOffset, 4).getUint32(0)).toBe(1);
    } finally { vi.useRealTimers(); }
  });

  it('closes only the failing stream when a Telegram write exceeds its deadline', async () => {
    vi.useFakeTimers();
    try {
      const connection = new MockConnection();
      connection.write = vi.fn(() => new Promise<void>(() => undefined));
      const closeCarrier = vi.fn();
      const sent: Uint8Array[] = [];
      const core = new RelayCore({
        limits: DEFAULT_LIMITS,
        connector: { open: () => connection },
        send: (value) => sent.push(value.slice()), closeCarrier, writeTimeoutMs: 50
      });
      void core.receive(new Uint8Array([...encodeFrame(FrameType.Open, 1), ...encodeFrame(FrameType.Data, 1, new Uint8Array([1]))]));
      await vi.advanceTimersByTimeAsync(49);
      expect(closeCarrier).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(closeCarrier).not.toHaveBeenCalled();
      expect(connection.closed).toBe(true);
      expect(sent.flatMap(parseRelayBatch)).toMatchObject([{ type: FrameType.Close, streamId: 1 }]);
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
