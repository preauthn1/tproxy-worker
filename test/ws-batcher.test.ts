import { describe, expect, it, vi } from 'vitest';
import { DEFAULT_WEBSOCKET_BATCHER_OPTIONS, WebSocketBatcher } from '../src/ws-batcher';

describe('small downlink aggregation and large direct send', () => {
  it('gates small chunks into one send and sends large chunks directly', async () => {
    vi.useFakeTimers();
    const output: Uint8Array[] = [];
    const batcher = new WebSocketBatcher((value) => output.push(value), { packBytes: 8, directBytes: 8, delayMs: 1 });
    batcher.send(new Uint8Array([1, 2]));
    batcher.send(new Uint8Array([3, 4]));
    expect(output).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(output.map((value) => Array.from(value))).toEqual([[1, 2, 3, 4]]);
    const large = new Uint8Array(9);
    batcher.send(new Uint8Array([5]));
    batcher.send(large);
    expect(output.at(-1)).toBe(large);
    expect(output.at(-2)).toEqual(new Uint8Array([5]));
    vi.useRealTimers();
  });

  it('packs ordinary 64 KiB DATA frames under the production profile', async () => {
    vi.useFakeTimers();
    try {
      const output: Uint8Array[] = [];
      const batcher = new WebSocketBatcher((value) => output.push(value), DEFAULT_WEBSOCKET_BATCHER_OPTIONS);
      for (let index = 0; index < 7; index++) batcher.send(new Uint8Array(64 * 1024 + 8));
      expect(output).toHaveLength(0);
      await vi.advanceTimersByTimeAsync(DEFAULT_WEBSOCKET_BATCHER_OPTIONS.delayMs);
      expect(output).toHaveLength(1);
      expect(output[0]?.byteLength).toBe(7 * (64 * 1024 + 8));
    } finally { vi.useRealTimers(); }
  });

  it('flushes before pack overflow instead of emitting an avoidable tail message', () => {
    const output: Uint8Array[] = [];
    const batcher = new WebSocketBatcher((value) => output.push(value), DEFAULT_WEBSOCKET_BATCHER_OPTIONS);
    for (let index = 0; index < 32; index++) batcher.send(new Uint8Array(64 * 1024 + 8));
    batcher.flush();
    expect(output).toHaveLength(5);
    expect(output.every((value) => value.byteLength <= DEFAULT_WEBSOCKET_BATCHER_OPTIONS.packBytes)).toBe(true);
  });

  it('preserves byte and item headroom for control after DATA fills its partition', () => {
    const output: Uint8Array[] = [];
    const batcher = new WebSocketBatcher((value) => output.push(value), {
      packBytes: 1024,
      directBytes: 2048,
      delayMs: 1000,
      maxPendingBytes: 100,
      maxPendingItems: 3,
      controlReserveBytes: 20,
      controlReserveItems: 1
    });
    batcher.send(new Uint8Array(40), false);
    batcher.send(new Uint8Array(40), false);
    expect(() => batcher.send(Uint8Array.of(1), false)).toThrow(/overflow/i);
    expect(() => batcher.send(new Uint8Array(20), true)).not.toThrow();
    batcher.flush();
    expect(output.reduce((sum, value) => sum + value.byteLength, 0)).toBe(100);
  });
});
