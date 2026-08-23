/* SPDX-License-Identifier: GPL-3.0-only */
import { describe, expect, it, vi } from 'vitest';
import { IdleLiveness, SerializedInboundQueue, readBoundedBody } from '../src/session-guards';

describe('session resource guards', () => {
  it('charges WebSocket bytes before serialized work begins and rejects overflow', async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const handled: number[] = [];
    const queue = new SerializedInboundQueue<number>({
      maxBytes: 6, maxItems: 2,
      size: (value) => value,
      handle: async (value) => { handled.push(value); await gate; }
    });
    expect(queue.push(3)).toBe(true);
    expect(queue.push(3)).toBe(true);
    expect(queue.push(1)).toBe(false);
    expect(queue.pendingBytes).toBe(6);
    release();
    await queue.drained();
    expect(handled).toEqual([3, 3]);
  });

  it('does not accept more work after a serialized handler failure', async () => {
    const queue = new SerializedInboundQueue<number>({
      maxBytes: 10, maxItems: 10, size: () => 1,
      handle: async () => { throw new Error('protocol failure'); }
    });
    expect(queue.push(1)).toBe(true);
    await expect(queue.drained()).rejects.toThrow(/protocol failure/i);
    expect(queue.push(2)).toBe(false);
  });

  it('reads at most limit+1 bytes and cancels an oversized create body', async () => {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) { controller.enqueue(new Uint8Array(65)); },
      cancel() { cancelled = true; }
    });
    await expect(readBoundedBody(body, 64, 100)).rejects.toThrow(/limit/i);
    expect(cancelled).toBe(true);
  });

  it('times out a stalled create body independently', async () => {
    vi.useFakeTimers();
    try {
      const pending = readBoundedBody(new ReadableStream<Uint8Array>({ pull() { return new Promise<void>(() => undefined); } }), 64, 25);
      const rejected = expect(pending).rejects.toThrow(/deadline/i);
      await vi.advanceTimersByTimeAsync(25);
      await rejected;
    } finally { vi.useRealTimers(); }
  });

  it('pings after one idle period and closes after the next period without activity', async () => {
    vi.useFakeTimers();
    try {
      const ping = vi.fn();
      const close = vi.fn();
      const liveness = new IdleLiveness(20, ping, close);
      liveness.start();
      await vi.advanceTimersByTimeAsync(20);
      expect(ping).toHaveBeenCalledTimes(1);
      expect(close).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(20);
      expect(close).toHaveBeenCalledTimes(1);
      liveness.stop();
    } finally { vi.useRealTimers(); }
  });

  it('activity after a ping resets the idle close decision', async () => {
    vi.useFakeTimers();
    try {
      const ping = vi.fn();
      const close = vi.fn();
      const liveness = new IdleLiveness(20, ping, close);
      liveness.start();
      await vi.advanceTimersByTimeAsync(20);
      liveness.touch();
      await vi.advanceTimersByTimeAsync(20);
      expect(ping).toHaveBeenCalledTimes(2);
      expect(close).not.toHaveBeenCalled();
      liveness.stop();
    } finally { vi.useRealTimers(); }
  });
});
