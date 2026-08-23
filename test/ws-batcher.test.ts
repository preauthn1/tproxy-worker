import { describe, expect, it, vi } from 'vitest';
import { WebSocketBatcher } from '../src/ws-batcher';

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
});
