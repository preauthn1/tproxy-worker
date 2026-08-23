import { describe, expect, it } from 'vitest';
import { GrainCollector } from '../src/grain';

describe('GrainTCP-derived collector', () => {
  it('batches adjacent small byte chunks without exceeding its cap', () => {
    const collector = new GrainCollector(8);
    collector.push(new Uint8Array([1, 2]));
    collector.push(new Uint8Array([3, 4, 5]));
    collector.push(new Uint8Array([6, 7, 8, 9]));
    expect(Array.from(collector.take()!)).toEqual([1, 2, 3, 4, 5]);
    expect(Array.from(collector.take()!)).toEqual([6, 7, 8, 9]);
    expect(collector.take()).toBeNull();
  });

  it('sends a large first chunk directly without copying', () => {
    const collector = new GrainCollector(8);
    const chunk = new Uint8Array(16);
    collector.push(chunk);
    expect(collector.take()).toBe(chunk);
  });

  it('keeps backing storage bounded across long FIFO drains', () => {
    const collector = new GrainCollector(1);
    for (let index = 0; index < 10_000; index++) {
      collector.push(Uint8Array.of(index));
      expect(collector.take()).toEqual(Uint8Array.of(index));
    }
    expect(collector.retainedItems).toBeLessThan(1024);
  });
});
