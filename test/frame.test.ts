import { describe, expect, it } from 'vitest';
import { FrameType, encodeFrame, parseClientBatch, parseHello } from '../src/frame';

describe('shared frame codec', () => {
  it('round-trips a valid client batch', () => {
    const batch = new Uint8Array([
      ...encodeFrame(FrameType.Open, 7),
      ...encodeFrame(FrameType.Data, 7, new Uint8Array([1, 2, 3])),
      ...encodeFrame(FrameType.Window, 7, new Uint8Array([0, 0, 0, 4])),
      ...encodeFrame(FrameType.Close, 7)
    ]);
    expect(parseClientBatch(batch).map(({ type, streamId, payload }) => [type, streamId, payload.byteLength])).toEqual([
      [FrameType.Open, 7, 0], [FrameType.Data, 7, 3], [FrameType.Window, 7, 4], [FrameType.Close, 7, 0]
    ]);
  });

  it.each([
    ['short header', new Uint8Array([1])],
    ['truncated payload', new Uint8Array([2, 0, 0, 1, 0, 0, 0, 2, 1])],
    ['empty DATA', encodeFrame(FrameType.Data, 1)],
    ['oversized payload header', new Uint8Array([2, 0, 0, 1, 0, 16, 0, 1])],
    ['unknown type', encodeFrame(0x77 as FrameType, 1)],
    ['relay-only WELCOME', encodeFrame(FrameType.Welcome, 0)],
    ['OPEN on stream zero', encodeFrame(FrameType.Open, 0)],
    ['zero WINDOW', encodeFrame(FrameType.Window, 1, new Uint8Array(4))]
  ])('rejects malformed/wrong-direction input: %s', (_name, bytes) => {
    expect(() => parseClientBatch(bytes)).toThrow();
  });

  it('accepts only the exact HELLO frame', () => {
    expect(() => parseHello(encodeFrame(FrameType.Hello, 0, new Uint8Array([1])))).not.toThrow();
    expect(() => parseHello(encodeFrame(FrameType.Hello, 0, new Uint8Array([2])))).toThrow();
  });
});
