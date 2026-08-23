export const FRAME_HEADER_BYTES = 8;
export const MAX_FRAME_PAYLOAD = 1024 * 1024;
export const MAX_BATCH_FRAMES = 4096;
export const INITIAL_STREAM_CREDIT = 4 * 1024 * 1024;
export const RELAY_DATA_CHUNK = 64 * 1024;

export enum FrameType {
  Open = 0x01, Data = 0x02, Close = 0x03, Window = 0x04,
  Ping = 0x05, Pong = 0x06, Hello = 0x10, Welcome = 0x11, Bye = 0x1f
}

export interface Frame { type: FrameType; streamId: number; payload: Uint8Array }

export function encodeFrame(type: FrameType, streamId: number, payload: Uint8Array<ArrayBufferLike> = new Uint8Array()): Uint8Array<ArrayBuffer> {
  if (!Number.isInteger(streamId) || streamId < 0 || streamId > 0xffffff) throw new Error('stream id exceeds 24 bits');
  if (payload.byteLength > MAX_FRAME_PAYLOAD) throw new Error('frame payload exceeds limit');
  const result = new Uint8Array(FRAME_HEADER_BYTES + payload.byteLength);
  result[0] = type;
  result[1] = streamId >>> 16;
  result[2] = streamId >>> 8;
  result[3] = streamId;
  new DataView(result.buffer).setUint32(4, payload.byteLength);
  result.set(payload, FRAME_HEADER_BYTES);
  return result;
}

function parseBatch(input: Uint8Array): Frame[] {
  if (input.byteLength === 0) throw new Error('empty frame batch');
  const frames: Frame[] = [];
  let offset = 0;
  while (offset < input.byteLength) {
    if (frames.length >= MAX_BATCH_FRAMES) throw new Error('frame batch contains too many frames');
    if (input.byteLength - offset < FRAME_HEADER_BYTES) throw new Error('incomplete frame header');
    const view = new DataView(input.buffer, input.byteOffset + offset, FRAME_HEADER_BYTES);
    const length = view.getUint32(4);
    if (length > MAX_FRAME_PAYLOAD) throw new Error('frame payload exceeds limit');
    const end = offset + FRAME_HEADER_BYTES + length;
    if (end > input.byteLength) throw new Error('incomplete frame payload');
    frames.push({
      type: input[offset]! as FrameType,
      streamId: (input[offset + 1]! << 16) | (input[offset + 2]! << 8) | input[offset + 3]!,
      payload: input.subarray(offset + FRAME_HEADER_BYTES, end)
    });
    offset = end;
  }
  return frames;
}

export function windowAmount(payload: Uint8Array): number {
  if (payload.byteLength !== 4) throw new Error('WINDOW payload must be four bytes');
  const amount = new DataView(payload.buffer, payload.byteOffset, 4).getUint32(0);
  if (amount === 0) throw new Error('WINDOW delta must be nonzero');
  return amount;
}

export function windowPayload(amount: number): Uint8Array<ArrayBuffer> {
  if (!Number.isInteger(amount) || amount <= 0 || amount > 0xffffffff) throw new Error('invalid WINDOW delta');
  const result = new Uint8Array(new ArrayBuffer(4));
  new DataView(result.buffer).setUint32(0, amount);
  return result;
}

export function parseClientBatch(input: Uint8Array): Frame[] {
  const frames = parseBatch(input);
  for (const frame of frames) {
    if (frame.streamId === 0) {
      if (frame.type !== FrameType.Pong || frame.payload.byteLength > 64) throw new Error('wrong direction or invalid stream-zero frame');
      continue;
    }
    if (frame.type === FrameType.Open || frame.type === FrameType.Close) {
      if (frame.payload.byteLength !== 0) throw new Error('OPEN/CLOSE payload must be empty');
    } else if (frame.type === FrameType.Data) {
      if (frame.payload.byteLength === 0) throw new Error('DATA payload must be nonempty');
    } else if (frame.type === FrameType.Window) windowAmount(frame.payload);
    else throw new Error('wrong direction or unknown client frame');
  }
  return frames;
}

export function parseRelayBatch(input: Uint8Array): Frame[] {
  const frames = parseBatch(input);
  for (const frame of frames) {
    if (frame.streamId === 0) {
      if (frame.type === FrameType.Welcome && frame.payload.byteLength === 0) continue;
      if ((frame.type === FrameType.Ping || frame.type === FrameType.Bye) && frame.payload.byteLength <= 64) continue;
      throw new Error('invalid relay stream-zero frame');
    }
    if ((frame.type === FrameType.Close && frame.payload.byteLength === 0) ||
        (frame.type === FrameType.Data && frame.payload.byteLength > 0) || frame.type === FrameType.Window) {
      if (frame.type === FrameType.Window) windowAmount(frame.payload);
      continue;
    }
    throw new Error('wrong direction or unknown relay frame');
  }
  return frames;
}

export function parseHello(input: Uint8Array): void {
  const frames = parseBatch(input);
  const frame = frames[0];
  if (frames.length !== 1 || frame?.type !== FrameType.Hello || frame.streamId !== 0 || frame.payload.byteLength !== 1 || frame.payload[0] !== 1) {
    throw new Error('invalid HELLO frame');
  }
}
