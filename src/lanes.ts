/* SPDX-License-Identifier: GPL-3.0-only */
import { FrameType, MAX_BATCH_FRAMES } from './frame';
import { DEFAULT_WEBSOCKET_BATCHER_OPTIONS, WebSocketBatcher } from './ws-batcher';

/** Official websocket-lanes per-lane bounds (tproxy-server PROTOCOL.md). */
export const LANE_MAX_PENDING_BYTES = 8 * 1024 * 1024;
export const LANE_MAX_PENDING_ITEMS = 1024;

export const LANE_PROTOCOL = /^tproxy-lane-v1\.([A-Za-z0-9_-]{43})\.([1-9][0-9]{0,7})$/;

export function parseLaneProtocol(protocol: string): { token: string; streamId: number } | null {
  const match = LANE_PROTOCOL.exec(protocol);
  if (!match) return null;
  const streamId = Number(match[2]);
  if (!Number.isSafeInteger(streamId) || streamId <= 0 || streamId > 0xffffff) return null;
  return { token: match[1]!, streamId };
}

export class LaneOverflow extends Error {
  constructor(readonly streamId: number) { super('lane overflow'); }
}

export interface LaneSocket {
  send(value: Uint8Array): void;
  close(code?: number, reason?: string): void;
}

interface Lane {
  socket: LaneSocket;
  batcher: WebSocketBatcher;
  opened: boolean;
}

/**
 * Validates that every frame in a client lane message belongs to that lane and that
 * the first message begins with OPEN. Returns an error string or null.
 */
export function validateLaneMessage(bytes: Uint8Array, streamId: number, firstMessage: boolean): string | null {
  let offset = 0;
  let frames = 0;
  while (offset < bytes.byteLength) {
    if (bytes.byteLength - offset < 8 || frames >= MAX_BATCH_FRAMES) return 'malformed batch';
    const type = bytes[offset]!;
    const id = (bytes[offset + 1]! << 16) | (bytes[offset + 2]! << 8) | bytes[offset + 3]!;
    const size = new DataView(bytes.buffer, bytes.byteOffset + offset + 4, 4).getUint32(0);
    const end = offset + 8 + size;
    if (end > bytes.byteLength) return 'malformed batch';
    if (id !== streamId) return 'cross-lane frame';
    if (firstMessage && frames === 0 && type !== FrameType.Open) return 'lane must begin with OPEN';
    if (!firstMessage || frames > 0) if (type === FrameType.Open) return 'duplicate OPEN';
    frames++;
    offset = end;
  }
  return frames ? null : 'empty batch';
}

/**
 * Routes relay-to-client frames to the per-stream lane socket. Each lane owns its own
 * batcher so one lane's bulk downlink cannot delay another lane's frames.
 */
export class LaneRouter {
  readonly #lanes = new Map<number, Lane>();
  readonly #used = new Set<number>();
  readonly #maxLanes: number;

  constructor(maxLanes: number) { this.#maxLanes = maxLanes; }

  get size(): number { return this.#lanes.size; }

  canAttach(streamId: number): boolean {
    return !this.#used.has(streamId) && this.#lanes.size < this.#maxLanes;
  }

  attach(streamId: number, socket: LaneSocket): void {
    if (!this.canAttach(streamId)) throw new Error('lane unavailable');
    this.#used.add(streamId);
    const batcher = new WebSocketBatcher((value) => socket.send(value), {
      ...DEFAULT_WEBSOCKET_BATCHER_OPTIONS,
      maxPendingBytes: LANE_MAX_PENDING_BYTES,
      maxPendingItems: LANE_MAX_PENDING_ITEMS
    });
    this.#lanes.set(streamId, { socket, batcher, opened: false });
  }

  markOpened(streamId: number): void {
    const lane = this.#lanes.get(streamId);
    if (lane) lane.opened = true;
  }

  isOpened(streamId: number): boolean { return this.#lanes.get(streamId)?.opened ?? false; }

  has(streamId: number): boolean { return this.#lanes.has(streamId); }

  /**
   * Delivers a relay batch (one or more encoded frames). Each frame is routed to
   * its stream's lane; stream-zero frames are dropped (lanes use WebSocket-level
   * liveness). A CLOSE completes the lane: flush, then close the socket.
   * Throws a LaneOverflow carrying the stream id if that lane's queue is full.
   */
  route(batch: Uint8Array, control = false): void {
    let offset = 0;
    while (offset + 8 <= batch.byteLength) {
      const size = new DataView(batch.buffer, batch.byteOffset + offset + 4, 4).getUint32(0);
      const end = offset + 8 + size;
      if (end > batch.byteLength) return;
      const type = batch[offset]!;
      const streamId = (batch[offset + 1]! << 16) | (batch[offset + 2]! << 8) | batch[offset + 3]!;
      const frame = offset === 0 && end === batch.byteLength ? batch : batch.subarray(offset, end);
      offset = end;
      if (streamId === 0) continue;
      const lane = this.#lanes.get(streamId);
      if (!lane) continue;
      try { lane.batcher.send(frame, control || type !== FrameType.Data); }
      catch { throw new LaneOverflow(streamId); }
      if (type === FrameType.Close) this.detach(streamId, 1000, 'stream closed');
    }
  }

  detach(streamId: number, code = 1000, reason = ''): boolean {
    const lane = this.#lanes.get(streamId);
    if (!lane) return false;
    this.#lanes.delete(streamId);
    try { lane.batcher.flush(); } catch { /* best effort */ }
    lane.batcher.close();
    try { lane.socket.close(code, reason); } catch { /* already closed */ }
    return true;
  }

  closeAll(code = 1000, reason = ''): void {
    for (const id of [...this.#lanes.keys()]) this.detach(id, code, reason);
  }
}
