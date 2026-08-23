import { FrameType, INITIAL_STREAM_CREDIT, RELAY_DATA_CHUNK, encodeFrame, parseClientBatch, windowAmount, windowPayload, type Frame } from './frame';
import { GrainCollector } from './grain';
import { DEFAULT_LIMITS, QUEUE_ITEM_COST, type RelayLimits } from './limits';

export const WINDOW_FLUSH_BYTES = 256 * 1024;
export const WINDOW_FLUSH_DELAY_MS = 20;
const MAX_STREAM_PENDING_BYTES = 8 * 1024 * 1024;
const MAX_STREAM_PENDING_ITEMS = 1024;
const WRITER_WORK_ITEMS = 256;

export interface TelegramConnection {
  write(data: Uint8Array): Promise<void>;
  read(): AsyncIterable<Uint8Array>;
  close(): void;
}

export interface TelegramConnectorLike {
  open(): TelegramConnection;
  close?(): void;
}

interface StreamState {
  connection: TelegramConnection;
  receiveCredit: number;
  sendCredit: number;
  creditWaiters: Array<() => void>;
  pendingDownlinkBytes: number;
  writes: Array<Uint8Array | undefined>;
  writeHead: number;
  writerRunning: boolean;
  writeWaiters: Array<() => void>;
  pendingWriteCost: number;
  pendingWriteItems: number;
  pendingWindow: number;
}

export interface RelayCoreOptions {
  limits?: RelayLimits;
  connector: TelegramConnectorLike;
  send(batch: Uint8Array, control?: boolean): void;
  closeCarrier(): void;
  writeTimeoutMs?: number;
  onValidationSnapshotCount?(count: number): void;
}

interface Snapshot {
  receiveCredit: number;
  sendCredit: number;
  pendingWriteCost: number;
  pendingWriteItems: number;
}

export class RelayCore {
  readonly #options: RelayCoreOptions;
  readonly #limits: RelayLimits;
  readonly #streams = new Map<number, StreamState>();
  readonly #closed = new Set<number>();
  readonly #closedOrder: number[] = [];
  #closedStart = 0;
  #pendingBytes = 0;
  #pendingItems = 0;
  #pendingDownlinkBytes = 0;
  readonly #downlinkWaiters: Array<() => void> = [];
  #windowTimer: ReturnType<typeof setTimeout> | undefined;
  #ended = false;

  constructor(options: RelayCoreOptions) {
    this.#options = options;
    this.#limits = options.limits ?? DEFAULT_LIMITS;
  }

  get pendingBytes(): number { return this.#pendingBytes; }
  get pendingItems(): number { return this.#pendingItems; }

  async receive(batch: Uint8Array): Promise<void> {
    if (this.#ended) throw new Error('session closed');
    const frames = parseClientBatch(batch);
    const reservation = this.#validateAndReserve(frames);
    this.#pendingBytes += reservation.bytes;
    this.#pendingItems += reservation.items;
    this.#apply(frames, reservation);
  }

  #validateAndReserve(frames: Frame[]): { bytes: number; items: number } {
    const touched = new Map<number, Snapshot | null>();
    const closedInBatch = new Set<number>();
    let bytes = 0;
    let items = 0;
    const streamByteLimit = Math.min(this.#limits.maxPendingBytes, MAX_STREAM_PENDING_BYTES);
    const streamItemLimit = Math.min(this.#limits.maxPendingItems, MAX_STREAM_PENDING_ITEMS);
    for (const frame of frames) {
      if (frame.streamId === 0) continue;
      let previous: Snapshot | undefined;
      if (touched.has(frame.streamId)) previous = touched.get(frame.streamId) ?? undefined;
      else {
        const stream = this.#streams.get(frame.streamId);
        if (stream) {
          previous = {
            receiveCredit: stream.receiveCredit,
            sendCredit: stream.sendCredit,
            pendingWriteCost: stream.pendingWriteCost,
            pendingWriteItems: stream.pendingWriteItems
          };
          touched.set(frame.streamId, previous);
        }
      }
      const closed = this.#closed.has(frame.streamId) || closedInBatch.has(frame.streamId);
      if (frame.type === FrameType.Open) {
        if (previous || closed) throw new Error('stream id reuse');
        touched.set(frame.streamId, {
          receiveCredit: INITIAL_STREAM_CREDIT,
          sendCredit: INITIAL_STREAM_CREDIT,
          pendingWriteCost: 0,
          pendingWriteItems: 0
        });
      } else if (frame.type === FrameType.Data) {
        if (closed) continue;
        if (!previous) throw new Error('DATA for unknown stream');
        if (frame.payload.byteLength > previous.receiveCredit) throw new Error('DATA beyond receive credit');
        const cost = frame.payload.byteLength + QUEUE_ITEM_COST;
        if (previous.pendingWriteCost + cost > streamByteLimit || previous.pendingWriteItems + 1 > streamItemLimit) {
          throw new Error('stream pending queue overflow');
        }
        previous.receiveCredit -= frame.payload.byteLength;
        previous.pendingWriteCost += cost;
        previous.pendingWriteItems++;
        bytes += cost;
        items++;
      } else if (frame.type === FrameType.Window) {
        if (closed) continue;
        if (!previous) throw new Error('WINDOW for unknown stream');
        previous.sendCredit = Math.min(0xffffffff, previous.sendCredit + windowAmount(frame.payload));
      } else if (frame.type === FrameType.Close) {
        if (closed) continue;
        if (!previous) throw new Error('CLOSE for unknown stream');
        touched.set(frame.streamId, null);
        closedInBatch.add(frame.streamId);
      }
    }
    this.#options.onValidationSnapshotCount?.(touched.size);
    if (this.#pendingBytes + bytes > this.#limits.maxPendingBytes || this.#pendingItems + items > this.#limits.maxPendingItems) {
      throw new Error('pending queue overflow');
    }
    return { bytes, items };
  }

  #apply(frames: Frame[], reservation: { bytes: number; items: number }): void {
    let unusedBytes = reservation.bytes;
    let unusedItems = reservation.items;
    const uploads = new Map<number, GrainCollector>();
    try {
      for (const frame of frames) {
        if (frame.streamId === 0) continue;
        if (frame.type === FrameType.Open) {
          if (this.#streams.size >= this.#limits.maxStreams) {
            this.#rememberClosed(frame.streamId);
            this.#sendControl(encodeFrame(FrameType.Close, frame.streamId));
            continue;
          }
          let connection: TelegramConnection;
          try { connection = this.#options.connector.open(); }
          catch {
            this.#rememberClosed(frame.streamId);
            this.#sendControl(encodeFrame(FrameType.Close, frame.streamId));
            continue;
          }
          const stream: StreamState = {
            connection,
            receiveCredit: INITIAL_STREAM_CREDIT,
            sendCredit: INITIAL_STREAM_CREDIT,
            creditWaiters: [],
            pendingDownlinkBytes: 0,
            writes: [],
            writeHead: 0,
            writerRunning: false,
            writeWaiters: [],
            pendingWriteCost: 0,
            pendingWriteItems: 0,
            pendingWindow: 0
          };
          this.#streams.set(frame.streamId, stream);
          this.#background(this.#pumpBackend(frame.streamId, stream));
        } else if (frame.type === FrameType.Data) {
          const stream = this.#streams.get(frame.streamId);
          if (!stream) continue;
          stream.receiveCredit -= frame.payload.byteLength;
          let collector = uploads.get(frame.streamId);
          if (!collector) {
            collector = new GrainCollector(RELAY_DATA_CHUNK);
            uploads.set(frame.streamId, collector);
          }
          collector.push(frame.payload.slice());
        } else if (frame.type === FrameType.Window) {
          const stream = this.#streams.get(frame.streamId);
          if (stream) {
            const amount = windowAmount(frame.payload);
            stream.sendCredit = Math.min(0xffffffff, stream.sendCredit + amount);
            const acknowledged = Math.min(amount, stream.pendingDownlinkBytes);
            stream.pendingDownlinkBytes -= acknowledged;
            this.#pendingDownlinkBytes -= acknowledged;
            for (const wake of stream.creditWaiters.splice(0)) wake();
            for (const wake of this.#downlinkWaiters.splice(0)) wake();
          }
        } else if (frame.type === FrameType.Close) this.#closeStream(frame.streamId, false);
      }
      for (const [id, collector] of uploads) {
        const stream = this.#streams.get(id);
        if (!stream) continue;
        for (let grain = collector.take(); grain; grain = collector.take()) {
          const cost = grain.byteLength + QUEUE_ITEM_COST;
          stream.pendingWriteCost += cost;
          stream.pendingWriteItems++;
          stream.writes.push(grain);
          for (const wake of stream.writeWaiters.splice(0)) wake();
          unusedBytes -= cost;
          unusedItems--;
        }
        this.#startWriter(id, stream);
      }
    } finally {
      this.#releasePending(unusedBytes, unusedItems);
    }
  }

  #startWriter(id: number, stream: StreamState): void {
    if (stream.writerRunning) return;
    stream.writerRunning = true;
    this.#background(this.#runWriter(id, stream));
  }

  async #runWriter(id: number, stream: StreamState): Promise<void> {
    let processed = 0;
    try {
      while (!this.#ended && this.#streams.get(id) === stream) {
        const value = stream.writes[stream.writeHead];
        if (!value) {
          await new Promise<void>((resolve) => stream.writeWaiters.push(resolve));
          continue;
        }
        await this.#writeWithDeadline(stream.connection, value);
        if (this.#ended || this.#streams.get(id) !== stream) return;
        stream.writes[stream.writeHead++] = undefined;
        const cost = value.byteLength + QUEUE_ITEM_COST;
        stream.pendingWriteCost -= cost;
        stream.pendingWriteItems--;
        this.#releasePending(cost, 1);
        this.#backendDrained(id, stream, value.byteLength);
        if (stream.writeHead === stream.writes.length) {
          stream.writes = [];
          stream.writeHead = 0;
        } else if (stream.writeHead >= 1024 && stream.writeHead * 2 >= stream.writes.length) {
          stream.writes = stream.writes.slice(stream.writeHead);
          stream.writeHead = 0;
        }
        if (++processed === WRITER_WORK_ITEMS) {
          processed = 0;
          await new Promise<void>((resolve) => setTimeout(resolve, 0));
        }
      }
    } catch {
      this.#closeStream(id, true, stream);
    } finally { stream.writerRunning = false; }
  }

  #backendDrained(id: number, stream: StreamState, amount: number): void {
    if (this.#streams.get(id) !== stream || amount <= 0) return;
    stream.pendingWindow += amount;
    if (stream.pendingWindow >= WINDOW_FLUSH_BYTES) this.#flushWindow(id, stream);
    else this.#scheduleWindowFlush();
  }

  #scheduleWindowFlush(): void {
    if (this.#windowTimer || this.#ended) return;
    this.#windowTimer = setTimeout(() => {
      this.#windowTimer = undefined;
      for (const [id, stream] of this.#streams) this.#flushWindow(id, stream);
    }, WINDOW_FLUSH_DELAY_MS);
  }

  #flushWindow(id: number, stream: StreamState): void {
    if (this.#ended || this.#streams.get(id) !== stream || stream.pendingWindow === 0) return;
    let remaining = stream.pendingWindow;
    stream.pendingWindow = 0;
    while (remaining > 0) {
      const amount = Math.min(remaining, 0xffffffff);
      stream.receiveCredit += amount;
      this.#sendControl(encodeFrame(FrameType.Window, id, windowPayload(amount)));
      remaining -= amount;
    }
    this.#cancelWindowTimerIfIdle();
  }

  async #pumpBackend(id: number, streamIdentity: StreamState): Promise<void> {
    const connection = streamIdentity.connection;
    try {
      for await (const value of connection.read()) {
        if (this.#ended || this.#streams.get(id) !== streamIdentity) return;
        let offset = 0;
        while (offset < value.byteLength) {
          let stream = this.#streams.get(id);
          if (stream !== streamIdentity) return;
          while (stream.sendCredit === 0) {
            const current = stream;
            await new Promise<void>((resolve) => current.creditWaiters.push(resolve));
            stream = this.#streams.get(id)!;
            if (stream !== streamIdentity) return;
          }
          while (this.#pendingDownlinkBytes >= this.#limits.maxPendingBytes) {
            await new Promise<void>((resolve) => this.#downlinkWaiters.push(resolve));
            stream = this.#streams.get(id)!;
            if (stream !== streamIdentity) return;
          }
          const length = Math.min(
            RELAY_DATA_CHUNK,
            value.byteLength - offset,
            stream.sendCredit,
            this.#limits.maxPendingBytes - this.#pendingDownlinkBytes
          );
          const payload = value.subarray(offset, offset + length);
          stream.sendCredit -= length;
          stream.pendingDownlinkBytes += length;
          this.#pendingDownlinkBytes += length;
          this.#options.send(encodeFrame(FrameType.Data, id, payload), false);
          offset += length;
        }
      }
    } catch { /* backend failure is represented by CLOSE */ }
    if (!this.#ended && this.#streams.get(id) === streamIdentity) this.#closeStream(id, true, streamIdentity);
  }

  #closeStream(id: number, notify: boolean, expected?: StreamState): void {
    const stream = this.#streams.get(id);
    if (!stream || (expected && stream !== expected)) return;
    this.#streams.delete(id);
    this.#pendingDownlinkBytes -= stream.pendingDownlinkBytes;
    this.#releasePending(stream.pendingWriteCost, stream.pendingWriteItems);
    stream.pendingWriteCost = 0;
    stream.pendingWriteItems = 0;
    stream.pendingWindow = 0;
    stream.writes = [];
    stream.writeHead = 0;
    for (const wake of stream.writeWaiters.splice(0)) wake();
    this.#rememberClosed(id);
    stream.connection.close();
    for (const wake of stream.creditWaiters.splice(0)) wake();
    for (const wake of this.#downlinkWaiters.splice(0)) wake();
    this.#cancelWindowTimerIfIdle();
    if (notify) this.#sendControl(encodeFrame(FrameType.Close, id));
  }

  #sendControl(frame: Uint8Array): void { this.#options.send(frame, true); }

  #background(task: Promise<void>): void {
    void task;
  }

  #releasePending(bytes: number, items: number): void {
    this.#pendingBytes -= bytes;
    this.#pendingItems -= items;
  }

  #cancelWindowTimerIfIdle(): void {
    if (!this.#windowTimer) return;
    for (const stream of this.#streams.values()) if (stream.pendingWindow !== 0) return;
    clearTimeout(this.#windowTimer);
    this.#windowTimer = undefined;
  }

  #rememberClosed(id: number): void {
    if (this.#closed.has(id)) return;
    if (this.#limits.maxClosedStreamIds === 0) return;
    if (this.#closedOrder.length >= this.#limits.maxClosedStreamIds) {
      this.#closed.delete(this.#closedOrder[this.#closedStart]!);
      this.#closedOrder[this.#closedStart] = id;
      this.#closedStart = (this.#closedStart + 1) % this.#closedOrder.length;
    } else {
      this.#closedOrder.push(id);
    }
    this.#closed.add(id);
  }

  async #writeWithDeadline(connection: TelegramConnection, grain: Uint8Array): Promise<void> {
    const timeoutMs = this.#options.writeTimeoutMs ?? 30_000;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        connection.write(grain),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error('Telegram write deadline exceeded')), timeoutMs);
        })
      ]);
    } finally { if (timer) clearTimeout(timer); }
  }

  close(): void {
    if (this.#ended) return;
    this.#ended = true;
    if (this.#windowTimer) clearTimeout(this.#windowTimer);
    this.#windowTimer = undefined;
    for (const stream of this.#streams.values()) {
      stream.connection.close();
      for (const wake of stream.creditWaiters.splice(0)) wake();
      for (const wake of stream.writeWaiters.splice(0)) wake();
    }
    this.#streams.clear();
    this.#options.connector.close?.();
    this.#pendingBytes = 0;
    this.#pendingItems = 0;
    this.#pendingDownlinkBytes = 0;
    for (const wake of this.#downlinkWaiters.splice(0)) wake();
  }
}
