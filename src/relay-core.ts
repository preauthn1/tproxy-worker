import { FrameType, INITIAL_STREAM_CREDIT, RELAY_DATA_CHUNK, encodeFrame, parseClientBatch, windowAmount, windowPayload, type Frame } from './frame';
import { GrainCollector } from './grain';
import { DEFAULT_LIMITS, QUEUE_ITEM_COST, type RelayLimits } from './limits';

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
}

interface RelayCoreOptions {
  limits?: RelayLimits;
  connector: TelegramConnectorLike;
  send(batch: Uint8Array): void;
  closeCarrier(): void;
  writeTimeoutMs?: number;
}

interface Snapshot { receiveCredit: number; sendCredit: number }

export class RelayCore {
  readonly #options: RelayCoreOptions;
  readonly #limits: RelayLimits;
  readonly #streams = new Map<number, StreamState>();
  readonly #closed = new Set<number>();
  readonly #closedOrder: number[] = [];
  #maxSeenStreamId = 0;
  #pendingBytes = 0;
  #pendingItems = 0;
  #pendingDownlinkBytes = 0;
  readonly #downlinkWaiters: Array<() => void> = [];
  #ended = false;

  constructor(options: RelayCoreOptions) {
    this.#options = options;
    this.#limits = options.limits ?? DEFAULT_LIMITS;
  }

  async receive(batch: Uint8Array): Promise<void> {
    if (this.#ended) throw new Error('session closed');
    const frames = parseClientBatch(batch);
    const reservation = this.#validateAndReserve(frames);
    this.#pendingBytes += reservation.bytes;
    this.#pendingItems += reservation.items;
    try { await this.#apply(frames); }
    finally {
      this.#pendingBytes -= reservation.bytes;
      this.#pendingItems -= reservation.items;
    }
  }

  #validateAndReserve(frames: Frame[]): { bytes: number; items: number } {
    const live = new Map<number, Snapshot>();
    for (const [id, stream] of this.#streams) live.set(id, { receiveCredit: stream.receiveCredit, sendCredit: stream.sendCredit });
    const closedInBatch = new Set<number>();
    let maxSeen = this.#maxSeenStreamId;
    let bytes = 0;
    let items = 0;
    for (const frame of frames) {
      if (frame.streamId === 0) continue;
      const previous = live.get(frame.streamId);
      const closed = this.#closed.has(frame.streamId) || closedInBatch.has(frame.streamId);
      if (frame.type === FrameType.Open) {
        if (previous || closed || frame.streamId <= maxSeen) throw new Error('stream id reuse');
        maxSeen = frame.streamId;
        live.set(frame.streamId, { receiveCredit: INITIAL_STREAM_CREDIT, sendCredit: INITIAL_STREAM_CREDIT });
      } else if (frame.type === FrameType.Data) {
        if (closed) continue;
        if (!previous) throw new Error('DATA for unknown stream');
        if (frame.payload.byteLength > previous.receiveCredit) throw new Error('DATA beyond receive credit');
        previous.receiveCredit -= frame.payload.byteLength;
        bytes += frame.payload.byteLength + QUEUE_ITEM_COST;
        items++;
      } else if (frame.type === FrameType.Window) {
        if (closed) continue;
        if (!previous) throw new Error('WINDOW for unknown stream');
        previous.sendCredit = Math.min(0xffffffff, previous.sendCredit + windowAmount(frame.payload));
      } else if (frame.type === FrameType.Close) {
        if (closed) continue;
        if (!previous) throw new Error('CLOSE for unknown stream');
        live.delete(frame.streamId);
        closedInBatch.add(frame.streamId);
      }
    }
    if (this.#pendingBytes + bytes > this.#limits.maxPendingBytes || this.#pendingItems + items > this.#limits.maxPendingItems) {
      throw new Error('pending queue overflow');
    }
    return { bytes, items };
  }

  async #apply(frames: Frame[]): Promise<void> {
    const uploads = new Map<number, GrainCollector>();
    for (const frame of frames) {
      if (frame.streamId === 0) continue;
      if (frame.type === FrameType.Open) {
        this.#maxSeenStreamId = frame.streamId;
        if (this.#streams.size >= this.#limits.maxStreams) {
          this.#rememberClosed(frame.streamId);
          this.#options.send(encodeFrame(FrameType.Close, frame.streamId));
          continue;
        }
        let connection: TelegramConnection;
        try { connection = this.#options.connector.open(); }
        catch {
          this.#rememberClosed(frame.streamId);
          this.#options.send(encodeFrame(FrameType.Close, frame.streamId));
          continue;
        }
        this.#streams.set(frame.streamId, {
          connection, receiveCredit: INITIAL_STREAM_CREDIT, sendCredit: INITIAL_STREAM_CREDIT,
          creditWaiters: [], pendingDownlinkBytes: 0
        });
        void this.#pumpBackend(frame.streamId, connection);
      } else if (frame.type === FrameType.Data) {
        const stream = this.#streams.get(frame.streamId);
        if (!stream) continue;
        stream.receiveCredit -= frame.payload.byteLength;
        let collector = uploads.get(frame.streamId);
        if (!collector) { collector = new GrainCollector(RELAY_DATA_CHUNK); uploads.set(frame.streamId, collector); }
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
      let drained = 0;
      try {
        for (let grain = collector.take(); grain; grain = collector.take()) {
          await this.#writeWithDeadline(stream.connection, grain);
          drained += grain.byteLength;
        }
      } catch { this.#closeStream(id, true); continue; }
      if (drained > 0 && this.#streams.has(id)) {
        stream.receiveCredit += drained;
        const encoded = encodeFrame(FrameType.Window, id, windowPayload(drained));
        const output = new Uint8Array(encoded.byteLength);
        output.set(encoded);
        this.#options.send(output);
      }
    }
  }

  async #pumpBackend(id: number, connection: TelegramConnection): Promise<void> {
    try {
      for await (const value of connection.read()) {
        if (this.#ended || this.#streams.get(id)?.connection !== connection) return;
        let offset = 0;
        while (offset < value.byteLength) {
          let stream = this.#streams.get(id);
          if (!stream) return;
          while (stream.sendCredit === 0) {
            await new Promise<void>((resolve) => stream!.creditWaiters.push(resolve));
            stream = this.#streams.get(id);
            if (!stream) return;
          }
          while (this.#pendingDownlinkBytes >= this.#limits.maxPendingBytes) {
            await new Promise<void>((resolve) => this.#downlinkWaiters.push(resolve));
            stream = this.#streams.get(id);
            if (!stream) return;
          }
          const length = Math.min(
            RELAY_DATA_CHUNK,
            value.byteLength - offset,
            stream.sendCredit,
            this.#limits.maxPendingBytes - this.#pendingDownlinkBytes
          );
          const payload = value.subarray(offset, offset + length).slice();
          stream.sendCredit -= length;
          stream.pendingDownlinkBytes += length;
          this.#pendingDownlinkBytes += length;
          this.#options.send(encodeFrame(FrameType.Data, id, payload));
          offset += length;
        }
      }
    } catch { /* backend failure is represented by CLOSE */ }
    if (!this.#ended && this.#streams.get(id)?.connection === connection) this.#closeStream(id, true);
  }

  #closeStream(id: number, notify: boolean): void {
    const stream = this.#streams.get(id);
    if (!stream) return;
    this.#streams.delete(id);
    this.#pendingDownlinkBytes -= stream.pendingDownlinkBytes;
    this.#rememberClosed(id);
    stream.connection.close();
    for (const wake of stream.creditWaiters.splice(0)) wake();
    for (const wake of this.#downlinkWaiters.splice(0)) wake();
    if (notify) this.#options.send(encodeFrame(FrameType.Close, id));
  }

  #rememberClosed(id: number): void {
    if (this.#closed.has(id)) return;
    if (this.#closedOrder.length >= this.#limits.maxClosedStreamIds) this.#closed.delete(this.#closedOrder.shift()!);
    this.#closed.add(id);
    this.#closedOrder.push(id);
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
    } catch (error) {
      if (error instanceof Error && /deadline/.test(error.message)) {
        this.close();
        this.#options.closeCarrier();
      }
      throw error;
    } finally { if (timer) clearTimeout(timer); }
  }

  close(): void {
    if (this.#ended) return;
    this.#ended = true;
    for (const stream of this.#streams.values()) {
      stream.connection.close();
      for (const wake of stream.creditWaiters.splice(0)) wake();
    }
    this.#streams.clear();
    this.#options.connector.close?.();
    this.#pendingDownlinkBytes = 0;
    for (const wake of this.#downlinkWaiters.splice(0)) wake();
  }
}
