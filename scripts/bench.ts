/* SPDX-License-Identifier: GPL-3.0-only */
import { performance } from 'node:perf_hooks';
import { StreamingAes256Ctr } from '../src/aes-ctr';
import { FrameType, encodeFrame, parseClientBatch } from '../src/frame';
import { GrainCollector } from '../src/grain';
import { RelayCore, type TelegramConnection } from '../src/relay-core';
import { SerializedInboundQueue } from '../src/session-guards';
import { DEFAULT_WEBSOCKET_BATCHER_OPTIONS, WebSocketBatcher } from '../src/ws-batcher';

interface TimedResult { label: string; milliseconds: number; units: number; unit: string }

class LegacyGrainCollector {
  readonly #capacity: number;
  #queue: Uint8Array[] = [];
  constructor(capacity: number) { this.#capacity = capacity; }
  push(value: Uint8Array): void { this.#queue.push(value); }
  take(): Uint8Array | null {
    const first = this.#queue.shift();
    if (!first) return null;
    if (first.byteLength >= this.#capacity || this.#queue.length === 0) return first;
    let total = first.byteLength;
    let count = 0;
    while (count < this.#queue.length && total + this.#queue[count]!.byteLength <= this.#capacity) total += this.#queue[count++]!.byteLength;
    if (!count) return first;
    const result = new Uint8Array(total);
    result.set(first);
    let offset = first.byteLength;
    for (const value of this.#queue.splice(0, count)) { result.set(value, offset); offset += value.byteLength; }
    return result;
  }
}

class LegacySerializedQueue<T> {
  readonly #values: T[] = [];
  readonly #handle: (value: T) => void;
  constructor(handle: (value: T) => void) { this.#handle = handle; }
  push(value: T): void { this.#values.push(value); }
  drain(): void { for (let value = this.#values.shift(); value !== undefined; value = this.#values.shift()) this.#handle(value); }
}

function timed(label: string, units: number, unit: string, run: () => void): TimedResult {
  const start = performance.now();
  run();
  return { label, milliseconds: performance.now() - start, units, unit };
}

async function timedAsync(label: string, units: number, unit: string, run: () => Promise<void>): Promise<TimedResult> {
  const start = performance.now();
  await run();
  return { label, milliseconds: performance.now() - start, units, unit };
}

function frameBenchmarks(): TimedResult[] {
  const payload = new Uint8Array(64 * 1024);
  const encoded = encodeFrame(FrameType.Data, 1, payload);
  const iterations = 2_000;
  return [
    timed('frame encode 64KiB', iterations * payload.byteLength, 'bytes', () => {
      for (let index = 0; index < iterations; index++) encodeFrame(FrameType.Data, 1, payload);
    }),
    timed('frame parse 64KiB', iterations * encoded.byteLength, 'bytes', () => {
      for (let index = 0; index < iterations; index++) parseClientBatch(encoded);
    })
  ];
}

function grainBenchmarks(): TimedResult[] {
  const items = 50_000;
  const values = Array.from({ length: items }, () => new Uint8Array(32));
  return [
    timed('legacy GrainCollector drain', items, 'items', () => {
      const collector = new LegacyGrainCollector(32);
      for (const value of values) collector.push(value);
      while (collector.take()) { /* drain */ }
    }),
    timed('current GrainCollector drain', items, 'items', () => {
      const collector = new GrainCollector(32);
      for (const value of values) collector.push(value);
      while (collector.take()) { /* drain */ }
    })
  ];
}

async function inboundBenchmarks(): Promise<TimedResult[]> {
  const items = 50_000;
  let checksum = 0;
  const legacy = timed('legacy inbound queue drain', items, 'items', () => {
    const queue = new LegacySerializedQueue<number>((value) => { checksum += value; });
    for (let index = 0; index < items; index++) queue.push(index);
    queue.drain();
  });
  const current = await timedAsync('current inbound queue drain', items, 'items', async () => {
    const queue = new SerializedInboundQueue<number>({
      maxBytes: items, maxItems: items, size: () => 1,
      handle: async (value) => { checksum += value; }
    });
    for (let index = 0; index < items; index++) {
      if (!queue.push(index)) throw new Error('benchmark queue overflow');
    }
    await queue.drained();
  });
  if (!Number.isSafeInteger(checksum)) throw new Error('invalid benchmark checksum');
  return [legacy, current];
}

function batcherMetrics(): Array<Record<string, number | string>> {
  const frames = Array.from({ length: 32 }, () => new Uint8Array(64 * 1024 + 8));
  const profiles = [
    { label: 'legacy 32KiB profile', packBytes: 32 * 1024, directBytes: 32 * 1024, delayMs: 1 },
    { label: 'current 512KiB profile', ...DEFAULT_WEBSOCKET_BATCHER_OPTIONS }
  ];
  return profiles.map((profile) => {
    const output: Uint8Array[] = [];
    const batcher = new WebSocketBatcher((value) => output.push(value), profile);
    for (const frame of frames) batcher.send(frame);
    batcher.flush();
    return {
      label: profile.label,
      inputFrames: frames.length,
      websocketMessages: output.length,
      bytes: output.reduce((sum, value) => sum + value.byteLength, 0)
    };
  });
}

async function aesBenchmarks(): Promise<TimedResult[]> {
  const results: TimedResult[] = [];
  for (const size of [4 * 1024, 64 * 1024, 512 * 1024]) {
    const iterations = Math.max(8, Math.floor((32 * 1024 * 1024) / size));
    const cipher = await StreamingAes256Ctr.create(new Uint8Array(32), new Uint8Array(16));
    const input = new Uint8Array(size);
    results.push(await timedAsync(`AES-CTR ${size / 1024}KiB chunks`, iterations * size, 'bytes', async () => {
      for (let index = 0; index < iterations; index++) await cipher.transform(input);
    }));
    cipher.close();
  }
  return results;
}

async function relayBenchmark(): Promise<TimedResult> {
  class Connection implements TelegramConnection {
    async write(data: Uint8Array): Promise<void> { void data; }
    async *read(): AsyncIterable<Uint8Array> {
      const pending: Uint8Array[] = [];
      for (const value of pending) yield value;
      await new Promise<void>(() => undefined);
    }
    close(): void { /* benchmark no-op */ }
  }
  const core = new RelayCore({ connector: { open: () => new Connection() }, send: () => undefined, closeCarrier: () => undefined });
  await core.receive(encodeFrame(FrameType.Open, 1));
  const frame = encodeFrame(FrameType.Data, 1, new Uint8Array(4096));
  const batch = new Uint8Array(frame.byteLength * 16);
  for (let index = 0; index < 16; index++) batch.set(frame, index * frame.byteLength);
  const iterations = 60;
  const result = await timedAsync('RelayCore 16-frame upload batches', iterations * 16, 'frames', async () => {
    for (let index = 0; index < iterations; index++) await core.receive(batch);
  });
  core.close();
  return result;
}

async function relayHolMetrics(): Promise<Record<string, number | string>> {
  class Connection implements TelegramConnection {
    writes: Uint8Array[] = [];
    release: (() => void) | undefined;
    readonly blockFirst: boolean;
    constructor(blockFirst = false) { this.blockFirst = blockFirst; }
    async write(data: Uint8Array): Promise<void> {
      this.writes.push(data.slice());
      if (this.blockFirst && this.writes.length === 1) await new Promise<void>((resolve) => { this.release = resolve; });
    }
    async *read(): AsyncIterable<Uint8Array> {
      const pending: Uint8Array[] = [];
      for (const value of pending) yield value;
      await new Promise<void>(() => undefined);
    }
    close(): void { this.release?.(); }
  }
  const first = new Connection(true);
  const second = new Connection();
  const connections = [first, second];
  const core = new RelayCore({ connector: { open: () => connections.shift()! }, send: () => undefined, closeCarrier: () => undefined });
  const start = performance.now();
  let completedReceives = 0;
  await core.receive(new Uint8Array([
    ...encodeFrame(FrameType.Open, 1),
    ...encodeFrame(FrameType.Data, 1, Uint8Array.of(1))
  ]));
  completedReceives++;
  await core.receive(new Uint8Array([
    ...encodeFrame(FrameType.Data, 1, Uint8Array.of(2)),
    ...encodeFrame(FrameType.Open, 2),
    ...encodeFrame(FrameType.Data, 2, Uint8Array.of(3))
  ]));
  completedReceives++;
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
  const blockedStreamWritesBeforeRelease = first.writes.length;
  const independentStreamWritesBeforeRelease = second.writes.length;
  first.release?.();
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
  const orderedChecksum = first.writes.reduce((sum, value, index) => sum + (index + 1) * (value[0] ?? 0), 0);
  const milliseconds = performance.now() - start;
  core.close();
  return {
    label: 'RelayCore cross-message HOL',
    milliseconds: Number(milliseconds.toFixed(2)),
    completedReceivesBeforeRelease: completedReceives,
    blockedStreamWritesBeforeRelease,
    independentStreamWritesBeforeRelease,
    blockedStreamWritesAfterRelease: first.writes.length,
    orderedChecksum
  };
}

function printTimed(result: TimedResult): void {
  const perSecond = result.units / (result.milliseconds / 1000);
  const rate = result.unit === 'bytes' ? `${(perSecond / (1024 * 1024)).toFixed(1)} MiB/s` : `${Math.round(perSecond)} ${result.unit}/s`;
  process.stdout.write(`${result.label}: ${result.milliseconds.toFixed(2)} ms, ${rate}\n`);
}

async function main(): Promise<void> {
  process.stdout.write(`runtime: ${process.version} ${process.platform}/${process.arch}\n`);
  for (const result of frameBenchmarks()) printTimed(result);
  for (const result of grainBenchmarks()) printTimed(result);
  for (const result of await inboundBenchmarks()) printTimed(result);
  for (const metric of batcherMetrics()) process.stdout.write(`${JSON.stringify(metric)}\n`);
  for (const result of await aesBenchmarks()) printTimed(result);
  printTimed(await relayBenchmark());
  process.stdout.write(`${JSON.stringify(await relayHolMetrics())}\n`);
}

await main();
