# Performance work

## Scope and authoritative baseline

This work studies Telegram Desktop commit `49a7f3a87363cdda122a9308df697f14ebee667f`
and `telegramdesktop/tproxy-server` commit
`2873a08806d6e4d84830b9b5c4b0ec0f46af91f8`. Measurements are local Node.js or
Cloudflare workerd evidence only; they are not production throughput claims.

The repository started clean at commit `472ab5d18dde6936db2a670632678448762e0a4e`.

## Baseline findings

- `RelayCore` rejects any `OPEN` whose ID is not globally increasing. Official
  Telegram tracks only active IDs and a bounded closed-ID tombstone set; a fresh
  out-of-order ID is valid, and an evicted tombstone may be reused.
- `WebSocketBatcher` uses 32 KiB for both its pack and direct thresholds. A normal
  64 KiB relay `DATA` frame therefore bypasses aggregation although the carrier
  accepts 2 MiB messages. The initial safe profile selected here is 512 KiB packs
  and a direct threshold above a normal encoded 64 KiB frame.
- The browser bridge validates and copies/transfers every relay frame separately,
  even when a carrier batch contains no `PING`. The official WebSocket bridge
  transfers a validated `ArrayBuffer` as one message.
- `GrainCollector`, `SerializedInboundQueue`, and the tombstone FIFO remove from
  the front of arrays with `shift`/`splice(0)`, producing repeated O(n) element
  movement.
- Downlink currently copies a backend slice, then copies it again in
  `encodeFrame`. A newly allocated encoded frame is already an owned async-boundary
  buffer, so encoding directly from the backend view is sufficient.
- Atomic carrier validation clones every live stream snapshot on every batch.
  Only streams touched by a batch need speculative state.
- Upload grains for independent streams are drained serially. Per-stream order
  must remain serial, but independent streams can drain concurrently and return
  credit only after their own writes finish.
- `StreamingAes256Ctr` intentionally serializes WebCrypto calls to preserve a
  continuous counter and partial-block state. No parallel CTR change is planned
  without a byte-exact proof.
- The official server additionally coalesces adjacent `DATA`, coalesces `WINDOW`,
  batches carrier frames, and reserves capacity for control traffic. This pass
  obtains carrier batching from the larger pack and coalesces upload data through
  `GrainCollector`; more elaborate class-specific reservation is deferred unless
  deterministic tests show the current bounded queue can starve control traffic.

## Design

1. Add deterministic tests for each behavior before implementation and observe
   the expected failure.
2. Use active/tombstoned duplicate checks only. Store tombstones in a fixed-size
   circular order once full.
3. Give array-backed queues a logical head, periodically compact only after a
   bounded amount of consumed storage, and expose test-only-neutral metrics where
   deterministic regression assertions need them.
4. Validate relay batches with a per-batch overlay containing only touched stream
   state. Do not mutate live state until the complete batch and pending budget
   validate.
5. Drain each stream's collector in its own promise and await all drains. Preserve
   order within one stream and return one atomic `WINDOW` per successful drain.
6. Encode downlink directly from the backend read view. `encodeFrame` owns the
   resulting allocation before it crosses the asynchronous batching boundary.
7. Validate an entire bridge message first. If no stream-zero `PING` exists,
   transfer it once; otherwise retain the exact per-frame PING/PONG path.
8. Add `npm run bench` covering frame parse/encode, both queues, WebSocket message
   counts/bytes, AES-CTR representative sizes, and a RelayCore multi-frame path.
   Time is report-only. Deterministic counts are asserted in tests.

## Measurements

`npm run bench` was run five times on Node.js `v22.23.2`, Linux x64. The legacy
queue/batcher rows are exact local models of the replaced `shift`/`splice(0)` and
32 KiB profile. Median throughput is report-only and is not a test threshold.

| Benchmark | Five-run median |
| --- | ---: |
| Frame encode, 64 KiB payload | 3,033.3 MiB/s |
| Frame parse, 64 KiB frame | 9,588.6 MiB/s |
| Legacy `GrainCollector` drain | 223,110 items/s |
| Current head-index `GrainCollector` drain | 2,161,418 items/s |
| Legacy serialized inbound drain | 211,457 items/s |
| Current head-index inbound drain | 1,553,883 items/s |
| AES-CTR, 4 KiB chunks | 23.6 MiB/s |
| AES-CTR, 64 KiB chunks | 124.2 MiB/s |
| AES-CTR, 512 KiB chunks | 199.5 MiB/s |
| RelayCore, 16-frame upload batches | 192,272 frames/s |

The deterministic batching result for 32 encoded normal-size `DATA` frames was
32 WebSocket messages / 2,097,408 bytes with the legacy 32 KiB profile and 5
WebSocket messages / the same 2,097,408 bytes with the 512 KiB profile. The
regression test also asserts that every resulting message is at most 512 KiB.

The queue benchmark suggests approximately 9.8x higher collector drain rate and
7.4x higher serialized inbound drain rate in this Node run. These ratios measure
the synthetic local models, not end-to-end service capacity. WebCrypto results
show why representative chunk size matters, but no CTR concurrency or state
semantics were changed.

Cloudflare workerd evidence comes from the Vitest Worker integration suite and
the Wrangler dry-run gate. It verifies protocol behavior and bundle compatibility;
it does not provide production network, CPU, memory, or Telegram-DC throughput
evidence. No production performance claim is made.
