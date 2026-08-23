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
- Phase 1 changed one `receive()` call to `Promise.all` its per-stream upload
  drains. That removed only intra-batch serialization. `RelaySession` still
  awaited `receive()`, so a backend write blocked every later WebSocket message,
  including `WINDOW`, `CLOSE`, and unrelated-stream `DATA`.
- `StreamingAes256Ctr` intentionally serializes WebCrypto calls to preserve a
  continuous counter and partial-block state. No parallel CTR change is planned
  without a byte-exact proof.
- Desktop flushes receive credit at 256 KiB or 20 ms, limits one synchronous
  stream-flush turn to 256 frames, and keeps 64 KiB / 64 items of local carrier
  capacity for control. The server also accounts queued backend writes until
  completion and releases them on stream close.

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
5. Give each stream an owned, bounded backend-write queue and one long-lived
   writer pump that sleeps on an explicit wake queue when idle. `receive()`
   atomically validates/reserves and copies accepted `DATA` into those
   queues, applies `OPEN`/`WINDOW`/`CLOSE`, and returns without awaiting a backend
   write. Preserve exact same-stream order across carrier messages. A close drops
   queued byte/item charges and identity-checks prevent a stale completion from
   mutating a reused stream. The relay stays on the standard non-hibernating
   WebSocket API because its live TCP/cipher state is in memory; Durable Object
   `waitUntil()` is not used as a lifetime mechanism because it has no effect in
   Durable Objects.
6. Return receive credit only for successful backend writes. Coalesce it per
   stream at 256 KiB or 20 ms, cancel it on stream/session close, and split only
   above the `uint32` frame limit.
7. Close only the failed stream on a backend write deadline. Phase 1 closed the
   whole carrier, but neither the frame contract nor the reference server
   requires an unrelated-stream failure for one backend write timeout.
8. Preserve carrier control headroom: relay `WINDOW`/`CLOSE` and session `PING`
   use the control class, while `DATA` cannot use the final 64 KiB / 64 items.
9. Yield a hot writer pump after 256 completed items. This bounds a synchronous
   microtask turn without relaxing per-stream order.
10. Encode downlink directly from the backend read view. `encodeFrame` owns the
   resulting allocation before it crosses the asynchronous batching boundary.
11. Validate an entire bridge message first. If no stream-zero `PING` exists,
   transfer it once; otherwise retain the exact per-frame PING/PONG path.
12. Keep the standard Durable Object WebSocket API for the lifetime of the live
   TCP relay. Do not enable WebSocket hibernation for an in-memory relay whose
   TCP sockets, ciphers, credits, queues, and timers cannot be reconstructed after
   eviction. The non-hibernating socket also keeps delayed batching in the same
   live object context.
13. Extend `npm run bench` with cross-message HOL counts: both `receive()` calls
   complete while stream 1 is blocked, stream 2 writes before release, and stream
   1 completes in order afterward. Timing is report-only and has no threshold.

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
This is a generic `WebSocketBatcher` measurement. The same profile is used by
the non-hibernating Durable Object relay, but the count remains a deterministic
local batching result rather than a production-network throughput claim.

The queue benchmark suggests approximately 9.8x higher collector drain rate and
7.4x higher serialized inbound drain rate in this Node run. These ratios measure
the synthetic local models, not end-to-end service capacity. WebCrypto results
show why representative chunk size matters, but no CTR concurrency or state
semantics were changed.

Cloudflare workerd evidence comes from the Vitest Worker integration suite and
the Wrangler dry-run gate. It verifies protocol behavior and bundle compatibility;
it does not provide production network, CPU, memory, or Telegram-DC throughput
evidence. No production performance claim is made.

## Phase 2 verification targets

- Cross-message HOL: a blocked stream-1 write does not hold a later carrier
  message containing stream-1 `WINDOW`/`CLOSE` and stream-2 `DATA`.
- Ordering: separate carrier messages remain exactly ordered within one stream.
- Accounting: queued and in-flight upload cost is released on `CLOSE`; stale
  completion cannot emit `WINDOW` or alter a tombstone-evicted/reused stream.
- Credit: `WINDOW` totals never exceed bytes actually written, and pending credit
  is canceled on stream/session close.
- Backpressure: each stream has its own 8 MiB / 1024-item cap, within the session
  cap, and carrier DATA cannot consume the control reserve.

## Phase 2 measurements

After the writer-pump change, `npm run bench` was run five times on Node.js
`v22.23.2`, Linux x64. Medians remain report-only.

| Benchmark | Five-run median |
| --- | ---: |
| Frame encode, 64 KiB payload | 1,904.8 MiB/s |
| Frame parse, 64 KiB frame | 9,213.8 MiB/s |
| Legacy `GrainCollector` drain | 230,710 items/s |
| Current head-index `GrainCollector` drain | 2,071,530 items/s |
| Legacy serialized inbound drain | 249,009 items/s |
| Current head-index inbound drain | 1,810,039 items/s |
| AES-CTR, 4 KiB chunks | 24.2 MiB/s |
| AES-CTR, 64 KiB chunks | 146.3 MiB/s |
| AES-CTR, 512 KiB chunks | 215.2 MiB/s |
| RelayCore, 16-frame upload batches | 149,212 frames/s |
| Cross-message HOL elapsed | 3.04 ms |

All five HOL runs reported the same semantic counts: two `receive()` calls
completed before releasing stream 1; stream 1 had one blocked write; stream 2
completed one write; after release stream 1 had two writes with ordered checksum
5 (`1*1 + 2*2`). The elapsed value is diagnostic only and has no pass/fail
threshold. The upload benchmark was reduced from 500 to 60 batches so its total
input stays within the protocol's initial 4 MiB receive credit; it does not
synthetically grant credit that the peer did not receive.
