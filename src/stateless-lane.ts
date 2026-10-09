/* SPDX-License-Identifier: GPL-3.0-only */
// Durable-Object-free websocket-lanes backend (idea from ToiCF/CF-Workers-TGProxy):
// every lane is one plain Worker invocation that owns exactly one Telegram stream.
// Unlike that project, it keeps this repo's obfuscated2 termination, direct TCP to
// the DC allowlist with WSS fallback, credit-based backpressure (RelayCore), bounded
// per-lane queues, and long-lived session tokens.
import { decodeSecret } from './capability';
import { diag, errorKind } from './diag';
import { DEFAULT_LIMITS } from './limits';
import { LANE_MAX_PENDING_BYTES, LANE_MAX_PENDING_ITEMS, LaneOverflow, LaneRouter, validateLaneMessage } from './lanes';
import { TelegramConnector } from './mtproxy';
import { RelayCore } from './relay-core';
import { SerializedInboundQueue } from './session-guards';
import { CloudflareTelegramDialer } from './tcp';
import { dialTelegramWss } from './wss';

interface LaneEnv { WEB_SECRET: string; WSS_FALLBACK?: string | undefined }

// Best-effort, isolate-local accounting (a session's lanes may land on different isolates).
const MAX_TRACKED_SESSIONS = 4096;
const MAX_ACTIVE_LANES = 128;
const MAX_USED_LANE_IDS = 4096;
interface Lease { expiresAt: number; active: Set<number>; used: Set<number>; revoked: boolean }
const leases = new Map<string, Lease>();

function lease(token: string, expiresAt: number, now = Date.now()): Lease | null {
  let entry = leases.get(token);
  if (!entry) {
    if (leases.size >= MAX_TRACKED_SESSIONS) {
      for (const [key, value] of leases) if (value.expiresAt <= now || (!value.active.size && value.revoked)) leases.delete(key);
      if (leases.size >= MAX_TRACKED_SESSIONS) for (const [key, value] of leases) { if (!value.active.size) { leases.delete(key); break; } }
      if (leases.size >= MAX_TRACKED_SESSIONS) return null;
    }
    entry = { expiresAt, active: new Set(), used: new Set(), revoked: false };
    leases.set(token, entry);
  }
  return entry;
}

/** DELETE /api/v1/session for stateless sessions: revokes in this isolate and closes its lanes. */
export function revokeStatelessSession(token: string, expiresAt: number): void {
  const entry = lease(token, expiresAt);
  if (entry) entry.revoked = true;
  const sockets = laneSockets.get(token);
  if (sockets) for (const close of sockets) close();
}
const laneSockets = new Map<string, Set<() => void>>();

const MAX_BOOTSTRAP_REDEMPTIONS = 4;
const redeemedBootstraps = new Map<string, { expiresAt: number; count: number }>();
/**
 * Best-effort bootstrap redemption cap within an isolate (a few redemptions are
 * allowed so a client retry after a network error still works). Cross-isolate
 * replay is bounded by the 2-minute bootstrap TTL and the per-host capability.
 */
export function consumeBootstrap(token: string, expiresAt: number): boolean {
  const now = Date.now();
  if (redeemedBootstraps.size > 8192) for (const [key, value] of redeemedBootstraps) if (value.expiresAt <= now) redeemedBootstraps.delete(key);
  const entry = redeemedBootstraps.get(token) ?? { expiresAt, count: 0 };
  if (entry.count >= MAX_BOOTSTRAP_REDEMPTIONS) return false;
  entry.count++;
  redeemedBootstraps.set(token, entry);
  return true;
}

export function statelessLane(env: LaneEnv, token: string, expiresAt: number, streamId: number, protocol: string): Response {
  const entry = lease(token, expiresAt);
  if (!entry) { diag('lane_rejected', { backend: 'stateless', stream: streamId, reason: 'lease_table_full' }); return new Response(null, { status: 503 }); }
  if (entry.revoked || entry.used.has(streamId) || entry.active.size >= MAX_ACTIVE_LANES || entry.used.size >= MAX_USED_LANE_IDS) {
    diag('lane_rejected', { backend: 'stateless', stream: streamId, reason: entry.revoked ? 'revoked' : entry.used.has(streamId) ? 'reused' : 'full', active: entry.active.size });
    return new Response(null, { status: 409 });
  }
  entry.used.add(streamId);
  entry.active.add(streamId);

  const pair = new WebSocketPair();
  const [client, server] = Object.values(pair) as [WebSocket, WebSocket];
  server.accept({ allowHalfOpen: true });
  server.binaryType = 'arraybuffer';
  const openedAt = Date.now();
  let closed = false;
  const router = new LaneRouter(1);
  router.attach(streamId, server);

  const secret = decodeSecret(env.WEB_SECRET);
  let core: RelayCore;
  try {
    core = new RelayCore({
      limits: { ...DEFAULT_LIMITS, maxStreams: 1, maxPendingBytes: LANE_MAX_PENDING_BYTES, maxPendingItems: LANE_MAX_PENDING_ITEMS },
      connector: new TelegramConnector(secret, new CloudflareTelegramDialer(), {
        dialTimeoutMs: 5_000,
        fallbackOnly: env.WSS_FALLBACK === 'force',
        fallback: env.WSS_FALLBACK === '0' ? undefined : (dc, tag, signal) => dialTelegramWss(dc, tag, { timeoutMs: 5_000, signal })
      }),
      send: (batch, control) => {
        try { router.route(batch, control); }
        catch (error) { shutdown(1011, error instanceof LaneOverflow ? 'lane overflow' : 'relay error'); return; }
        if (!router.has(streamId)) shutdown(1000, 'stream closed');
      },
      closeCarrier: () => shutdown(1011, 'carrier closed')
    });
  } finally { secret.fill(0); }

  const expiry = setTimeout(() => shutdown(1001, 'session expired'), Math.max(0, Math.min(expiresAt - Date.now(), 0x7fffffff)));
  const sockets = laneSockets.get(token) ?? new Set<() => void>();
  const closer = () => shutdown(1000, 'session deleted');
  sockets.add(closer);
  laneSockets.set(token, sockets);
  let opened = false;

  const queue = new SerializedInboundQueue<Uint8Array>({
    maxBytes: LANE_MAX_PENDING_BYTES,
    maxItems: LANE_MAX_PENDING_ITEMS,
    size: (value) => value.byteLength + 256,
    handle: async (value) => {
      const error = validateLaneMessage(value, streamId, !opened);
      if (error) { diag('lane_protocol_error', { backend: 'stateless', stream: streamId, error }); shutdown(1002, 'lane protocol error'); return; }
      opened = true;
      await core.receive(value);
      if (!core.hasStream(streamId)) shutdown(1000, 'stream closed');
    }
  });

  function shutdown(code: number, reason: string): void {
    if (closed) return;
    closed = true;
    clearTimeout(expiry);
    queue.clear();
    try { router.closeAll(code, reason); } catch { /* already closed */ }
    try { core.close(); } catch { /* shutdown must continue */ }
    entry!.active.delete(streamId);
    sockets.delete(closer);
    if (!sockets.size) laneSockets.delete(token);
    diag('lane_closed', { backend: 'stateless', stream: streamId, code, reason, lifeMs: Date.now() - openedAt });
  }

  server.addEventListener('message', (event) => {
    if (closed) return;
    if (entry.revoked) { shutdown(1000, 'session deleted'); return; }
    if (typeof event.data === 'string') { shutdown(1003, 'binary messages required'); return; }
    const owned = event.data instanceof ArrayBuffer;
    const bytes = owned
      ? new Uint8Array(event.data as ArrayBuffer)
      : ArrayBuffer.isView(event.data) ? new Uint8Array(event.data.buffer, event.data.byteOffset, event.data.byteLength) : null;
    if (!bytes || bytes.byteLength === 0 || bytes.byteLength > DEFAULT_LIMITS.maxCarrierBatchBytes) { shutdown(1009, 'message limit'); return; }
    if (!queue.push(owned ? bytes : bytes.slice())) shutdown(1009, 'lane queue limit');
  });
  // Close our end explicitly so the invocation can finish (avoids the runtime's "hung" cancellation).
  server.addEventListener('close', (event) => { shutdown(1000, 'peer_close'); try { server.close((event as CloseEvent).code === 1005 ? 1000 : (event as CloseEvent).code || 1000, 'closed'); } catch { /* already closed */ } });
  server.addEventListener('error', (event) => { diag('lane_error', { backend: 'stateless', stream: streamId, error: errorKind((event as ErrorEvent).error) }); shutdown(1011, 'socket error'); });
  diag('lane_open', { backend: 'stateless', stream: streamId, active: entry.active.size });
  return new Response(null, { status: 101, webSocket: client, headers: { 'Sec-WebSocket-Protocol': protocol, 'Sec-WebSocket-Extensions': '' } });
}
