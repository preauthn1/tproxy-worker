/* SPDX-License-Identifier: GPL-3.0-only */
export type CarrierMode = 'websocket' | 'websocket-lanes';

interface CarrierEnv { CARRIER_MODE?: string | undefined; LANES_BACKEND?: string | undefined }

/** websocket-lanes is the default; set CARRIER_MODE=websocket to fall back to the single multiplexed carrier. */
export function carrierModeOf(env: CarrierEnv): CarrierMode { return env.CARRIER_MODE === 'websocket' ? 'websocket' : 'websocket-lanes'; }

/**
 * Lanes use the per-session Durable Object by default. LANES_BACKEND=stateless runs each lane as a
 * plain Worker invocation (no DO). Measured 2026-10 on a Workers Free account: stateless lanes were
 * load-shed after 60-180 s while DO lanes held >5 min, so stateless is opt-in and needs Workers Paid.
 */
export function statelessLanes(env: CarrierEnv): boolean { return carrierModeOf(env) === 'websocket-lanes' && env.LANES_BACKEND === 'stateless'; }
