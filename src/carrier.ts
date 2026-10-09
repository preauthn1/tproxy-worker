/* SPDX-License-Identifier: GPL-3.0-only */
export type CarrierMode = 'websocket' | 'websocket-lanes';

interface CarrierEnv { CARRIER_MODE?: string | undefined; LANES_BACKEND?: string | undefined }

/** websocket-lanes is the default; set CARRIER_MODE=websocket to fall back to the single multiplexed carrier. */
export function carrierModeOf(env: CarrierEnv): CarrierMode { return env.CARRIER_MODE === 'websocket' ? 'websocket' : 'websocket-lanes'; }

/** Lanes run Durable-Object-free by default; LANES_BACKEND=durable restores the per-session Durable Object. */
export function statelessLanes(env: CarrierEnv): boolean { return carrierModeOf(env) === 'websocket-lanes' && env.LANES_BACKEND !== 'durable'; }
