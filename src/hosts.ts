/* SPDX-License-Identifier: GPL-3.0-only */
import { validateHostname } from './capability';

export interface HostEnv {
  PUBLIC_HOSTNAME?: string | undefined;
  PUBLIC_HOSTNAMES?: string | undefined;
  ALLOW_ANY_HOST?: string | undefined;
}

function configuredHosts(env: HostEnv): string[] {
  return [env.PUBLIC_HOSTNAME ?? '', ...(env.PUBLIC_HOSTNAMES ?? '').split(',')]
    .map((value) => value.trim().toLowerCase().replace(/\.$/, ''))
    .filter(Boolean);
}

/**
 * Multi-host support (idea from ToiCF/CF-Workers-TGProxy): the capability,
 * bridge page and tokens are bound to the hostname the client actually used.
 * - PUBLIC_HOSTNAME and/or PUBLIC_HOSTNAMES (comma list) set: only those hosts are served.
 * - none configured, or ALLOW_ANY_HOST=1: any canonical request host is served; the
 *   per-host HMAC capability is still required, so an unknown host learns nothing.
 */
export function servedHost(request: Request, env: HostEnv): string | null {
  const host = new URL(request.url).hostname.toLowerCase();
  try { validateHostname(host); } catch { return null; }
  if (/^[0-9.]+$/.test(host) || !/[a-z]/.test(host.split('.').at(-1) ?? '')) return null;
  const allowed = configuredHosts(env);
  if (!allowed.length || env.ALLOW_ANY_HOST === '1' || env.ALLOW_ANY_HOST === 'true') return host;
  return allowed.includes(host) ? host : null;
}

/** Same-origin guard for browser-initiated API calls (an absent Origin header is allowed for native adapters). */
export function sameOrigin(request: Request, host: string): boolean {
  const origin = request.headers.get('Origin');
  return origin === null || origin === `https://${host}`;
}
