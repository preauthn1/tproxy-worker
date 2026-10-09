/* SPDX-License-Identifier: GPL-3.0-only */
import { randomToken } from './capability';
import { regionForColo } from './colo-region';

/**
 * Durable Object placement. A Durable Object is created near the first caller
 * unless a location hint is supplied. Without hints every session and the single
 * global bootstrap registry were created in US-East while clients were in Asia,
 * so each relayed byte crossed the Pacific twice before reaching Telegram.
 */
export const REGIONS = ['wnam', 'enam', 'sam', 'weur', 'eeur', 'apac', 'oc', 'afr', 'me'] as const;
export type Region = typeof REGIONS[number];

// One leading token character encodes the regional bootstrap shard. Every
// character is a valid base64url digit, so token syntax and the bridge contract
// are unchanged; 250 random bits remain.
const SHARD_CHARS = 'abcdefghi';
const GLOBAL_CHAR = 'G';

const MIDDLE_EAST = new Set(['AE', 'BH', 'IL', 'IQ', 'IR', 'JO', 'KW', 'LB', 'OM', 'PS', 'QA', 'SA', 'SY', 'TR', 'YE']);
const EASTERN_EUROPE = new Set(['BG', 'BY', 'CZ', 'EE', 'FI', 'HU', 'LT', 'LV', 'MD', 'PL', 'RO', 'RU', 'SK', 'UA', 'GR', 'RS', 'HR', 'SI', 'BA', 'MK', 'AL', 'ME', 'CY']);

interface CfLike { colo?: unknown; continent?: unknown; country?: unknown; longitude?: unknown }

export function regionFor(cf: CfLike | undefined | null): Region | undefined {
  if (!cf) return undefined;
  // Entry colo wins over client geography (see colo-region.ts).
  const byColo = regionForColo(cf.colo);
  if (byColo) return byColo;
  const continent = typeof cf.continent === 'string' ? cf.continent : '';
  const country = typeof cf.country === 'string' ? cf.country : '';
  if (MIDDLE_EAST.has(country)) return 'me';
  switch (continent) {
    case 'AS': return 'apac';
    case 'EU': return EASTERN_EUROPE.has(country) ? 'eeur' : 'weur';
    case 'NA': {
      const longitude = Number(cf.longitude);
      return Number.isFinite(longitude) && longitude < -100 ? 'wnam' : 'enam';
    }
    case 'SA': return 'sam';
    case 'OC': return 'oc';
    case 'AF': return 'afr';
    default: return undefined;
  }
}

export function isRegion(value: string | null | undefined): value is Region {
  return !!value && (REGIONS as readonly string[]).includes(value);
}

export function regionalToken(region: Region | undefined): string {
  const token = randomToken();
  const index = region ? REGIONS.indexOf(region) : -1;
  return (index >= 0 ? SHARD_CHARS[index]! : GLOBAL_CHAR) + token.slice(1);
}

/** Returns the region shard a token was issued from; undefined means the legacy global registry. */
export function tokenRegion(token: string): Region | undefined {
  const index = SHARD_CHARS.indexOf(token[0] ?? '');
  return index >= 0 ? REGIONS[index] : undefined;
}

export function registryStub(namespace: DurableObjectNamespace, region: Region | undefined): DurableObjectStub {
  if (!region) return namespace.get(namespace.idFromName('global'));
  return namespace.get(namespace.idFromName(`region:${region}`), { locationHint: region });
}
