/* SPDX-License-Identifier: GPL-3.0-only */

export interface TelegramEndpoint { hostname: string; port: 443 }

// Telegram Desktop built-in production DC front doors, pinned from
// telegramdesktop/tdesktop f3b9a109c7de09fb8ce9d25c699851742b30f8b2.
// IPv6 is the independently-addressed second candidate for each DC. DC2 also
// carries Telegram Desktop's second built-in IPv4 endpoint.
const ENDPOINTS: Readonly<Record<number, readonly TelegramEndpoint[]>> = Object.freeze({
  1: Object.freeze([
    { hostname: '149.154.175.50', port: 443 as const },
    { hostname: '2001:0b28:f23d:f001:0000:0000:0000:000a', port: 443 as const }
  ]),
  2: Object.freeze([
    { hostname: '149.154.167.51', port: 443 as const },
    { hostname: '95.161.76.100', port: 443 as const },
    { hostname: '2001:067c:04e8:f002:0000:0000:0000:000a', port: 443 as const }
  ]),
  3: Object.freeze([
    { hostname: '149.154.175.100', port: 443 as const },
    { hostname: '2001:0b28:f23d:f003:0000:0000:0000:000a', port: 443 as const }
  ]),
  4: Object.freeze([
    { hostname: '149.154.167.91', port: 443 as const },
    { hostname: '2001:067c:04e8:f004:0000:0000:0000:000a', port: 443 as const }
  ]),
  5: Object.freeze([
    { hostname: '149.154.171.5', port: 443 as const },
    { hostname: '2001:0b28:f23f:f005:0000:0000:0000:000a', port: 443 as const }
  ])
});

export function telegramDcCandidates(signedDc: number): readonly TelegramEndpoint[] {
  if (!Number.isInteger(signedDc) || signedDc === 0) throw new Error('invalid Telegram DC id');
  const candidates = ENDPOINTS[Math.abs(signedDc)];
  if (!candidates) throw new Error('unknown Telegram DC id');
  // Telegram Desktop uses a negative protocol DC id for media/file sessions.
  // Its built-in direct table does not publish separate media-only addresses,
  // so negative ids deliberately use the same absolute-DC allowlist.
  return candidates;
}
