/* SPDX-License-Identifier: GPL-3.0-only */
import type { Region } from './region';

// Durable Object placement should follow the Cloudflare colo the client actually
// enters through, not the client's country: carriers often route mainland-China
// traffic to FRA/LAX/SJC, and pinning those sessions to `apac` doubled every byte's
// path (FRA -> KIX -> Telegram DC -> KIX -> FRA). Unknown colos fall back to geo.
const COLO_REGION: Readonly<Record<string, Region>> = Object.freeze({
  // Western Europe
  FRA: 'weur', AMS: 'weur', LHR: 'weur', CDG: 'weur', MAD: 'weur', LIS: 'weur', MRS: 'weur', MXP: 'weur', ZRH: 'weur',
  DUS: 'weur', HAM: 'weur', MUC: 'weur', BRU: 'weur', DUB: 'weur', MAN: 'weur', CPH: 'weur', ARN: 'weur', OSL: 'weur',
  VIE: 'weur', FCO: 'weur', BCN: 'weur', STR: 'weur', TXL: 'weur', BER: 'weur', LUX: 'weur', GVA: 'weur', EDI: 'weur',
  // Eastern Europe
  WAW: 'eeur', PRG: 'eeur', BUD: 'eeur', OTP: 'eeur', SOF: 'eeur', HEL: 'eeur', RIX: 'eeur', TLL: 'eeur', VNO: 'eeur', KIV: 'eeur', KBP: 'eeur', ATH: 'eeur', BEG: 'eeur', ZAG: 'eeur',
  // Asia-Pacific
  HKG: 'apac', NRT: 'apac', KIX: 'apac', ICN: 'apac', SIN: 'apac', TPE: 'apac', BKK: 'apac', KUL: 'apac', MNL: 'apac', CGK: 'apac', SGN: 'apac', HAN: 'apac',
  BOM: 'apac', DEL: 'apac', MAA: 'apac', BLR: 'apac', HYD: 'apac', CCU: 'apac', FUK: 'apac', OKA: 'apac', MFM: 'apac',
  // Oceania
  SYD: 'oc', MEL: 'oc', BNE: 'oc', PER: 'oc', AKL: 'oc', ADL: 'oc',
  // North America
  LAX: 'wnam', SJC: 'wnam', SEA: 'wnam', PDX: 'wnam', SFO: 'wnam', LAS: 'wnam', PHX: 'wnam', DEN: 'wnam', SLC: 'wnam', YVR: 'wnam',
  IAD: 'enam', EWR: 'enam', JFK: 'enam', ORD: 'enam', ATL: 'enam', MIA: 'enam', DFW: 'enam', BOS: 'enam', YYZ: 'enam', YUL: 'enam', IAH: 'enam', MSP: 'enam', CLT: 'enam',
  // South America, Africa, Middle East
  GRU: 'sam', GIG: 'sam', EZE: 'sam', SCL: 'sam', BOG: 'sam', LIM: 'sam',
  JNB: 'afr', CPT: 'afr', LOS: 'afr', NBO: 'afr', CAI: 'afr',
  DXB: 'me', DOH: 'me', TLV: 'me', BAH: 'me', KWI: 'me', RUH: 'me', JED: 'me', AMM: 'me', IST: 'me'
});

export function regionForColo(colo: unknown): Region | undefined {
  return typeof colo === 'string' ? COLO_REGION[colo.toUpperCase()] : undefined;
}
