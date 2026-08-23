/* SPDX-License-Identifier: GPL-3.0-only */
import { describe, expect, it } from 'vitest';
import { telegramDcCandidates } from '../src/telegram-dc';

describe('Telegram DC allowlist', () => {
  it('contains bounded direct port-443 candidates for signed DC 1 through 5', () => {
    for (const dc of [1, 2, 3, 4, 5, -1, -2, -3, -4, -5]) {
      const candidates = telegramDcCandidates(dc);
      expect(candidates.length).toBeGreaterThanOrEqual(2);
      expect(candidates.length).toBeLessThanOrEqual(3);
      expect(candidates.every((candidate) => candidate.port === 443 && candidate.hostname.length > 0)).toBe(true);
    }
  });

  it('rejects zero and unknown DC ids without accepting a hostname or port', () => {
    for (const dc of [0, 6, -6, 32767, -32768]) expect(() => telegramDcCandidates(dc)).toThrow(/dc/i);
  });

  it('maps negative production ids to the same bounded absolute-DC list', () => {
    for (const dc of [1, 2, 3, 4, 5]) expect(telegramDcCandidates(-dc)).toEqual(telegramDcCandidates(dc));
  });
});
