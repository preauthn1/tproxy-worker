import { describe, expect, it } from 'vitest';
import { decodeSecret, deriveCapability } from '../src/capability';

describe('bridge capability', () => {
  it.each([
    ['000102030405060708090a0b0c0d0e0f', 'MHLEY5PmW1GWqJkSrlmJpvJUiLhBH_QKy6yKg8a0JPk'],
    ['dd000102030405060708090a0b0c0d0e0f', 'IpJrt3e7sKtzPyoXy6w-Zj6GGEvsvclN66JzQEfPYLA']
  ])('matches the normative vector for %s', async (secret, expected) => {
    await expect(deriveCapability('proxy.example.com', decodeSecret(secret))).resolves.toBe(expected);
  });

  it('rejects a 17-byte secret without the dd prefix', () => {
    expect(() => decodeSecret('aa000102030405060708090a0b0c0d0e0f')).toThrow();
  });
});
