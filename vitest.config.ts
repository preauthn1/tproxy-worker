import { cloudflareTest } from '@cloudflare/vitest-pool-workers';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [cloudflareTest({
    wrangler: { configPath: './wrangler.jsonc' },
    miniflare: {
      bindings: {
        WEB_SECRET: '000102030405060708090a0b0c0d0e0f',
        PUBLIC_HOSTNAME: 'proxy.example.com',
        PUBLIC_SITE_TITLE: 'Example Public Site'
      }
    }
  })],
  test: {
    include: ['test/**/*.test.ts']
  }
});
