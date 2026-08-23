#!/usr/bin/env tsx
import { decodeSecret } from '../src/capability';
import { encodeWebConnectionSecret } from '../src/web-secret';

const [serverName, edgeAddress, rawSecret] = process.argv.slice(2);
if (!serverName || !edgeAddress || !rawSecret) {
  console.error('usage: npm run web-secret -- <server-name> <preferred-edge-host-or-ip> <mt-secret>');
  process.exit(2);
}
const mtSecret = decodeSecret(rawSecret);
try { console.log(encodeWebConnectionSecret({ mtSecret, serverName, edgeAddress })); }
finally { mtSecret.fill(0); }
