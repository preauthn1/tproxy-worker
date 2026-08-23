import { decodeSecret, deriveCapability, validateHostname } from '../src/capability';

const [hostname, secret] = process.argv.slice(2);
if (!hostname || !secret) {
  console.error('usage: npm run capability -- <canonical-hostname> <secret>');
  process.exitCode = 2;
} else {
  validateHostname(hostname);
  console.log(await deriveCapability(hostname, decodeSecret(secret)));
}
