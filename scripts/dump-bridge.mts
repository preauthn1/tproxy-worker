// Render the bridge page in both carrier modes and dump the inline scripts.
import { writeFileSync } from 'node:fs';
import { bridgeResponse } from '../src/bridge';

for (const mode of ['websocket', 'websocket-lanes'] as const) {
  const html = await bridgeResponse('proxy.example.com', 'A'.repeat(43), mode).text();
  const script = /<script nonce="[^"]+">([\s\S]*?)<\/script>/.exec(html)?.[1];
  if (!script) throw new Error('no script');
  writeFileSync(`/tmp/bridge-${mode}.js`, script);
  console.log(mode, 'script bytes', script.length);
}
