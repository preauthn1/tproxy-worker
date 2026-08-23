# Security policy and threat model

## Security boundary

This Worker is a WEB transport relay only. It does not parse or implement MTProxy. `DATA` payloads are opaque bytes produced after the Telegram client's normal MTProxy transform.

Every `OPEN` connects to exactly `BACKEND_HOST:BACKEND_PORT` from Worker configuration. The shared frame protocol contains no destination field, and no request header, query, WebSocket subprotocol suffix, or DATA prefix can override the backend. This prevents the deployment from becoming a general-purpose TCP proxy.

Bridge, bootstrap, and session values are bearer capabilities:

- The bridge capability is HMAC-SHA256 over the canonical public hostname with `WEB_SECRET`.
- A bridge response mints a cryptographically random 32-byte bootstrap token with a two-minute lifetime.
- A valid `HELLO` atomically exchanges the bootstrap for a cryptographically random 32-byte session token.
- Tokens are intentionally not IP-bound. `CF-Connecting-IP` is retained only as accounting metadata, matching the upstream protocol.
- Unknown, malformed, expired, or wrong credentials receive the same ordinary public 404 page as an unknown path.

Do not log request headers, full URLs, query strings, authorization headers, WebSocket subprotocols, bridge HTML, tokens, secrets, or DATA. Cloudflare observability should be configured to exclude sensitive header and query values. Treat access to Worker/DO request traces as sensitive.

## Resource and protocol controls

Defaults are explicit and sized for a 128 MiB isolate:

- 16 live streams per session.
- 4 MiB initial credit in each direction per stream.
- 1 MiB maximum shared-frame payload.
- 64 KiB maximum relay DATA chunk.
- 2 MiB maximum WebSocket carrier message and 4096 frames per message.
- 12 MiB charged pending data and 8192 pending items per session.
- 4096 recently closed stream IDs retained as tombstones.
- Encoded queued bytes are charged with an additional conservative 256 bytes per item.

Malformed frames, wrong-direction frames, stream reuse, DATA beyond credit, oversized carrier messages, text messages, and queue overflow terminate the affected WebSocket session. An `OPEN` above the stream limit receives stream-level `CLOSE`, preserving other streams.

The implementation serializes inbound WebSocket messages. Upload credit is returned only after `socket.writable.write()` resolves. Backend reads pause when client credit reaches zero. WebSocket loss closes the session and every TCP socket; v1 does not resume a partially delivered WebSocket session.

## Durable Object lifecycle and Hibernation

One `RelaySession` Durable Object owns each session's state and live outbound TCP sockets. The implementation deliberately does not claim Durable Object WebSocket Hibernation support: a hibernated object cannot safely preserve live `cloudflare:sockets` TCP socket objects and their readers/writers. The accepted WebSocket and TCP relay therefore require the object to remain active. A WebSocket close/error or authenticated DELETE closes all backend sockets.

## Deployment hardening

- Use a dedicated custom hostname and TLS.
- Store `WEB_SECRET` with `wrangler secret put`; never place it in `wrangler.jsonc`.
- Make the fixed backend reachable only as intended and keep it patched.
- Do not add a route that accepts a client destination.
- Do not enable cookies on the bridge origin. HTTP APIs reject cookie-bearing requests; WebSocket upgrades cannot reliably do so in browsers.
- Keep Cloudflare and package dependencies current and rerun all quality gates after upgrades.
- Review `git diff` and dry-run output before deployment. This repository does not contain an account ID, route, live domain, or API token.

## Reporting

Report vulnerabilities privately to the deployment operator. Include reproduction steps without real secrets, bearer tokens, user traffic, or opaque DATA contents.
