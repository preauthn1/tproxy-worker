# Security policy and threat model

## Security boundary

This Worker implements the WEB relay and terminates Telegram MTProxy obfuscated2 inside each logical stream. It is a pure Worker-to-Telegram-DC transport: there is no external MTProxy and no configurable or client-selected generic TCP backend.

The first 64 transformed bytes are buffered and decrypted only to advance AES-CTR state and validate the transport tag and signed DC id. No raw or decrypted header byte is written to the network. Only after validation and a successful allowlisted dial does the Worker synthesize the clear Telegram marker (`eeeeeeee`, `dddddddd`, or `ef`) and then forward decrypted payload. The DC id maps only to embedded Telegram production addresses on port 443.

Bridge, bootstrap, and session values are bearer capabilities:

- The bridge capability is HMAC-SHA256 over the canonical public hostname with `WEB_SECRET`.
- A bridge response mints a cryptographically random 32-byte bootstrap token with a two-minute lifetime.
- A valid `HELLO` atomically exchanges the bootstrap for a cryptographically random 32-byte session token.
- Tokens are intentionally not IP-bound. `CF-Connecting-IP` is accounting metadata only.
- Unknown, malformed, expired, or wrong credentials receive the same ordinary public 404 page as an unknown path.

Do not log request headers, full URLs, query strings, authorization headers, WebSocket subprotocols, bridge HTML, tokens, secrets, obfuscated2 headers, or DATA. Treat Worker/Durable Object traces as sensitive.

## Resource and protocol controls

- 16 live streams per session; stream IDs must strictly increase and are never reusable.
- 4 MiB initial credit in each direction per stream.
- 1 MiB maximum frame payload, 64 KiB relay DATA chunks, 2 MiB WebSocket messages, and 4096 frames/message.
- 12 MiB and 8192-item bounded queues with conservative per-item charging.
- Bounded 64-byte create bodies with independent read deadlines; bounded bootstrap count/rate and expiry alarms.
- Serialized inbound WebSocket processing, Telegram write deadlines, and idle ping/close.
- Upload credit is returned only after the complete terminator write resolves, including header validation, dial, marker write, and payload write.
- Downlink reads pause at client-credit and session pending-byte limits.

Malformed frames, wrong-direction frames, stream reuse, DATA beyond credit, invalid obfuscated2 secrets/tags/DCs, oversized carrier messages, text messages, and queue overflow terminate the affected session. An `OPEN` above the stream limit receives stream-level `CLOSE`.

## Lifecycle and failure handling

One `RelaySession` Durable Object owns a session's WebSocket state and live outbound sockets. Live `cloudflare:sockets` objects cannot be restored after hibernation, so the implementation does not claim hibernation support. Carrier close/error, authenticated DELETE, expiry, idle timeout, protocol failure, or write timeout closes all streams.

DC candidates are tried in a fixed allowlisted order. A connection that fails its initial marker write is closed before the next candidate is attempted. Closing during a pending dial closes any socket that subsequently arrives.

## Deployment hardening

- Current route publication is intentionally absent until review; `workers.dev`
  and preview URLs are disabled in configuration.
- Store `WEB_SECRET` with the platform secret store; never place a real secret in tracked files.
- Do not add a destination field, generic connect endpoint, backend environment variable, or route that accepts a client destination.
- Use a dedicated custom hostname and TLS if deployment is later approved.
- Keep observability configured to exclude sensitive headers, query values, and bodies.
- Review the diff, test results, dependency audit, and Wrangler dry-run before any deployment.

The repository contains no account ID, live route, custom domain, API token, or real secret.

## Reporting

Report vulnerabilities privately to the deployment operator. Include bounded reproduction steps without real secrets, bearer tokens, user traffic, or captured obfuscated payloads.
