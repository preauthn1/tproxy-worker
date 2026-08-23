# Pure Worker MTProxy termination design

Status: implementation target, 2026-08-23.

## Hard architecture boundary

```text
Telegram WEB adapter
  (existing Telegram MTProxy obfuscated2 transform)
        |
        | tproxy WEB shared frames over WSS
        v
Cloudflare Worker + one RelaySession Durable Object
        |
        | cloudflare:sockets raw TCP
        v
Telegram DC front door selected by the authenticated obfuscated2 DC id
```

There is no stock, remote, public, private, or localhost MTProxy process. There is
no configurable generic TCP backend and no client-selected address.

## Per logical WEB stream

1. Accumulate exactly the first 64 transformed bytes.
2. Derive inbound AES-256-CTR key/IV from bytes 8..55 plus the configured 16-byte
   MTProxy secret, and derive the reverse outbound key/IV from the reversed slice.
3. Decrypt a copy of the 64-byte header while advancing only the inbound CTR state.
4. Validate transport tag (`0xeeeeeeee`, `0xdddddddd`, or `0xefefefef`) and signed
   16-bit DC id at bytes 60..61. Reject zero/unknown IDs and all invalid headers.
5. Map the signed DC id only through an embedded/fetched allowlisted Telegram DC
   table. Open one direct TCP socket to that DC. Client bytes can never select a host.
6. Consume all 64 obfuscated-header bytes, then synthesize the validated clear
   Telegram DC transport marker (`eeeeeeee` -> four `ee` bytes, `dddddddd` -> four
   `dd` bytes, or `efefefef` -> one `ef` byte) before decrypted payload. No raw or
   decrypted header byte is forwarded.
7. Forward DC bytes back through the outbound AES-CTR state unchanged otherwise.

The DC itself speaks the same abridged/intermediate/padded-intermediate MTProto
transport selected by the first marker, so the Worker does not need the official
MTProxy private RPC_PROXY_REQ protocol or getProxySecret.

## Source basis

Inbound transform behavior is derived from the LGPL-2.0-or-later official MTProxy
source at TelegramMessenger/MTProxy commit
`f36d8af769ffaeac36978d38c2c0f6d1104c2137`, especially
`net/net-tcp-rpc-ext-server.c` around the 64-byte header and AES-CTR setup.
Direct Telegram DC transport was independently probed with `req_pq_multi` against
DC 1..5 endpoints before implementation.

## Security and resource rules

- Only embedded Telegram DC IP/port allowlists are dialed.
- Header parsing is bounded to 64 bytes and runs before any dial.
- One authenticated WEB stream maps to one DC TCP socket.
- Existing per-session stream, byte, frame, and flow-control limits remain active.
- Incoming WebSocket queue must be bounded before serialization.
- Dial, write, read, idle, and close races have deterministic tests.
- WEB_SECRET is also the MTProxy obfuscation secret; a leading `dd` requests padded
  intermediate mode client-side, while key derivation uses the following 16 bytes.

## Validation gates

- Byte-exact deterministic obfuscated2 vectors produced by a reference implementation.
- Mock DC integration: fragmented 64-byte header, direct decrypted payload, encrypted
  response, malformed secret/tag/DC, close races and backpressure.
- Real Worker `cloudflare:sockets` probes are intentionally outside automated tests
  and require a separately reviewed deployment/network-test decision.
- Independent security and protocol review before route publication.
