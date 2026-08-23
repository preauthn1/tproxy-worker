# Third-party notices

This repository is licensed as a whole under GPL-3.0-only because it adapts GPL-3.0 optimization code and concepts from GrainTCP.

## Telegram Desktop tproxy-server

- Source: `https://github.com/telegramdesktop/tproxy-server`
- Pinned commit: `2873a08806d6e4d84830b9b5c4b0ec0f46af91f8`
- Upstream license: no repository-level license file or GitHub-detected SPDX license was present at the pinned commit. The protocol and implementation remain attributed here; downstream users should independently confirm permission before redistributing material derived from that repository.
- Adapted material: WEB protocol v1 frame layout and validation rules; capability derivation and secret decoding; session/stream lifecycle and flow-control semantics; dynamic bridge-page behavior, WebView/loopback boundaries, carrier queue limits, and security response headers.
- Not included: MTProxy implementation, admin server, native clients, HTTPS/HTTPS-lanes carrier implementation, or WebSocket-lanes carrier implementation.

The TypeScript implementation is a clean rewrite for Cloudflare Workers and Durable Objects, with behavior compared against the pinned `PROTOCOL.md` and `internal/bridge/page.go`/session code.

## GrainTCP

- Source: `https://github.com/ToiCF/GrainTCP`
- Pinned commit: `1d22628f4d1413989f521f1d41591b0c72e658eb`
- Upstream license: GPL-3.0-only; the verbatim license is reproduced in `LICENSE`.
- Adapted material: the bounded grain collector/bundler design (`mkK`/`mkQ`); opportunistic upload aggregation; BYOB-first socket reads with a default-reader fallback; 64 KiB backend read chunks; direct WebSocket sends for large downlink chunks; short-gated small-downlink aggregation; and an empty `Sec-WebSocket-Extensions` response header to avoid compression negotiation where the runtime permits.
- Explicitly not adapted: UUID/VLESS authentication, early-data destination parsing, arbitrary client-selected destinations, `request.fetcher.connect()`, concurrent destination racing, or any unrestricted TCP proxy behavior.

This project uses the public `cloudflare:sockets` `connect()` API and accepts no destination bytes from clients.
