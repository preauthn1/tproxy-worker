# Third-party notices

This repository is licensed as a whole under GPL-3.0-only because it adapts GPL-3.0 optimization code and concepts from GrainTCP. Components derived from LGPL sources remain identified below under their upstream terms; GPL-3.0-only is compatible with LGPL-2.0-or-later's upgrade-and-combine path.

## TelegramMessenger/MTProxy

- Source: `https://github.com/TelegramMessenger/MTProxy`
- Pinned commit: `f36d8af769ffaeac36978d38c2c0f6d1104c2137`
- Upstream license: LGPL-2.0-or-later for the adapted library code (repository
  `LGPLv2`; the pinned tree also includes `GPLv2`).
- Adapted material: obfuscated2 64-byte header key/IV derivation, reversed outbound state, AES-256-CTR stream advancement, transport-tag validation, signed DC extraction, and the rule that the obfuscated header itself is consumed.
- Local direct-DC behavior: after validation, the Worker synthesizes the clear intermediate, padded-intermediate, or abridged marker before decrypted payload. It does not implement a standalone MTProxy process or private proxy RPC protocol.

## Telegram Desktop endpoint table

- Source: `https://github.com/telegramdesktop/tdesktop`
- Pinned commit: `f3b9a109c7de09fb8ce9d25c699851742b30f8b2`
- Upstream license: GPL-3.0 with the repository's OpenSSL linking exception.
- Adapted material: built-in production DC IPv4/IPv6 front-door addresses used as a bounded port-443 allowlist.

## Telegram Desktop tproxy-server

- Source: `https://github.com/telegramdesktop/tproxy-server`
- Pinned commit: `2873a08806d6e4d84830b9b5c4b0ec0f46af91f8`
- Upstream license: no repository-level license file or GitHub-detected SPDX license was present at the pinned commit. The protocol and implementation remain attributed here; downstream users should independently confirm permission before redistributing material derived from that repository.
- License note: the pinned repository does not carry a repository-level license.
  Consequently this project does **not** copy, translate, or modify its source code
  and does not claim a right to relicense that implementation.
- Interoperability reference only: public wire facts (frame field sizes and values,
  endpoint names, bearer/header names, and observable request/response behavior)
  were used to build an independent TypeScript implementation. Those facts are
  also exercised through independently written tests.
- No source expression from its Go bridge/session implementation is intentionally
  included. The Worker/DO lifecycle, bounds, bridge program, and direct-DC path are
  independently structured for the Cloudflare runtime.

## GrainTCP

- Source: `https://github.com/ToiCF/GrainTCP`
- Pinned commit: `1d22628f4d1413989f521f1d41591b0c72e658eb`
- Upstream commit author/rightsholder attribution available from the pinned tree:
  HiinEnkelte `<140715298+XyHK-HUC@users.noreply.github.com>`.
- Upstream license: GPL-3.0-only; the verbatim license is reproduced in `LICENSE`.
- Modified/adapted by preauthn1 on 2026-08-23. This is a modified work and
  differs materially from upstream GrainTCP.
- Adapted material: the bounded grain collector/bundler design (`mkK`/`mkQ`);
  DATA coalescing only within one received carrier batch; BYOB-first socket reads
  with a default-reader fallback; 64 KiB Telegram reads; bypassing the extra
  short-gate aggregation step for large downlink frames; short-gated small-downlink
  aggregation; and an empty `Sec-WebSocket-Extensions` response header where the
  runtime permits.
- Explicitly not adapted: UUID/VLESS authentication, early-data destination parsing, arbitrary client-selected destinations, `request.fetcher.connect()`, concurrent destination racing, or any unrestricted TCP proxy behavior.

This project uses the public `cloudflare:sockets` `connect()` API and accepts no destination hostname or port from clients.
