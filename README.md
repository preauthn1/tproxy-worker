# tproxy-worker

纯 Cloudflare Workers + Durable Objects 的 Telegram WEB relay v1 独立实现。它是 **pure Worker-to-Telegram-DC** 数据路径：Worker 在每条逻辑流内终止 Telegram obfuscated2、校验目标 DC，并通过 `cloudflare:sockets` 直接连接内置 allowlist 中的 Telegram DC。**不需要也不连接任何外部 MTProxy。**

The implemented carrier is multiplexed `carrier_mode=websocket`. The repository includes no account ID, route, custom domain, or secret; deployments must provide those externally.

## 架构

```text
Telegram WEB adapter / bridge page
        │ shared tproxy v1 frames over one WebSocket
        ▼
Cloudflare Worker ── BootstrapRegistry Durable Object
        │ session bearer routes by token
        ▼
one RelaySession Durable Object per session
        │ obfuscated2 termination + one allowlisted TCP socket per OPEN
        ▼
Telegram DC selected by the authenticated 64-byte obfuscated2 header
```

- 根 Worker 伪装成普通公开网站，仅精确的 `GET /?bridge=<43-char capability>` 返回一次性 bridge page。
- `BootstrapRegistry` 对 bootstrap 签发设定容量、速率、两分钟有效期、原子兑换及同请求幂等重试。
- 每个随机 session token 对应一个 `RelaySession`。只有该对象持有流表、窗口、墓碑、WebSocket 与出站 TCP sockets。
- 客户端不能提供 hostname 或 port。解密后的 signed DC id 只能映射到内置 Telegram DC 1..5 的 port-443 allowlist。
- 每条流先完整消费并解密 64-byte obfuscated2 header；不转发任何原始或解密 header bytes。校验 tag 后，Worker 先写 clear Telegram transport marker，再写解密 payload：`eeeeeeee -> ee ee ee ee`、`dddddddd -> dd dd dd dd`、`efefefef -> ef`。
- `CF-Connecting-IP` 只记录为签发/创建时的 accounting metadata；token 不绑定 IP。

## 已实现范围

- 严格 capability：`HMAC-SHA256(WEB_SECRET, "tdesktop-web-proxy-bridge-v1\n" + PUBLIC_HOSTNAME)`；支持 16-byte secret 和 `dd` 前缀格式。
- 动态、`no-store`、每响应 CSP nonce 的 bridge page；origin-scoped `TelegramWebProxy` 和 `http://127.0.0.1:<port>` 单次 `MessagePort` 边界。
- 精确 `HELLO`、有界 request body、bootstrap 幂等兑换、`WELCOME`、session expiry alarm。
- 精确 `tproxy-v1.<session-token>` WebSocket subprotocol、单 WebSocket/session、binary-only、2 MiB carrier cap、有界串行输入队列和 idle ping/close。
- `OPEN/DATA/CLOSE/WINDOW`、4 MiB 双向初始 credit、1 MiB frame payload、64 KiB relay chunks；stream ID 只拒绝 active/tombstoned 重用，bounded tombstone 淘汰后允许 24-bit 计数回绕。
- 64 streams/session、12 MiB / 8192 items pending/session、每流有界有序 writer pump、单流 write deadline、256 KiB / 20 ms WINDOW 合并、closed-ID tombstones、WebSocket 断开时关闭所有 TCP sockets。
- Streaming AES-256-CTR obfuscated2 termination、三种 transport marker、signed DC validation、确定性 DC candidate failover、加密 DC response 回程。
- GrainTCP 衍生的 bounded grain batching、BYOB-first socket reads 和小下行短门控聚合。
- 未认证路径、错误 method/header/token/capability 均表现为公开网站或相同的无描述 404 页面。

当前只实现 `websocket` carrier；未实现 HTTPS up/down、lane carriers 或 WebSocket resume。Live TCP sockets 无法跨 Durable Object hibernation 保存，所以 carrier 存续期间 session object 必须保持 active。

## 测试

要求 Node.js 20+：

```bash
npm ci
npm run lint
npm test
npm run typecheck
npx wrangler deploy --dry-run --outdir dist
git diff --check
npm audit --json
```

Vitest 使用 `@cloudflare/vitest-pool-workers` 执行 Worker/Durable Object integration tests。测试不会访问公网；Telegram socket 由内存 fake DC 替代。覆盖范围包括 deterministic obfuscated2 vectors、三种 marker 的 exact marker+payload 写入、fragmented header/payload、dial ordering、malformed secret/tag/DC、failover/close races、真实 `RelaySession` WebSocket data plane、flow control、queue/body bounds、session expiry、bridge headers、bootstrap concurrency 和 WebSocket subprotocol。

## 配置与部署

`wrangler.jsonc` 声明两个 SQLite-backed Durable Object class 及 migration，但不包含 account ID、routes、custom domain 或 secret。仓库中的 dry-run 命令只构建 bundle，不部署、不创建 route。

必需配置：

- `WEB_SECRET`: 32 hex characters，或 Telegram 支持的 `dd` + 32 hex characters；只通过 secret store 或未跟踪的本地配置提供。
- `PUBLIC_HOSTNAME`: canonical lowercase ASCII/IDNA hostname，不带 scheme/port/path。
- `PUBLIC_SITE_TITLE`: 可选公开站点标题。

不存在可配置的 backend hostname 或 backend port。目标仅来自校验后的 Telegram DC id 和代码内 allowlist。

### 优选 Cloudflare 边缘连接

定制客户端可以使用 `web1.` 连接 secret，将三项客户端连接信息放在一个可移植字符串中：

- 原始 16-byte 或 `dd` + 16-byte MTProxy secret；
- 必须保持不变的 TLS SNI / HTTP Host：例如 `proxy.example.com`；
- 优选连接地址：Cloudflare 优选域名或公网 IPv4。

生成命令：

```bash
npm run web-secret -- proxy.example.com <优选域名或公网IP> <MTProxy-secret>
```

这是**定制 WEB adapter 配置**，不是传给 Worker 的 MTProxy secret。Native adapter 必须：

1. TCP 连接优选地址的 `443`；
2. TLS SNI 仍使用 `proxy.example.com`，并正常验证该主机证书；
3. HTTP `Host` / HTTP/2 `:authority` 仍为 `proxy.example.com`；
4. Bridge/WSS URL 的逻辑 origin 仍为 `https://proxy.example.com`；
5. 仅将 envelope 内的原始 MTProxy secret 用于 obfuscated2 和 capability 派生。

浏览器原生 `fetch()` / `WebSocket` 无法指定“连接优选地址，但使用另一 SNI/Host”；此功能必须由定制客户端 native 网络层实现。不要关闭 TLS 证书验证，也不要把 Host 改成优选域名。

本地 `.dev.vars` 示例使用明显占位符，必须替换且不能提交：

```dotenv
WEB_SECRET=<32-hex-development-secret>
PUBLIC_HOSTNAME=proxy.example.com
PUBLIC_SITE_TITLE=Example Site
```

设置部署 secret 的命令（仅在审查后自行执行）：

```bash
npx wrangler secret put WEB_SECRET
```

本地开发可运行 `npx wrangler dev`。如未来获准发布，需另外配置自有 custom domain；不要添加允许客户端指定目标的 route 或协议字段。Cloudflare `connect()` 的平台网络限制仍然适用。

## Capability 与 smoke test

离线计算 capability：

```bash
npm run capability -- proxy.example.com <development-secret>
```

对未来已审查部署运行短 session smoke test：

```bash
npm run smoke -- https://proxy.example.com <development-secret>
```

脚本不打印 secret、capability 或 token。客户端先发送唯一 `HELLO` batch，读取 `WELCOME`，再连接同 origin `/api/v1/ws`；每个 logical stream 使用从未复用的递增非零 ID。

## License 与来源

仓库整体为 GPL-3.0-only。Telegram MTProxy obfuscated2 derivation 的 LGPL-2.0-or-later 来源、Telegram Desktop endpoint table、tproxy-server protocol attribution 和 GrainTCP 改编范围见 `THIRD_PARTY_NOTICES.md`；安全边界见 `SECURITY.md`。
