# tproxy-worker

纯 Cloudflare Workers + Durable Objects 的 Telegram Desktop `tproxy-server` WEB relay v1 重写。它只实现 WEB 代理传输层，不实现 MTProxy；所有 `DATA` 都被当作不透明字节。

This is a production-oriented TypeScript rewrite of only the WEB relay portion of `tproxy-server`. The currently implemented carrier is multiplexed `carrier_mode=websocket`.

## 架构

```text
Telegram WEB adapter / bridge page
        │ shared tproxy v1 frames over one WebSocket
        ▼
Cloudflare Worker ── bootstrap registry Durable Object
        │ session bearer routes by token
        ▼
one RelaySession Durable Object per session
        │ one cloudflare:sockets TCP connection per OPEN
        ▼
fixed BACKEND_HOST:BACKEND_PORT (stock MTProxy)
```

- 根 Worker 伪装成普通公开网站，仅精确的 `GET /?bridge=<43-char capability>` 返回一次性 bridge page。
- `BootstrapRegistry` Durable Object 串行化 bootstrap 的签发、两分钟有效期、原子兑换及同请求幂等重试。
- 每个随机 session token 对应一个 `RelaySession` Durable Object。只有该对象持有流表、窗口、墓碑、WebSocket 与出站 TCP sockets。
- 客户端不能指定目标地址。`OPEN` 总是使用部署配置中的 `BACKEND_HOST` 和 `BACKEND_PORT`。
- `CF-Connecting-IP` 只记录为签发/创建时的 accounting metadata；token 不绑定 IP。

## 兼容性

已实现：

- `GET /?bridge=capability`，能力值严格使用 `HMAC-SHA256(WEB_SECRET, "tdesktop-web-proxy-bridge-v1\n" + PUBLIC_HOSTNAME)`，支持 16-byte hex 和 `dd` 前缀 17-byte hex secret。
- 动态、`no-store`、每响应 CSP nonce 的 bridge page；支持 origin-scoped `TelegramWebProxy` 边界及 `http://127.0.0.1:<port>` 单次 `MessagePort` 边界。
- `POST /api/v1/session`：精确 `HELLO`、bootstrap 原子兑换、字节相同重试幂等、`WELCOME`、session header。
- `GET /api/v1/ws`：精确 `tproxy-v1.<session-token>` subprotocol、单 WebSocket/session、binary-only、2 MiB carrier message cap。
- `DELETE /api/v1/session`：认证后关闭 session 和全部 TCP streams。
- 共享 frame wire format：`type:u8 | stream_id:u24 | payload_length:u32 | payload`。
- `OPEN/DATA/CLOSE/WINDOW` 生命周期、4 MiB 双向初始 credit、1 MiB payload、relay DATA 不超过 64 KiB、4096 frames/batch、stream ID 禁止复用、closed-ID tombstones。
- 16 streams/session、12 MiB / 8192 items pending/session，queued item 额外计 256 bytes；超 stream 限额只返回该流 `CLOSE`，协议/队列错误关闭 session。
- GrainTCP 衍生优化：上传机会性 grain 合包、BYOB-first 后端读取、64 KiB chunks、大下行块直发、小下行块短门控聚合、尽可能显式避免 WebSocket compression。
- 普通未认证路径、错误 method/header/token/capability 均表现为公开网站或相同的无描述 404 页面。

暂未实现：

- serialized HTTPS `/api/v1/up` 和 `/api/v1/down`；
- `https-lanes`、`websocket-lanes`；
- shared-frame PING/BYE emission and upstream-style idle ping timers;
- resumable WebSocket sessions；WebSocket 中断会终止 session；
- 跨 session/IP/global 的集中 rate limiting 和容量配额。当前限制以每个 session Durable Object 为主。

因此本项目与 `PROTOCOL.md` 的 `websocket` carrier 核心 wire/session/flow semantics 兼容，但不是全部四种 carrier 的完整替代品。

## 测试

要求 Node.js 20+：

```bash
npm ci
npm test
npm run typecheck
npm run lint
npx wrangler deploy --dry-run --outdir dist
git diff --check
```

Vitest 使用 `@cloudflare/vitest-pool-workers` 执行 Worker/DO integration tests。纯单元测试覆盖 frame codec、规范 capability vectors、流状态/flow control、queue overflow 和 GrainTCP 衍生 batching。TCP 测试使用内存 `BackendConnector`/`BackendConnection`，不访问公网。

测试范围包括 malformed frames、wrong direction、DATA beyond credit、stream reuse、隐藏 invalid credentials、固定 backend、queue overflow、session close、byte batching、bridge headers、bootstrap idempotency 和 WebSocket subprotocol。

## 配置与部署

测试实例：`https://proxy.example.com`。该实例仅用于验证 Worker/DO 的 bridge、session 和 WebSocket carrier 路径，固定 TCP 后端为无关的公开测试端点，**不是可用的 MTProxy 服务**；生产使用前必须替换为你控制的固定 MTProxy 后端。

`wrangler.jsonc` 已声明两个 SQLite-backed Durable Object class 及 migration，但不包含 account ID、route、真实域名或密钥。先编辑/通过部署环境提供普通 vars，然后设置 secret：

```bash
npx wrangler secret put WEB_SECRET
```

必需配置：

- `WEB_SECRET`: 32 hex characters，或 `dd` + 32 hex characters。不要写进 Git。
- `PUBLIC_HOSTNAME`: 自定义域名的 canonical lowercase ASCII/IDNA hostname，不带 scheme/port/path。
- `BACKEND_HOST`: 固定 MTProxy backend hostname/IP。
- `BACKEND_PORT`: `1..65535` 的固定端口字符串。
- `PUBLIC_SITE_TITLE`: 可选公开站点标题。

示例本地 `.dev.vars`（该文件被 gitignore；请自行填写，不要提交）：

```dotenv
WEB_SECRET=00000000000000000000000000000000
PUBLIC_HOSTNAME=proxy.example.com
BACKEND_HOST=192.0.2.10
BACKEND_PORT=2398
PUBLIC_SITE_TITLE=Example Site
```

本地开发：

```bash
npx wrangler dev
```

部署前将 Worker 绑定到你自己的 custom domain。仓库没有自动部署脚本，也不会创建 Cloudflare/GitHub 资源。Cloudflare Workers 的 public TCP `connect()` 不能连接禁止的 Cloudflare/私有地址范围；backend 必须符合平台网络规则。

## Capability 与 smoke test

离线计算 capability：

```bash
npm run capability -- proxy.example.com 000102030405060708090a0b0c0d0e0f
```

对已部署测试域运行只读/短 session smoke test（脚本不打印 secret、capability 或 token）：

```bash
npm run smoke -- https://proxy.example.com 000102030405060708090a0b0c0d0e0f
```

测试 client 应先发送唯一 `HELLO` batch，读取 `WELCOME`，再连接相同 origin 的 `/api/v1/ws`，subprotocol 必须是响应中的 `tproxy-v1.<session-token>`。每个 logical TCP connection 使用从未复用的非零 stream ID。

## Hibernation 限制

本项目不宣称 live TCP socket 支持 Durable Object Hibernation。Hibernation 可以保存 WebSocket attachment/state，但无法安全保存 `cloudflare:sockets` 的 live socket、reader 和 writer 对象。为了保持 TCP relay 正确性，session DO 在 carrier 存续期间保持 active；carrier close/error 或认证 DELETE 会关闭所有 sockets。

## License 与来源

整个仓库使用 GPL-3.0-only。精确上游 commit、license 和改编内容见 `THIRD_PARTY_NOTICES.md`；安全边界和 threat model 见 `SECURITY.md`。
