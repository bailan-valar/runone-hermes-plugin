# RunOne 平台适配器 —— Hermes 插件草稿（评审用）

这是 `docs/ai-channel-design.md` §8「Hermes 侧插件骨架」的**可运行草稿**：把 RunOne 做成 Hermes 的一个聊天平台，
让 **Hermes 主动出站**拉取消息并回写，**完全不需要内网穿透**。

```
RunOne 前端 /ai ──► RunOne Worker + D1（消息与权限的唯一真相）
                            ▲
                            │ GET  /ai/inbox?cursor&wait=25   （长轮询，出站）
                            │ POST /ai/messages/:id/claim     （认领 + 租约）
                            │ POST /ai/messages/:id/reply     （回写，幂等）
                            │ POST /ai/conversations/:id/typing
                            │
                   本插件（跑在 Hermes 网关进程里，无新服务）
```

与 Telegram（HTTP 长轮询）、企业微信/Discord（WS 客户端）、飞书（SDK 长连接）**同一类**：都是出站客户端。

## 文件

| 文件 | 作用 |
|---|---|
| `plugin.yaml` | 插件清单：`kind: platform`、`requires_env: RUNONE_BASE_URL / RUNONE_TOKEN`、可选 `RUNONE_HOME_CONVERSATION`（cron 投递目标）、`RUNONE_ALLOWED_USERS` |
| `adapter.py` | 适配器本体（~380 行）：长轮询循环 + 入站投递 + send/typing/get_chat_info + 进程外投递（cron） + `register(ctx)` |
| `__init__.py` | 导出 `register`（目录插件加载器按此发现） |

关键方法（基类契约见 `gateway/platforms/ADDING_A_PLATFORM.md`）：

| 方法 | 做什么 |
|---|---|
| `connect()` | 建 httpx 客户端（Bearer PAT）→ 起 `_poll_loop` → `_mark_connected()` |
| `_poll_loop()` / `_poll_once()` | 长轮询 `/ai/inbox`；指数退避 + 抖动重连；**游标只在整批投递成功后推进**（不丢消息） |
| `_deliver(item)` | 去重 → `claim` → `build_source()` 造 `MessageEvent` → `handle_message()`（先认领后干活） |
| `send(chat_id, text)` | 有入站锚点走 `/ai/messages/:id/reply`（带 `clientMsgId` 幂等键），无锚点（cron）直接写 assistant 消息 |
| `send_typing(chat_id)` | `/ai/conversations/:id/typing` 心跳（服务端 5s 过期） |
| `get_chat_info(chat_id)` | `/ai/conversations/:id` |
| `_standalone_send(...)` | cron 独立进程投递（`deliver=runone:<会话 id>`），签名按 `PlatformEntry.standalone_sender_fn` 契约 |

## 安装（批一落地时用）

```bash
# 1. 放进当前 profile 的插件目录（零核心改动）
cp -r docs/ai-channel-plugin "$LOCALAPPDATA/hermes/profiles/worker/plugins/runone"

# 2. 配置（profiles/worker/.env）
RUNONE_BASE_URL=https://runone-api.capdien.site
RUNONE_TOKEN=rn_xxxxxxxx_yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy   # 设置页 → 个人令牌（PAT）

# 3. 重启网关后可见
hermes --profile worker gateway status      # 平台列表里 RunOne 与飞书/微信并列
```

## RunOne 侧要提供的接口（本草稿按此契约实现）

| 方法 | 路径 | 请求 | 响应（字段名以此为准） |
|---|---|---|---|
| GET | `/ai/inbox` | `cursor`、`wait`、`limit` | `{data:[{id, seq, conversationId, conversationTitle, authorUserId, authorName, text, attachmentIds?}], nextCursor}`；无新消息则挂满 `wait` 秒返回空数组 |
| POST | `/ai/messages/:id/claim` | `{}` | `200 {ok:true, lease_seconds}`；已被认领 → `409` |
| POST | `/ai/messages/:id/reply` | `{text, clientMsgId, attachmentIds?}` | `{id, text, deduped?}`；同 `clientMsgId` 重放返回原行 |
| POST | `/ai/messages/:id/fail` | `{reason}` | `200` |
| POST | `/ai/conversations/:id/typing` | `{}` | `200` |
| GET | `/ai/conversations/:id` | — | `{id, title, projectId?}` |
| POST | `/ai/conversations/:id/messages` | `{role:"assistant", text, clientMsgId}` | `{id}`（cron 投递用，同前端发消息的端点） |

鉴权：`Authorization: Bearer <PAT>`，与现有 MCP/脚本同一套（`apps/server/src/middleware/auth.ts`）。

## 已验证（2026-09-16，本机）

1. **骨架真跑通**：`workspace/runone-plugin-demo/drive.py`（假 RunOne 端点 + 真适配器 + 模拟网关处理器）→
   `RESULT: ALL GREEN`。观察到的一次完整会话：

   ```
   GET  /ai/inbox                      （带 cursor=0 & wait）
   POST /ai/messages/m-user-1/claim     ← 先认领
   POST /ai/conversations/conv-1/typing
   POST /ai/messages/m-user-1/reply     ← 带 clientMsgId
   GET  /ai/inbox                       （游标已推进到 1，空返回后立即再拉）
   GET  /ai/conversations/conv-1        （get_chat_info 拿到标题）
   POST /ai/conversations/conv-1/messages  （cron 投递，role=assistant）
   ```

   同时验证：`Platform.RUNONE` 能解析、事件里 `chat_id/chat_name/user_id/text` 都对、
   回复里带幂等键、全程 `Authorization: Bearer …`。
2. **网关能发现插件**：临时把目录装进 `profiles/worker/plugins/runone` 后
   `hermes --profile worker plugins list` 出现 `runone-platform | not enabled | 0.1.0 | … | user`（`user` 源 = profile 插件目录）；
   验完已删除，profile 插件目录恢复为空。

## 跑的时候踩到的两个坑（已写进代码）

1. **系统代理会劫持本机地址**：Windows 上 httpx 从注册表读到 Clash 的系统代理，连 `127.0.0.1` 也走代理 →
   假端点返回 502、一次都没打通。改成默认 `trust_env=False`（要经代理显式设 `RUNONE_USE_SYSTEM_PROXY=1`）。
   这台机器上 Python 侧踩过同样的坑（见技能里的 runone 代理条目）。
2. **Cloudflare 会按浏览器指纹拒掉裸 UA**（`1010 browser_signature_banned`）：请求要带一个明确的 `User-Agent`。

## 本草稿**未做**（各自对应设计文档里的批二/批三）

- 游标持久化（现在是内存变量；批二接 `plugins/plugin_storage.py` 的 `plugin_db("runone")`，重启不重投）；
- 流式回复（`PATCH /ai/messages/:id` 草稿行）、工具进度（"正在执行「create_task」…"）；
- 附件/语音（音频附件 → 本地 whisper → 回复）、未读/已读；
- clarify / 危险命令审批渲染成原生按钮；
- `/ai/inbox` 之外的多会话并发（Hermes 侧目前单会话串行）。
