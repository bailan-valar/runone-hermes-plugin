# RunOne × Hermes Agent 插件

一个插件，两件事，一份凭据：

* **入站对话通道**（`kind: platform`）—— RunOne 的 AI 助手页当成一个聊天平台。Hermes **主动出站**长轮询取消息、回写回复，**不需要内网穿透、不需要常驻服务**。
* **出站工具集**（`provides_tools`）—— 任务 / 目标 / 指标 / 重复任务等 **22 个工具**，agent 在任何会话里都能读写 RunOne。

```
RunOne 前端 /ai ──► RunOne Worker + D1（消息、任务与权限的唯一真相）
                            ▲
                            │ GET  /ai/inbox?cursor&wait=25   （长轮询，出站）
                            │ POST /ai/messages/:id/claim     （认领 + 租约）
                            │ POST /ai/messages/:id/reply     （回写，幂等）
                            │ POST /ai/conversations/:id/typing
                            │
                 ┌──────────┴───────────┐
                 │ 本插件（Hermes 进程内）│  adapter.py：对话通道
                 └──────────┬───────────┘  tools.py  ：22 个工具
                            │ POST /mcp  tools/call（同一枚 PAT）
                            └──► RunOne 服务端工具实现
```

平台这一侧与 Telegram（HTTP 长轮询）、企业微信 / Discord（WS 客户端）、飞书（SDK 长连接）**同一类**：都是出站客户端。

## 安装

```bash
hermes -p worker plugins install bailan-valar/runone-hermes-plugin --enable
```

按提示填两个环境变量（写进该 profile 的 `.env`，令牌走遮蔽输入）：

| 变量 | 值 |
|---|---|
| `RUNONE_BASE_URL` | `https://runone-api.capdien.site`（本地开发 `http://127.0.0.1:8787`） |
| `RUNONE_TOKEN` | RunOne「设置 → 个人访问令牌」新建的 `rn_…`（**一枚同时管对话与工具**） |

然后重启网关（`hermes -p worker gateway restart`）。可选：

| 变量 | 作用 |
|---|---|
| `RUNONE_ALLOWED_USERS` | 允许跟 agent 说话的 RunOne 账号 id，逗号分隔 |
| `RUNONE_ALLOW_ALL_USERS` | `true` = 不限账号（单人自用等价） |
| `RUNONE_HOME_CONVERSATION` | cron / 通知投递的默认会话 id |

## 工具集（22 个，`runone` toolset）

| 分类 | 工具 |
|---|---|
| 任务 | `list_today_tasks`、`list_assigned_tasks`、`find_tasks`、`create_task`、`update_task`、`complete_task`、`take_task`、`submit_task_result` |
| 项目 / 账号 | `list_projects`、`list_my_account` |
| 目标 | `create_goal`、`list_goals`、`update_goal`、`link_task_to_goal` |
| 指标 | `create_metric`、`list_metrics`、`add_metric_record`、`list_metric_records` |
| 重复任务 | `create_recurrence`、`list_recurrences`、`update_recurrence`、`skip_occurrence` |

设计上刻意分了三层，避免「插件里的工具」与「服务端实现」两处漂移：

| 层 | 来源 | 说明 |
|---|---|---|
| **声明**（工具名、字段、必填项、中文描述） | `catalog.py` | 由服务端 `/mcp` 的 `tools/list` **生成**，与 MCP 客户端今天看到的是同一份 |
| **执行** | `tools.py` → `POST /mcp` `tools/call` | 复用服务端同一套实现，所以鉴权、AI 署名（`submit_task_result` 记成 AI 写的进度）、变更记录、revision 全都一致；插件里没有第二份业务逻辑 |
| **配置** | `RUNONE_BASE_URL` / `RUNONE_TOKEN` | 与对话通道共用，一处配置、同一身份 |

与「在 Hermes 里配一个 MCP 服务器」的差别只在：工具落在插件的 `runone` 工具集里、名字是平铺的（`list_today_tasks` 而不是 `mcp__runone__list_today_tasks`），**插件启用即可用**，不必再维护 `mcp_servers` 那一份配置与连接。

## 文件

| 文件 | 作用 |
|---|---|
| `plugin.yaml` | 清单：`kind: platform`、`provides_tools`（22 个工具名）、`requires_env` |
| `adapter.py` | 对话通道（~380 行）：长轮询 + 入站投递 + send/typing/get_chat_info + cron 投递 + `register(ctx)` |
| `tools.py` | 工具集：传输（`/mcp` 转发）、错误映射、结果渲染、`register_tools(ctx)` |
| `catalog.py` | **自动生成**的工具目录（勿手改） |
| `scripts/gen_catalog.py` | 重新生成 `catalog.py`（打 `/mcp` `tools/list`） |
| `scripts/verify_tools.py` | 对着真实 API 把 22 个工具跑一遍，逐项 PASS/FAIL |

关键方法（基类契约见 Hermes 的 `gateway/platforms/ADDING_A_PLATFORM.md`）：

| 方法 | 做什么 |
|---|---|
| `connect()` | 建 httpx 客户端（Bearer PAT）→ 起 `_poll_loop` → `_mark_connected()` |
| `_poll_loop()` / `_poll_once()` | 长轮询 `/ai/inbox`；指数退避 + 抖动重连；**游标只在整批投递成功后推进**（不丢消息） |
| `_deliver(item)` | 去重 → `claim` → `build_source()` 造 `MessageEvent` → `handle_message()`（先认领后干活） |
| `send(chat_id, text)` | 有入站锚点走 `/ai/messages/:id/reply`（带 `clientMsgId` 幂等键），无锚点（cron）直接写 assistant 消息 |
| `send_typing(chat_id)` | `/ai/conversations/:id/typing` 心跳（服务端 5s 过期） |
| `_standalone_send(...)` | cron 独立进程投递（`deliver=runone:<会话 id>`） |

## 已验证（2026-09-16，本机 8787 + 真实账号 PAT）

1. **22 个工具逐个真跑**：`python scripts/verify_tools.py --from-profile worker` → **27 项用例 / 失败 0**。覆盖读（今日任务、项目、目标、指标、重复任务、我的身份）、任务全生命周期（建 → 承接 → 改 → 提交结果（状态转「待测试」）→ 完成）、目标（建 → 关联任务 → 改）、指标（建 → 打卡 → 查记录）、重复任务（建 → 改 → 跳过某一次）。走的是**插件自己的代码路径**，不是另写一份调用。
2. **`hermes plugins validate`** —— 10 项检查全 ✓，含「declared tools — matches registrations」（22 个声明名与实际注册逐一对应）与「built-in tool collisions — no collisions」。
3. **`hermes plugins doctor runone-platform`** —— `registrations: 22 tool(s), 0 hook(s)`。
4. **CLI 进程也能拿到工具**（`provides_tools` 契约的关键）：`resolve_toolset('runone')` 在干净进程里返回 **22 个工具名**，`hermes tools list --platform cli` 里 `✓ enabled runone`。平台适配器本身仍是延迟加载，不拖慢 CLI 启动。
5. **网关侧发现插件**：`hermes -p worker plugins list` → `enabled | git | 0.2.0 | runone-platform`。

## 维护

改完 `apps/server/src/mcp/tools.ts`（工具 schema）之后：

```bash
RUNONE_BASE_URL=https://runone-api.capdien.site RUNONE_TOKEN=rn_… \
  python scripts/gen_catalog.py          # 重新生成 catalog.py，提交即可
```

本仓库是插件源码的**唯一来源**；monorepo 里 `docs/ai-channel-plugin/` 只保留设计期草稿，不要在那里改。

## 跑起来踩到的坑（都已写进代码）

1. **系统代理会劫持本机地址**：Windows 上 httpx 从注册表读到 Clash 的系统代理，连 `127.0.0.1` 也走代理 → 假端点返回 502、一次都没打通。本机地址一律 `trust_env=False`。
2. **Cloudflare 会按浏览器指纹拒掉裸 UA**（`1010 browser_signature_banned`）：请求要带明确的 `User-Agent`。
3. **本机开发库里「我的空间」有多条**（每个注册账号一条同名空间），按名字解析会挑到别人的那条 → 报「没有权限」。验证脚本因此自建一个唯一名字的空间；生产上用户的三个空间名字唯一，不会撞。

## 还没做（对应设计文档的批二 / 批三）

- 流式回复的**适配器接线**（服务端与前端已就绪：草稿行 + `PATCH /ai/messages/:id`）；
- 工具进度（「正在执行「create_task」…」）、未读/已读、会话深链 `/ai?conversation=<id>`；
- 附件 / 语音（音频附件 → 本机 whisper）、clarify 与危险命令审批渲染成原生按钮；
- 游标持久化（现在是内存变量，重启重扫；批二接 `plugin_db("runone")`）；
- `/ai/inbox` 之外的多会话并发（Hermes 侧目前单会话串行）。
