"""RunOne 平台适配器（Hermes 插件）—— 让 Hermes **主动出站**把 RunOne 当成一个聊天平台。

设计文档：`docs/ai-channel-design.md`（本文件是它的 §8「插件骨架」的可运行草稿）。

方向与内网穿透相反：RunOne 不连 Hermes，Hermes 连 RunOne。

    前端 /ai ──POST /ai/… ──► RunOne Worker + D1（消息与权限的唯一真相）
                                        ▲
                                        │ 推送 GET /ai/inbox/ws（WSS，只发 kick 信号）★ 主用
                                        │ 拉取 GET /ai/inbox?cursor&wait=0（收到 kick 后 / 每 60s 兜底）
                                        │ 认领 POST /ai/messages/:id/claim
                                        │ 回写 POST /ai/messages/:id/reply
                                        │ 心跳 POST /ai/conversations/:id/typing
                                        │
                                 本适配器（跑在 Hermes 网关进程里）

入站有**两种模式，契约与游标语义完全一样**（都走 `/ai/inbox`），区别只在「什么时候去拉」：

* **推送模式（默认）**：连 `GET /ai/inbox/ws`（服务端是 Durable Object），消息落库即被 kick
  一下 → 立刻拉一次。空闲时不再每秒重查 —— 老的长轮询是 30,556 次查询 / 466 万行读每天
  （占 D1 免费额度 86%，打满后整站 500）。
* **长轮询模式（兜底）**：服务端没有这条路由（旧版本 / 还没部署）、`websockets` 库缺失、
  `RUNONE_WS=0`、或连续连不上时自动退到它 —— 功能不降级，只是回到「挂着 wait=25 等」。

骨架照 `plugins/platforms/ntfy/adapter.py`（HTTP 传输 + 退避重连）抄，与之并列的还有
telegram（HTTP 长轮询）、wecom / discord（WS 客户端）、feishu（SDK 长连接）。

零核心改动：本目录放进 `$HERMES_HOME/plugins/`（本机 = `%LOCALAPPDATA%\\hermes\\profiles\\worker\\plugins\\`），
重启网关即生效。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

try:  # httpx 是 Hermes 自带依赖；缺失时平台整体不可用（照 ntfy 的处理）
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:  # pragma: no cover
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

try:  # 推送面（WSS）用；缺失时自动退长轮询，功能不受影响
    import websockets

    WEBSOCKETS_AVAILABLE = True
except ImportError:  # pragma: no cover
    websockets = None  # type: ignore[assignment]
    WEBSOCKETS_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms._shared import extra_or_secret, get_scoped_secret
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.helpers import MessageDeduplicator

logger = logging.getLogger(__name__)

PLATFORM_NAME = "runone"


def _audio_mime(name: str) -> str:
    """按扩展名给 MIME：语音附件用（COS 直传要在 presign 时定 mime）。"""
    return {
        ".mp3": "audio/mpeg",
        ".m4a": "audio/mp4",
        ".wav": "audio/wav",
        ".ogg": "audio/ogg",
        ".opus": "audio/ogg",
        ".webm": "audio/webm",
    }.get(Path(name).suffix.lower(), "application/octet-stream")
MAX_MESSAGE_LENGTH = 8000

# Cloudflare 前置会按浏览器指纹拒掉裸 UA（仓库里踩过 1010）——HTTP 与 WS 都用这一个标识
USER_AGENT = "HermesAgent-RunoneAdapter/0.4"

# ---- 传输：推送面（WSS）优先，长轮询兜底 ------------------------------------
#
# 契约不变：推送只发 `{"type":"kick"}`，消息本体仍从 `/ai/inbox` 拉
# （项目上下文注入、成员/空间收窄、游标语义都留在服务端那一份实现里）。
# 长轮询：服务端最多挂 WAIT 秒（设计文档 §5-1 口径），客户端超时留足余量
POLL_WAIT_SECONDS = 25
POLL_TIMEOUT_SECONDS = 40
# 挂着推送时的「兜底拉取」间隔（秒）：定期单次拉一次（wait=0），做三件事 ——
# 防漏 kick、给服务端「Hermes 在线」打点、顺手跑 stale 扫尾。
# 服务端判在线的有效期是 150s（AI_AGENT_SEEN_TTL_SECONDS = 2.5 × 这个间隔）。0 = 关掉（只靠 kick）
WS_SAFETY_POLL_SECONDS_DEFAULT = 60.0
# WS 保活走协议级 ping/pong（服务端 runtime 自动回，不唤醒 Durable Object）：
# ping_interval 内没等到 pong 就抛异常 → 断开 → 上层重连（这就是「链路死了」的判据）
WS_PING_INTERVAL_SECONDS = 20.0
WS_PING_TIMEOUT_SECONDS = 20.0
WS_OPEN_TIMEOUT_SECONDS = 15.0
WS_CLOSE_TIMEOUT_SECONDS = 5.0
WS_MAX_FRAME_BYTES = 64 * 1024
# 一次会话短于这个秒数就当「没连上 / 被立刻关掉」，按失败计入退避（防重连风暴）
WS_MIN_SESSION_SECONDS = 5.0
# 连续失败几次之后先回长轮询一段时间再试（服务端还没部署推送面时的常态）
WS_FALLBACK_AFTER_FAILURES = 3
WS_RETRY_AFTER_SECONDS = 300.0
# 重连退避（指数 + 抖动），照 base 文档的「streaming 连接必须带退避」要求
BACKOFF_START_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 60.0
# 同一批最多处理多少条，避免一次醒来吃下过多（其余下一拍继续）
POLL_BATCH_LIMIT = 20
# typing 心跳间隔：比服务端 5s 有效期略短，掉了就自然过期（设计文档 §5-6）
TYPING_HEARTBEAT_SECONDS = 4.0
# 认领租约续期间隔（秒）：服务端租约 90s，回合可能跑几分钟，搭在 typing 心跳上续租。
# 不续租的后果是实测过的：91s 的回合回写被服务端拒成 409，用户什么也看不到。
LEASE_RENEW_SECONDS = 15.0
# 语音附件的合并窗口（秒）：语音先到、文字随后；超过这个窗口还没等到文字回复，
# 就把语音单独发一条（旧行为）——宁可分成两条，也不能把音频丢了。
VOICE_MERGE_TIMEOUT_SECONDS = 60.0
DEDUP_MAX_SIZE = 2000
DEDUP_WINDOW_SECONDS = 600


class WsUnsupported(RuntimeError):
    """服务端没有推送面（路由不存在 / 不是 WebSocket 端点）——不是故障，退回长轮询即可。"""


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _setting(extra: Dict[str, Any], *keys: str, env: str = "", default: str = "") -> str:
    """config.yaml 的 platforms.runone.extra 优先，其次环境变量（与 wecom/ntfy 同款）。"""
    return str(
        next((extra[k] for k in keys if extra.get(k)), None)
        or (get_scoped_secret(env, default) if env else "")
    ).strip()


def _float_setting(extra: Dict[str, Any], *keys: str, env: str = "", default: float) -> float:
    """数值型配置：写坏了就回默认值（配置错误不该让适配器起不来）。"""
    raw = _setting(extra, *keys, env=env, default=str(default))
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


class RunoneAdapter(BasePlatformAdapter):
    # 逐字流式（批二）：编辑已发出的消息 = `PATCH /ai/messages/:id`（服务端同 seq 改正文）。
    # 网关据此在「发分片 → 改同一条 → 定稿」这条路上跑；关掉时行为与旧版完全一致。
    SUPPORTS_MESSAGE_EDITING = True
    # 定稿那一次编辑不能因为「正文与上一片一样」被跳过：它同时是释放入站消息租约、
    # 把草稿行从 `streaming` 转成 `replied` 的唯一动作。跳过就会留下永远在长的脏草稿。
    REQUIRES_EDIT_FINALIZE = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config=config, platform=Platform(PLATFORM_NAME))
        extra = config.extra or {}
        self._base_url: str = _setting(extra, "base_url", "baseUrl", env="RUNONE_BASE_URL").rstrip("/")
        self._token: str = _setting(extra, "token", env="RUNONE_TOKEN")
        # 默认 **不继承系统代理**：Windows 上 httpx 会从注册表读到 Clash 之类的系统代理，
        # 连 127.0.0.1 也会被劫持成 502（本机实测）。要经代理访问 RunOne 时显式打开。
        self._trust_env: bool = _truthy(
            _setting(extra, "use_system_proxy", "useSystemProxy", env="RUNONE_USE_SYSTEM_PROXY", default="0")
        )
        # 推送面（WSS）：默认开。`RUNONE_WS=0` 强制回长轮询；服务端没有这条路由时也会自动退。
        self._ws_enabled: bool = _truthy(_setting(extra, "ws", "push", env="RUNONE_WS", default="1"))
        self._ws_safety_poll_seconds: float = _float_setting(
            extra,
            "ws_safety_poll_seconds",
            env="RUNONE_WS_SAFETY_POLL_SECONDS",
            default=WS_SAFETY_POLL_SECONDS_DEFAULT,
        )
        self._http: Optional["httpx.AsyncClient"] = None
        self._ingress_task: Optional[asyncio.Task] = None
        self._stopping = False
        # 拉取串行化：kick 触发的拉取与兜底拉取可能同时想跑，两条一起拉会让同一条消息
        # 走两次投递（服务端认领是原子的，能挡住重复处理，但没必要制造这种竞争）
        self._poll_lock = asyncio.Lock()
        self._ws_connected = False
        self._ws_last_session_seconds = 0.0
        self._kicks = 0
        # 内存游标：本会话已投递到的 seq（持久化在批二接 plugin_db，见 README「未做」）
        # inbox 游标：**不透明字符串**（服务端给 `<createdAt>|<id>`）。
        # 别自己拿 seq 当游标——seq 是会话内序号，跨会话比较会漏掉新会话的消息。
        self._cursor: str = ""
        self._dedup = MessageDeduplicator(max_size=DEDUP_MAX_SIZE, ttl_seconds=DEDUP_WINDOW_SECONDS)
        # chat_id(会话 id) → 当前正在回复的那条用户消息 id，send() 用它走 /reply 拿幂等
        self._reply_anchor: Dict[str, str] = {}
        # chat_id(会话 id) → **本轮**认领到的那条入站消息 id（整轮保留，只在下一条入站消息认领时覆盖）。
        # 与 `_reply_anchor` 分开：锚点是「这一笔要回写到哪条消息」，会被任何一笔写入用掉；
        # 而这一条是「这一轮在回谁」，草稿行在被前置写入挤掉锚点之后还要靠它挂回去。
        self._turn_inbound: Dict[str, str] = {}
        # chat_id(会话 id) → 该会话是否「AI 回复带语音」。开关注在服务端（会话列），
        # 随 inbox 投递下来；关掉时 send_voice 直接不发，连生成都省掉。
        self._voice_enabled: Dict[str, bool] = {}
        # 入站消息 id → 上次续租的时刻（单调钟）：typing 心跳顺带续租，别每 2s 打一次
        self._lease_touched: Dict[str, float] = {}
        # chat_id(会话 id) → 已上传但还没挂上文字的语音附件（见 send_voice：正文与音频合并成一条消息）。
        # 条目带 `inbound`（属于哪一轮入站消息）：语音**只与本轮正文合并**，跨轮的一律立刻单独投递
        # ——否则它会挂到下一问的回复上，且因为「手上有待合并语音就不开流式草稿」把下一轮的逐字流式
        # 整条挤掉（线上实测：一条半截带光标的气泡 + 两条内容重复的正文，一条有语音一条没有）。
        self._pending_voice: Dict[str, Dict[str, Any]] = {}
        # chat_id(会话 id) → 本轮**已经发出去的那条正文**（id / inbound / text）。
        # 用途：TTS 比正文回写慢，语音到达时这一轮往往已经定稿；此时把附件补挂到那条正文上
        # （`PATCH` 正文不动、只加附件），而不是再发一条同文消息。
        self._last_reply: Dict[str, Dict[str, Any]] = {}
        # chat_id(会话 id) → 本轮流式草稿的助手行 id（服务端 status='streaming' 那条）。
        # 有它就意味着「这一轮的回复已经长在界面上了」，后面的写入走编辑/定稿而不是新开一条。
        self._draft_rows: Dict[str, str] = {}

    # ---- 连接生命周期 -------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not HTTPX_AVAILABLE:
            logger.warning("[%s] httpx not installed. Run: pip install httpx", self.name)
            return False
        if not self._base_url or not self._token:
            logger.warning(
                "[%s] RUNONE_BASE_URL / RUNONE_TOKEN not configured — platform stays disconnected",
                self.name,
            )
            return False
        try:
            self._http = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=POLL_TIMEOUT_SECONDS,
                trust_env=self._trust_env,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Accept": "application/json",
                    "User-Agent": USER_AGENT,
                },
            )
            self._stopping = False
            self._ingress_task = asyncio.create_task(self._ingress_loop())
            self._mark_connected()
            if self._ws_enabled and WEBSOCKETS_AVAILABLE:
                logger.info(
                    "[%s] Connected — 推送面 %s（连不上自动退长轮询 %s/ai/inbox）",
                    self.name,
                    self._ws_url(),
                    self._base_url,
                )
            else:
                reason = "websockets 未安装" if not WEBSOCKETS_AVAILABLE else "RUNONE_WS=0"
                logger.info("[%s] Connected — 长轮询 %s/ai/inbox（%s）", self.name, self._base_url, reason)
            self._wire_plugin_handlers(None)  # ctx.register_platform_handler 钩子
            return True
        except Exception as exc:  # noqa: BLE001 — 连接失败要落成可读状态，不能抛出打断网关启动
            logger.error("[%s] Failed to connect: %s", self.name, exc, exc_info=True)
            await self._teardown()
            return False

    async def disconnect(self) -> None:
        self._stopping = True
        self._mark_disconnected()
        task, self._ingress_task = self._ingress_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                logger.debug("[%s] ingress task ended with error during shutdown", self.name, exc_info=True)
        await self._teardown()
        self._ws_connected = False
        self._dedup = MessageDeduplicator(max_size=DEDUP_MAX_SIZE, ttl_seconds=DEDUP_WINDOW_SECONDS)
        self._reply_anchor.clear()
        self._turn_inbound.clear()
        self._voice_enabled.clear()
        self._lease_touched.clear()
        self._pending_voice.clear()
        self._last_reply.clear()
        self._draft_rows.clear()
        logger.info("[%s] Disconnected", self.name)

    async def _teardown(self) -> None:
        client, self._http = self._http, None
        if client is not None:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001
                logger.debug("[%s] closing HTTP client failed", self.name, exc_info=True)

    # ---- 入站：推送（WSS）优先 + 长轮询兜底 ---------------------------------

    async def _ingress_loop(self) -> None:
        """入站总调度，一直跑到 disconnect()。

        两种模式**契约与游标语义完全一样**（都走 `/ai/inbox`），区别只在「什么时候去拉」：
        推送模式（连 `/ai/inbox/ws`，消息落库即 kick）优先；WS 不可用 / 连续失败时退回长轮询
        （老行为：挂着 wait=25 等）。功能不降级，只是回到「每秒重查」的老成本上。
        """
        ws_mode = self._ws_enabled and WEBSOCKETS_AVAILABLE
        if self._ws_enabled and not WEBSOCKETS_AVAILABLE:
            logger.info("[%s] websockets 未安装 —— 用长轮询（装上它即可开推送模式）", self.name)
        ws_failures = 0
        ws_retry_at = 0.0  # 冷却期：连续失败后先回长轮询一段时间再试
        backoff = BACKOFF_START_SECONDS

        while not self._stopping:
            if ws_mode and time.monotonic() >= ws_retry_at:
                reason: Optional[str] = None
                try:
                    await self._ws_session()
                except asyncio.CancelledError:
                    raise
                except WsUnsupported as exc:
                    # 服务端还没部署推送面：不是故障，长期用长轮询
                    logger.info("[%s] 服务端没有推送面（%s）—— 长期用长轮询", self.name, exc)
                    ws_mode = False
                    continue
                except Exception as exc:  # noqa: BLE001
                    reason = f"{type(exc).__name__}: {exc}"
                lasted = self._ws_last_session_seconds
                if reason is None and lasted >= WS_MIN_SESSION_SECONDS:
                    ws_failures = 0
                    backoff = BACKOFF_START_SECONDS
                    continue
                ws_failures += 1
                logger.warning(
                    "[%s] 推送连接结束（%s，持续 %.1fs）；%.1fs 后重连",
                    self.name, reason or "服务端关闭", lasted, backoff,
                )
                if ws_failures >= WS_FALLBACK_AFTER_FAILURES:
                    logger.info(
                        "[%s] 推送面连续 %d 次连不上 —— 先回长轮询 %.0fs（%s/ai/inbox 一直可用）",
                        self.name, ws_failures, WS_RETRY_AFTER_SECONDS, self._base_url,
                    )
                    ws_retry_at = time.monotonic() + WS_RETRY_AFTER_SECONDS
                await asyncio.sleep(backoff + random.uniform(0, backoff / 4))
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
                # 断开期间落下的消息：立刻对一次账（失败只记日志，交给下一拍）
                await self._safe_poll_once(0)
                continue

            # ---- 长轮询模式（老行为）----
            try:
                await self._poll_once(POLL_WAIT_SECONDS)
                backoff = BACKOFF_START_SECONDS  # 一轮成功就重置退避
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                # 带上异常类型：httpx 的 ReadError/RemoteProtocolError 常常是空消息，
                # 只打 str(exc) 会得到「poll failed ()」这种没法定位的日志。
                logger.warning(
                    "[%s] poll failed (%s: %s); retrying in %.1fs",
                    self.name, type(exc).__name__, exc, backoff,
                )
                await asyncio.sleep(backoff + random.uniform(0, backoff / 4))
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)

    async def _ws_session(self) -> None:
        """一次推送会话：连上 → 先对一次账 → 之后每个 kick 拉一次；断开即返回或抛异常。

        保活走 `websockets` 的**协议级 ping/pong**（服务端 runtime 自动回，既不唤醒 Durable
        Object 也不产生计费请求）：一个 ping 周期内没等到 pong 就抛 ConnectionClosed，这就是
        「链路已经死了」的判据 —— 比另加一条应用级心跳省得多。
        """
        if websockets is None:  # pragma: no cover — 调用方已按 WEBSOCKETS_AVAILABLE 判过
            raise WsUnsupported("websockets 未安装")
        url = self._ws_url()
        if self._cursor:
            # 游标只是给服务端排障展示用（推送本身不带载荷），带上可以看到「这条连接拉到哪了」
            url = f"{url}?cursor={quote(self._cursor, safe='')}"
        started = time.monotonic()
        try:
            async with websockets.connect(
                url,
                additional_headers={"Authorization": f"Bearer {self._token}"},
                user_agent_header=USER_AGENT,
                open_timeout=WS_OPEN_TIMEOUT_SECONDS,
                ping_interval=WS_PING_INTERVAL_SECONDS,
                ping_timeout=WS_PING_TIMEOUT_SECONDS,
                close_timeout=WS_CLOSE_TIMEOUT_SECONDS,
                max_size=WS_MAX_FRAME_BYTES,
            ) as ws:
                self._ws_connected = True
                logger.info(
                    "[%s] 推送模式已连上 %s（消息落库即到；每 %.0fs 兜底拉一次）",
                    self.name, url.split("?")[0], self._ws_safety_poll_seconds,
                )
                await self._safe_poll_once(0)  # 断线 / 重连期间落下的消息，连上先对一次账
                safety = asyncio.create_task(self._safety_loop())
                try:
                    async for raw in ws:
                        if not self._is_kick(raw):
                            continue
                        self._kicks += 1
                        logger.debug("[%s] push kick #%d → 拉一次 inbox", self.name, self._kicks)
                        await self._safe_poll_once(0)
                finally:
                    safety.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await safety
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            status = getattr(getattr(exc, "response", None), "status_code", None)
            # 401/403 是凭证问题（长轮询那边会报同一句话），只有「根本没这条路由」才算不支持
            if status in (404, 405, 426, 501):
                raise WsUnsupported(f"HTTP {status}") from exc
            raise
        finally:
            self._ws_connected = False
            self._ws_last_session_seconds = max(0.1, time.monotonic() - started)

    @staticmethod
    def _is_kick(raw: Any) -> bool:
        """推送帧只认 `{"type":"kick"}`；其它帧一律忽略（不猜语义，将来加帧型时再改这里）。"""
        try:
            frame = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8", "ignore"))
        except Exception:  # noqa: BLE001
            return False
        return isinstance(frame, dict) and frame.get("type") == "kick"

    async def _safety_loop(self) -> None:
        """兜底对账：挂着推送也定期单次拉一次（wait=0）。

        三个作用：① kick 万一丢了（DO 重启、连接刚断、服务端 bug）消息不会卡在队列里；
        ② 给服务端「Hermes 在线」打点（判在线的有效期 150s）；③ 顺手跑服务端的 stale 扫尾
        （认领后没回写的消息会变成界面上一句看得见的失败）。
        成本：每 60s 一次查询 —— 老的长轮询是每秒一次（30,556 次/天、≈466 万行读/天，实测）。
        """
        interval = self._ws_safety_poll_seconds
        if interval <= 0:
            logger.info("[%s] 兜底拉取已关闭（RUNONE_WS_SAFETY_POLL_SECONDS=0）—— 只靠 kick", self.name)
            return
        while not self._stopping:
            await asyncio.sleep(interval)
            await self._safe_poll_once(0)

    async def _safe_poll_once(self, wait_seconds: float) -> int:
        """拉一次；失败只记日志 —— 不能让一次网络抖动把推送会话打断（重连交给上层）。"""
        try:
            return await self._poll_once(wait_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] inbox fetch failed (%s: %s)", self.name, type(exc).__name__, exc)
            return 0

    def _ws_url(self) -> str:
        """base_url → 推送面地址（http→ws / https→wss），路径与服务端路由一致。"""
        base = self._base_url
        if base.startswith("https://"):
            base = "wss://" + base[len("https://"):]
        elif base.startswith("http://"):
            base = "ws://" + base[len("http://"):]
        return f"{base}/ai/inbox/ws"

    async def _poll_once(self, wait_seconds: float = POLL_WAIT_SECONDS) -> int:
        """拉一次并投递。串行化：kick 触发的拉取与兜底拉取不许并发跑。

        `wait_seconds=0` = 服务端做一次查询就返回（推送模式就是这个用法）；
        `wait_seconds=25` = 老的长轮询（挂着等，到点空返回）。
        """
        async with self._poll_lock:
            return await self._fetch_and_deliver(wait_seconds)

    async def _fetch_and_deliver(self, wait_seconds: float) -> int:
        assert self._http is not None
        # 兜底：等不到文字的语音附件，超窗口后单独发一条（宁可两条，也不丢音频）
        await self._flush_stale_voice()
        params: Dict[str, Any] = {"wait": wait_seconds, "limit": POLL_BATCH_LIMIT}
        if self._cursor:
            params["cursor"] = self._cursor
        res = await self._http.get("/ai/inbox", params=params)
        if res.status_code == 401:
            logger.error("[%s] RunOne rejected the PAT (401) — check RUNONE_TOKEN", self.name)
            raise RuntimeError("runone_pat_rejected")
        if res.status_code != 200:
            raise RuntimeError(f"inbox returned HTTP {res.status_code}")

        body = res.json() or {}
        items: List[Dict[str, Any]] = body.get("data") or []
        next_cursor = body.get("nextCursor")
        delivered = 0
        for item in items[:POLL_BATCH_LIMIT]:
            if await self._deliver(item):
                delivered += 1
        # 游标只在整批投递（或明确跳过）之后推进：中途异常就不会"吃掉"消息
        if isinstance(next_cursor, str) and next_cursor > self._cursor:
            self._cursor = next_cursor
        elif items:
            # 服务端没给游标时用最后一条的 payload 自建（createdAt|id，字典序＝时间序）
            last = items[-1]
            fallback = f"{last.get('createdAt') or ''}|{last.get('id') or ''}"
            if fallback > self._cursor:
                self._cursor = fallback
        return delivered

    async def _deliver(self, item: Dict[str, Any]) -> bool:
        """认领 → 造 MessageEvent → 交给网关处理器。返回是否真的投递了一条。"""
        conversation_id = str(item.get("conversationId") or item.get("conversation_id") or "")
        message_id = str(item.get("id") or "")
        text = str(item.get("text") or "")
        if not conversation_id or not message_id or not text:
            logger.warning("[%s] skipping malformed inbox item: %s", self.name, sorted(item.keys()))
            return False
        if self._dedup.is_duplicate(message_id):
            logger.debug("[%s] duplicate message %s ignored", self.name, message_id)
            return False

        # 先认领、后干活：租约过期的消息服务端会重回队列，谁认领到谁负责（设计文档 §5-2/5-4）
        claim = await self._post(f"/ai/messages/{message_id}/claim", {})
        if claim is None:
            logger.info("[%s] claim rejected for %s (already taken) — skipping", self.name, message_id)
            return False

        author_id = item.get("authorUserId") or item.get("author_user_id") or ""
        author_name = item.get("authorName") or item.get("author_name")
        self._reply_anchor[conversation_id] = message_id
        self._turn_inbound[conversation_id] = message_id
        # 新一轮开始：上一轮没等到正文的语音**不能再等下去**（等下去就是跨轮串台 —— 它会挂到
        # 这一问的回复上，并让这一轮的流式草稿开不出来）。立刻单独投递，宁可两条也不能错挂。
        await self._drop_stale_turn_voice(conversation_id, message_id)
        self._last_reply.pop(conversation_id, None)
        # 「AI 回复带语音」由服务端按会话存（默认开）；旧后端不带这个字段时按开处理
        self._voice_enabled[conversation_id] = item.get("voiceReplies") is not False

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            user_id=str(author_id) or None,
            user_name=author_name,
            source=self.build_source(
                chat_id=conversation_id,
                chat_name=str(item.get("conversationTitle") or conversation_id),
                chat_type="dm",
                user_id=str(author_id) or None,
                user_name=author_name,
                message_id=message_id,
            ),
            message_id=message_id,
            raw_message=item,
        )
        logger.info("[%s] inbound from conversation %s (msg %s)", self.name, conversation_id, message_id)
        await self.handle_message(event)
        return True

    # ---- 出站：回复 / typing / 会话信息 ------------------------------------

    async def _write_reply(
        self, anchor: Optional[str], chat_id: str, payload: Dict[str, Any]
    ) -> tuple:
        """写一次 `/reply`（没有锚点时写 `/conversations/:id/messages`），409 时重新认领再试一次。

        单独抽出来是因为流式那条路要打同一条口子三次（首片草稿 / 后续分片走 PATCH / 定稿兜底），
        三处的「幂等 + 409 重认领」口径必须一份实现。
        """
        if anchor:
            path = f"/ai/messages/{anchor}/reply"
        else:
            path = f"/ai/conversations/{chat_id}/messages"
            payload.setdefault("role", "assistant")
        status, data = await self._request_status("POST", path, payload)
        if status == 409 and anchor:
            # 租约过期（回合比 90s 长）：服务端允许重新认领「租约已过期的 processing」消息，
            # 认领到就立刻重试一次；幂等键没变，重试不会写第二条回复。
            logger.info("[%s] reply rejected (409) — re-claiming %s and retrying once", self.name, anchor)
            if await self._post(f"/ai/messages/{anchor}/claim", {}) is not None:
                status, data = await self._request_status("POST", path, payload)
        return status, data

    async def _open_draft(
        self, chat_key: str, anchor: Optional[str], inbound: Optional[str], content: str
    ) -> Optional[SendResult]:
        """开一条 `status='streaming'` 的草稿行并记住它（返回 None = 这一轮只能整段发送）。

        两条路，按代价排序：

        ① `POST /ai/messages/<入站消息 id>/reply {streaming:true}` —— 正路：入站消息还在可回写
           状态时走它，租约、状态流转、定稿后放租约都由服务端那一套管（`anchor`）。
        ② `POST /ai/conversations/<会话 id>/messages {role:assistant, streaming:true,
           replyToMessageId:<入站消息 id>}` —— 兜底：**这一轮里先落的任何一笔助手写入**
           （工具进度气泡、提示文本）都会把入站消息置成 `replied`，之后 ① 就被 409 挡住。

        为什么必须有 ②：以前这里只有 ①，开不出草稿就退回「整段发送」——而网关的逐字流式是
        「先发一片、再编辑同一条」，于是每一片编辑都打在一条**非 streaming** 的行上。旧服务端对
        这种编辑回 200「成功但什么都不改」，网关便以为正文已经在屏幕上，把这一轮的完整回复
        抑制掉：线上实测气泡停在 `今天 2026-09-22（ ▉`，783 字的回答再也没落库。
        ② 不依赖入站可写性，只要手上还有本轮入站消息 id（`_turn_inbound`）就能开出草稿。

        两条都失败（旧服务端不认识 ②）时返回 None：调用方照旧整段发送 —— 宁可不要逐字，
        也不能把这条回复弄丢。
        """
        if anchor:
            status, draft = await self._write_reply(anchor, chat_key, {"text": content, "streaming": True})
            draft_id = str((draft or {}).get("id") or "")
            if draft_id:
                self._draft_rows[chat_key] = draft_id
                logger.info("[%s] 流式草稿已开（%s，seq %s）", self.name, draft_id, (draft or {}).get("seq"))
                return SendResult(success=True, message_id=draft_id, raw_response=draft)
            logger.info("[%s] /reply 开草稿被拒（status=%s）——改用会话消息端点再试一次", self.name, status)
        payload: Dict[str, Any] = {"role": "assistant", "text": content, "streaming": True}
        if inbound:
            payload["replyToMessageId"] = inbound
        status, draft = await self._request_status(
            "POST", f"/ai/conversations/{chat_key}/messages", payload, quiet=True
        )
        draft_id = str((draft or {}).get("id") or "")
        if not draft_id or str((draft or {}).get("status") or "") != "streaming":
            logger.warning("[%s] 草稿创建失败（status=%s）——本轮退回整段发送", self.name, status)
            return None
        self._draft_rows[chat_key] = draft_id
        logger.info("[%s] 流式草稿已开（%s，seq %s，走会话消息端点）", self.name, draft_id, (draft or {}).get("seq"))
        return SendResult(success=True, message_id=draft_id, raw_response=draft)

    async def _send_notice(self, chat_key: str, content: str) -> SendResult:
        """写一条「不是这一轮回复本身」的助手消息（工具进度、审批/提示文本、分段尾巴）。

        为什么不走 `/reply`：
        - `/reply` 会把入站消息置成 `replied`（＝「这条已经答过了」）。中间态文本不是答案，
          置早了后面的流式首片就被 409 挡在 `/reply` 外面；
        - 锚点（入站消息 id）要留给这一轮真正的回复，不能被中间态写入消耗掉。
        """
        status, data = await self._request_status(
            "POST", f"/ai/conversations/{chat_key}/messages", {"role": "assistant", "text": content}
        )
        if data is None:
            logger.warning("[%s] 中间态消息写入失败（status=%s）", self.name, status)
            return SendResult(success=False, error="RunOne 写入失败（详见网关日志）", retryable=True)
        return SendResult(success=True, message_id=str(data.get("id") or ""), raw_response=data)

    def _release_turn(self, chat_key: str) -> None:
        """这一轮的回复已经写完了：放掉草稿锚点（下一条入站消息认领时会重新记上）。"""
        self._turn_inbound.pop(chat_key, None)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        chat_key = str(chat_id)
        anchor = self._reply_anchor.get(chat_key)
        # 锚点可能已经被本轮前面某笔写入用掉（工具进度气泡等），但「这一轮在回谁」另记一份
        inbound = self._turn_inbound.get(chat_key) or anchor
        meta = metadata or {}

        # ① 流式首片：网关带着 `expect_edits` 发来的分片 → 先建一条 `status='streaming'` 的草稿行，
        #    之后每片由 edit_message 改这一行（服务端的 draft 字段单给前端，增量里看不到它）。
        #    **手上有待合并的语音也要照开**：以前这里写成「有待合并语音就不开草稿」，
        #    于是上一轮遗留的语音会把下一轮的逐字流式整条挤掉（首片被当正式回复发出去、后续分片全 409）。
        #    合并只发生在最后那一拍（`send()` 正文写入 / `edit_message(finalize=True)`）。
        if bool(meta.get("expect_edits")) and inbound:
            draft = await self._open_draft(chat_key, anchor, inbound, content)
            if draft is not None:
                return draft

        # ② 中间态发送（工具进度 / 审批提示 / 分段尾巴）：不是这一轮的回复本身，
        #    不消耗锚点、也不把入站消息置成 replied（理由见 `_send_notice`）。
        if bool(meta.get("_interim_send")):
            return await self._send_notice(chat_key, content)

        # clientMsgId 幂等键：网络抖动重试时服务端复用同一条助手消息（设计文档 §5-3）。
        # 已经开过草稿的轮次**不带**这个键，让服务端用可推导的 `reply:<入站消息 id>` ——
        # 这样这一笔会命中那条草稿行并把它定稿，而不是在旁边再冒出一条新消息。
        payload: Dict[str, Any] = {"text": content}
        draft_open = bool(self._draft_rows.get(chat_key))
        if not draft_open:
            payload["clientMsgId"] = f"hermes-{uuid.uuid4().hex}"

        # 语音先到、正文随后：**合并成一条消息**（正文 + 音频附件）。
        # 分成两条时，只要文字那条失败，界面上就只剩一个没有正文的语音条 —— 看着像「没回复」。
        # **只合并本轮的语音**：`inbound` 对不上说明它是上一轮遗留（上一轮正文早已定稿，答案已经发出去了），
        # 挂到这一轮的正文上就是错挂，立刻单独投递。
        voice = self._pending_voice.pop(chat_key, None)
        if voice and str(voice.get("inbound") or "") != str(inbound or ""):
            await self._post_voice_alone(chat_key, voice)
            voice = None
        if voice and voice.get("attachmentId"):
            payload["attachmentIds"] = [voice["attachmentId"]]

        status, data = await self._write_reply(anchor, chat_key, payload)
        if data is None:
            if voice:  # 正文没写成功：把音频还给兜底逻辑，稍后单独发，别弄丢
                voice["at"] = time.monotonic()
                self._pending_voice[chat_key] = voice
            return SendResult(success=False, error="RunOne 写入失败（详见网关日志）", retryable=True)
        self._draft_rows.pop(chat_key, None)
        self._reply_anchor.pop(chat_key, None)  # 一条入站消息一条回复
        self._lease_touched.pop(anchor, None)
        # 记下「本轮已经发出去的那条正文」：语音若比它更晚到，附件补挂到这一条上（不再发同文消息）
        self._last_reply[chat_key] = {
            "id": str(data.get("id") or ""),
            "inbound": str(inbound or ""),
            "text": content,
        }
        if draft_open or bool(meta.get("notify")):
            # 这一笔就是本轮的回复（定稿了那条草稿行 / 网关标了 notify 的终答）→ 整轮收口
            self._release_turn(chat_key)
        return SendResult(success=True, message_id=str(data.get("id") or ""), raw_response=data)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """流式分片/定稿：`PATCH /ai/messages/:id` 改那条草稿行的正文。

        `finalize=True` 是这一轮的最后一次写入 —— 服务端在同一个请求里把草稿行转成 `replied`、
        并把入站消息置 replied（释放租约），所以这一步不做就等于「回复写完了但没人知道」。
        语音附件在这一步并上（正文 + 音频 = 一条消息），与 send() 的口径一致。
        """
        if self._http is None:
            return SendResult(success=False, error="adapter 未连接")
        chat_key = str(chat_id)
        payload: Dict[str, Any] = {"text": content, "finalize": bool(finalize)}
        inbound = str(self._turn_inbound.get(chat_key) or "")
        voice = self._pending_voice.pop(chat_key, None) if finalize else None
        if voice and str(voice.get("inbound") or "") != inbound:
            # 上一轮遗留的语音：这一轮的正文不是它的答案，别并进来（错挂比多一条更糟）
            await self._post_voice_alone(chat_key, voice)
            voice = None
        if voice and voice.get("attachmentId"):
            payload["attachmentIds"] = [voice["attachmentId"]]

        status, data = await self._request_status("PATCH", f"/ai/messages/{message_id}", payload)
        if data is None:
            if voice:
                voice["at"] = time.monotonic()
                self._pending_voice[chat_key] = voice
            if status == 409:
                # 这条草稿已经不是 `streaming` 了（被扫尾定稿、或已被别的写入定稿）：这一片编辑作废。
                # 丢掉草稿行登记，让网关的兜底「整段发送」新开一条、把没显示出来的部分补上。
                # （旧服务端在这里回 200「成功但什么都不改」，网关会以为正文已经在屏幕上 ——
                #   那一轮的完整回复就被抑制掉，只剩半截气泡。）
                self._draft_rows.pop(chat_key, None)
                logger.info(
                    "[%s] 草稿行 %s 已不是 streaming（409）——丢掉草稿登记，等网关整段兜底",
                    self.name, message_id,
                )
            # 其余失败（超时、5xx）不动 `_draft_rows` / 锚点：网关会退回「整段发送」，而那条路凭
            # `reply:<入站消息 id>` 命中同一行把它定稿，不会留下永远在长的脏草稿。
            logger.warning(
                "[%s] edit_message 失败（status=%s，finalize=%s）——交给整段发送兜底",
                self.name, status, finalize,
            )
            return SendResult(success=False, error="RunOne 编辑失败（详见网关日志）", retryable=True)
        if finalize:
            self._draft_rows.pop(chat_key, None)
            anchor = self._reply_anchor.pop(chat_key, None)
            self._lease_touched.pop(anchor, None)
            self._release_turn(chat_key)
            # 记下本轮已定稿的那条正文：语音更晚到时补挂到它上面（见 `send_voice`）
            self._last_reply[chat_key] = {
                "id": str(data.get("id") or message_id),
                "inbound": inbound,
                "text": content,
            }
        return SendResult(success=True, message_id=str(data.get("id") or message_id), raw_response=data)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        """语音回复 = **音频附件**（照抄 Telegram/Discord 的路线，不做实时流）。

        顺序：预签名 → 直传 COS → complete → 往会话写一条 assistant 消息挂上附件。
        为什么不走 `/ai/messages/:id/reply` 那个锚点：语音常常**先于**文本回复发出，
        用 reply 锚点会把入站消息提前置成 replied，随后真正的文本回复就被幂等键判成重复、发不出去。
        """
        if self._http is None:
            return SendResult(success=False, error="adapter 未连接")
        # 会话里关掉了「AI 回复带语音」：不发。返回 success 而不是失败——
        # 这是用户的选择，不是投递出错，网关不该把它报成错误。
        if not self._voice_enabled.get(str(chat_id), True):
            logger.debug("[%s] voice replies disabled for %s — skipped", self.name, chat_id)
            return SendResult(success=True, message_id="")
        try:
            source = Path(audio_path)
            audio = source.read_bytes()
        except OSError as exc:
            logger.warning("[%s] voice: 读取音频失败：%s", self.name, exc)
            return SendResult(success=False, error=f"读取音频失败：{exc}")
        if not audio:
            return SendResult(success=False, error="音频为空")

        name = source.name or "voice.mp3"
        presign = await self._post(
            "/attachments/presign",
            # conversationId 是必须的：AI 对话的附件没有项目归属，服务端按会话可见性授权
            {"name": name, "size": len(audio), "mimeType": _audio_mime(name), "conversationId": chat_id},
        )
        if not presign or not presign.get("uploadUrl"):
            logger.warning("[%s] voice: 预签名失败（%s）", self.name, name)
            return SendResult(success=False, error="附件预签名失败", retryable=True)
        attachment_id = str(presign.get("id") or "")
        try:
            res = await self._http.put(
                presign["uploadUrl"], content=audio, headers=presign.get("headers") or {}
            )
            res.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] voice: 上传 COS 失败：%s", self.name, exc)
            return SendResult(success=False, error="音频上传失败", retryable=True)
        if not attachment_id or await self._request("PATCH", f"/attachments/{attachment_id}/complete", {}) is None:
            return SendResult(success=False, error="附件 complete 失败", retryable=True)

        # 正文随后由 send() 一起写：**正文 + 音频附件 = 一条消息**。
        # 为什么改：以前语音单独写一条消息，回写失败时就留下「只有语音、没有正文」的假回复
        # （生产实测：用户以为 Hermes 没回）。合并后要么整条到，要么整条不到。
        # 同一个会话里上一轮还没合并的语音先落地，避免被这一条顶掉。
        chat_key = str(chat_id)
        await self._flush_stale_voice(chat_key, force=True)
        # TTS 比正文慢：语音常常**晚于这一轮的正文**到达，甚至这一轮已经定稿收口。
        # 那种情况下不能再等「下一笔正文」——等下去要么被下一轮吞掉（错挂到别的问答上），
        # 要么让下一轮再发一条同文消息（线上实测：同一条回复出现两条，一条带语音一条不带）。
        # 正确落点是本轮那条正文：**只补附件、正文不动**。
        if await self._attach_voice_to_last_reply(chat_key, attachment_id):
            return SendResult(success=True, message_id="", raw_response={"attachmentId": attachment_id})
        self._pending_voice[chat_key] = {
            "attachmentId": attachment_id,
            "caption": (caption or "").strip(),
            "at": time.monotonic(),
            "inbound": str(self._turn_inbound.get(chat_key) or ""),
        }
        logger.info("[%s] voice: 附件已上传 %s（%d 字节），等正文合并成一条消息", self.name, attachment_id, len(audio))
        return SendResult(success=True, message_id="", raw_response={"attachmentId": attachment_id})

    async def _flush_stale_voice(self, chat_id: Optional[str] = None, *, force: bool = False) -> None:
        """兜底出口：等不到正文的语音附件，按旧行为单独写一条消息（宁可两条，也不能丢音频）。"""
        if self._http is None or not self._pending_voice:
            return
        now = time.monotonic()
        for cid, item in list(self._pending_voice.items()):
            if chat_id is not None and cid != chat_id:
                continue
            if not force and now - float(item.get("at") or 0.0) < VOICE_MERGE_TIMEOUT_SECONDS:
                continue
            self._pending_voice.pop(cid, None)
            await self._post_voice_alone(cid, item)

    async def _post_voice_alone(self, chat_key: str, item: Dict[str, Any]) -> None:
        """把一条无法与正文合并的语音单独投递（宁可两条，也不能丢音频）。"""
        data = await self._request(
            "POST",
            f"/ai/conversations/{chat_key}/messages",
            {
                "role": "assistant",
                "text": str(item.get("caption") or ""),
                "attachmentIds": [item["attachmentId"]],
                "clientMsgId": f"hermes-voice-{uuid.uuid4().hex}",
            },
        )
        if data is None:
            logger.warning(
                "[%s] voice: 单独投递语音附件 %s 失败（附件已上传，未挂到消息）", self.name, item.get("attachmentId")
            )
        else:
            logger.info(
                "[%s] voice: 没等到正文，已单独投递语音附件 %s", self.name, item.get("attachmentId")
            )

    async def _drop_stale_turn_voice(self, chat_key: str, inbound: str) -> None:
        """新一轮开始时，把上一轮遗留的待合并语音就地投递掉。

        为什么不能留：留下的那条会被「这一轮的正文」当成待合并附件取走 —— 上一问的答案挂到
        这一问的回复上（错挂），而且它还会把这一轮的流式草稿挤掉（旧 `send()` 的判定条件）。
        """
        item = self._pending_voice.get(chat_key)
        if not item or str(item.get("inbound") or "") == str(inbound or ""):
            return
        self._pending_voice.pop(chat_key, None)
        logger.info(
            "[%s] voice: 上一轮遗留的附件 %s 不再跨轮等待 —— 立刻单独投递", self.name, item.get("attachmentId")
        )
        await self._post_voice_alone(chat_key, item)

    async def _attach_voice_to_last_reply(self, chat_key: str, attachment_id: str) -> bool:
        """把语音补挂到**本轮已经发出去的那条正文**上（正文一字不动，只加附件）。

        什么时候走到这里：TTS 合成慢于正文回写，`send_voice` 被调用时这一轮已经定稿收口
        （`_turn_inbound` 已释放、`_last_reply` 里还剩本轮那条正文）。这时如果按老办法压进
        `_pending_voice` 等下一笔正文，就会出现「同一条回复两条、一条带语音一条不带」。

        服务端对这一支的口径：已定稿行**正文相同、只补附件** → 200 并把附件并进去；
        正文不同仍 409。旧服务端会回 200「成功但什么都不改」，所以这里**必须回读附件列表**
        来判定真的挂上了，不能只看 HTTP 状态（否则语音会静默消失）。
        """
        last = self._last_reply.get(chat_key) or {}
        reply_id = str(last.get("id") or "")
        if not reply_id or self._turn_inbound.get(chat_key):
            return False  # 本轮还没收口（正文随后会自己带附件）或没有可挂的正文
        status, data = await self._request_status(
            "PATCH",
            f"/ai/messages/{reply_id}",
            {"text": str(last.get("text") or ""), "attachmentIds": [attachment_id]},
            quiet=True,
        )
        attached = data is not None and attachment_id in (data.get("attachmentIds") or [])
        if attached:
            logger.info(
                "[%s] voice: 附件 %s 已补挂到本轮回复 %s（正文不动）", self.name, attachment_id, reply_id
            )
        else:
            logger.info(
                "[%s] voice: 补挂到本轮回复失败（status=%s）——改为单独投递", self.name, status
            )
        return attached

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """typing 是心跳式的：服务端 5s 过期，网关的 _keep_typing 每 2s 会调到这里。

        顺带**续租**：认领租约固定 90s，而一次回合可能跑好几分钟；不续租的结果就是
        「回复生成完了、回写却被判租约过期」→ 409 → 用户什么也看不到（生产实测 91s 被拒）。
        搭在 typing 上是因为它本来每 2s 一次、且只在处理期间发；这里按 LEASE_RENEW_SECONDS 节流。
        """
        if self._http is None:
            return
        await self._post(f"/ai/conversations/{chat_id}/typing", {})
        anchor = self._reply_anchor.get(str(chat_id))
        if not anchor:
            return
        now = time.monotonic()
        if now - self._lease_touched.get(anchor, 0.0) < LEASE_RENEW_SECONDS:
            return
        self._lease_touched[anchor] = now
        status, body = await self._request_status("POST", f"/ai/messages/{anchor}/heartbeat", {}, quiet=True)
        if status == 404:
            logger.debug("[%s] heartbeat endpoint not in this server build (404) — lease relies on claim only", self.name)
        elif status == 200 and isinstance(body, dict) and body.get("ok") is False:
            logger.debug("[%s] heartbeat: %s is already %s", self.name, anchor, body.get("status"))

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        data = await self._request("GET", f"/ai/conversations/{chat_id}")
        if not data:
            return {"name": chat_id, "type": "dm", "chat_id": chat_id}
        return {
            "name": str(data.get("title") or chat_id),
            "type": "dm",
            "chat_id": chat_id,
            "project_id": data.get("projectId"),
        }

    # ---- HTTP 小工具 -------------------------------------------------------

    async def _request(
        self, method: str, path: str, payload: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        return (await self._request_status(method, path, payload))[1]

    async def _request_status(
        self, method: str, path: str, payload: Optional[Dict[str, Any]] = None, *, quiet: bool = False
    ) -> tuple:
        """底层请求：返回 `(status_code, body)`。

        为什么要把状态码留给调用方：`/reply` 的 409 是**有意义的信号**（租约过期，可重新认领后重试），
        而旧的写法把状态码吞掉了，只回一个 None，调用方只能当成「写入失败」放弃。
        `quiet=True` 用于探测式调用（例如给旧服务端打 heartbeat 会 404），不刷警告日志。
        """
        if self._http is None:
            return 0, None
        try:
            res = await self._http.request(method, path, json=payload)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] %s %s failed: %s", self.name, method, path, exc)
            return 0, None
        if res.status_code >= 400:
            if not quiet or res.status_code >= 500:
                logger.warning("[%s] %s %s → HTTP %s: %s", self.name, method, path, res.status_code, res.text[:200])
            return res.status_code, None
        try:
            return res.status_code, res.json()
        except Exception:  # noqa: BLE001 — 204 之类没有 body 也算成功
            return res.status_code, {}

    async def _post(self, path: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return await self._request("POST", path, payload)


# ---- 进程外投递（cron 用）--------------------------------------------------

async def _standalone_send(
    pconfig: Any,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Any = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """网关进程之外（cron 独立进程）直接往某个 RunOne 会话写一条 assistant 消息。

    签名按 `PlatformEntry.standalone_sender_fn` 的契约；没有它，`deliver=runone` 的 cron
    作业会返回 `No live adapter for platform 'runone'`。
    """
    if not HTTPX_AVAILABLE:
        return {"error": "httpx 不可用"}
    extra = getattr(pconfig, "extra", None) or {}
    base_url = _setting(extra, "base_url", "baseUrl", env="RUNONE_BASE_URL").rstrip("/")
    token = _setting(extra, "token", env="RUNONE_TOKEN")
    if not base_url or not token:
        return {"error": "RUNONE_BASE_URL / RUNONE_TOKEN 未配置"}
    payload: Dict[str, Any] = {
        "role": "assistant",
        "text": message,
        "clientMsgId": f"hermes-cron-{uuid.uuid4().hex}",
    }
    try:
        async with httpx.AsyncClient(
            base_url=base_url,
            timeout=30.0,
            trust_env=_setting(extra, "use_system_proxy", "useSystemProxy", env="RUNONE_USE_SYSTEM_PROXY", default="0").lower()
            in {"1", "true", "yes", "on"},
            headers={"Authorization": f"Bearer {token}", "User-Agent": USER_AGENT},
        ) as client:
            res = await client.post(f"/ai/conversations/{chat_id}/messages", json=payload)
    except Exception as exc:  # noqa: BLE001
        return {"error": f"请求 RunOne 失败：{exc}"}
    if res.status_code >= 400:
        return {"error": f"RunOne 返回 HTTP {res.status_code}: {res.text[:200]}"}
    try:
        body = res.json() or {}
    except Exception:  # noqa: BLE001
        body = {}
    return {"success": True, "message_id": str(body.get("id") or "")}


# ---- 插件入口 -------------------------------------------------------------

def check_requirements() -> bool:
    """被动探测：依赖是否可用（不能在这里装东西）。"""
    return HTTPX_AVAILABLE


def is_connected(config: Any = None) -> bool:
    """env-only 配置也要在 `hermes gateway status` 里正确显示已配置。"""
    from gateway.platforms._shared import env_is_connected

    return env_is_connected("RUNONE_BASE_URL", "RUNONE_TOKEN")(config)


def validate_config(config: Any = None) -> bool:
    """配置是否合法。

    ⚠ **返回 True 才算合法**：Hermes 网关把本钩子当布尔谓词用
    （`if not entry.validate_config(config): logger.warning("config validation failed"); return None`）。
    早先写成「返回错误文案表示不合法、正常返回 None」，结果配置正确时反而被判失败、
    适配器从来没被创建过（`Gateway running with 4 platform(s)`、日志里只有
    「Platform 'RunOne' config validation failed」）。所以这里只回 bool，原因自己打日志。
    """
    import os

    base = (os.getenv("RUNONE_BASE_URL") or "").strip()
    token = (os.getenv("RUNONE_TOKEN") or "").strip()
    if base.startswith("http://") and "127.0.0.1" not in base and "localhost" not in base:
        logger.warning(
            "[%s] RUNONE_BASE_URL 用明文 http 且不是本机地址 —— 请用 https（PAT 会明文过网）", PLATFORM_NAME
        )
        return False
    if token and not token.startswith("rn_"):
        logger.warning("[%s] RUNONE_TOKEN 看起来不是 RunOne 的 PAT（应以 rn_ 开头）", PLATFORM_NAME)
        return False
    return True


def _env_enablement() -> Dict[str, Any]:
    """env-only 配置：把 env 里的值种进 PlatformConfig.extra，状态页才看得到。"""
    import os

    extra: Dict[str, Any] = {}
    base = (os.getenv("RUNONE_BASE_URL") or "").strip()
    if base:
        extra["base_url"] = base
    if (os.getenv("RUNONE_TOKEN") or "").strip():
        extra["token"] = get_scoped_secret("RUNONE_TOKEN")
    home = (os.getenv("RUNONE_HOME_CONVERSATION") or "").strip()
    return {"extra": extra, "home_channel": {"chat_id": home} if home else None}


def register(ctx) -> None:
    """插件入口 —— Hermes 插件系统在启动时调用。"""
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="RunOne",
        adapter_factory=lambda cfg: RunoneAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["RUNONE_BASE_URL", "RUNONE_TOKEN"],
        install_hint="httpx 已是 Hermes 依赖；缺插件本身请检查 plugins/runone/ 目录",
        env_enablement_fn=_env_enablement,
        is_connected=is_connected,
        allowed_users_env="RUNONE_ALLOWED_USERS",
        allow_all_env="RUNONE_ALLOW_ALL_USERS",
        cron_deliver_env_var="RUNONE_HOME_CONVERSATION",
        standalone_sender_fn=_standalone_send,
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="📋",
        platform_hint=(
            "You are replying inside RunOne（用户的个人任务管理应用）的 AI 助手页。"
            "回复用简洁中文，可以带轻 markdown（列表、加粗、行内代码）；"
            "任务/目标/指标的读写一律用 runone 工具集"
            "（list_today_tasks、find_tasks、create_task、update_task、list_goals、add_metric_record 等），"
            "不要凭记忆回答任务内容。"
        ),
        allow_update_command=True,
    )
