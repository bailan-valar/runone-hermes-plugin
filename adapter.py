"""RunOne 平台适配器（Hermes 插件）—— 让 Hermes **主动出站**把 RunOne 当成一个聊天平台。

设计文档：`docs/ai-channel-design.md`（本文件是它的 §8「插件骨架」的可运行草稿）。

方向与内网穿透相反：RunOne 不连 Hermes，Hermes 连 RunOne。

    前端 /ai ──POST /ai/… ──► RunOne Worker + D1（消息与权限的唯一真相）
                                        ▲
                                        │ 长轮询 GET /ai/inbox?cursor&wait=25
                                        │ 认领 POST /ai/messages/:id/claim
                                        │ 回写 POST /ai/messages/:id/reply
                                        │ 心跳 POST /ai/conversations/:id/typing
                                        │
                                 本适配器（跑在 Hermes 网关进程里）

骨架照 `plugins/platforms/ntfy/adapter.py`（HTTP 传输 + 退避重连）抄，与之并列的还有
telegram（HTTP 长轮询）、wecom / discord（WS 客户端）、feishu（SDK 长连接）。

零核心改动：本目录放进 `$HERMES_HOME/plugins/`（本机 = `%LOCALAPPDATA%\\hermes\\profiles\\worker\\plugins\\`），
重启网关即生效。
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

try:  # httpx 是 Hermes 自带依赖；缺失时平台整体不可用（照 ntfy 的处理）
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:  # pragma: no cover
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms._shared import extra_or_secret, get_scoped_secret
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.helpers import MessageDeduplicator

logger = logging.getLogger(__name__)

PLATFORM_NAME = "runone"
MAX_MESSAGE_LENGTH = 8000

# 长轮询：服务端最多挂 WAIT 秒（设计文档 §5-1 口径），客户端超时留足余量
POLL_WAIT_SECONDS = 25
POLL_TIMEOUT_SECONDS = 40
# 重连退避（指数 + 抖动），照 base 文档的「streaming 连接必须带退避」要求
BACKOFF_START_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 60.0
# 同一批最多处理多少条，避免一次醒来吃下过多（其余下一拍继续）
POLL_BATCH_LIMIT = 20
# typing 心跳间隔：比服务端 5s 有效期略短，掉了就自然过期（设计文档 §5-6）
TYPING_HEARTBEAT_SECONDS = 4.0
DEDUP_MAX_SIZE = 2000
DEDUP_WINDOW_SECONDS = 600


def _setting(extra: Dict[str, Any], *keys: str, env: str = "", default: str = "") -> str:
    """config.yaml 的 platforms.runone.extra 优先，其次环境变量（与 wecom/ntfy 同款）。"""
    return str(
        next((extra[k] for k in keys if extra.get(k)), None)
        or (get_scoped_secret(env, default) if env else "")
    ).strip()


class RunoneAdapter(BasePlatformAdapter):
    def __init__(self, config: PlatformConfig):
        super().__init__(config=config, platform=Platform(PLATFORM_NAME))
        extra = config.extra or {}
        self._base_url: str = _setting(extra, "base_url", "baseUrl", env="RUNONE_BASE_URL").rstrip("/")
        self._token: str = _setting(extra, "token", env="RUNONE_TOKEN")
        # 默认 **不继承系统代理**：Windows 上 httpx 会从注册表读到 Clash 之类的系统代理，
        # 连 127.0.0.1 也会被劫持成 502（本机实测）。要经代理访问 RunOne 时显式打开。
        self._trust_env: bool = _setting(
            extra, "use_system_proxy", "useSystemProxy", env="RUNONE_USE_SYSTEM_PROXY", default="0"
        ).lower() in {"1", "true", "yes", "on"}
        self._http: Optional["httpx.AsyncClient"] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._stopping = False
        # 内存游标：本会话已投递到的 seq（持久化在批二接 plugin_db，见 README「未做」）
        # inbox 游标：**不透明字符串**（服务端给 `<createdAt>|<id>`）。
        # 别自己拿 seq 当游标——seq 是会话内序号，跨会话比较会漏掉新会话的消息。
        self._cursor: str = ""
        self._dedup = MessageDeduplicator(max_size=DEDUP_MAX_SIZE, ttl_seconds=DEDUP_WINDOW_SECONDS)
        # chat_id(会话 id) → 当前正在回复的那条用户消息 id，send() 用它走 /reply 拿幂等
        self._reply_anchor: Dict[str, str] = {}

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
                    # Cloudflare 前置会按浏览器指纹拒掉裸 UA（仓库里踩过 1010），带一个明确的标识：
                    "User-Agent": "HermesAgent-RunoneAdapter/0.1",
                },
            )
            self._stopping = False
            self._poll_task = asyncio.create_task(self._poll_loop())
            self._mark_connected()
            logger.info("[%s] Connected — long-polling %s/ai/inbox", self.name, self._base_url)
            self._wire_plugin_handlers(None)  # ctx.register_platform_handler 钩子
            return True
        except Exception as exc:  # noqa: BLE001 — 连接失败要落成可读状态，不能抛出打断网关启动
            logger.error("[%s] Failed to connect: %s", self.name, exc, exc_info=True)
            await self._teardown()
            return False

    async def disconnect(self) -> None:
        self._stopping = True
        self._mark_disconnected()
        task, self._poll_task = self._poll_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                logger.debug("[%s] poll task ended with error during shutdown", self.name, exc_info=True)
        await self._teardown()
        self._dedup = MessageDeduplicator(max_size=DEDUP_MAX_SIZE, ttl_seconds=DEDUP_WINDOW_SECONDS)
        self._reply_anchor.clear()
        logger.info("[%s] Disconnected", self.name)

    async def _teardown(self) -> None:
        client, self._http = self._http, None
        if client is not None:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001
                logger.debug("[%s] closing HTTP client failed", self.name, exc_info=True)

    # ---- 入站：长轮询 -------------------------------------------------------

    async def _poll_loop(self) -> None:
        """一直拉到 disconnect()；网络错误按指数退避重连（游标不前进＝不丢消息）。"""
        backoff = BACKOFF_START_SECONDS
        while not self._stopping:
            try:
                delivered = await self._poll_once()
                backoff = BACKOFF_START_SECONDS  # 一轮成功就重置退避
                if delivered == 0:
                    continue  # 空返回 = 服务端挂满 wait 秒，立刻再拉
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

    async def _poll_once(self) -> int:
        assert self._http is not None
        params: Dict[str, Any] = {"wait": POLL_WAIT_SECONDS, "limit": POLL_BATCH_LIMIT}
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

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        anchor = self._reply_anchor.get(str(chat_id))
        # clientMsgId 幂等键：网络抖动重试时服务端复用同一条助手消息（设计文档 §5-3）
        payload: Dict[str, Any] = {"text": content, "clientMsgId": f"hermes-{uuid.uuid4().hex}"}
        if anchor:
            path = f"/ai/messages/{anchor}/reply"
        else:
            # 没有入站锚点（例如 cron 主动投递）：直接往会话里写一条 assistant 消息
            path = f"/ai/conversations/{chat_id}/messages"
            payload["role"] = "assistant"
        data = await self._request("POST", path, payload)
        if data is None:
            return SendResult(success=False, error="RunOne 写入失败（详见网关日志）", retryable=True)
        self._reply_anchor.pop(str(chat_id), None)  # 一条入站消息一条回复
        return SendResult(success=True, message_id=str(data.get("id") or ""), raw_response=data)

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
            {"name": name, "size": len(audio), "mimeType": _audio_mime(name)},
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
        if attachment_id and await self._request("PATCH", f"/attachments/{attachment_id}/complete", {}) is None:
            return SendResult(success=False, error="附件 complete 失败", retryable=True)

        data = await self._request(
            "POST",
            f"/ai/conversations/{chat_id}/messages",
            {
                "role": "assistant",
                "text": (caption or "").strip(),
                "attachmentIds": [attachment_id] if attachment_id else [],
                "clientMsgId": f"hermes-voice-{uuid.uuid4().hex}",
            },
        )
        if data is None:
            return SendResult(success=False, error="RunOne 写入语音消息失败", retryable=True)
        logger.info("[%s] voice: 已投递音频附件 %s（%d 字节）", self.name, attachment_id, len(audio))
        return SendResult(success=True, message_id=str(data.get("id") or ""), raw_response=data)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """typing 是心跳式的：服务端 5s 过期，网关的 _keep_typing 每 2s 会调到这里。"""
        await self._post(f"/ai/conversations/{chat_id}/typing", {}) if self._http else None

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

    async def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        if self._http is None:
            return None
        try:
            res = await self._http.request(method, path, json=payload)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] %s %s failed: %s", self.name, method, path, exc)
            return None
        if res.status_code >= 400:
            logger.warning("[%s] %s %s → HTTP %s: %s", self.name, method, path, res.status_code, res.text[:200])
            return None
        try:
            return res.json()
        except Exception:  # noqa: BLE001 — 204 之类没有 body 也算成功
            return {}

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
            headers={"Authorization": f"Bearer {token}", "User-Agent": "HermesAgent-RunoneAdapter/0.1"},
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
