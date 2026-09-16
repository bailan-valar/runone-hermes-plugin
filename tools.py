"""RunOne 工具集：把任务 / 目标 / 指标等 22 个操作做成 Hermes 原生工具。

三件事分开看：

* **声明**（工具名、字段、必填项、中文描述）来自 ``catalog.py``，由服务端 ``/mcp`` 的
  ``tools/list`` 生成 —— 与 MCP 客户端今天看到的是同一份，不手写、不漂移。
* **执行**转发到 RunOne 服务端同一套工具实现（``POST /mcp`` ``tools/call``），因此鉴权、
  AI 署名、变更记录、revision 递增全都与 MCP 一致；插件这边没有第二份业务逻辑要维护。
* **配置**与平台适配器共用 ``RUNONE_BASE_URL`` / ``RUNONE_TOKEN`` 两个环境变量 —— 一处配置，
  对话与工具同身份。

与「配一个 MCP 服务器」的差别只在：工具落在插件的 ``runone`` 工具集里、名字是平铺的
（``list_today_tasks`` 而不是 ``mcp__runone__list_today_tasks``），只要插件启用就可用，
不需要再维护 ``mcp_servers`` 那一份配置与连接。
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Callable, Dict, List, Optional

from tools.registry import tool_error

from .catalog import TOOLS

logger = logging.getLogger(__name__)

TOOLSET = "runone"
EMOJI = "\U0001f4cb"  # clipboard 📋

DEFAULT_TIMEOUT = 60.0
MAX_RESULT_CHARS = 100_000

NOT_CONFIGURED = (
    "RunOne 未配置：在 Hermes 的 .env 里填 RUNONE_BASE_URL（例如 https://runone-api.capdien.site）"
    "与 RUNONE_TOKEN（RunOne「设置 → 个人访问令牌」里建的 rn_… 令牌），然后重启网关。"
)

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}


# ─── 配置 ─────────────────────────────────────────────────────────────────────


def base_url() -> str:
    return (os.getenv("RUNONE_BASE_URL") or "").strip().rstrip("/")


def token() -> str:
    return (os.getenv("RUNONE_TOKEN") or "").strip()


def configured() -> bool:
    """两个环境变量都在才算配好 —— 与平台适配器的判断一致（少一个都不连、不空转）。"""
    return bool(base_url() and token())


def tools_available(**_kwargs: Any) -> bool:
    """``check_fn``：没配就不把工具塞给模型。"""
    return configured()


# ─── 传输 ─────────────────────────────────────────────────────────────────────


class TransportError(RuntimeError):
    """网络层 / 协议层失败，消息已是可以直接给模型看的中文。"""


def _client(host: str):
    """本机地址绕开系统代理：Windows 上本机 Python 会走 Clash，localhost 会被 reset。"""
    import httpx  # 延迟到调用时导入，别拖慢插件的发现期

    if host in _LOCAL_HOSTS:
        return httpx.Client(trust_env=False)
    return httpx.Client()


def _rpc(method: str, params: Dict[str, Any], *, timeout: float = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    if not configured():
        raise TransportError(NOT_CONFIGURED)
    url = f"{base_url()}/mcp"
    host = re.sub(r"^https?://", "", url).split("/")[0].split(":")[0]
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "Authorization": f"Bearer {token()}",
    }
    try:
        with _client(host) as client:
            response = client.post(url, json=payload, headers=headers, timeout=timeout)
    except Exception as exc:  # 连不上 / 超时 / TLS
        raise TransportError(
            f"连不上 RunOne（{base_url()}）：{type(exc).__name__}: {exc}。"
            "检查网络与 RUNONE_BASE_URL 是否正确。"
        ) from exc

    if response.status_code == 401:
        raise TransportError(
            "RunOne 拒绝了这个令牌（401）：PAT 可能被撤销或抄漏了。"
            "到 RunOne「设置 → 个人访问令牌」重建一个，更新 RUNONE_TOKEN 后重启网关。"
        )
    if response.status_code == 403:
        raise TransportError("RunOne 返回 403：这枚令牌没有访问该接口的权限。")
    if response.status_code >= 500:
        raise TransportError(f"RunOne 服务端错误（HTTP {response.status_code}），稍后重试。")
    try:
        body = response.json()
    except Exception as exc:
        raise TransportError(
            f"RunOne 返回了非 JSON 响应（HTTP {response.status_code}）：{response.text[:200]}"
        ) from exc
    if isinstance(body, dict) and body.get("error"):
        message = body["error"].get("message") if isinstance(body["error"], dict) else body["error"]
        raise TransportError(f"RunOne 接口报错：{message}")
    return body.get("result") or {}


def _text_of(result: Dict[str, Any]) -> str:
    """把 MCP 的 content 块拼成纯文本（与 Hermes 的 MCP 渲染口径一致）。"""
    parts: List[str] = []
    for block in result.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "text" and block.get("text"):
            parts.append(str(block["text"]))
    if not parts and result.get("structuredContent") is not None:
        parts.append(json.dumps(result["structuredContent"], ensure_ascii=False))
    text = "\n".join(parts)
    if len(text) > MAX_RESULT_CHARS:
        text = text[:MAX_RESULT_CHARS] + "… [结果过长已截断]"
    return text


def call_tool(name: str, arguments: Optional[Dict[str, Any]] = None, *,
              timeout: float = DEFAULT_TIMEOUT) -> str:
    """调用一个 RunOne 工具，返回给模型的字符串。

    成功 = ``{"result": "<文本>"}``（与 MCP 工具的渲染一致）；失败 = ``{"error": "…"}``。
    """
    try:
        result = _rpc("tools/call", {"name": name, "arguments": dict(arguments or {})}, timeout=timeout)
    except TransportError as exc:
        return tool_error(str(exc))
    text = _text_of(result)
    if result.get("isError"):
        return tool_error(text or f"RunOne 工具 {name} 返回错误")
    return json.dumps({"result": text}, ensure_ascii=False)


def _make_handler(name: str, timeout: float = DEFAULT_TIMEOUT) -> Callable[..., str]:
    def handler(args: Dict[str, Any], **_kwargs: Any) -> str:
        return call_tool(name, args or {}, timeout=timeout)

    handler.__name__ = f"runone_{name}"
    handler.__doc__ = f"RunOne 工具 {name}（转发到 RunOne 服务端实现）。"
    return handler


# ─── 注册 ─────────────────────────────────────────────────────────────────────


def tool_schema(spec: Dict[str, Any]) -> Dict[str, Any]:
    return {"name": spec["name"], "description": spec["description"], "parameters": spec["parameters"]}


def register_tools(ctx: Any) -> List[str]:
    """把 22 个工具注册进 ``runone`` 工具集，返回实际注册成功的工具名。

    Hermes 在发现期就会为声明了 ``provides_tools`` 的平台插件调这里（CLI/TUI 也拿得到工具），
    网关加载插件时再调一次 —— 两次都是同一批注册，重复注册由 registry 幂等处理。
    """
    registered: List[str] = []
    for spec in TOOLS:
        name = spec["name"]
        handle = ctx.register_tool(
            name=name,
            toolset=TOOLSET,
            schema=tool_schema(spec),
            handler=_make_handler(name),
            check_fn=tools_available,
            description=spec["description"],
            emoji=EMOJI,
        )
        if handle is not None:
            registered.append(name)
    logger.debug("RunOne: registered %d/%d tools", len(registered), len(TOOLS))
    return registered
