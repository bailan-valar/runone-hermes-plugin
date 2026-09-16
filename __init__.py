"""RunOne 插件：一个插件同时提供两样东西。

* **入站对话通道**（``kind: platform``）：RunOne 的 AI 助手页当作一个聊天平台 —— Hermes 出站
  长轮询取消息（``/ai/inbox``）、回写回复（``/ai/messages/:id/reply``），不需要入站暴露。
* **出站工具集**（``provides_tools``）：任务 / 目标 / 指标等 22 个工具，agent 在任意会话里
  都能调用（在网关里回答 RunOne 页面，或在桌面端直接改数据）。

两者共用同一份凭据（``RUNONE_BASE_URL`` / ``RUNONE_TOKEN``），所以「在界面里问一句」与
「让 agent 动数据」是同一个身份、同一套权限。

``hermes`` 的插件加载器按 ``plugins/<kind>/<name>/`` 发现本目录，导入本包并调用 ``register(ctx)``。
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

__all__ = ["register"]


def register(ctx) -> None:
    """插件入口：工具集与平台适配器分别注册，一个失败不影响另一个。"""
    try:
        from .tools import register_tools

        register_tools(ctx)
    except Exception:
        logger.warning("RunOne: 注册工具集失败", exc_info=True)
    try:
        from .adapter import register as register_platform

        register_platform(ctx)
    except Exception:
        logger.warning("RunOne: 注册平台适配器失败", exc_info=True)
