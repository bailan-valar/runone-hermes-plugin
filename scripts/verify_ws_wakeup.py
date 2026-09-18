"""验收：RunOne 的推送面（WSS `/ai/inbox/ws`）真的「消息落库即到」。

这是**真打 API** 的脚本（本机 dev server 或生产都行），不是单测 —— 它要证明的是
「适配器不用每秒重查，也能在用户发消息后立刻醒过来」。逐条断言、逐条打印证据。

用法（默认把测试数据清干净；`--keep` 留着看）：

  # 本机 dev server（自建状态目录，别打并行会话的 8787）
  python scripts/verify_ws_wakeup.py --base-url http://127.0.0.1:8797 --token rn_xxx

  # 生产（会建一条测试会话再删掉）
  python scripts/verify_ws_wakeup.py --base-url https://runone-api.capdien.site --token rn_xxx

⚠️ 打生产时：这条测试会话会被**真的适配器认领**（它就是一条普通会话）——脚本删得快、适配器答得慢，
   于是日志里会出现十几行 `POST … /messages → HTTP 404 会话不存在`（实测 22 秒后自停）。
   要么先停网关再跑，要么接受这段噪声。

断言：
  1. 不带 Upgrade 头打 /ai/inbox/ws → 426（证明这条路由在这版服务端上存在）
  2. 带 PAT 升级 → 连上；GET /ai/inbox/hub 的连接数从 0 变 1
  3. POST 一条用户消息 → kick 帧在 5s 内到达（打印实测延迟）
  4. kick 之后 GET /ai/inbox?wait=0 能拉到这条消息（载荷仍走原来那条路）
  5. 静默 3 秒没有任何帧（不是「每秒推一帧」的假推送）
  6. 断开后 /ai/inbox/hub 的连接数回到 0（连接真的被回收）
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

try:
    import httpx
    import websockets
except ImportError as exc:  # pragma: no cover
    sys.exit(f"缺依赖：{exc}（pip install httpx websockets）")

USER_AGENT = "HermesAgent-RunoneAdapter/0.4"
KICK_TIMEOUT_SECONDS = 5.0
IDLE_SECONDS = 3.0

ok = 0
failures = []


def check(name: str, passed: bool, detail: str = "") -> None:
    global ok
    if passed:
        ok += 1
        print(f"  ✓ {name}" + (f" — {detail}" if detail else ""))
    else:
        failures.append(name)
        print(f"  ✗ {name} — {detail}")


def ws_url(base_url: str) -> str:
    base = base_url.rstrip("/")
    if base.startswith("https://"):
        base = "wss://" + base[len("https://"):]
    elif base.startswith("http://"):
        base = "ws://" + base[len("http://"):]
    return f"{base}/ai/inbox/ws"


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=os.environ.get("RUNONE_BASE_URL", "http://127.0.0.1:8787"))
    parser.add_argument("--token", default=os.environ.get("RUNONE_TOKEN", ""))
    parser.add_argument("--keep", action="store_true", help="跑完不删测试会话")
    args = parser.parse_args()
    if not args.token:
        sys.exit("缺 token：--token 或 RUNONE_TOKEN（RunOne 设置 → 个人访问令牌，rn_…）")

    base = args.base_url.rstrip("/")
    headers = {"Authorization": f"Bearer {args.token}", "User-Agent": USER_AGENT}
    # trust_env=False：Windows 上系统代理（Clash）会连 127.0.0.1 一起劫持 → 假 502
    client = httpx.AsyncClient(base_url=base, headers=headers, timeout=20.0, trust_env=False)
    conversation_id: Optional[str] = None

    try:
        print(f"[准备] base={base}")

        print("\n=== 1. 推送面这条路由在不在 ===")
        res = await client.get("/ai/inbox/ws")
        check(
            "不带 Upgrade 头 → 426（路由存在，且明确要求 WebSocket）",
            res.status_code == 426,
            f"HTTP {res.status_code} {res.text[:80]}",
        )

        print("\n=== 2. 带 PAT 升级 + 连接可见 ===")
        before = await hub_sockets(client)
        async with websockets.connect(
            ws_url(base),
            additional_headers=headers,
            user_agent_header=USER_AGENT,
            open_timeout=15.0,
            ping_interval=20.0,
            ping_timeout=20.0,
        ) as ws:
            check("WS 升级成功（101）", True, f"uri={ws_url(base)}")
            await asyncio.sleep(0.3)
            after = await hub_sockets(client)
            check(
                "/ai/inbox/hub 的连接数 +1",
                after is not None and before is not None and after == before + 1,
                f"before={before} after={after}",
            )

            print("\n=== 3. 发一条用户消息 → 立刻被 kick ===")
            res = await client.post("/ai/conversations", json={"title": "推送面验收（可删）"})
            if res.status_code != 201:
                check("准备：建一条测试会话 → 201", False, f"HTTP {res.status_code} {res.text[:120]}")
                return report()
            conversation_id = res.json()["id"]
            check("准备：建一条测试会话 → 201", True, f"conversation={conversation_id}")

            started = time.perf_counter()
            res = await client.post(
                f"/ai/conversations/{conversation_id}/messages",
                json={"role": "user", "text": "推送面验收：这条消息应该把适配器叫醒"},
            )
            check("写用户消息 → 201（status=new）", res.status_code == 201 and res.json().get("status") == "new",
                  f"HTTP {res.status_code} {res.text[:120]}")
            message_id = res.json().get("id")

            kick: Optional[Dict[str, Any]] = None
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=KICK_TIMEOUT_SECONDS)
                kick = json.loads(raw)
            except asyncio.TimeoutError:
                pass
            latency_ms = (time.perf_counter() - started) * 1000
            check(
                "收到 kick 帧（type=kick）",
                isinstance(kick, dict) and kick.get("type") == "kick",
                f"延迟 {latency_ms:.0f} ms，帧={kick}",
            )
            check("kick 里带得出是哪个会话", isinstance(kick, dict) and kick.get("conversationId") == conversation_id,
                  f"conversationId={kick.get('conversationId') if isinstance(kick, dict) else None}")

            print("\n=== 4. 载荷仍走 /ai/inbox（契约没变）===")
            res = await client.get("/ai/inbox", params={"wait": 0, "limit": 10})
            items = res.json().get("data") if res.status_code == 200 else []
            check(
                "wait=0 一次查询就拿到这条（含项目上下文注入等原口径）",
                res.status_code == 200 and any(i.get("id") == message_id for i in (items or [])),
                f"HTTP {res.status_code}，本批 {len(items or [])} 条",
            )

            print("\n=== 5. 空闲不推帧（不是「每秒推一帧」）===")
            extra = 0
            deadline = time.perf_counter() + IDLE_SECONDS
            while time.perf_counter() < deadline:
                try:
                    await asyncio.wait_for(ws.recv(), timeout=max(0.1, deadline - time.perf_counter()))
                    extra += 1
                except asyncio.TimeoutError:
                    break
            check(f"静默 {IDLE_SECONDS:.0f}s 内没有多余帧", extra == 0, f"多收到 {extra} 帧")

        print("\n=== 6. 断开后连接被回收 ===")
        sockets = None
        for _ in range(10):
            await asyncio.sleep(0.5)
            sockets = await hub_sockets(client)
            if sockets == before:
                break
        check("断开后 /ai/inbox/hub 的连接数回到断开前", sockets == before, f"sockets={sockets}（断开前 {before}）")
    finally:
        if conversation_id and not args.keep:
            res = await client.delete(f"/ai/conversations/{conversation_id}")
            gone = await client.get(f"/ai/conversations/{conversation_id}")
            print(f"\n[清理] 删测试会话 → HTTP {res.status_code}；再读 → HTTP {gone.status_code}")
        await client.aclose()

    return report()


async def hub_sockets(client: "httpx.AsyncClient") -> Optional[int]:
    """推送面上的连接数（排障接口；老服务端没有它时返回 None）。"""
    try:
        res = await client.get("/ai/inbox/hub")
    except Exception:  # noqa: BLE001
        return None
    if res.status_code != 200:
        return None
    try:
        return int(res.json().get("sockets"))
    except Exception:  # noqa: BLE001
        return None


def report() -> int:
    total = ok + len(failures)
    print(f"\n=== 结果：通过 {ok} / {total} ===")
    if failures:
        for name in failures:
            print(f"  - 失败：{name}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
