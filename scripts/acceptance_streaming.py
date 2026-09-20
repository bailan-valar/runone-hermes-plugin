#!/usr/bin/env python
"""逐字流式（批二）验收：**真 adapter 代码 + 真 RunOne API**（默认本机 dev server）。

验的是适配器这一侧的契约，逐条断言（每条独立成立，编号对应输出里的 [n]）：

  1. 流式首片（`metadata.expect_edits`）建出 `status='streaming'` 的草稿行，`message_id` = 该行 id；
  2. 草稿行**不进** `data`（增量看不到它），只从响应里的 `draft` 出来；
  3. 第二片改的是**同一行、同一 seq**，正文变长（不是新开一条）；
  4. 定稿（`finalize=True`）→ 草稿行转 `replied`、入站消息转 `replied`、语音附件并进**同一条**；
  5. 定稿之后 `draft` 归 null，定稿行出现在 `data` 里（增量能看到定稿）；
  6. 整轮只落 2 行（1 user + 1 assistant）——没有「草稿 + 定稿」两份正文；
  7. 兜底：草稿行迟迟等不到定稿时，被 `sweepStaleDrafts` 转成 `replied`（不再永远「正在长」）；
  8. 清理：删掉测试会话后列表里没有它。

跑法（**先起本机 dev server，端口别占并行会话的 8787/5173/8642**）：

    cd D:/Workspace/canger/runone/apps/server && npx wrangler dev --port 8791
    C:/Users/<u>/AppData/Local/hermes/hermes-agent/venv/Scripts/python.exe \
        scripts/acceptance_streaming.py --base http://127.0.0.1:8791 \
        --repo D:/Workspace/canger/runone --user-id <users 表里的一行 id>

为什么不连网关的 poll 循环：`connect()` 会真的去认领并投递给网关处理器；这里只驱动
`send`/`edit_message` 两个出站钩子（本轮改的那两个），claim 用适配器自己的 `_post` 打。
真网关那一段由 `gateway restart` 后的实聊验收覆盖（见 README「逐字流式」一节）。
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import hmac
import importlib.util
import json
import os
import sys
import time
from pathlib import Path

PASS = 0
FAIL = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  ✓ {label}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"  ✗ {label} — {detail}")


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def sign_session_jwt(secret: str, user_id: str, email: str, name: str, ttl: int = 3600) -> str:
    """账号会话 JWT（HS256，字段 sub/email/name/iat/exp），与 middleware/auth 同款。"""
    now = int(time.time())
    header = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    payload = _b64(
        json.dumps(
            {"sub": user_id, "email": email, "name": name, "iat": now, "exp": now + ttl},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    )
    signing_input = f"{header}.{payload}".encode()
    sig = _b64(hmac.new(secret.encode(), signing_input, hashlib.sha256).digest())
    return f"{header}.{payload}.{sig}"


def read_dev_vars(repo: Path) -> dict:
    out: dict = {}
    path = repo / "apps" / "server" / ".dev.vars"
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def load_adapter_module(plugin_dir: Path):
    """按路径加载插件仓库里的 adapter.py（跑的是仓库那份，不是 profile 里的安装副本）。"""
    for name in ("adapter", "runone_adapter_under_test"):
        spec = importlib.util.spec_from_file_location(name, plugin_dir / "adapter.py")
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
        return mod
    raise RuntimeError("无法加载 adapter.py")


def register_platform(mod) -> None:
    """网关外单跑要先注册平台名，否则 Platform('runone') 的 pseudo-member 解析不出来。"""
    from gateway.platform_registry import PlatformEntry, platform_registry

    try:
        platform_registry.register(
            PlatformEntry(
                name=mod.PLATFORM_NAME,
                label="RunOne",
                adapter_factory=lambda cfg: mod.RunoneAdapter(cfg),
                check_fn=lambda: True,
                required_env=[],
                install_hint="",
                source="plugin",
            )
        )
    except Exception as exc:  # 已注册 / 接口变化都不该让验收挂掉
        print(f"  （平台注册跳过：{type(exc).__name__}: {exc}）")


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8791", help="RunOne API 基址（本机 dev server）")
    ap.add_argument("--repo", default="D:/Workspace/canger/runone", help="runone 仓库路径（读 .dev.vars）")
    ap.add_argument("--user-id", default="", help="会话属主账号 id（不填则从本机 D1 取第一个）")
    ap.add_argument("--email", default="streaming-acceptance@runone.local")
    ap.add_argument("--name", default="流式验收账号")
    ap.add_argument("--keep", action="store_true", help="跑完不删测试会话（排障用）")
    args = ap.parse_args()

    repo = Path(args.repo)
    dev = read_dev_vars(repo)
    service_token = dev.get("API_TOKEN", "")
    auth_secret = dev.get("AUTH_SECRET", "")
    if not service_token or not auth_secret:
        print("缺 API_TOKEN / AUTH_SECRET（apps/server/.dev.vars）——先确认 --repo 指对了")
        return 2

    import httpx

    user_id = args.user_id
    if not user_id:
        import subprocess

        out = subprocess.run(
            ["npx", "wrangler", "d1", "execute", "runone-app-db", "--local", "--json",
             "--command", "select id from users limit 1"],
            cwd=str(repo / "apps" / "server"), capture_output=True, text=True, shell=os.name == "nt",
        )
        try:
            user_id = json.loads(out.stdout[out.stdout.index("["):])[0]["results"][0]["id"]
        except Exception:
            print("取不到本机用户 id（--user-id 传一个）:", out.stdout[-400:], out.stderr[-400:])
            return 2
    print(f"用户 {user_id} → {args.base}")

    plugin_dir = Path(__file__).resolve().parent.parent
    mod = load_adapter_module(plugin_dir)
    register_platform(mod)

    from gateway.config import PlatformConfig

    adapter = mod.RunoneAdapter(
        PlatformConfig(enabled=True, extra={"base_url": args.base, "token": service_token})
    )
    adapter._http = httpx.AsyncClient(
        base_url=args.base,
        timeout=30.0,
        trust_env=False,  # 本机 Clash 系统代理会劫持 127.0.0.1（仓库里踩过 502）
        headers={
            "Authorization": f"Bearer {service_token}",
            "Accept": "application/json",
            "User-Agent": "HermesAgent-RunoneAdapter/0.1",
        },
    )
    session_jwt = sign_session_jwt(auth_secret, user_id, args.email, args.name)
    # 账号身份建会话/发消息；适配器身份跑 claim/reply/edit（与生产同形）
    user_client = httpx.AsyncClient(
        base_url=args.base, timeout=30.0, trust_env=False,
        headers={"Authorization": f"Bearer {session_jwt}", "Content-Type": "application/json",
                 "User-Agent": "RunoneAcceptance/1.0", "Origin": args.base},
    )

    conv_id = ""
    draft_id = ""

    async def messages(after_seq: int = 0) -> dict:
        res = await user_client.get(f"/ai/conversations/{conv_id}/messages", params={"afterSeq": after_seq})
        res.raise_for_status()
        return res.json()

    try:
        print("\n[准备] 建会话 + 发一条用户消息（= 界面上打字发送）")
        res = await user_client.post("/ai/conversations", json={"title": "流式验收（可删）"})
        check("建会话 201/200", res.status_code in (200, 201), f"HTTP {res.status_code} {res.text[:120]}")
        conv_id = (res.json() or {}).get("id", "")
        if not conv_id:
            return 1
        res = await user_client.post(f"/ai/conversations/{conv_id}/messages",
                                     json={"role": "user", "text": "流式验收：请回一段稍长的文字"})
        check("发用户消息 201/200", res.status_code in (200, 201), f"HTTP {res.status_code} {res.text[:120]}")
        inbound_id = (res.json() or {}).get("id", "")
        inbound = (res.json() or {}).get("status", "")
        check("入站消息初始状态 = new（等人回）", inbound == "new", f"status={inbound}")

        # 适配器那一侧：认领 → 记锚点（与 _deliver 里同序）
        claimed = await adapter._post(f"/ai/messages/{inbound_id}/claim", {})
        check("适配器认领成功", claimed is not None, f"resp={str(claimed)[:80]}")
        adapter._reply_anchor[conv_id] = inbound_id

        print("\n[1] 流式首片 → 建草稿行")
        first = await adapter.send(conv_id, "第一片文字", metadata={"expect_edits": True})
        draft_id = str(first.message_id or "")
        check("[1] send 成功且回了 message_id", bool(first.success and draft_id), f"success={first.success} id={draft_id}")
        page = await messages()
        body = (page.get("draft") or {}).get("text", "")
        check("[2] 草稿行不在 data 里", all(m.get("status") != "streaming" for m in page.get("data", [])),
              f"data={[m.get('seq') for m in page.get('data', [])]}")
        check("[1] draft 单独给出且正文一致", body == "第一片文字", f"draft.text={body!r}")
        draft_seq = (page.get("draft") or {}).get("seq")

        print("\n[3] 第二片 → 改同一行、同一 seq")
        second = await adapter.edit_message(conv_id, draft_id, "第一片文字 + 第二片文字", finalize=False)
        check("[3] edit_message(不 finalize) 成功", bool(second.success), f"success={second.success} err={second.error}")
        page = await messages()
        d = page.get("draft") or {}
        check("[3] 还是同一条草稿（seq 未变）", d.get("seq") == draft_seq and d.get("id") == draft_id,
              f"seq {draft_seq} → {d.get('seq')}")
        check("[3] 正文变长了", d.get("text") == "第一片文字 + 第二片文字", f"text={d.get('text')!r}")
        check("[2] 这期间 data 里仍然没有草稿", all(m.get("status") != "streaming" for m in page.get("data", [])))

        print("\n[4] 定稿 → 转 replied + 入站 replied + 附件并进同一条")
        fake_attachment = "11111111-2222-4333-8444-555555555555"  # 附件入库走存储层，这里只验「并进同一条」这条写入契约
        adapter._pending_voice[conv_id] = {"attachmentId": fake_attachment, "caption": "", "at": time.monotonic()}
        final = await adapter.edit_message(conv_id, draft_id, "最终正文（定稿）", finalize=True)
        check("[4] edit_message(finalize=True) 成功", bool(final.success), f"success={final.success} err={final.error}")
        page = await messages()
        check("[5] 定稿后 draft 归 null", page.get("draft") is None, f"draft={page.get('draft')}")
        finals = [m for m in page.get("data", []) if m.get("role") == "assistant"]
        check("[5] 定稿行出现在 data 里", len(finals) == 1 and finals[0].get("text") == "最终正文（定稿）",
              f"assistant_rows={[(m.get('seq'), m.get('status')) for m in finals]}")
        check("[4] 附件并进了同一条", bool(finals) and fake_attachment in (finals[0].get("attachmentIds") or []),
              f"attachmentIds={finals[0].get('attachmentIds') if finals else None}")
        check("[4] 草稿行 id 与定稿行是同一条", bool(finals) and finals[0].get("id") == draft_id,
              f"row={finals[0].get('id') if finals else None} draft={draft_id}")
        inbound_now = next((m for m in page.get("data", []) if m.get("role") == "user"), {})
        check("[4] 入站消息已 replied（租约放掉）", inbound_now.get("status") == "replied",
              f"status={inbound_now.get('status')}")
        check("[6] 整轮只落 2 行（没有草稿副本）", len(page.get("data", [])) == 2,
              f"rows={[(m.get('role'), m.get('seq')) for m in page.get('data', [])]}")

        print("\n[7] 兜底：没人定稿的草稿被扫尾转成 replied")
        # 另起一条：开草稿行，但把它的 updated_at 改老（模拟适配器进程被杀），再打一次增量接口
        res = await user_client.post(f"/ai/conversations/{conv_id}/messages",
                                     json={"role": "user", "text": "流式验收：这条不会有人定稿"})
        inbound2 = (res.json() or {}).get("id", "")
        await adapter._post(f"/ai/messages/{inbound2}/claim", {})
        adapter._reply_anchor[conv_id] = inbound2
        stale = await adapter.send(conv_id, "半截草稿（没有定稿）", metadata={"expect_edits": True})
        stale_id = str(stale.message_id or "")
        check("[7] 第二条草稿已开出", bool(stale_id), f"id={stale_id}")
        age = await _age_out_draft(repo, stale_id)
        check("[7] 把 updated_at 改老（模拟进程被杀）", age, "wrangler d1 update")
        page = await messages()
        row = next((m for m in page.get("data", []) if m.get("id") == stale_id), None)
        check("[7] 扫尾后它变成一条普通助手消息", bool(row) and row.get("status") == "replied",
              f"row={row.get('status') if row else None}")
        check("[7] 之后 draft 归 null", page.get("draft") is None, f"draft={page.get('draft')}")
    finally:
        print("\n[8] 清理")
        if conv_id and not args.keep:
            res = await user_client.delete(f"/ai/conversations/{conv_id}")
            check("[8] 删会话回 204", res.status_code == 204, f"HTTP {res.status_code}")
            res = await user_client.get("/ai/conversations", params={"limit": 100})
            ids = [c.get("id") for c in (res.json() or [])]
            check("[8] 列表里已经没有它", conv_id not in ids)
        else:
            print(f"  （保留会话 {conv_id}）")
        await user_client.aclose()
        if adapter._http is not None:
            await adapter._http.aclose()

    print(f"\n结果：通过 {PASS} 项 / 失败 {FAIL} 项")
    return 0 if FAIL == 0 else 1


async def _age_out_draft(repo: Path, message_id: str) -> bool:
    """把草稿行的 updated_at 改到 20 分钟前（扫尾门槛是 10 分钟），验「没人定稿也会收口」。"""
    import subprocess

    old = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(time.time() - 1200))
    cmd = ["npx", "wrangler", "d1", "execute", "runone-app-db", "--local",
           "--command", f"update ai_messages set updated_at = '{old}' where id = '{message_id}'"]
    out = subprocess.run(cmd, cwd=str(repo / "apps" / "server"), capture_output=True, text=True,
                         shell=os.name == "nt")
    return out.returncode == 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
