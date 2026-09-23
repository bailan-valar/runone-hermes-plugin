#!/usr/bin/env python
"""逐字流式（批二）验收：**真 adapter 代码 + 真 RunOne API**（默认本机 dev server）。

验的是适配器这一侧的契约，逐条断言（每条独立成立，编号对应输出里的 [n]）：

  1. 流式首片（`metadata.expect_edits`）建出 `status='streaming'` 的草稿行，`message_id` = 该行 id；
  2. 草稿行**不进** `data`（增量看不到它），只从响应里的 `draft` 出来；
  3. 第二片改的是**同一行、同一 seq**，正文变长（不是新开一条）；
  4. 定稿（`finalize=True`）→ 草稿行转 `replied`、入站消息转 `replied`、语音附件并进**同一条**；
  5. 定稿之后 `draft` 归 null，定稿行出现在 `data` 里（增量能看到定稿）；
  6. 整轮只落 2 行（1 user + 1 assistant）——没有「草稿 + 定稿」两份正文；
  7. 兜底：草稿行迟迟等不到定稿时，被 `sweepStaleDrafts` 转成 `replied`（正文照原样留着），
     尾部光标被擦掉，并且它挂着的**入站消息也被放掉**（不再等 `sweepStaleProcessing` 判 failed）；
  8. 清理：删掉测试会话后列表里没有它；
  9. **编辑是严格的**：对已定稿行改不同正文 → 失败（网关会走整段兜底，不再被「假成功」吞掉回复）；
     同正文重放 → 仍然成功（幂等）；
 10. **入站被前置写入置成 replied 时也能开出草稿**：中间态写入（工具进度 / 提示）不消耗锚点、
     不把入站置 replied；`/reply` 被 409 挡住时走会话消息端点那条路开草稿，逐字流式照旧生效，
     定稿后入站租约照旧放掉。
 11. **语音晚到不跨轮**：手上有**本轮**待合并语音时流式草稿照样开（旧行为是被挤成整段发送）；
     定稿把本轮语音并进同一条；这一轮**收口之后**才到（TTS 慢）的语音补挂到本轮那条正文上，
     不新开一条同文消息、也不留给下一轮；上一轮遗留的语音在新一轮开始时立刻单独投递。

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
    ap.add_argument("--persist", default="", help="wrangler `--persist-to` 目录（与你跑着的 dev server 必须一致，否则改的是另一个库）")
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

    def use_inbound(message_id: str) -> None:
        """照 `_deliver()` 的顺序记两条：`_reply_anchor`（这一笔回写到哪条入站消息）
        + `_turn_inbound`（这一轮在回谁 —— 锚点被前置写入用掉之后，草稿行还要靠它挂回去）。"""
        adapter._reply_anchor[conv_id] = message_id
        adapter._turn_inbound[conv_id] = message_id

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
        use_inbound(inbound_id)

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
        # `inbound` 必须写**本轮**那条：跨轮的语音一律不并（见 [11]），拿不到就退化成单独投递。
        adapter._pending_voice[conv_id] = {
            "attachmentId": fake_attachment, "caption": "", "at": time.monotonic(), "inbound": inbound_id
        }
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

        print("\n[9] 编辑严格的：定稿行再改 → 409；同正文重放 → 200（幂等）")
        # 线上事故形态：这一片编辑打在一条**非 streaming** 的行上。旧服务端回 200「成功但什么都不改」，
        # 网关便以为正文已经送达、把这一轮的完整回复抑制掉（只剩半截 + 光标）。
        flip = await adapter.edit_message(conv_id, draft_id, "想改成别的内容", finalize=False)
        check("[9] 对已定稿行改不同正文 → 失败（网关会走整段兜底，不再吞回复）", flip.success is False,
              f"success={flip.success}")
        same = await adapter.edit_message(conv_id, draft_id, "最终正文（定稿）", finalize=False)
        check("[9] 同正文重放 → 仍然成功（幂等，适配器重试不会报错）", bool(same.success), f"success={same.success}")

        print("\n[10] 中间态写入不吃锚点；入站已被前置写入置 replied 时也能开出草稿")
        res = await user_client.post(f"/ai/conversations/{conv_id}/messages",
                                     json={"role": "user", "text": "流式验收：先有工具进度，再流式"})
        inbound3 = (res.json() or {}).get("id", "")
        await adapter._post(f"/ai/messages/{inbound3}/claim", {})
        use_inbound(inbound3)
        notice = await adapter.send(conv_id, "📚 工具进度（中间态）", metadata={"_interim_send": True})
        check("[10] 中间态发送成功", bool(notice.success), f"success={notice.success} err={notice.error}")
        page = await messages()
        row = next((m for m in page.get("data", []) if m.get("id") == notice.message_id), None)
        check("[10] 中间态是**普通助手消息**（不是草稿）", bool(row) and row.get("status") == "replied",
              f"row={row.get('status') if row else None}")
        inb = next((m for m in page.get("data", []) if m.get("id") == inbound3), None)
        check("[10] 入站消息**没有被置 replied**（中间态不是答案）",
              bool(inb) and inb.get("status") == "processing", f"status={inb.get('status') if inb else None}")
        first3 = await adapter.send(conv_id, "首片（有锚点，走 /reply）", metadata={"expect_edits": True})
        check("[10] 紧接着的流式首片仍然开出了草稿行", bool(first3.message_id),
              f"id={first3.message_id} err={first3.error}")
        page = await messages()
        check("[10] 草稿行状态是 streaming（可继续编辑）",
              (page.get("draft") or {}).get("id") == first3.message_id,
              f"draft={(page.get('draft') or {}).get('id')}")
        # 把入站置成 replied（= 这一轮里先落了别的助手写入），再开一条草稿：/reply 会被 409 挡住
        await adapter.send(conv_id, "整段回复（会把入站置成 replied）")
        res = await user_client.post(f"/ai/conversations/{conv_id}/messages",
                                     json={"role": "user", "text": "流式验收：入站已被前置写入置 replied"})
        inbound4 = (res.json() or {}).get("id", "")
        await adapter._post(f"/ai/messages/{inbound4}/claim", {})
        use_inbound(inbound4)
        await adapter.send(conv_id, "前置写入（把入站置 replied）")
        page = await messages()
        inb4 = next((m for m in page.get("data", []) if m.get("id") == inbound4), None)
        check("[10] 前置写入确实把入站置成了 replied",
              bool(inb4) and inb4.get("status") == "replied", f"status={inb4.get('status') if inb4 else None}")
        fallback = await adapter.send(conv_id, "首片（锚点已不可写，走会话消息端点）",
                                      metadata={"expect_edits": True})
        check("[10] /reply 开不出来时，兜底路仍然开出草稿行", bool(fallback.message_id),
              f"id={fallback.message_id} err={fallback.error}")
        page = await messages()
        draft2 = page.get("draft") or {}
        check("[10] 兜底开出的也是 streaming 草稿（逐字流式照旧生效）",
              draft2.get("id") == fallback.message_id and draft2.get("status") == "streaming",
              f"draft={draft2.get('id')} status={draft2.get('status')}")
        fin2 = await adapter.edit_message(conv_id, str(fallback.message_id), "兜底草稿定稿", finalize=True)
        check("[10] 兜底草稿能正常定稿", bool(fin2.success), f"success={fin2.success} err={fin2.error}")
        page = await messages()
        inb4 = next((m for m in page.get("data", []) if m.get("id") == inbound4), None)
        check("[10] 定稿放掉了入站租约", bool(inb4) and inb4.get("status") == "replied",
              f"status={inb4.get('status') if inb4 else None}")

        print("\n[7] 兜底：没人定稿的草稿被扫尾转成 replied（擦掉尾部光标 + 放掉入站）")
        # 另起一条：开草稿行，但把它的 updated_at 改老（模拟适配器进程被杀），再打一次增量接口
        res = await user_client.post(f"/ai/conversations/{conv_id}/messages",
                                     json={"role": "user", "text": "流式验收：这条不会有人定稿"})
        inbound2 = (res.json() or {}).get("id", "")
        await adapter._post(f"/ai/messages/{inbound2}/claim", {})
        use_inbound(inbound2)
        stale = await adapter.send(conv_id, "半截草稿（没有定稿） ▉", metadata={"expect_edits": True})
        stale_id = str(stale.message_id or "")
        check("[7] 第二条草稿已开出", bool(stale_id), f"id={stale_id}")
        age = await _age_out_draft(repo, stale_id, args.persist)
        check("[7] 把 updated_at 改老（模拟进程被杀）", age, "wrangler d1 update")
        page = await messages()
        row = next((m for m in page.get("data", []) if m.get("id") == stale_id), None)
        check("[7] 扫尾后它变成一条普通助手消息", bool(row) and row.get("status") == "replied",
              f"row={row.get('status') if row else None}")
        check("[7] 尾部光标被擦掉（不再留着 `… ▉`）",
              bool(row) and row.get("text") == "半截草稿（没有定稿）", f"text={row.get('text') if row else None!r}")
        inb2 = next((m for m in page.get("data", []) if m.get("id") == inbound2), None)
        check("[7] 入站消息被放掉（不再挂着等 sweepStaleProcessing 判 failed）",
              bool(inb2) and inb2.get("status") == "replied", f"status={inb2.get('status') if inb2 else None}")
        check("[7] 之后 draft 归 null", page.get("draft") is None, f"draft={page.get('draft')}")

        print("\n[11] 语音晚到：只补挂到本轮正文，不再多发一条（2026-09-23 线上报障）")
        # 线上形态：TTS 合成慢于正文回写 → 语音到达时这一轮已经定稿收口。老代码把它压进
        # `_pending_voice` 等「下一笔正文」，于是它跨到下一轮、错挂到下一问的回复上，还因为
        # 「手上有待合并语音就不开流式草稿」把下一轮的逐字流式整条挤掉 —— 线上实测是一个半截
        # 带光标的气泡 + 两条内容重复的正文（一条带语音、一条不带）。
        res = await user_client.post(f"/ai/conversations/{conv_id}/messages",
                                     json={"role": "user", "text": "流式验收：手上有待合并语音"})
        inbound5 = (res.json() or {}).get("id", "")
        await adapter._post(f"/ai/messages/{inbound5}/claim", {})
        use_inbound(inbound5)
        voice_a = "aaaaaaaa-1111-4111-8111-111111111111"
        adapter._pending_voice[conv_id] = {
            "attachmentId": voice_a, "caption": "", "at": time.monotonic(), "inbound": inbound5
        }
        draft3 = await adapter.send(conv_id, "首片（手上有本轮待合并语音）", metadata={"expect_edits": True})
        page = await messages()
        check("[11] 手上有本轮待合并语音时，流式草稿照开（旧行为：被挤成整段发送）",
              bool(draft3.message_id) and (page.get("draft") or {}).get("id") == draft3.message_id,
              f"id={draft3.message_id} draft={(page.get('draft') or {}).get('id')}")
        fin3 = await adapter.edit_message(conv_id, str(draft3.message_id), "这一轮的正文（带语音）", finalize=True)
        page = await messages()
        row3 = next((m for m in page.get("data", []) if m.get("id") == draft3.message_id), None)
        check("[11] 定稿把本轮语音并进了同一条正文",
              bool(fin3.success) and bool(row3) and voice_a in (row3.get("attachmentIds") or []),
              f"attachments={row3.get('attachmentIds') if row3 else None}")

        # 收口之后才到的语音（这次走真上传）：补挂到本轮那条正文，不新开一条消息
        rows_before = len(page.get("data", []))
        audio = Path(os.environ.get("TEMP", "/tmp")) / "runone-acceptance-voice.wav"
        _write_tiny_wav(audio)
        sent = await adapter.send_voice(conv_id, str(audio))
        real_att = str((sent.raw_response or {}).get("attachmentId") or "")
        check("[11] 收口后到达的语音上传成功", bool(sent.success and real_att), f"success={sent.success} att={real_att}")
        page = await messages()
        row_after = next((m for m in page.get("data", []) if m.get("id") == str(draft3.message_id)), None)
        check("[11] 补挂到本轮正文：附件挂上、正文一字不动",
              bool(row_after) and real_att in (row_after.get("attachmentIds") or [])
              and row_after.get("text") == "这一轮的正文（带语音）",
              f"attachments={row_after.get('attachmentIds') if row_after else None}")
        check("[11] 没有因此多出一条同文消息（旧行为：再发一条完整正文）",
              len(page.get("data", [])) == rows_before,
              f"rows {rows_before} → {len(page.get('data', []))}")
        check("[11] 也没有压进 `_pending_voice` 去等下一轮", conv_id not in adapter._pending_voice,
              f"pending={list(adapter._pending_voice)}")

        # 跨轮遗留的语音（上一轮没等到正文）：新一轮开始时立刻单独投递，不再错挂到这一问
        stale_voice = "bbbbbbbb-2222-4222-8222-222222222222"
        adapter._pending_voice[conv_id] = {
            "attachmentId": stale_voice, "caption": "", "at": time.monotonic(),
            "inbound": "00000000-0000-4000-8000-000000000000",
        }
        await adapter._drop_stale_turn_voice(conv_id, inbound5)
        page = await messages()
        dropped = [m for m in page.get("data", []) if stale_voice in (m.get("attachmentIds") or [])]
        check("[11] 跨轮遗留的语音被立刻单独投递（不再留给下一轮正文合并）", len(dropped) == 1,
              f"rows={[(m.get('seq'), m.get('attachmentIds')) for m in page.get('data', [])]}")
        check("[11] 投递后登记已清空（不会重复投）", conv_id not in adapter._pending_voice)
        if real_att:
            await user_client.delete(f"/attachments/{real_att}")
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


def _write_tiny_wav(path: Path) -> None:
    """0.2s 静音 wav —— [11] 只关心「附件上传后挂到哪条消息」，不关心音频内容。"""
    import struct
    import wave

    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as fh:
        fh.setnchannels(1)
        fh.setsampwidth(2)
        fh.setframerate(8000)
        fh.writeframes(struct.pack("<800h", *([0] * 800)))


async def _age_out_draft(repo: Path, message_id: str, persist: str = "") -> bool:
    """把草稿行的 updated_at 改到 20 分钟前（扫尾门槛是 10 分钟），验「没人定稿也会收口」。"""
    import subprocess

    old = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(time.time() - 1200))
    cmd = ["npx", "wrangler", "d1", "execute", "runone-app-db", "--local"]
    if persist:
        cmd += ["--persist-to", persist]
    cmd += ["--command", f"update ai_messages set updated_at = '{old}' where id = '{message_id}'"]
    out = subprocess.run(cmd, cwd=str(repo / "apps" / "server"), capture_output=True, text=True,
                         shell=os.name == "nt")
    return out.returncode == 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
