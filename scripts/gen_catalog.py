#!/usr/bin/env python3
"""重新生成 ``catalog.py``——插件声明的工具目录。

数据源是 RunOne 服务端 ``/mcp`` 的 ``tools/list``，与 MCP 客户端今天拿到的是同一份
schema，因此插件里的工具名、字段名、必填项和中文描述不会和服务端实现漂移。

用法（在插件仓库根目录）：

    RUNONE_BASE_URL=https://runone-api.capdien.site RUNONE_TOKEN=rn_… \
        python scripts/gen_catalog.py

    # 或复用 Hermes profile 里已配好的那两个环境变量：
    python scripts/gen_catalog.py --from-profile worker

跑完把 catalog.py 的改动提交即可。脚本不打日志、不回显 token。
"""

from __future__ import annotations

import argparse
import json
import os
import pprint
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

HEADER = '''"""RunOne 工具目录（自动生成 —— 勿手改）。

数据来自 RunOne 服务端 `/mcp` 的 `tools/list`——也就是 MCP 客户端今天看到的同一份
schema：字段名、必填项与中文描述与线上实现逐字一致。要更新就重跑：

    python scripts/gen_catalog.py            # 打生产 /mcp，覆盖本文件

生成而不手写，是为了不让「插件声明的工具」与「服务端实现」两处漂移。
"""

from __future__ import annotations

TOOLS = '''

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}


def load_profile_env(profile: str) -> None:
    """把 ``<hermes home>/profiles/<profile>/.env`` 里的 RUNONE_* 读进 os.environ（不覆盖已有值）。"""
    local = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~/AppData/Local")
    env_path = Path(local) / "hermes" / "profiles" / profile / ".env"
    if not env_path.is_file():
        sys.exit(f"找不到 profile 的 .env：{env_path}")
    for raw in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if key.startswith("RUNONE_") and not os.environ.get(key):
            os.environ[key] = value.strip().strip('"').strip("'")


def _opener(base_url: str) -> urllib.request.OpenerDirector:
    """本机地址绕开系统代理（本机 Python 走 Clash 时 localhost 会被 reset）。"""
    host = re.sub(r"^https?://", "", base_url).split("/")[0].split(":")[0]
    if host in _LOCAL_HOSTS:
        return urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return urllib.request.build_opener()


def fetch_tools(base_url: str, token: str) -> list[dict]:
    base = base_url.strip().rstrip("/")
    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    req = urllib.request.Request(
        f"{base}/mcp",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        with _opener(base).open(req, timeout=30) as resp:
            body = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        code = exc.code
        hint = "令牌无效或已撤销" if code == 401 else "服务端返回了非 JSON 响应"
        sys.exit(f"/mcp tools/list 失败：HTTP {code}（{hint}）")
    if "error" in body:
        sys.exit(f"/mcp tools/list 返回错误：{body['error'].get('message')}")
    return body["result"]["tools"]


def clean_params(schema: dict) -> dict:
    """只留 type / properties / required —— 与 Hermes 其他工具声明的形状一致。"""
    params: dict = {"type": "object"}
    properties = schema.get("properties") or {}
    if properties:
        params["properties"] = properties
    required = schema.get("required") or []
    if required:
        params["required"] = list(required)
    return params


def render(tools: list[dict]) -> str:
    entries = [
        {
            "name": tool["name"],
            "description": tool["description"],
            "parameters": clean_params(tool.get("inputSchema") or {}),
        }
        for tool in tools
    ]
    return HEADER + pprint.pformat(entries, sort_dicts=False, width=118, indent=1, compact=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="从 /mcp tools/list 重新生成 catalog.py")
    parser.add_argument("--from-profile", metavar="PROFILE",
                        help="从该 Hermes profile 的 .env 读 RUNONE_BASE_URL / RUNONE_TOKEN")
    parser.add_argument("--base-url", help="覆盖 RUNONE_BASE_URL")
    parser.add_argument("--token", help="覆盖 RUNONE_TOKEN（默认从环境变量读）")
    parser.add_argument("--out", default=str(Path(__file__).resolve().parent.parent / "catalog.py"))
    parser.add_argument("--dry-run", action="store_true", help="只打印统计，不写文件")
    args = parser.parse_args()

    if args.from_profile:
        load_profile_env(args.from_profile)

    base_url = args.base_url or os.environ.get("RUNONE_BASE_URL", "")
    token = args.token or os.environ.get("RUNONE_TOKEN", "")
    if not base_url or not token:
        sys.exit("缺 RUNONE_BASE_URL / RUNONE_TOKEN：要么设环境变量，要么用 --from-profile <名字>")

    tools = fetch_tools(base_url, token)
    out = render(tools)
    names = [t["name"] for t in tools]
    if not args.dry_run:
        Path(args.out).write_text(out, encoding="utf-8", newline="\n")
    print(f"{len(tools)} 个工具 → {args.out}{'（dry-run，未写）' if args.dry_run else ''}")
    print("工具集：" + ", ".join(names))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
