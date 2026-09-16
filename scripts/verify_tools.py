#!/usr/bin/env python3
"""对着真实 RunOne API 把 22 个工具逐个跑一遍（真 HTTP、真数据、真副作用）。

跑的是**插件自己的代码路径**（`tools.call_tool` → `/mcp` 转发），不是另写一份调用：
所以它同时验证了「schema 声明」「配置读取」「错误映射」「结果渲染」四件事。

用法（在插件仓库根目录，用 Hermes 自己的 Python）：

    set PYTHONPATH=C:/Users/ventu/AppData/Local/hermes/hermes-agent
    set RUNONE_BASE_URL=http://127.0.0.1:8787
    set RUNONE_TOKEN=rn_…
    venv/Scripts/python.exe scripts/verify_tools.py

或直接复用某个 profile 已配好的两个环境变量：

    python scripts/verify_tools.py --from-profile worker

测试会在该账号下**真实创建**任务 / 目标 / 指标 / 重复模板各一个（标题带「插件验证」前缀），
清理请用：`--cleanup`（删掉能找到的这些对象）。
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

TOOL_RESULT = re.compile(r"\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b")
MARK = "插件验证"
VERIFY_SPACE = f"{MARK}空间"


def _http(method: str, path: str, payload: dict | None = None) -> tuple[int, object]:
    """直接打 REST（只用来准备/清理验证用的空间；工具调用一律走插件自己的代码路径）。"""
    import urllib.error
    import urllib.request

    base = os.environ["RUNONE_BASE_URL"].rstrip("/")
    host = re.sub(r"^https?://", "", base).split("/")[0].split(":")[0]
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}) if host in {"localhost", "127.0.0.1", "::1"} else urllib.request.ProxyHandler()
    )
    request = urllib.request.Request(
        base + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {os.environ['RUNONE_TOKEN']}"},
        method=method,
    )
    try:
        with opener.open(request, timeout=30) as response:
            body = response.read().decode()
            return response.status, (json.loads(body) if body.strip() else None)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()[:200]


def ensure_verification_space() -> str:
    """目标/指标必须挂在某个空间里。

    本机开发库里「我的空间」有多条（每个注册账号一条同名空间），按名字解析会挑到别人的那条，
    于是「没有权限」——所以验证用一个唯一名字的空间，避免把库里的重名当成插件的问题。
    """
    status, spaces = _http("GET", "/spaces")
    if status == 200 and isinstance(spaces, list):
        for space in spaces:
            if space.get("name") == VERIFY_SPACE:
                return VERIFY_SPACE
    status, body = _http("POST", "/spaces", {"name": VERIFY_SPACE})
    if status not in (200, 201):
        sys.exit(f"建验证空间失败：HTTP {status} {body}")
    return VERIFY_SPACE


def load_package(plugin_dir: Path):
    """按目录把插件包导入进来（目录名带连字符，不能直接 import）。"""
    name = "runone_plugin_under_test"
    spec = importlib.util.spec_from_file_location(
        name, plugin_dir / "__init__.py", submodule_search_locations=[str(plugin_dir)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return importlib.import_module(f"{name}.tools")


def load_profile_env(profile: str) -> None:
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


class Runner:
    def __init__(self, tools) -> None:
        self.tools = tools
        self.results: list[tuple[str, bool, str]] = []

    def call(self, name: str, args: dict | None = None, *, expect: str | None = None,
             forbid_error: bool = True) -> str:
        raw = self.tools.call_tool(name, args or {})
        text = self._text(raw)
        ok = True
        detail = ""
        if forbid_error and '"error"' in raw and text.strip() == "":
            ok, detail = False, "返回了 error 信封"
        elif forbid_error and raw.strip().startswith('{"error"'):
            ok, detail = False, text[:160]
        if expect and expect not in text:
            ok, detail = False, f"结果里没有「{expect}」：{text[:160]}"
        self.results.append((name, ok, detail or text[:120].replace("\n", " ⏎ ")))
        return text

    @staticmethod
    def _text(raw: str) -> str:
        try:
            data = json.loads(raw)
        except ValueError:
            return raw
        if isinstance(data, dict):
            if "result" in data and isinstance(data["result"], str):
                return data["result"]
            if "error" in data:
                return str(data["error"])
        return json.dumps(data, ensure_ascii=False)

    def first_id(self, text: str) -> str | None:
        match = TOOL_RESULT.search(text)
        return match.group(1) if match else None


def main() -> int:
    parser = argparse.ArgumentParser(description="跑完 RunOne 插件的全部工具")
    parser.add_argument("--from-profile", metavar="PROFILE")
    parser.add_argument("--plugin-dir", default=str(Path(__file__).resolve().parent.parent))
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"), help="当成「今天」的日期")
    args = parser.parse_args()

    if args.from_profile:
        load_profile_env(args.from_profile)
    if not os.environ.get("RUNONE_BASE_URL") or not os.environ.get("RUNONE_TOKEN"):
        sys.exit("缺 RUNONE_BASE_URL / RUNONE_TOKEN（或用 --from-profile <名字>）")

    tools = load_package(Path(args.plugin_dir))
    print(f"工具数：{len(tools.TOOLS)}；目标：{os.environ['RUNONE_BASE_URL']}")

    run = Runner(tools)
    today = args.date
    # 每跑一次用一套新标题：本机库里可能已有上一次的遗留对象（没有删除工具），
    # 重名会让「按标题定位」的更新工具挑到旧对象，看着像插件坏了。
    stamp = datetime.now().strftime("%m%d-%H%M%S")
    task_title = f"{MARK}任务-{stamp}"
    goal_title = f"{MARK}目标-{stamp}"
    metric_name = f"{MARK}指标-{stamp}"
    recurrence_title = f"{MARK}重复-{stamp}"
    space_name = ensure_verification_space()

    # 1) 读
    run.call("list_my_account", expect="身份")
    run.call("list_projects")
    run.call("list_metrics")
    run.call("list_goals")
    run.call("list_recurrences")
    run.call("list_assigned_tasks")
    run.call("list_today_tasks")

    # 2) 任务全生命周期
    run.call("create_task", {"title": task_title, "priority": "medium", "dueDate": today},
             expect=MARK)
    found = run.call("find_tasks", {"query": MARK}, expect=MARK)
    task_id = run.first_id(found)
    if not task_id:
        print("找不到刚建的任务 id，后续用例跳过")
    else:
        run.call("take_task", {"task_id": task_id}, expect="进行中")
        run.call("update_task", {"task_id": task_id, "notes": "由插件验证脚本写入"}, expect="已更新")
        run.call("submit_task_result", {"task_id": task_id, "summary": "插件验证：工具链路已打通"},
                 expect="待测试")
        run.call("list_today_tasks", expect=MARK)
        run.call("complete_task", {"task_id": task_id}, expect="完成")

    # 3) 目标 / 指标
    run.call("create_goal", {"title": goal_title, "space_name": space_name}, expect=MARK)
    goals = run.call("list_goals", expect=MARK)
    if task_id:
        run.call("link_task_to_goal", {"task_id": task_id, "goal_title": goal_title}, expect=MARK)
    goal_id = run.first_id(goals.split(MARK)[-1]) if MARK in goals else None
    run.call("update_goal", {"goal_title": goal_title, "notes": "插件验证目标说明"}, expect="已更新")
    run.call("create_metric", {"name": metric_name, "space_name": space_name, "unit": "次"},
             expect=MARK)
    metrics = run.call("list_metrics", expect=MARK)
    metric_id = run.first_id(metrics.split(MARK)[-1]) if MARK in metrics else None
    record_args: dict = {"date": today, "value": 1, "note": "插件验证打卡"}
    if metric_id:
        record_args["metric_id"] = metric_id
    else:
        record_args["metric_name"] = metric_name
    run.call("add_metric_record", record_args, expect="已")
    run.call("list_metric_records", {"metric_name": metric_name}, expect=MARK)

    # 4) 重复任务
    run.call("create_recurrence", {"title": recurrence_title, "freq": "daily", "start_date": today},
             expect=MARK)
    run.call("list_recurrences", expect=MARK)
    run.call("update_recurrence", {"title": recurrence_title, "priority": "high"}, expect="已更新")
    instances = run.call("find_tasks", {"query": recurrence_title})
    instance_id = run.first_id(instances)
    if instance_id:
        run.call("skip_occurrence", {"task_id": instance_id}, expect="跳过")
    else:
        run.call("skip_occurrence", {"recurrence_title": recurrence_title, "date": today})

    print()
    width = max(len(name) for name, _ok, _d in run.results)
    failures = 0
    for name, ok, detail in run.results:
        mark = "PASS" if ok else "FAIL"
        if not ok:
            failures += 1
        print(f"[{mark}] {name.ljust(width)}  {detail}")
    print(f"\n{len(run.results)} 项，失败 {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
