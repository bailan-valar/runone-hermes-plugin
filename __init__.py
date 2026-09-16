"""RunOne 平台插件（Hermes 网关插件包）。

`hermes` 的目录插件加载器按 `plugins/<kind>/<name>/` 发现本目录，导入本包并调用 `register(ctx)`。
"""

from .adapter import register

__all__ = ["register"]
