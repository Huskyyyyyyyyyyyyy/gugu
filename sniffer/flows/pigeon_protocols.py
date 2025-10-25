# ──────────────────────────────────────────────────────────────────────────────
# 模块用途：声明“爬虫接口（协议）”，用于静态检查与解耦实现。
# 说明：
#   - 使用 typing.Protocol 约束爬虫实现需要提供的方法签名；
#   - 方便未来替换为其它实现（例如 HTTP 版或异步版），IDE 与 mypy 能尽早发现不兼容。
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
from typing import Protocol, Any, Optional, List


class PidsCrawlerProto(Protocol):
    """按 pid 爬取出价的同步爬虫接口（协议）。实现类需提供 run_crawl 等方法。"""
    def run_crawl(self, pid: int) -> Any: ...
    def close(self) -> None: ...  # 若没有 close，可在调用方判空 getattr 再调用

class CurrentCrawlerProto(Protocol):
    """获取“当前 pid”的同步爬虫接口（协议）。"""
    def get_current_pigeon_info(self): ...
    def close(self) -> None: ...
