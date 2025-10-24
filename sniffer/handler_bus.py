
# Handler 分发总线 HandlerBus

from __future__ import annotations
import asyncio
import json
from typing import Awaitable, Callable, List

from models import Event

Handler = Callable[[Event], Awaitable[None]]

class HandlerBus:
    """
    负责管理与并发调用所有已注册的异步 handler。
    - 并发 fan-out
    - 吞异常但输出一条结构化错误日志
    """

    def __init__(self):
        self._handlers: List[Handler] = []

    def add(self, handler: Handler) -> None:
        """注册 handler。约定：async def handler(ev: Event) -> None"""
        self._handlers.append(handler)

    async def emit(self, ev: Event) -> None:
        """并发执行所有 handler；保底输出错误日志。"""
        if not self._handlers:
            return
        results = await asyncio.gather(*(h(ev) for h in self._handlers), return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                print(json.dumps(
                    {"level": "error", "msg": "handler_failed", "err": repr(r)},
                    ensure_ascii=False
                ))
