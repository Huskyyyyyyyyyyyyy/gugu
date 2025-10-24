
#  丢头队列封装 DropHeadQueue

from __future__ import annotations
import asyncio
import contextlib
from typing import Any

class DropHeadQueue:
    """
    asyncio.Queue 的轻量封装：
    - 若队列已满：丢弃最旧元素（drop head），避免内存膨胀。
    - 不负责“哨兵”保留策略（由 Trigger 保证在停止阶段可达）。
    """

    def __init__(self, cap: int):
        self._q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=cap)

    async def put(self, item: dict[str, Any]) -> None:
        if self._q.full():
            with contextlib.suppress(Exception):
                self._q.get_nowait()
                self._q.task_done()
        await self._q.put(item)

    async def get(self) -> dict[str, Any]:
        return await self._q.get()

    def task_done(self) -> None:
        self._q.task_done()
