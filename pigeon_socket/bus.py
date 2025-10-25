# pigeon_socket/bus.py
from __future__ import annotations
import asyncio
from typing import Any, Optional

class SnapshotBus:
    """
    轻量广播总线：
      - 保存最近一次快照；
      - publish() 唤醒所有等待者；
      - wait_update() 可超时返回 None（用于 SSE 心跳）。
    """
    def __init__(self) -> None:
        self._snapshot: Optional[Any] = None
        self._event = asyncio.Event()
        self._lock = asyncio.Lock()

    async def publish(self, snapshot: Any) -> None:
        async with self._lock:
            self._snapshot = snapshot
            self._event.set()
            self._event = asyncio.Event()

    def peek(self) -> Optional[Any]:
        return self._snapshot

    async def wait_update(self, timeout: float = 15.0) -> Optional[Any]:
        try:
            await asyncio.wait_for(self._event.wait(), timeout=timeout)
            return self._snapshot
        except asyncio.TimeoutError:
            return None

# 模块级单例
bus = SnapshotBus()
