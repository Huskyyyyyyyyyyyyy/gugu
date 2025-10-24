# ──────────────────────────────────────────────────────────────────────────────
# 模块用途：管理“持久化爬虫实例池”
# 设计要点：
#   - 池大小 == 并发上限，每个槽位：一个爬虫实例 + 一个互斥锁 + 一个单线程执行器；
#   - 线程亲和：实例始终在自己的专属线程里调用，防止底层库的线程亲和/状态问题；
#   - 串行：同一实例上加 Lock，保证不会并行进入 run_crawl；
#   - 轮询：高并发均匀分配到不同槽位；
#   - 统一 close()：退出时优雅释放实例与执行器。
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import asyncio
import threading
import concurrent.futures
from itertools import count
from typing import Any, Callable, List, Tuple

from commons.base_logger import BaseLogger
from .pigeon_protocols import PidsCrawlerProto, CurrentCrawlerProto

class CrawlerSlot:
    """单个槽位：爬虫实例 + 互斥锁 + 专属单线程执行器。"""
    def __init__(self, crawler: PidsCrawlerProto, index: int):
        self.crawler = crawler
        self.lock = threading.Lock()
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix=f"pigeon-crawler-{index}"
        )

class CrawlerPool:
    """
    持久化爬虫实例池（线程亲和 + 串行 + 轮询分配）。
    外部只需调用 run_pid / get_current_pid / close 三个方法。
    """
    def __init__(
        self,
        *,
        pool_size: int,
        pids_crawler_factory: Callable[[], PidsCrawlerProto],
        current_crawler_factory: Callable[[], CurrentCrawlerProto],
        logger: BaseLogger,
    ):
        self.log = logger
        self.pool_size = pool_size

        # 构造 pids 槽位
        self.slots: List[CrawlerSlot] = [
            CrawlerSlot(pids_crawler_factory(), i) for i in range(pool_size)
        ]

        # 轮询指针与保护
        self._rr_guard = asyncio.Lock()
        self._rr_counter = count()

        # 构造 current 爬虫（单实例 + 专属线程 + 锁）
        self.current_crawler = current_crawler_factory()
        self._current_lock = threading.Lock()
        self._current_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="pigeon-current"
        )

    async def acquire_slot(self) -> Tuple[int, CrawlerSlot]:
        """轮询选择一个槽位，返回(槽位索引, 槽位)。"""
        async with self._rr_guard:
            idx = next(self._rr_counter) % self.pool_size
        return idx, self.slots[idx]

    async def run_pid(self, pid: int) -> Tuple[int, Any]:
        """
        在“槽位专属线程”执行同步 run_crawl。
        返回 (槽位索引, 爬虫返回值)。
        """
        idx, slot = await self.acquire_slot()

        def _work():
            # 严格串行化，避免实例内部状态被并发破坏
            with slot.lock:
                return slot.crawler.run_crawl(pid)

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(slot.executor, _work)
        return idx, result

    async def get_current_pid(self) -> int | None:
        """在 current 的专属线程上调用同步 get_current_pigeon_id。"""
        def _work():
            with self._current_lock:
                return self.current_crawler.get_current_pigeon_id()

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._current_executor, _work)

    async def close(self):
        """关闭所有实例与线程池（优雅收尾）。"""
        # 关闭 pids 槽位
        for slot in self.slots:
            close_fn = getattr(slot.crawler, "close", None) or getattr(slot.crawler, "quit", None)
            if callable(close_fn):
                try:
                    await asyncio.get_running_loop().run_in_executor(None, close_fn)
                except Exception as e:
                    self.log.log_warning(f"[Shutdown] close crawler failed: {e}")
            # 关闭该槽位专属执行器
            slot.executor.shutdown(wait=True, cancel_futures=False)

        # 关闭 current
        close_fn = getattr(self.current_crawler, "close", None) or getattr(self.current_crawler, "quit", None)
        if callable(close_fn):
            try:
                await asyncio.get_running_loop().run_in_executor(None, close_fn)
            except Exception as e:
                self.log.log_warning(f"[Shutdown] close current crawler failed: {e}")
        self._current_executor.shutdown(wait=True, cancel_futures=False)
