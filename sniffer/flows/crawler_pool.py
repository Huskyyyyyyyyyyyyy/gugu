# ──────────────────────────────────────────────────────────────────────────────
# 模块用途：管理“持久化爬虫实例池”
# 设计要点：
#   - 池大小 == 并发上限，每个槽位：一个爬虫实例 + 一个互斥锁 + 一个单线程执行器；
#   - 线程亲和：实例始终在自己的专属线程里调用，防止底层库的线程亲和/状态问题；
#   - 串行：同一实例上加 Lock，保证不会并行进入 run_crawl；
#   - 轮询：高并发均匀分配到不同槽位（pool_size==1 时走无锁快路径）；
#   - 自愈：执行器/实例损坏时按需重建（仅在必要时触发，不污染热点路径）；
#   - 统一 close()：退出时优雅释放实例与执行器（幂等）。
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import asyncio
import threading
import concurrent.futures
from itertools import count
from typing import Any, Callable, List, Tuple, Optional, Coroutine

from commons.base_logger import BaseLogger
from .pigeon_protocols import PidsCrawlerProto, CurrentCrawlerProto


class CrawlerSlot:
    """
    单个槽位：爬虫实例 + 互斥锁 + 专属单线程执行器。
    - 执行器限制为 1 线程，确保“同一实例只能在同一线程”上运行（线程亲和 + 状态安全）。
    """
    def __init__(self, crawler: PidsCrawlerProto, index: int):
        self.index = index
        self.crawler = crawler
        self.lock = threading.Lock()
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix=f"pigeon-crawler-{index}"
        )

    def shut_executor(self):
        """关闭当前执行器（幂等）。"""
        try:
            self.executor.shutdown(wait=True, cancel_futures=False)
        except Exception:
            # 忽略关闭异常，避免影响收尾流程
            pass


class CrawlerPool:
    """
    持久化爬虫实例池（线程亲和 + 串行 + 轮询分配 + 自愈）。
    外部接口：
      - run_pid(pid)                 # 同步 run_crawl，在槽位专属线程执行
      - get_current_pid()           # 在 current 专属线程获取“当前 PID”
      - run_current_once()          # 一站式：先 get_current_pid 再 run_pid
      - close()                     # 释放资源，幂等
    """

    def __init__(
        self,
        *,
        pool_size: int,
        pids_crawler_factory: Callable[[], PidsCrawlerProto],
        current_crawler_factory: Callable[[], CurrentCrawlerProto],
        logger: BaseLogger,
    ):
        assert pool_size >= 1, "pool_size 必须 >= 1"
        self.log = logger
        self.pool_size = pool_size

        # 保存工厂以便失败时重建实例（自愈）
        self._pids_factory = pids_crawler_factory
        self._current_factory = current_crawler_factory

        # 构造 pids 槽位
        self.slots: List[CrawlerSlot] = [
            CrawlerSlot(self._pids_factory(), i) for i in range(pool_size)
        ]

        # 轮询指针与保护（pool_size>1时启用；=1时走无锁快路径）
        self._rr_guard = asyncio.Lock()
        self._rr_counter = count()

        # 构造 current 爬虫（单实例 + 专属线程 + 锁）
        self.current_crawler = self._current_factory()
        self._current_lock = threading.Lock()
        self._current_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="pigeon-current"
        )

        # 关闭幂等标志
        self._closed = False

        self.log.log_info(
            f"[PoolInit] pool_size={pool_size} "
            f"threads={[s.executor._thread_name_prefix for s in self.slots]} "
            f"current_thread={self._current_executor._thread_name_prefix}"
        )

    # ──────────────────────────── 基础：槽位分配 ──────────────────────────── #
    async def acquire_slot(self) -> Tuple[int, CrawlerSlot]:
        """
        轮询选择一个槽位，返回 (槽位索引, 槽位)。
        - pool_size == 1 时建议直接走快路径（见 run_pid 内分支）；
        - 否则用异步锁保护计数器，避免高并发取同一索引。
        """
        if self.pool_size == 1:
            return 0, self.slots[0]
        async with self._rr_guard:
            idx = next(self._rr_counter) % self.pool_size
        return idx, self.slots[idx]

    # ──────────────────────────── 自愈：重建槽位 ──────────────────────────── #
    async def _recreate_slot(self, idx: int) -> None:
        """
        重建指定槽位的“爬虫实例 + 执行器”。
        触发时机：run_pid 过程中发现执行器已关闭或实例异常后。
        """
        old = self.slots[idx]
        self.log.log_warning(f"[SlotRecreate] rebuilding slot#{idx} ...")

        # 1) 尝试关闭旧实例
        close_fn = getattr(old.crawler, "close", None) or getattr(old.crawler, "quit", None)
        if callable(close_fn):
            try:
                await asyncio.get_running_loop().run_in_executor(None, close_fn)
            except Exception as e:
                self.log.log_warning(f"[SlotRecreate] slot#{idx} close crawler failed: {e}")

        # 2) 关闭旧执行器
        old.shut_executor()

        # 3) 新建实例与执行器
        new_slot = CrawlerSlot(self._pids_factory(), idx)
        self.slots[idx] = new_slot
        self.log.log_info(f"[SlotRecreate] slot#{idx} rebuilt OK")

    # ──────────────────────────── 小工具：同步段封装 ──────────────────────────── #
    @staticmethod
    def _run_sync_on_slot(slot: "CrawlerSlot", pid: int):
        """
        在槽位专属线程中以串行方式调用 crawler.run_crawl(pid)。
        用静态方法避免 run_pid 每次创建闭包/捕获外部变量的开销。
        """
        with slot.lock:  # 串行化，保护实例内部状态
            return slot.crawler.run_crawl(pid)

    @staticmethod
    def _get_current_sync(current_crawler: CurrentCrawlerProto, lock: threading.Lock) -> tuple[Any, Any]:
        """在 current 的专属线程中以串行方式调用 get_current_pigeon_info()。"""
        with lock:
            current_info,pid = current_crawler.get_current_pigeon_info()
            return current_info,pid

    # ──────────────────────────── 业务：按 PID 执行 ──────────────────────────── #
    async def run_pid(self, pid: int) -> Tuple[int, Any]:
        """
        在“槽位专属线程”执行同步 run_crawl(pid)，返回 (槽位索引, 爬虫返回值)。

        优化点：
          - pool_size==1 时走无锁快路径（不进入 acquire_slot()）；
          - 不创建闭包：使用静态方法执行同步段，减少分配；
          - 发现执行器被关闭时轻量自愈（仅当必要时触发，不影响热点路径）。
        """
        if self._closed:
            raise RuntimeError("CrawlerPool 已关闭，无法调用 run_pid()")

        loop = asyncio.get_running_loop()

        # —— 快路径：只有 1 个槽位时，避免异步锁与轮询开销 —— #
        if self.pool_size == 1:
            idx = 0
            slot = self.slots[0]

            # 执行器若被意外关闭（极少见），重建后继续
            if getattr(slot.executor, "_shutdown", False):
                await self._recreate_slot(idx)
                slot = self.slots[0]

            result = await loop.run_in_executor(
                slot.executor, CrawlerPool._run_sync_on_slot, slot, pid
            )
            # 成功路径仅打 debug，减少日志 IO
            self.log.log_debug(f"[RunPID] slot#{idx} pid={pid} ok")
            return idx, result

        # —— 通用路径：多槽位时才走轮询 —— #
        idx, slot = await self.acquire_slot()
        if getattr(slot.executor, "_shutdown", False):
            await self._recreate_slot(idx)
            slot = self.slots[idx]

        result = await loop.run_in_executor(
            slot.executor, CrawlerPool._run_sync_on_slot, slot, pid
        )
        self.log.log_debug(f"[RunPID] slot#{idx} pid={pid} ok")
        return idx, result

    # ──────────────────────────── 业务：获取“当前 PID” ──────────────────────────── #
    async def get_current_pid(self) -> tuple[Any, Any] | None:
        """
        在 current 的专属线程上调用同步 get_current_pigeon_info()。
        ：
          - 不做通用超时/重试（热点路径最轻）；需要时由调用方包超时；
          - 执行器若被关闭时才自愈，避免常态额外分支。
        """
        if self._closed:
            self.log.log_warning("[Current] pool closed; get_current_pid() returns None")
            return None

        # 发现执行器被关闭（极少见），自愈 current 槽
        if getattr(self._current_executor, "_shutdown", False):
            try:
                close_fn = getattr(self.current_crawler, "close", None) or getattr(self.current_crawler, "quit", None)
                if callable(close_fn):
                    await asyncio.get_running_loop().run_in_executor(None, close_fn)
            except Exception as e:
                self.log.log_warning(f"[Current] close failed during recreate: {e}")

            try:
                self._current_executor.shutdown(wait=True, cancel_futures=False)
            except Exception:
                pass

            # 重新构造实例与执行器
            self.current_crawler = self._current_factory()
            self._current_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="pigeon-current"
            )

        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(
                self._current_executor, CrawlerPool._get_current_sync, self.current_crawler, self._current_lock
            )
        except Exception as e:
            self.log.log_warning(f"[Current] error: {e}")
            return None

    # ──────────────────────────── 业务：一站式 current → run ─────────────────── #
    async def run_current_once(self) -> Optional[Tuple[dict[str,Any], Any]]:
        """
        一站式热点路径：获取当前 PID → 立即 run_pid(pid)
        - 不并发、不重试；
        - 快路径优先（pool_size==1 无锁）；
        - 获取不到 PID 则返回 None。
        """
        current_info,pid = await self.get_current_pid()
        if not pid:
            self.log.log_warning("[RunCurrent] no current pid")
            return None
        records = await self.run_pid(pid)
        print(current_info)
        return  current_info, records

    # ──────────────────────────── 资源关闭（幂等） ──────────────────────────── #
    async def close(self):
        """
        关闭所有实例与线程池（幂等）：
          - 关闭各槽位的 crawler（close/quit）
          - 关闭各槽位的 executor
          - 关闭 current 的 crawler 与 executor
        """
        if self._closed:
            return
        self._closed = True

        # 关闭 pids 槽位
        for slot in self.slots:
            close_fn = getattr(slot.crawler, "close", None) or getattr(slot.crawler, "quit", None)
            if callable(close_fn):
                try:
                    await asyncio.get_running_loop().run_in_executor(None, close_fn)
                except Exception as e:
                    self.log.log_warning(f"[Shutdown] close crawler failed (slot#{slot.index}): {e}")
            slot.shut_executor()

        # 关闭 current
        close_fn = getattr(self.current_crawler, "close", None) or getattr(self.current_crawler, "quit", None)
        if callable(close_fn):
            try:
                await asyncio.get_running_loop().run_in_executor(None, close_fn)
            except Exception as e:
                self.log.log_warning(f"[Shutdown] close current crawler failed: {e}")

        try:
            self._current_executor.shutdown(wait=True, cancel_futures=False)
        except Exception:
            pass

        self.log.log_info("[Shutdown] crawler pool closed")
