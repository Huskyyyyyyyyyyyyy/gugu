# ──────────────────────────────────────────────────────────────────────────────
# 模块用途：承载“业务逻辑”，即：去抖、调用池执行、冷启动流程。
# 说明：
#   - 不参与路由与实例化（这些放在 pigeon_flow.py）；
#   - 只做纯逻辑，可单独做单元测试；
#   - 不复用结果，只复用实例（开销更小，逻辑与现状一致）。
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import asyncio
import re
import time
from pprint import pformat
from typing import Dict, List

from commons.base_logger import BaseLogger
from sniffer.models import Event
from .crawler_pool import CrawlerPool
from .pigeon_config import PigeonConfig

class PigeonHandlers:
    """Pigeon 业务处理：去抖、冷启动、执行爬虫调用。"""

    def __init__(self, *, pool: CrawlerPool, cfg: PigeonConfig, logger: BaseLogger):
        self.pool = pool
        self.cfg = cfg
        self.log = logger
        # 去抖状态：记录每个 pid 最近一次处理的 monotonic 时间戳
        self._last_run_at: Dict[int, float] = {}

    @staticmethod
    def _extract_pid(m: re.Match) -> int:
        """从路由正则中提取 pid（字符串 → int）。"""
        return int(m.group("pigeon"))

    async def handle_pigeon_bid(self, ev: Event, m: re.Match):
        """
        实时报价消息处理：
          - 去抖（monotonic，抗系统时钟跳变）
          - 通过爬虫池在“线程亲和 + 串行”的槽位上执行 run_crawl
          - 不复用结果（按需可扩展）
        """
        pid = self._extract_pid(m)

        # —— 去抖：同一 pid 在冷却窗口内忽略
        now, last = time.monotonic(), self._last_run_at.get(pid, 0.0)
        if (now - last) < self.cfg.cooldown_sec:
            if self.cfg.debug_verbose:
                self.log.log_debug(f"[Debounce] pid={pid} Δ={round(now - last, 3)}s < {self.cfg.cooldown_sec}s")
            return
        self._last_run_at[pid] = now

        # —— 执行：槽位线程上同步调用 run_crawl（外层可叠加信号量限流）
        idx, bids = await self.pool.run_pid(pid)
        if self.cfg.debug_verbose:
            size = (len(bids) if hasattr(bids, "__len__") and bids is not None else 0)
            # pformat(bids)
            self.log.log_debug(f"[Bids] pid={pid} size={size} via slot#{idx}")

    async def cold_start_once(self):
        """
        冷启动流程：
          1) 从 env 解析显式 pid 列表；
          2) 若无且允许回退，则通过 current 接口拉取“当前 pid”；
          3) 对最终列表并发抓取（并发上限 = cfg.max_concurrency）。
        """
        # 1) 解析 pid 列表
        pids = self._parse_bootstrap_pids(self.cfg.bootstrap_pids_raw)

        # 2) 回退：current 接口
        if not pids and self.cfg.use_current_bootstrap:
            try:
                pid = await self.pool.get_current_pid()
                if pid:
                    pids = [pid]
                    self.log.log_info(f"[Bootstrap] 当前 PID 获取成功：pid={pid}")
                else:
                    self.log.log_warning("[Bootstrap] 未获取到当前 PID（可能接口返回空）")
            except Exception as e:
                self.log.log_error(f"[Bootstrap] 获取当前 PID 失败：{e}")

        # 3) 无 pid → 直接跳过
        if not pids:
            self.log.log_info("[Bootstrap] 跳过冷启动：无可用 PID 列表")
            return

        # 4) 并发抓取（使用一个局部信号量控制并发即可）
        self.log.log_info(f"[Bootstrap] 启动预抓任务：pids={pids}, 并发={self.cfg.max_concurrency}")
        sem = asyncio.Semaphore(self.cfg.max_concurrency)

        async def _one(pid: int):
            async with sem:
                try:
                    idx, _ = await self.pool.run_pid(pid)
                    self.log.log_info(f"[Bootstrap] PID={pid} 抓取成功 via slot#{idx}")
                except Exception as e:
                    self.log.log_error(f"[Bootstrap] PID={pid} 抓取失败：{e}")

        await asyncio.gather(*(_one(pid) for pid in pids))

    @staticmethod
    def _parse_bootstrap_pids(env_val: str) -> List[int]:
        """env 字符串 → int 列表；非法项静默跳过（可按需改为日志告警）。"""
        if not env_val:
            return []
        out: List[int] = []
        for tok in env_val.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                out.append(int(tok))
            except ValueError:
                pass
        return out
