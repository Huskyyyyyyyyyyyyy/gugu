# ──────────────────────────────────────────────────────────────────────────────
# 模块用途：承载 “flow 扳机” 入口，即统一触发业务流程。
# 说明：
#   - 实际业务逻辑（抓取、查询、排序等）已迁移至 services/pigeon_service.py；
#   - 本类保留统一入口 process_current_pid；
#   - 同时保留历史注释与接口（例如 _query_deal_counts、handle_pigeon_bid 等）；
#   - 方便后续 WebSocket / flow 调用使用该入口触发数据推送。
# ──────────────────────────────────────────────────────────────────────────────

from __future__ import annotations
from typing import List, Any, Coroutine
from commons.base_logger import BaseLogger
from mydataclass.record import BidRecord
from .crawler_pool import CrawlerPool
from .pigeon_config import PigeonConfig
from ..pigeon_pids_query.pigeon_bis_query import PigeonService


class PigeonHandlers:
    """Pigeon Flow 扳机类：保留触发入口与历史注释。"""

    def __init__(self, *, pool: CrawlerPool, cfg: PigeonConfig, logger: BaseLogger):
        # 将核心逻辑交由 PigeonService 执行
        self.service = PigeonService(pool=pool, cfg=cfg, logger=logger)

        # 去抖状态（仍可保留，供 handle_pigeon_bid 使用）
        self._last_run_at: dict[int, float] = {}

    # ─────────────────────────────────────────────
    # flow 扳机主入口：供启动/实时触发/WebSocket调用使用
    # ─────────────────────────────────────────────
    async def process_current_pid(self, reason: str = "realtime", debounce: bool = True) -> tuple[Any, Any]:
        """
        通用处理流程（启动 / 实时触发统一使用）：
          1. 调用 current 爬虫获取当前 PID；
          2. 若获取成功，则直接用爬虫池抓取；
          3. 默认顺序执行（无并发）；
          4. 可选去抖（默认启用，仅实时触发时有效）；
          5. 返回 List[BidRecord]（后续 Socket 可直接推送）。
        """
        # 若后续添加去抖逻辑，可在此层实现，不影响业务类
        current_info,records = await self.service.run_once(reason=reason)
        return current_info,records

    # ─────────────────────────────────────────────
    # 以下保留历史注释与接口（供未来扩展/兼容）
    # ─────────────────────────────────────────────

    # async def _query_deal_counts(self, bids: list[dict]) -> dict:
    #     """
    #     接收上游爬虫返回的 bids 列表（list[dict]），
    #     从中提取在线出价人的 user_code，批量查询他们的历史成交。
    #     """
    #     if not bids:
    #         self.log.log_info("[DB] 无 bids，跳过查询")
    #         return {}
    #     user_codes = list({
    #         bid.get("usercode") or bid.get("bid_user_code")
    #         for bid in bids
    #         if (bid.get("type") == "online") and (bid.get("usercode") or bid.get("bid_user_code"))
    #     })
    #     if not user_codes:
    #         self.log.log_info("[DB] 无有效在线 user_code，跳过查询")
    #         return {}
    #     self.log.log_info(f"[DB] 开始查询历史成交: count={len(user_codes)}, 示例={user_codes[:5]}")
    #     def _run_query_sync():
    #         return self.dao.query_user_deal_records(
    #             user_codes=user_codes,
    #             status_whitelist=("已完成", "已结拍"),
    #             chunk_size=100,
    #         )
    #     try:
    #         results = await asyncio.to_thread(_run_query_sync)
    #     except Exception as e:
    #         self.log.log_error(f"[DB] 查询失败: {e}")
    #         return {}
    #     # ✅ 打印结构化结果
    #     if not results:
    #         self.log.log_info("[DB] 查询结果为空")
    #     else:
    #         for uc, rows in results.items():
    #             self.log.log_info(f"[DB] === 出价人: {uc} ===")
    #             grouped = {}
    #             for r in rows:
    #                 grouped.setdefault(r["matcher_name"], []).append(r)
    #             for matcher_name, recs in grouped.items():
    #                 self.log.log_info(f"鸽主: {matcher_name} | 成交次数: {len(recs)}")
    #                 for rec in recs:
    #                     self.log.log_info(
    #                         f"  └─ 鸽子: {rec['name']} 环号: {rec['foot_ring']} 价格: {rec['quote']}"
    #                     )
    #     return results

    async def handle_pigeon_auction(self, ev, m):
        """预留：拍卖事件触发处理（尚未实现）"""
        pass

    # @staticmethod
    # def _extract_pid(m: re.Match) -> int:
    #     """从路由正则中提取 pid（字符串 → int）。"""
    #     return int(m.group("pigeon"))

    # async def handle_pigeon_bid(self, ev: Event, m: re.Match):
    #     """
    #     实时报价消息处理：
    #       - 去抖（monotonic，抗系统时钟跳变）
    #       - 通过爬虫池在“线程亲和 + 串行”的槽位上执行 run_crawl
    #       - 不复用结果（按需可扩展）
    #     """
    #     pid = self._extract_pid(m)
    #     now, last = time.monotonic(), self._last_run_at.get(pid, 0.0)
    #     if (now - last) < self.cfg.cooldown_sec:
    #         if self.cfg.debug_verbose:
    #             self.log.log_debug(
    #                 f"[Debounce] pid={pid} Δ={round(now - last, 3)}s < {self.cfg.cooldown_sec}s"
    #             )
    #         return
    #     self._last_run_at[pid] = now
    #     idx, bids = await self.pool.run_pid(pid)
    #     if self.cfg.debug_verbose:
    #         size = (len(bids) if hasattr(bids, "__len__") and bids is not None else 0)
    #         self.log.log_debug(f"[Bids] pid={pid} size={size} via slot#{idx}")
