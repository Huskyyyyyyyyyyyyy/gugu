# ──────────────────────────────────────────────────────────────────────────────
# 模块用途：承载“纯业务逻辑”
# 说明：
#   - 只做：抓取当前 PID → 装配 BidRecord → 查询历史成交 → 注入 → 排序/可视化字段；
#   - 不做：路由、实例化、网络传输（WebSocket/SSE）、去抖等（留在 handlers 或更外层）；
#   - 便于单元测试：所有逻辑为纯函数/可控 I/O（DB 查询放入后台线程，避免阻塞事件循环）。
# 依赖：
#   - CrawlerPool：提供 run_current_once()；
#   - PigeonDao：提供 query_user_deal_records()；
#   - PigeonConfig：支持 debug 开关与可选 fuzzy_threshold；
# ──────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import asyncio
import difflib
from pprint import pprint
from typing import Dict, List, Tuple, Any, Iterable, Set, Coroutine, Optional

from commons.base_logger import BaseLogger
from mydataclass.record import BidRecord
from dao.pigeon_dao import PigeonDao
from sniffer.flows.crawler_pool import CrawlerPool
from sniffer.flows.pigeon_config import PigeonConfig


class PigeonService:
    """
    纯业务服务（无传输/扳机职责）：
      - 暴露 run_once(reason="...")：拉取当前场次数据并完成全流程装配；
      - 暴露 build_bid_records_with_history(raw_bids)：接收上游 bids 并做装配与排序。

    """

    def __init__(self, *, pool: CrawlerPool, cfg: PigeonConfig, logger: BaseLogger):
        self.pool = pool
        self.cfg = cfg
        self.log = logger
        self.dao = PigeonDao()

    # =========================================================================
    # 对外主方法
    # =========================================================================
    @staticmethod
    def _default_current_info() -> Dict[str, Any]:
        # 始终给出规范结构，缺失字段为 None
        return {"id": None, "footring": None, "matchername": None}

    async def run_once(self, *, reason: str = "realtime") -> Tuple[Dict[str, Any], List[BidRecord]]:
        """
        拉取 + 装配的一次性流程：
          1) 调用 current 爬虫获取当前 PID；
          2) 若有数据，组装为 BidRecord 列表并查询历史成交；
          3) 注入/排序后返回 List[BidRecord]。
        备注：
          - 不负责“去抖/限流”，扳机层（handlers）按需实现；
          - 返回结果可直接被 WebSocket/SSE 序列化后推送。
        返回：
          (current_info, bids) —— 即使没有当前 PID，也返回 (默认 current_info, [])，不返回 None 或单独的 []。
        """
        # 期望 pool.run_current_once() 返回：current_info, ret
        # 其中 ret 为 None 或 (idx, bids)
        try:
            current_info, ret = await self.pool.run_current_once()
        except Exception as e:
            self.log.log_warning(f"[{reason}] run_current_once() error: {e}")
            return self._default_current_info(), []

        # 兜底 current_info 结构
        if not isinstance(current_info, dict):
            current_info = self._default_current_info()
        else:
            # 确保关键字段存在
            current_info.setdefault("id", None)
            current_info.setdefault("footring", None)
            current_info.setdefault("matchername", None)

        # 无当前 PID 或拉取失败 —— 返回空记录，但保持二元组结构
        if ret is None:
            self.log.log_warning(f"[{reason}] skip: no current pid")
            return current_info, []

        # ret 结构校验
        if not (isinstance(ret, tuple) and len(ret) == 2):
            self.log.log_warning(f"[{reason}] unexpected ret from pool.run_current_once(): {type(ret)}")
            return current_info, []

        idx, bids = ret

        # 统一 bids 类型
        if bids is None:
            bids = []
        else:
            bids = list(bids)

        size = len(bids)
        if self.cfg.debug_verbose:
            self.log.log_debug(f"[{reason}] fetched via slot#{idx}, size={size}")
        else:
            self.log.log_info(f"[{reason}] fetched via slot#{idx}")

        # 组装历史，保证返回 List[BidRecord]
        try:
            bids = await self.build_bid_records_with_history(bids,current_info["matchername"])
        except Exception as e:
            self.log.log_warning(f"[{reason}] build_bid_records_with_history() error: {e}")
            bids = []

        return current_info, bids

    async def build_bid_records_with_history(self, raw_bids: List[Dict[str, Any]],compare_name:str) -> List[BidRecord]:
        """
        装配流程（异步）：
          1) 封装：上游 bids(list[dict]) → List[BidRecord]（内部已按 user_code 统计 count）
          2) 抽取：筛选唯一在线 user_code（避免重复查询）
          3) 查询：后台线程执行 DB 批量查询（结构化结果）
          4) 注入：record.results = { user_code: [...] }（仅挂该记录的 user_code 对应结果）
          5) 排序：应用相似度/聚合规则并附加可视化字段（_match_*、_agg_*、_match_spans）
          6) 返回：List[BidRecord]
        """
        # 1) 封装为 BidRecord；count 在 from_list 内部已按 user_code 统计并填充
        records = BidRecord.from_list(raw_bids, strict=False, log_errors=True)

        # 2) 抽取需查询的在线出价人编码
        user_codes = self._extract_unique_online_user_codes(records)

        if not user_codes:
            # 无需查询时，也为前端填入空 results，避免判空逻辑
            for r in records:
                uc = r.user_code or ""
                r.results = {uc: []}
            self.log.log_info("[DB] 无在线出价人需要查询，results 置为空集合")
            return records

        # 3) 在后台线程里执行同步 DB 查询，避免阻塞事件循环
        result_map = await self._query_history_in_background(user_codes)

        # 4) 注入每条记录的 results：只挂与该记录 user_code 对应的结果（引用，不深拷贝）
        self._inject_results_into_records(records, result_map)

        # 5) 排序与可视化字段（阈值可从配置读取）
        fuzzy_threshold = getattr(self.cfg, "fuzzy_threshold", 0.8) or 0.8
        self._apply_custom_sort_rules_with_fuzzy(records, fuzzy_threshold=float(fuzzy_threshold),compare_name=compare_name)

        # 可选调试输出：简要核对装配情况
        if self.cfg.debug_verbose:
            self._debug_dump(records)

        return records

    # =========================================================================
    # 内部工具方法
    # =========================================================================
    @staticmethod
    def _extract_unique_online_user_codes(records: Iterable[BidRecord]) -> List[str]:
        """
        从记录中提取“在线出价”的唯一 user_code 列表。
        这样可以避免对同一批中重复 user_code 重复查询。
        排序仅为日志与测试可重复性，非业务必要。
        """
        unique: Set[str] = {
            r.user_code for r in records
            if (r.type == "online" and r.user_code)
        }
        return sorted(unique)

    async def _query_history_in_background(self, user_codes: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        在后台线程中进行数据库查询（同步函数），避免阻塞事件循环。
        返回结构：
          {
            "<user_code>": [
              {"matcher_name": "...", "name": "...", "foot_ring": "...", "quote": 1234.0, ...},
              ...
            ],
            ...
          }
        """
        def _run_db():
            return self.dao.query_user_deal_records(
                user_codes=user_codes,
                status_whitelist=("已完成", "已结拍"),
                chunk_size=100,
            )

        try:
            return await asyncio.to_thread(_run_db)
        except Exception as e:
            self.log.log_error(f"[DB] 历史成交查询失败: {e}")
            return {}

    @staticmethod
    def _inject_results_into_records(
        records: Iterable[BidRecord],
        result_map: Dict[str, List[Dict[str, Any]]],
    ) -> None:
        """
        将数据库查询结果注入到每条 BidRecord 的 results 字段中：
          record.results = { record.user_code: result_map.get(record.user_code, []) }
        注意：这里挂的是对同一 list 的引用，不做深拷贝，减少内存开销。
        """
        for r in records:
            uc = r.user_code or ""
            r.results = {uc: result_map.get(uc, [])}


    # -------------------------
    # 相似度与可视化辅助
    # -------------------------
    @staticmethod
    def _normalize_name(s: str) -> str:
        """
        轻量规范化：去两端空白、压缩内部连续空白为单空格、统一为小写。
        如需更激进（移除标点、全半角转换、繁简转换），可在此扩展。
        """
        if not s:
            return ""
        s = " ".join(str(s).strip().split())
        return s.lower()

    @staticmethod
    def _similarity(a: str, b: str) -> float:
        """
        使用标准库 difflib 计算相似度（0~1）。无需额外依赖。
        如果未来要更强的相似度（编辑距离/模糊匹配），可切到 rapidfuzz。
        """
        if not a or not b:
            return 0.0
        return difflib.SequenceMatcher(None, a, b).ratio()

    @staticmethod
    def _lcs_highlight_spans(a: str, b: str) -> List[Tuple[int, int]]:
        """
        计算 a 与 b 的最长公共子序列(LCS)在 a 中的高亮区间列表。
        用于“相同字符红色显示”的位置标注（前端渲染）。
        返回为不重叠的半开区间 [start, end)，已合并相邻连续片段。
        复杂度 O(len(a)*len(b))，适合昵称级别的短字符串。
        """
        n, m = len(a), len(b)
        if n == 0 or m == 0:
            return []

        # 动态规划求 LCS 长度表
        dp = [[0] * (m + 1) for _ in range(n + 1)]
        for i in range(n - 1, -1, -1):
            for j in range(m - 1, -1, -1):
                if a[i] == b[j]:
                    dp[i][j] = dp[i + 1][j + 1] + 1
                else:
                    dp[i][j] = max(dp[i + 1][j], dp[i][j + 1])

        # 回溯 LCS，拿到 a 中匹配到的索引集合
        i = j = 0
        idxs: List[int] = []
        while i < n and j < m:
            if a[i] == b[j]:
                idxs.append(i)
                i += 1
                j += 1
            elif dp[i + 1][j] >= dp[i][j + 1]:
                i += 1
            else:
                j += 1

        if not idxs:
            return []

        # 合并相邻索引为区间
        spans: List[Tuple[int, int]] = []
        start = prev = idxs[0]
        for k in idxs[1:]:
            if k == prev + 1:
                prev = k
            else:
                spans.append((start, prev + 1))
                start = prev = k
        spans.append((start, prev + 1))
        return spans

    def _apply_custom_sort_rules_with_fuzzy(
        self,
        records: Iterable['BidRecord'],
        *,
        fuzzy_threshold: float = 0.8,
        compare_name: str | None = None,   # 参数：与 matcher_name 比较
    ) -> None:
        """
        对每条记录的 results[user_code] 应用自定义排序（含模糊匹配）并附加可视化字段。
        排序优先级：
          1) 完全相等匹配（matcher_name == usernickname）
          2) 模糊匹配命中（相似度 >= fuzzy_threshold）
          3) 相似度分值（降序）
          4) 该 matcher_name 聚合成交次数（降序）
          5) 该 matcher_name 聚合成交总价（降序）

        同时为每条 row 附加以下字段，供前端使用：
          - _agg_count: 该 matcher_name 的成交次数
          - _agg_total: 该 matcher_name 的总成交价
          - _match_score: 与当前记录 usernickname 的相似度（0~1）
          - _match_exact: 是否完全相等匹配（规范化后）
          - _match_hit: 是否达到模糊匹配阈值（_match_score >= threshold）
          - _match_spans: 在 matcher_name 原始字符串内需要高亮的区间列表（LCS得到）
        """
        for record in records:
            uc = getattr(record, "user_code", "") or ""
            rows = (getattr(record, "results", {}) or {}).get(uc, [])
            if not rows:
                continue

            raw_nick = compare_name or ""
            norm_nick = self._normalize_name(raw_nick)


            # 统计每个 matcher_name 的聚合
            agg: Dict[str, Dict[str, float]] = {}
            for r in rows:
                mn = (r.get("matcher_name") or "").strip()
                a = agg.setdefault(mn, {"count": 0, "total": 0.0})
                a["count"] += 1
                try:
                    a["total"] += float(r.get("quote") or 0)
                except (TypeError, ValueError):
                    pass

            # 预计算每行的相似度与高亮信息
            for r in rows:
                mn_raw = (r.get("matcher_name") or "").strip()
                mn_norm = self._normalize_name(mn_raw)

                score = self._similarity(mn_norm, norm_nick) if (mn_norm and norm_nick) else 0.0
                exact = (mn_norm == norm_nick) and (mn_norm != "")
                hit = (score >= fuzzy_threshold)

                r["_match_score"] = float(score)
                r["_match_exact"] = bool(exact)
                r["_match_hit"] = bool(hit)

                # 高亮区间：在原始字符串上做 LCS（保持前端展示的可感知位置）
                r["_match_spans"] = self._lcs_highlight_spans(mn_raw, raw_nick) if (mn_raw and raw_nick) else []

                # 附上聚合统计，便于前端显示
                a = agg.get(mn_raw, {"count": 0, "total": 0.0})
                r["_agg_count"] = a["count"]
                r["_agg_total"] = a["total"]

            # 排序键：完全相等 > 模糊命中 > 分值 > 次数 > 总价
            def sort_key(r: Dict[str, Any]):
                return (
                    -int(r.get("_match_exact", False)),
                    -int(r.get("_match_hit", False)),
                    -float(r.get("_match_score", 0.0)),
                    -int(r.get("_agg_count", 0)),
                    -float(r.get("_agg_total", 0.0)),
                )

            rows.sort(key=sort_key)

    @staticmethod
    def _debug_dump(records: Iterable[BidRecord]) -> None:
        """
        调试输出：用于在开发/联调阶段快速核对装配结果。
        生产环境可降低日志级别或关闭。
        """
        # pprint(records)
        pass

        # for r in records:
        #     uc = r.user_code or "-"
        #     rows = (r.results or {}).get(uc, [])
        #     self.log.log_info(
        #         f"[RECORD] code={r.code} user={uc} count={r.count} history={len(rows)}"
        #         )
        #     #展开打印单条历史
        #     for item in rows:
        #         self.log.log_info(
        #             f"  └─ 鸽主:{item.get('matcher_name','')} "
        #             f"鸽子:{item.get('name','')} "
        #             f"环号:{item.get('foot_ring','')} "
        #             f"价格:{item.get('quote','')}"
        #         )