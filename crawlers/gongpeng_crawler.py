# -*- coding: utf-8 -*-
from __future__ import annotations

from pprint import pprint
from typing import Any, Dict, List, Optional
from copy import deepcopy
from requests import Response

from commons.base_crawler import BaseCrawler
from commons.base_logger import BaseLogger
from mydataclass.gongpeng import GongpengInfo
from tools.config_loader import load_config
from tools.request_utils import _clean_url, _normalized_params


class GongpengCrawler(BaseCrawler):
    """
    公棚（auction）列表抓取器
    - 读取 YAML：api_url / delay / timeout / max_retries / params
    - 使用 BaseCrawler 的会话/重试/节流
    - 支持分页抓取；解析为 GongpengInfo 列表
    """

    def __init__(
        self,
        *,
        headers: Optional[Dict[str, str]] = None,
        proxies: Optional[List[Dict[str, str]]] = None,
        min_delay: float | None = None,
        max_delay: float | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        config_path: str = r"config\spider.yaml",
        config_section: str = "gongpeng",
        **kwargs
    ):
        # 读取配置放到实例级（避免类属性被多个实例共享）
        cfg = load_config(config_section, config_path)
        if not cfg:
            raise RuntimeError(f"load_config failed: section={config_section}, path={config_path}")

        self.api_url: str = _clean_url(cfg["api_url"])
        self.cfg_delay: float = float(cfg.get("delay", 2))
        self.cfg_timeout: float = float(cfg.get("timeout", 10))
        self.cfg_retries: int = int(cfg.get("max_retries", 3))
        self.param_template: Dict[str, Any] = deepcopy(cfg.get("params", {}))

        # 确保 pagesize 存在且合理（>0）。若未配置则默认 50。
        ps = int(self.param_template.get("pagesize", 0) or 0)
        if ps <= 0:
            self.param_template["pagesize"] = 50

        super().__init__(
            headers=headers,
            proxies=proxies,
            min_delay=min_delay if min_delay is not None else 0.2,
            max_delay=max_delay if max_delay is not None else self.cfg_delay,
            timeout=timeout if timeout is not None else self.cfg_timeout,
            max_retries=max_retries if max_retries is not None else self.cfg_retries,
            logger=BaseLogger(name="GongPengCrawler", to_file=True), **kwargs
        )

        self.logger.log_info(
            f"GongpengCrawler ready | url={self.api_url} "
            f"| min_delay={self.min_delay}s max_delay={self.max_delay}s "
            f"| timeout={self.timeout} | retries={self.max_retries} | pagesize={self.param_template.get('pagesize')}"
        )

    # ---------------- 便捷设置 ----------------

    def set_time_window(self, start_ts: int | None = None, end_ts: int | None = None) -> None:
        """设置抓取时间窗口（finishstarttime, finishendtime），传入 0 或空可清空（将被参数归一移除）。"""
        if start_ts is not None:
            self.param_template["finishstarttime"] = start_ts
        if end_ts is not None:
            self.param_template["finishendtime"] = end_ts

    def set_keyword(self, key: str | None) -> None:
        """设置查询关键字；None/空串视为“不限制”（在请求时会被剔除）。"""
        self.param_template["key"] = (key or "")

    # ---------------- 内部工具 ----------------

    @staticmethod
    def _extract_list(page_json: Dict[str, Any] | List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """统一抽取列表数据：支持顶层 list，或 dict 的 data/list/records。"""
        # ① 顶层 list：直接返回
        if isinstance(page_json, list):
            return page_json
        # ② dict：查 data/list/records
        if isinstance(page_json, dict):
            for k in ("data", "list", "records"):
                v = page_json.get(k)
                if isinstance(v, list):
                    return v
        return []

    # ---------------- 基类要求的 parse（只接收 Response） ----------------

    def parse(self, response: Response) -> List[GongpengInfo]:
        try:
            raw = response.json()
        except Exception as e:
            self.logger.log_error(f"parse response.json failed: {e}")
            return []

        # 先处理“顶层 list”
        if isinstance(raw, list):
            try:
                return GongpengInfo.from_list(raw)
            except Exception as e:
                self.logger.log_error(f"GongpengInfo.from_list error: {e}")
                return []

        # 再兼容 dict 的 data/list/records
        data_list = self._extract_list(raw)
        if not data_list:
            self.logger.log_warning("parse ignored: data/list/records is empty or not list")
            return []

        try:
            return GongpengInfo.from_list(data_list)
        except Exception as e:
            self.logger.log_error(f"GongpengInfo.from_list error: {e}")
            return []

    # ---------------- 单页抓取（返回 dict，供分页循环使用） ----------------

    def fetch_page(self, page_no: int) -> Dict[str, Any]:
        """
        获取单页原始 JSON 数据（dict）。失败返回 {}。
        注意：不要把 dict 传给 parse()，parse 只接受 Response。
        """
        params = deepcopy(self.param_template)
        # 统一设置分页参数
        params["pageno"] = int(page_no)
        # 确保 pagesize 合理
        if int(params.get("pagesize", 0) or 0) <= 0:
            params["pagesize"] = 50

        params = _normalized_params(params, keep_zero_keys=["pageno", "pagesize"])

        # 合并 Accept 头，避免覆盖默认头或 BaseCrawler 级别头
        req_headers: Dict[str, str] = {}
        if hasattr(self, "headers") and isinstance(self.headers, dict):
            req_headers.update(self.headers)
        req_headers.setdefault("Accept", "application/json")

        self.logger.log_debug(f"fetch_page begin | page_no={page_no} | params={params}")

        resp = self.fetch(self.api_url, params=params, headers=req_headers)
        if resp is None:
            self.logger.log_warning(f"fetch_page failed | page_no={page_no}")
            return {}

        self.logger.log_debug(f"fetch_page resp | page_no={page_no} | status={resp.status_code}")

        try:
            data = resp.json() or {}
        except ValueError as e:
            self.logger.log_error(f"JSON decode error: {e} | page_no={page_no} | url={self.api_url}")
            return {}

        data_list = self._extract_list(data)  # 吃顶层 list
        self.logger.log_info(
            f"fetch_page ok | page_no={page_no} | returned={len(data_list)} | pagesize={params.get('pagesize')}"
            )
        return data

    # ---------------- 将 list[dict] 转为数据类列表 ----------------

    def _parse_list(self, data_list: List[Dict[str, Any]]) -> List[GongpengInfo]:
        """把接口的 data(list) 转为 GongpengInfo 列表。"""
        if not isinstance(data_list, list):
            return []
        try:
            return GongpengInfo.from_list(data_list)
        except Exception as e:
            self.logger.log_error(f"GongpengInfo.from_list error: {e}")
            return []

    # ---------------- 主流程（分页抓全量，不覆盖基类 crawl） ----------------

    def crawl_all(self) -> List[GongpengInfo]:
        """
        分页抓取并解析全部数据。
        结束条件：返回数量 < pagesize，或接口返回空。
        """
        all_items: List[GongpengInfo] = []

        # 默认从 1 开始，除非模板里显式设置了 pageno
        start_page = int(self.param_template.get("pageno", 1))
        page_no = start_page
        req_pagesize = int(self.param_template.get("pagesize", 50))

        self.logger.log_info(
            f"crawl_all begin | start_page={start_page} | pagesize={req_pagesize}"
        )

        while True:
            page_json = self.fetch_page(page_no)
            data_list = self._extract_list(page_json)

            if not data_list:
                if page_no == start_page:
                    self.logger.log_info("no data on first page, stop")
                else:
                    self.logger.log_info(f"empty page detected at {page_no}, stop")
                break

            parsed = self._parse_list(data_list)
            if parsed:
                all_items.extend(parsed)

            self.logger.log_info(
                f"page progress | page={page_no} | added={len(parsed)} | total={len(all_items)}"
            )

            # 以请求 pagesize 判定是否还有下一页
            if len(data_list) < req_pagesize:
                self.logger.log_info(
                    f"stop condition met: returned({len(data_list)}) < pagesize({req_pagesize})"
                )
                break

            page_no += 1
            # 节流由 BaseCrawler 统一控制

        self.logger.log_info(f"crawl_all done | total={len(all_items)}")
        if all_items:
            first = all_items[0]
            preview = first.to_dict() if hasattr(first, "to_dict") else getattr(first, "__dict__", {})
            self.logger.log_debug(f"first item preview | {preview}")

        return all_items


if __name__ == "__main__":
    gp = GongpengCrawler()
    all_items = gp.crawl_all()
    pprint(all_items)