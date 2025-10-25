# crawlers/jcurrent_pigeons.py
from __future__ import annotations

import json
from copy import deepcopy
from pprint import pprint
from typing import Any, Optional, Dict, List
from requests import Response
from scrapy.crawler import Crawler

from commons.base_crawler import BaseCrawler
from commons.base_logger import BaseLogger
from tools.config_loader import load_config
from tools.request_utils import _clean_url


class CurrentPigeonsCrawler(BaseCrawler):
    """
    获取current当前直播鸽子信息
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
        config_section: str = "current_pigeons",
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


        super().__init__(
            headers=headers,
            proxies=proxies,
            min_delay=min_delay if min_delay is not None else 0.2,
            max_delay=max_delay if max_delay is not None else self.cfg_delay,
            timeout=timeout if timeout is not None else self.cfg_timeout,
            max_retries=max_retries if max_retries is not None else self.cfg_retries,
            logger=BaseLogger(name="CurrentPigeonCrawler", to_file=True), **kwargs
        )

        self.logger.log_info(
            f"CurrentPigeon_Crawler ready | url={self.api_url} "
        )

    # ------------------ 通用方法 ------------------

    def on_response(self, response: Response) -> None:
        """
        响应回调钩子：在每次请求响应后被调用。
        - 主要用于打印速率限制信息（X-RateLimit-Remaining）。
        """
        remain = response.headers.get("X-RateLimit-Remaining")
        if remain is not None:
            self.logger.log_debug(f"RateLimit remaining: {remain}")

    def parse(self, response: Response) -> Any:
        """
        将HTTP响应解析为JSON对象。
        若解析失败，则返回原始文本并打印警告，方便排查。
        """
        try:
            return response.json()
        except ValueError:
            self.logger.log_warning("响应不是有效 JSON，返回原始文本用于排查")
            return response.text

    # ------------------ 主体逻辑 ------------------

    def get_current_pigeon_raw(self) -> Optional[Dict[str, Any]]:
        """
        获取“当前拍卖鸽”的原始JSON数据。
        """

        resp = self.fetch(self.api_url, headers=self._base_headers)
        if resp is None:
            self.logger.log_warning("获取当前鸽子失败：请求无响应")
            return None

        data = self.parse(resp)
        if not isinstance(data, dict):
            self.logger.log_warning("当前鸽子响应不是对象")
            return None

        return data


    def get_current_pigeon_id(self) -> Optional[int]:
        """
        从当前鸽子的原始数据中直接取出顶层字段“id”。
        若取值异常或非整数，则返回 None。
        """
        data = self.get_current_pigeon_raw()
        if not data:
            return None

        pid = data.get("id")
        try:
            return int(pid)
        except Exception:
            self.logger.log_warning(f"顶层 id 不是整数：{pid}")
            return None

    def get_current_pigeon_info(self) -> tuple[dict[Any, Any], Any | None] | None:
        """
        从当前鸽子的原始数据中直接取出顶层字段 “id”、"footring"、"matchername"。
        若取值异常或字段不存在，则对应值为 None。
        """
        data = self.get_current_pigeon_raw()
        if not data:
            return None

        result = {}

        # 获取 id
        pid = data.get("id")
        try:
            result["id"] = int(pid)
        except Exception:
            self.logger.log_warning(f"顶层 id 不是整数：{pid}")
            result["id"] = None

        # 获取 footring
        result["footring"] = data.get("footring")
        if result["footring"] is None:
            self.logger.log_warning("未找到 footring 字段")

        # 获取 matchername
        result["matchername"] = data.get("matchername")
        if result["matchername"] is None:
            self.logger.log_warning("未找到 matchername 字段")


        return result,pid

    def crawl_run(self, url: str | None = None) -> Optional[Any]:
        """
        爬虫入口
        """




# ------------------ 调试用示例 ------------------

if __name__ == "__main__":
    currentCrawler = CurrentPigeonsCrawler()
    run = currentCrawler.crawl_run()
    pprint(run)
