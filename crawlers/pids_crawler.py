# crawlers/jcurrent_pigeons.py
from __future__ import annotations

import json
from copy import deepcopy
from pprint import pprint, pformat
from typing import Any, Optional, Dict, List
from requests import Response
from commons.base_crawler import BaseCrawler
from commons.base_logger import BaseLogger
from mydataclass import record
from mydataclass.pigeon import PigeonInfo
from mydataclass.record import BidRecord
from tools.config_loader import load_config
from tools.request_utils import _clean_url
from crawlers.current_crawler import CurrentPigeonsCrawler


class PidsPigeonsCrawler(BaseCrawler):
    """
    用于获取“指定鸽子pid”的出价记录。
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
        config_section: str = "pid_pigeons",
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


        super().__init__(
            headers=headers,
            proxies=proxies,
            min_delay=min_delay if min_delay is not None else 0.2,
            max_delay=max_delay if max_delay is not None else self.cfg_delay,
            timeout=timeout if timeout is not None else self.cfg_timeout,
            max_retries=max_retries if max_retries is not None else self.cfg_retries,
            logger=BaseLogger(name="CurrentPigeonCrawler", to_file=True),
            **kwargs
        )

        self.logger.log_info(
            f"PidsPigeon_Crawler ready | url={self.api_url} "
        )

    def parse(self, resp: Response) -> Optional[List[Dict[str, Any]]]:
        """
        解析 get_bids_for_pigeon 返回的 HTTP 响应对象。

        功能：
          1. 尝试将响应内容解析为 JSON；
          2. 若结构为 {"code":0,"data":[...]}，则直接取 data；
          3. 若结构为 list，则直接返回；
          4. 若解析失败或 data 字段不存在，返回 None。

        返回：
          list[dict] | None
        """
        if resp is None:
            return None

        try:
            data = resp.json()
        except json.JSONDecodeError:
            print("⚠️ 响应不是有效 JSON：", resp.text[:200])
            return None

        # 结构一：dict 包含 data
        if isinstance(data, dict):
            if "data" in data:
                val = data["data"]
                if isinstance(val, list):
                    return val
                elif isinstance(val, dict):
                    # 有时 data 是 dict，再取里层的 list
                    for k, v in val.items():
                        if isinstance(v, list):
                            return v
                    return [val]
            # 如果本身是 bids / records 等别名
            for k in ("bids", "records", "list"):
                if k in data and isinstance(data[k], list):
                    return data[k]

        # 结构二：顶层就是 list
        if isinstance(data, list):
            return data

        # 都不是预期格式
        print(f"⚠️ 未识别的出价响应结构：{type(data)}")
        return None

    def get_bids_for_pigeon(self, pigeon_id: int) -> Response | None:
        """
        获取指定鸽子ID的出价记录。

        """
        url = self.api_url.format(pid=pigeon_id)
        print(f"get_bids_for_pigeon | url={url}")
        resp = self.fetch(url, headers=self._base_headers,params=self.param_template)
        if resp is None:
            self.logger.log_warning(f"获取出价失败：请求无响应（pid={pigeon_id}）")
            return None

        return resp

    def parse_pigeons(self,data: Any) -> List[record]:
        """
        将接口返回的“当前拍卖鸽”列表转成 PigeonInfo 对象列表。
        - data 可以是 list，也可以是 dict（数据在 data['data'] 或 data['list']）。
        - 非严格模式：单条失败会被跳过并记录日志。
        """
        if isinstance(data, dict):
            items = data.get("data") or data.get("list") or []
        elif isinstance(data, list):
            items = data
        else:
            self.logger.log_warning("未知数据结构: %s", type(data).__name__)
            return []
        return BidRecord.from_list(items, yield_items=False, strict=False)


    def run_crawl(self,pigeon_id: int) -> list[Any] | None:
        """
        通过current_crawler获取pid,返回record[]
        """
        if pigeon_id is not None:
            res = self.get_bids_for_pigeon(pigeon_id)
            if res is not None:
                pigs_data = self.parse(res)
                if pigs_data is not None:
                    # print(type(pigs_data))
                    # pprint(pigs_data)
                    # 弃用去下游封装
                    # pids = self.parse_pigeons(pigs_data)
                    # pprint(pids)
                    return pigs_data
        self.logger.log_info(f"pid={pigeon_id}无出价记录")
        return None



if __name__ == "__main__":
    bis = PidsPigeonsCrawler()
    pigList=bis.run_crawl(187099)