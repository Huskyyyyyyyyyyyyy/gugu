import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
from copy import deepcopy
from pprint import pprint
from typing import List, Any, Dict, Iterable
from requests import Response
from commons.base_crawler import BaseCrawler
from commons.base_logger import BaseLogger
from crawlers.sections_clawler import SectionCrawler
from mydataclass.pigeon import PigeonInfo
from tools.config_loader import load_config


class PigeonCrawler(BaseCrawler):
    """
    拍卖鸽子（pigeons）抓取：
    - 通过 (gongpeng_id, section_id) 组合拉取对应 pigeons
    - 复用 BaseCrawler 的重试 / 节流 / 代理 / UA 注入
    - 并发时：每个 worker 独立实例（独立 Session/节流基准），保证线程安全
    """

    MAX_WORKERS = 20  # 可按目标站点负载能力调整

    # ---------------- 必要的抽象方法实现 ----------------
    def parse(self, response: Response) -> Any:
        """
        说明：
        - 本子类主流程直接调用 fetch_pigeons()/fetchall_pigeons()，不走 crawl()
        - 为避免误用，这里直接抛出提示
        """
        raise NotImplementedError("PigeonCrawler 不走 crawl() 流程，请调用 fetch_pigeons / fetchall_pigeons")

    # ---------------- 构造与初始化 ----------------
    def __init__(
        self,
        *,
        delay: float = 3,                      # 最大节流延迟（秒），最小延迟固定传 0.2，父类将在 [min_delay, max_delay] 内抖动
        max_retries: int = 3,                  # 父类 Retry 总重试次数（connect/read/status）
        timeout: float | tuple[float, float] = 10,  # 超时（秒或 (connect, read)）
        headers: Dict[str, str] | None = None,      # 作为“基础头”传给父类（UA 不要在子类设置）
        proxies: Iterable[Dict[str, str]] | None = None,  # 代理池（可选）
        config_path: str = "config/spider.yaml",
        config_section: str = "auction_pigeons",
        logger: BaseLogger | None = None,
        **kwargs
    ):
        """
        初始化 PigeonCrawler：
        - 读取配置（实例级，避免类属性共享）
        - 交由父类创建 Session/Adapter/Retry
        - 不在子类层面设置 UA，防止被父类覆盖导致不一致
        """
        cfg = load_config(config_section, config_path)
        if not cfg:
            raise RuntimeError(f"load_config failed: section={config_section}, path={config_path}")

        # 保存配置（深拷贝参数模板，避免跨实例污染）
        self.base_url: str = cfg["api_url"]
        self.param_template: Dict[str, Any] = deepcopy(cfg.get("params", {}))
        self._cfg_path = config_path
        self._cfg_section = config_section

        # 父类初始化（会创建带重试/连接池的 Session、设置节流等）
        super().__init__(
            headers=headers,
            proxies=proxies,
            min_delay=0.2,
            max_delay=delay,
            timeout=timeout,
            max_retries=max_retries,
            logger=logger or BaseLogger(to_file=True, name="PigeonCrawler"),
            **kwargs
        )

        self.logger.log_info(
            f"PigeonCrawler ready | url={self.base_url} | delay≤{delay}s | "
            f"timeout={self.timeout} | retries={self.max_retries} | params={self.param_template}"
        )

    # ---------------- 单个请求 ----------------
    def fetch_pigeons(self, gongpeng_id: int, section_id: int) -> List[PigeonInfo]:
        """
        抓取单个 (gongpeng_id, section_id) 的 pigeons 列表
        - 使用父类 fetch()（统一重试/节流/代理/UA）
        - 仅补充 Accept 头；不要在子类设置 UA（父类会统一注入）
        """
        # URL 路径变量替换
        url = self.base_url.format(gongpeng_id=gongpeng_id)

        # 每次请求复制参数模板，避免并发污染
        params = deepcopy(self.param_template)
        params["sectionid"] = section_id

        # 只补充不会被父类覆盖的头；UA 交由父类注入
        req_headers = {"Accept": "application/json"}

        self.logger.log_debug(f"[fetch_pigeons] gp={gongpeng_id}, section={section_id} | GET {url} | params={params}")

        # 调用父类统一请求入口（带重试/节流/代理/UA）
        resp = self.fetch(url, params=params, headers=req_headers)
        if resp is None:
            self.logger.log_warning(f"[fetch_pigeons] gp={gongpeng_id}, section={section_id} | fetch failed (None)")
            return []

        # 解析 JSON
        try:
            raw = resp.json()
        except Exception as e:
            self.logger.log_error(f"[fetch_pigeons] gp={gongpeng_id}, section={section_id} | json decode error: {e}")
            return []

        data_list = self._extract_list(raw)
        self.logger.log_info(f"[fetch_pigeons] gp={gongpeng_id}, section={section_id} | returned={len(data_list)}")

        try:
            return PigeonInfo.from_list(data_list)
        except Exception as e:
            self.logger.log_error(f"[fetch_pigeons] gp={gongpeng_id}, section={section_id} | PigeonInfo.from_list error: {e}")
            return []

    # ---------------- 并发抓取 ----------------
    def fetchall_pigeons(self, section_id_and_gongpeng_id: List[dict]) -> List[PigeonInfo]:
        """
        并发抓取多个 (gongpeng_id, section_id) 的 pigeons 列表

        线程安全策略（不改父类的前提下的最小代价方案）：
        - ❌ 不复用同一个 PigeonCrawler 实例去跑多线程（会共享 Session 与节流基准）
        - ✅ 用 ExitStack 预创建 MAX_WORKERS 个“等价配置”的新实例（每个 worker 拥有独立 Session/节流基准）
             块退出时自动调用每个实例的 __exit__ / close()，无需 finally
        """
        if not section_id_and_gongpeng_id:
            return []

        # 轻度打散，避免瞬时打到相邻 id
        shuffled = list(section_id_and_gongpeng_id)
        random.shuffle(shuffled)

        all_pigeons: List[PigeonInfo] = []
        self.logger.log_info(f"[fetchall] total tasks={len(shuffled)} | workers={self.MAX_WORKERS}")

        with ExitStack() as stack:
            # 为每个 worker 预创建独立实例并注册到 ExitStack
            workers = [stack.enter_context(self._new_worker_instance()) for _ in range(self.MAX_WORKERS)]

            def _task(worker_idx: int, item: dict) -> List[PigeonInfo]:
                return workers[worker_idx].fetch_pigeons(item["gongpeng_id"], item["section_id"])

            # 线程池只负责调度；生命周期交给 ExitStack
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                futures: Dict[Any, dict] = {}
                for i, item in enumerate(shuffled):
                    wid = i % self.MAX_WORKERS
                    futures[executor.submit(_task, wid, item)] = item

                for fut in as_completed(futures):
                    meta = futures[fut]
                    try:
                        result = fut.result()
                        all_pigeons.extend(result)
                        self.logger.log_info(
                            f"[fetchall] gp={meta['gongpeng_id']}, section={meta['section_id']} "
                            f"| count={len(result)} | total={len(all_pigeons)}"
                        )
                    except Exception as e:
                        self.logger.log_error(
                            f"[fetchall] gp={meta.get('gongpeng_id')}, section={meta.get('section_id')} | exception: {e}"
                        )

        self.logger.log_info(f"[fetchall] done | total_pigeons={len(all_pigeons)}")
        return all_pigeons

    # ---------------- 工具：为线程创建“等价配置”的新实例 ----------------
    def _new_worker_instance(self) -> "PigeonCrawler":
        """
        为线程创建一个“等价配置”的新实例：
        - 各自拥有独立 requests.Session / 重试 / 节流基准（避免并发时互相影响）
        - 保持与当前实例行为一致：delay / max_retries / timeout / headers / proxies / 配置来源
        - headers/proxies 从父类的 _base_headers/_proxy_pool 取快照
        - 因为 BaseCrawler 实现了 __enter__/__exit__，实例可被 ExitStack 托管自动 close()
        """
        return PigeonCrawler(
            delay=self.max_delay,
            max_retries=self.max_retries,
            timeout=self.timeout,
            headers=deepcopy(self._base_headers),
            proxies=deepcopy(self._proxy_pool),
            config_path=self._cfg_path,
            config_section=self._cfg_section,
            logger=self.logger,  # 复用同一个 logger（线程安全由 logger 自身决定；若不安全可创建新 logger）
        )

    # ---------------- 辅助：统一抽取列表 ----------------
    @staticmethod
    def _extract_list(raw: Any) -> List[Dict[str, Any]]:
        """
        兼容多种返回结构：
        - 顶层 list
        - dict 的 data / list / records 字段
        """
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            for k in ("data", "list", "records"):
                v = raw.get(k)
                if isinstance(v, list):
                    return v
        return []


# ---------------- 示例：独立运行入口（可选） ----------------
if __name__ == "__main__":
    from crawlers.gongpeng_crawler import GongpengCrawler

    # 1) 拿拍卖列表（根据你的 GongpengCrawler 实现，可能也支持并发/分页）
    with GongpengCrawler() as gp_crawler:
        gp_list = gp_crawler.crawl_all()
        ids = [info.id for info in gp_list]

    # 2) 并发抓各拍卖的 section
    with SectionCrawler() as sc:
        sections = sc.fetchall_sections(ids)
    pprint(sections)

    section_id_and_gongpeng_id = [
        {"section_id": s.id, "gongpeng_id": s.auction_id}
        for s in sections
        ]
    pc = PigeonCrawler()
    pigeons = pc.fetchall_pigeons(section_id_and_gongpeng_id)

    # pprint(pigeons)

    from dao.pigeon_dao import PigeonDao

    mysqlconfig = load_config('mysqlconfig', 'config\\db_config.yaml')
    pgDao = PigeonDao(**mysqlconfig)
    pgDao.ensure_table_pigeon_info()
    pgDao.insert_or_update_pigeon_info_batch(pigeons)

    # pg = PigeonCrawler()
    # pigeons = pg.fetch_pigeons(309,2061)
    # pprint(pigeons)
