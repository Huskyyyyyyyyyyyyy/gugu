import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Any, Dict
from copy import deepcopy
from requests import Response
from commons.base_crawler import BaseCrawler  # 已继承 LogMixin 的版本
from commons.base_logger import BaseLogger
from mydataclass.section import SectionInfo
from tools.config_loader import load_config
import tools.request_utils
from contextlib import ExitStack

class SectionCrawler(BaseCrawler):
    """
    拍卖分组（section）爬虫：
    - 按 auction_id 拉取对应拍卖分组
    - 支持并发（每个 worker 使用独立的 SectionCrawler 实例，避免共享 Session 带来的线程安全与节流基准混乱）
    - 解析为 SectionInfo 列表
    """

    MAX_WORKERS = 10  # 可在实例化后按需覆盖

    # ---------------- 必要的抽象方法实现 ----------------
    def parse(self, response: Response) -> Any:
        """
        说明：
        - BaseCrawler.crawl() 会使用 parse()；本子类主流程是 fetch_sections/fetchall_sections，
          非 crawl() 场景。因此这里明确给出提示，避免误用。
        """
        raise NotImplementedError("SectionCrawler 不走 crawl() 流程，请调用 fetch_sections / fetchall_sections")

    # ---------------- 构造与初始化 ----------------
    def __init__(
        self,
        delay: float = 3,
        max_retries: int = 3,
        timeout: float = 10,
        headers: Dict[str, str] | None = None,
        proxies: Any = None,
        config_path: str = "config/spider.yaml",
        config_section: str = "auction_sections",
        **kwargs
    ):
        """
        参数说明：
        - delay: 最大节流延迟（秒），最小延迟固定传给父类 0.2s，父类会在 [min_delay, max_delay] 内加抖动
        - max_retries: 父类的 Retry 总重试次数
        - timeout: 超时（秒），直接传给父类（父类也接受 (connect, read) 元组）
        - headers: 作为“基础头”传给父类（建议不要在子类里设置 User-Agent，UA 注入交给父类完成）
        - proxies: 代理池（可迭代的映射），传给父类；父类会随机选取 per-request 代理
        - config_path/config_section: 读取接口配置
        """
        # 读取配置（实例级，避免类属性共享）
        cfg = load_config(config_section, config_path)
        if not cfg:
            raise RuntimeError(f"load_config failed: section={config_section}, path={config_path}")

        # 记录用于抓取的基础 URL 与参数模板（深拷贝避免跨实例污染）
        self.base_url: str = tools.request_utils._clean_url(cfg["api_url"])
        self.param_template: Dict[str, Any] = deepcopy(cfg.get("params", {}))

        # 额外保存配置来源，供“为每个线程克隆等价实例”使用（见 _new_worker_instance）
        self._cfg_path = config_path
        self._cfg_section = config_section

        # 基类初始化（带上 logger / session / 重试 / 节流）
        # 注意：不在子类设置 User-Agent，UA 由父类的 USER_AGENTS 池统一注入，避免被覆盖导致不一致
        super().__init__(
            headers=headers,
            proxies=proxies,
            min_delay=0.2,
            max_delay=delay,
            timeout=timeout,
            max_retries=max_retries,
            logger=BaseLogger(name="SectionCrawler", to_file=True),
            **kwargs,

        )

        self.logger.log_info(
            f"SectionCrawler ready | url={self.base_url} | delay≤{delay}s | "
            f"timeout={self.timeout} | retries={self.max_retries} | params={self.param_template}"
        )

    # ---------------- 核心抓取：单个拍卖分组 ----------------
    def fetch_sections(self, gongpeng_id: int) -> List[SectionInfo]:
        """
        抓取单个 auction_id 的所有 section

        关键点：
        - URL 路径变量替换
        - 每次请求都复制参数模板并归一化（保留某些 0 值与显式空 key）
        - 仅补充 Accept 头；User-Agent 交由父类在 _request 中统一注入
        - 使用父类的 fetch（带重试/节流/代理/UA）
        """
        # URL 路径变量替换
        url = self.base_url.format(gongpeng_id=gongpeng_id)

        # 每次请求复制参数模板，避免并发下相互污染
        params = deepcopy(self.param_template)
        params = tools.request_utils._normalized_params(
            params,
            keep_zero_keys=["pageno", "pagesize", "finishstarttime", "finishendtime"],
            keep_empty_keys=["key"],  # 如果接口需要显式空 key
        )

        # 不再访问 self.headers（父类未暴露）；仅补充不会被父类覆盖的头
        # ⚠️ 切记：User-Agent 不要在子类设置，父类会统一随机注入并覆盖
        req_headers: Dict[str, str] = {"Accept": "application/json"}

        self.logger.log_debug(f"[fetch_sections] id={gongpeng_id} | GET {url} | params={params}")

        # 用 BaseCrawler 的会话（带重试/节流）
        resp = self.fetch(url, params=params, headers=req_headers)
        if resp is None:
            self.logger.log_warning(f"[fetch_sections] id={gongpeng_id} | fetch failed (None)")
            return []

        # 解析 JSON（兼容顶层 list / dict.data|list|records）
        try:
            raw = resp.json()
        except Exception as e:
            # 可根据需求补充 resp.text[:N] 便于排查反爬/验证码页（注意脱敏）
            self.logger.log_error(f"[fetch_sections] id={gongpeng_id} | json decode error: {e}")
            return []

        data_list = self._extract_list(raw)
        self.logger.log_info(f"[fetch_sections] id={gongpeng_id} | returned={len(data_list)}")

        try:
            parsed = SectionInfo.from_list(data_list)
        except Exception as e:
            self.logger.log_error(f"[fetch_sections] id={gongpeng_id} | SectionInfo.from_list error: {e}")
            return []
        return parsed

    # ---------------- 并发抓取：多个拍卖分组 ----------------
    def fetchall_sections(self, auction_ids: List[int]) -> List[SectionInfo]:
        """并发抓取多个 auction_id 的 section 列表（使用 ExitStack 确保资源释放）"""
        if not auction_ids:
            return []

        shuffled = list(auction_ids)
        random.shuffle(shuffled)

        all_sections: List[SectionInfo] = []
        self.logger.log_info(f"[fetchall] total ids={len(shuffled)} | workers={self.MAX_WORKERS}")

        # 用 ExitStack 统一托管所有 worker 的 __exit__/close
        with ExitStack() as stack:
            # 为每个 worker 创建“等价配置”的新实例，并注册到 ExitStack
            workers = [stack.enter_context(self._new_worker_instance()) for _ in range(self.MAX_WORKERS)]

            def _task(worker_idx: int, aid: int) -> List[SectionInfo]:
                return workers[worker_idx].fetch_sections(aid)

            # 线程池只负责调度任务；worker 生命周期交给 ExitStack
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                futures: Dict[Any, int] = {}
                for i, aid in enumerate(shuffled):
                    wid = i % self.MAX_WORKERS
                    futures[executor.submit(_task, wid, aid)] = aid

                for fut in as_completed(futures):
                    aid = futures[fut]
                    try:
                        res = fut.result()
                        all_sections.extend(res)
                        self.logger.log_info(f"[fetchall] id={aid} ok | count={len(res)} | total={len(all_sections)}")
                    except Exception as e:
                        self.logger.log_error(f"[fetchall] id={aid} exception: {e}")

        self.logger.log_info(f"[fetchall] done | total_sections={len(all_sections)}")
        return all_sections
    # ---------------- 工具：为线程创建“等价配置”的新实例 ----------------
    def _new_worker_instance(self) -> "SectionCrawler":
        """
        为线程创建一个“等价配置”的新实例：
        - 各自拥有独立 requests.Session / 重试 / 节流基准（避免并发时互相影响）
        - 尽量保持当前实例的行为一致：delay/max_retries/timeout/headers/proxies/配置来源
        - 这里 headers/proxies 取自父类的 _base_headers 和 _proxy_pool（均为当前实例的快照）
        """
        # timeout 可能是 float 或 (connect, read)；直接透传给 __init__，类型注解不影响运行
        return SectionCrawler(
            delay=self.max_delay,
            max_retries=self.max_retries,
            timeout=self.timeout,
            headers=deepcopy(self._base_headers),  # 父类中基础头部的深拷贝，避免共享引用
            proxies=deepcopy(self._proxy_pool),    # 代理池快照
            config_path=self._cfg_path,
            config_section=self._cfg_section,
        )

    # ---------------- 辅助：统一抽取列表 ----------------
    @staticmethod
    def _extract_list(raw: Any) -> List[Dict[str, Any]]:
        """支持 顶层 list 或 dict 的 data/list/records"""
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            for k in ("data", "list", "records"):
                v = raw.get(k)
                if isinstance(v, list):
                    return v
        return []


# ---------------- 示例：独立运行入口 ----------------
if __name__ == "__main__":
    # 建议使用 with 保证资源释放；这里演示串行跑一下获取 ids，再并发抓 section
    from crawlers.gongpeng_crawler import GongpengCrawler

    # 1) 拿拍卖列表（根据你的 GongpengCrawler 实现，可能也支持并发/分页）
    with GongpengCrawler() as gp_crawler:
        gp_list = gp_crawler.crawl_all()
        ids = [info.id for info in gp_list]

    # 2) 并发抓各拍卖的 section
    with SectionCrawler() as sc:
        sections = sc.fetchall_sections(ids)

    print("总分组：", len(sections))
    for s in sections:
        # 兼容 to_dict 或 __dict__ 输出
        print(getattr(s, "to_dict", lambda: s.__dict__)())
