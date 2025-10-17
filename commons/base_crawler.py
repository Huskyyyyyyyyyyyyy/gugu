# commons/base_crawler.py
from __future__ import annotations

import time
import random
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Mapping, Optional

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from commons.base_logger import BaseLogger


class BaseCrawler(ABC):
    """
    爬虫基类：
    - 统一 _request，fetch/fetch_post 是薄封装
    - 不污染 session 的全局 headers/proxies（按请求传入）
    - 完整的重试策略（connect/read/status），尊重 Retry-After
    - 超时、节流（min/max delay + 抖动），可复用 TCP/Cookies
    - 可选代理池、UA 池；钩子 on_response/on_error 便于扩展
    """

    # 默认 UA 列表（可在 __init__ 传自己的）
    USER_AGENTS: Iterable[str] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/16.5 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    )

    # 默认可重试方法
    RETRY_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})  # 需要对 POST 重试时会在 __init__ 动态加入

    # 默认可重试状态码（含 429，补充 408/421）
    RETRY_STATUS = (408, 421, 429, 500, 502, 503, 504)

    def __init__(
        self,
        *,
        headers: Optional[Mapping[str, str]] = None,
        proxies: Optional[Iterable[Mapping[str, str]]] = None,
        # 节流策略：每次请求之间的随机等待（秒）
        min_delay: float = 0.5,
        max_delay: float = 3.0,
        # 超时（秒或 (connect, read)）
        timeout: float | tuple[float, float] = (5.0, 15.0),
        # 重试参数
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        retry_on_post: bool = False,
        # 代理/UA
        user_agents: Optional[Iterable[str]] = None,
        logger: Optional[BaseLogger] = None,
        # 是否在 403/429/503 时重建 session
        recreate_session_on_block: bool = True,
    ):
        # 初始化 LogMixin（创建默认 logger）

        # 若外部传入了自定义 logger，则覆盖
        if logger is not None:
            self.logger = logger
        else:
            self.logger = BaseLogger(name=self.__class__.__name__, to_file=True)

        # 归一化节流配置
        self.min_delay = float(min_delay)
        self.max_delay = float(max_delay)
        if self.max_delay < self.min_delay:
            self.min_delay, self.max_delay = self.max_delay, self.min_delay

        # 其它配置
        self._base_headers: Dict[str, str] = dict(headers or {})
        self._proxy_pool: list[Mapping[str, str]] = list(proxies or [])
        self.timeout = timeout
        self.max_retries = int(max_retries)
        self.backoff_factor = float(backoff_factor)
        self.retry_on_post = bool(retry_on_post)
        self.user_agents = list(user_agents or self.USER_AGENTS)
        self.recreate_session_on_block = recreate_session_on_block

        # 创建 Session
        self.session: Session = self._create_session()

        # 节流参照时间（monotonic 级防止系统时间回拨影响），以“上一次请求结束时刻”为基准
        self._last_request_ts: float = 0.0

    # --------------------- Session / Retry ---------------------

    def _create_session(self) -> Session:
        """创建带重试机制的 Session。"""
        s = requests.Session()

        allowed_methods = set(self.RETRY_METHODS)
        if self.retry_on_post:
            allowed_methods.add("POST")

        retry = Retry(
            total=self.max_retries,
            connect=self.max_retries,
            read=self.max_retries,
            status=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.RETRY_STATUS,
            allowed_methods=frozenset(m.upper() for m in allowed_methods),
            respect_retry_after_header=True,
            raise_on_status=False,  # 不在 adapter 层抛，让我们在 _request 里统一处理
        )

        adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
        s.mount("http://", adapter)
        s.mount("https://", adapter)

        # 注意：不在 session 层设置 base headers，避免污染全局
        return s

    def _recreate_session(self) -> None:
        """重建 Session（被封或持续失败时使用）。"""
        try:
            self.session.close()
        except Exception:
            pass
        self.session = self._create_session()

    # --------------------- 钩子（子类可覆写） ---------------------

    def on_response(self, response: Response) -> None:
        """子类可覆写：成功响应后的扩展处理（如速率估计、配额读取）。"""
        pass

    def on_error(self, exc: Exception, url: str, method: str, attempt_info: str = "") -> None:
        """子类可覆写：错误处理（如黑名单某代理、上报监控）。"""
        self.logger.log_error(f"{method} {url} 失败: {exc} {attempt_info}")

    # --------------------- 统一请求入口 ---------------------

    def _throttle(self) -> None:
        """
        按照 min/max_delay 进行节流，避免被封。
        以“上一次请求结束”的时间为基准：
        - 至少等待 min_delay
        - 叠加 0 ~ (max_delay - min_delay) 的随机抖动
        """
        now = time.monotonic()
        elapsed = now - self._last_request_ts
        wait_min = max(0.0, self.min_delay - elapsed)
        jitter = 0.0 if self.max_delay <= self.min_delay else random.uniform(0.0, self.max_delay - self.min_delay)
        sleep_s = wait_min + jitter
        if sleep_s > 0:
            time.sleep(sleep_s)

    def _pick_proxy(self) -> Optional[Mapping[str, str]]:
        """随机挑选一个代理（若无代理则返回 None）。"""
        if not self._proxy_pool:
            return None
        return random.choice(self._proxy_pool)

    def _pick_user_agent(self) -> Optional[str]:
        """随机挑选一个 UA（若未配置 UA 列表则返回 None）。"""
        if not self.user_agents:
            return None
        return random.choice(self.user_agents)

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        data: Optional[Mapping[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[float | tuple[float, float]] = None,
        allow_status: Optional[Iterable[int]] = None,
    ) -> Optional[Response]:
        """
        统一请求入口：
        - 不修改 session 的全局 headers/proxies
        - 每次请求动态注入 UA、代理、额外 headers
        - 超时/重试/节流在这里统一处理
        """
        self._throttle()

        # per-request headers（仅在请求层合并，避免污染 session 全局）
        req_headers: Dict[str, str] = dict(self._base_headers)
        if headers:
            req_headers.update(headers)
        ua = self._pick_user_agent()
        if ua:
            req_headers["User-Agent"] = ua

        # per-request proxies
        proxy = self._pick_proxy()

        # DEBUG：请求前日志（注意：必要时可做脱敏）
        self.logger.log_debug(
            f"REQUEST {method.upper()} {url} | params={params} json={'set' if json is not None else 'none'} "
            f"data={'set' if data is not None else 'none'} | proxy={'set' if proxy else 'none'}"
        )

        try:
            resp = self.session.request(
                method=method.upper(),
                url=url,
                params=params,
                data=data if json is None else None,  # 避免 data/json 同时传
                json=json,
                headers=req_headers,
                proxies=proxy,
                timeout=timeout or self.timeout,
            )

            ok = resp.ok or (allow_status and resp.status_code in allow_status)
            if not ok:
                self.logger.log_warning(
                    f"BAD_STATUS {method.upper()} {url} -> {resp.status_code} | proxy={'set' if proxy else 'none'}"
                )
                if self.recreate_session_on_block and resp.status_code in (403, 429, 503):
                    self._recreate_session()
                resp.raise_for_status()

            try:
                self.on_response(resp)
            except Exception as hook_err:
                self.logger.log_warning(f"on_response 处理异常: {hook_err}", exc_info=True)

            return resp

        except requests.RequestException as e:
            # 将关键信息通过 attempt_info 传入
            self.on_error(e, url, method, attempt_info=f"proxy={'set' if proxy else 'none'}")
            if self.recreate_session_on_block:
                self._recreate_session()
            return None

        finally:
            # 以“请求完成”的时间为节流基准（无论成功/失败）
            self._last_request_ts = time.monotonic()

    # --------------------- 对外方法 ---------------------

    def fetch(
        self,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        allow_status: Optional[Iterable[int]] = None,
    ) -> Optional[Response]:
        """GET 请求"""
        return self._request("GET", url, params=params, headers=headers, allow_status=allow_status)

    def fetch_post(
        self,
        url: str,
        *,
        data: Optional[Mapping[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Mapping[str, str]] = None,
        allow_status: Optional[Iterable[int]] = None,
    ) -> Optional[Response]:
        """POST 请求（支持 data/json 二选一）"""
        if data is not None and json is not None:
            raise ValueError("fetch_post: data 与 json 不能同时传")
        return self._request("POST", url, data=data, json=json, headers=headers, allow_status=allow_status)

    def crawl(self, url: str) -> Any:
        """
        入口：GET 获取并交给 parse 解析。
        子类也可以自己组合 fetch/fetch_post 实现复杂流程。
        """
        resp = self.fetch(url)
        if resp is not None:
            return self.parse(resp)
        return None

    # --------------------- 资源管理 ---------------------

    def close(self) -> None:
        """显式关闭底层 Session 连接池。"""
        try:
            self.session.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # --------------------- 需子类实现 ---------------------

    @abstractmethod
    def parse(self, response: Response) -> Any:
        """解析页面/JSON 的逻辑，由子类实现"""
        raise NotImplementedError
