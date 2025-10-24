# ──────────────────────────────────────────────────────────────────────────────
# 模块用途：薄 orchestrator（组装配置、爬虫池、业务处理），并绑定路由装饰器。
# 说明：
#   - 把复杂逻辑放到 handlers / pool / config 内，本文件保持“可读与可替换”；
#   - 需要时可在 main.py 退出阶段调用 _pool.close() 做资源回收。
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import re

from commons.base_logger import BaseLogger
from sniffer.models import Event
from crawlers.pids_crawler import PidsPigeonsCrawler
from crawlers.current_crawler import CurrentPigeonsCrawler
from sniffer.flows.registry import on_topic, on_startup

from .pigeon_config import PigeonConfig
from .crawler_pool import CrawlerPool
from .pigeon_handlers import PigeonHandlers

# 1) 载入配置与日志器
_cfg = PigeonConfig.from_env()
_log = BaseLogger(name="pigeon_flow", to_file=True)

# 2) 构造爬虫池（持久化实例 + 轮询槽位 + 线程亲和）
_pool = CrawlerPool(
    pool_size=_cfg.max_concurrency,
    pids_crawler_factory=PidsPigeonsCrawler,
    current_crawler_factory=CurrentPigeonsCrawler,
    logger=_log,
)

# 3) 业务处理器（去抖、冷启动、执行）
_handlers = PigeonHandlers(pool=_pool, cfg=_cfg, logger=_log)

@on_topic(r"^bid/pigeons/(?P<pigeon>\d+)$")
async def handle_pigeon_bid(ev: Event, m: re.Match):
    """路由绑定：处理实时出价主题。"""
    return await _handlers.handle_pigeon_bid(ev, m)

@on_startup
async def cold_start_once():
    """路由绑定：进程启动后执行一次的冷启动预抓。"""
    return await _handlers.cold_start_once()

# 可选：在 main.py 的 finally 里调用此函数做资源回收
async def _shutdown():
    await _pool.close()
