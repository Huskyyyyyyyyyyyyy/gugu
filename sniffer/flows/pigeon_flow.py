# ──────────────────────────────────────────────────────────────────────────────
# 模块用途：薄 orchestrator（组装配置、爬虫池、业务处理），并绑定路由装饰器。
# 说明：
#   - 把复杂逻辑放到 handlers / pool / config 内，本文件保持“可读与可替换”；
#   - 需要时可在 main.py 退出阶段调用 _pool.close() 做资源回收。
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import re

from commons.base_logger import BaseLogger
from pigeon_socket.adapters.bidrecord_payload import records_to_payload
from pigeon_socket.bus import bus
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
#对应网址缺少bis,暂时弃用
# @on_topic(r"^bid/pigeons/(?P<pigeon>\d+)$")
# async def handle_pigeon_bid(ev: Event, m: re.Match):
#     """路由绑定：处理实时出价主题。"""
#     return await _handlers.handle_pigeon_bid(ev, m)

# 正则触发：实时处理（MQTT）
@on_topic(r"^pigeon/auctions/(?P<auction>\d+)/pigeons/(?P<pigeon>\d+)$")
async def handle_pigeon_auction(ev: Event, m: re.Match):
    """实时触发：获取当前 PID 并抓取，然后发布给 SSE。"""
    current_info,records = await _handlers.process_current_pid(reason="realtime", debounce=True)
    await bus.publish(records_to_payload(records,current_info))

@on_startup
async def trigger_on_startup():
    """启动时执行一次抓取，然后发布给 SSE。"""
    current_info,records = await _handlers.process_current_pid(reason="startup", debounce=False)
    await bus.publish(records_to_payload(records,current_info))

# 可选：在 main.py 的 finally 里调用此函数做资源回收
async def _shutdown():
    await _pool.close()

# ──────────────────────────────────────────────────────────────────────────────
# 对外公开访问器：让其他模块（例如 SSE 服务）复用同一套实例
# ──────────────────────────────────────────────────────────────────────────────
def get_config() -> PigeonConfig:
    """获取全局配置实例（模块级单例）。"""
    return _cfg

def get_logger() -> BaseLogger:
    """获取全局日志器实例（模块级单例）。"""
    return _log

def get_pool() -> CrawlerPool:
    """获取全局爬虫池实例（模块级单例）。"""
    return _pool

def get_handlers() -> PigeonHandlers:
    """获取全局业务处理器（扳机入口）实例（模块级单例）。"""
    return _handlers
