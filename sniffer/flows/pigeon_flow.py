# ╔══════════════════════════════════════════════════╗
# ║                 flows/pigeon_flow.py             ║
# ╚══════════════════════════════════════════════════╝
from __future__ import annotations
import asyncio
import json
import os
import re
import time
from typing import Dict, List, Optional, Callable, Any

from sniffer.models import Event
from crawlers.pids_crawler import PidsPigeonsCrawler
from crawlers.current_crawler import CurrentPigeonsCrawler
from .registry import on_topic, on_startup  # 路由 & 启动钩子

# ──────────────────────────
# 运行参数（可用环境变量覆盖）
# ──────────────────────────
MAX_CONCURRENCY = int(os.getenv("PIGEON_FLOW_MAX_CONCURRENCY", "4"))   # 同时爬虫上限
COOLDOWN_SEC    = float(os.getenv("PIGEON_FLOW_COOLDOWN_SEC", "2.0"))  # 同 pid 冷却期（去抖）
BOOTSTRAP_PIDS  = os.getenv("PIGEON_BOOTSTRAP_PIDS", "")               # 启动预抓 pid 列表: "101,102"
USE_CURRENT_BOOTSTRAP = os.getenv("PIGEON_BOOTSTRAP_USE_CURRENT", "true").lower() == "true"
# ↑ 若未提供 BOOTSTRAP_PIDS 且该开关为真，则冷启动会尝试通过 current 接口拿“当前 pid”

# ──────────────────────────
# 轻量运行时状态
# ──────────────────────────
_sema = asyncio.Semaphore(MAX_CONCURRENCY)  # 并发限流
_last_run_at: Dict[int, float] = {}         # per pid 去抖时间戳

# 只处理这一种：bid/pigeons/<pid>
@on_topic(r"^bid/pigeons/(?P<pigeon>\d+)$")
async def handle_pigeon_bid(ev: Event, m: re.Match):
    """
    实时报价 PUBLISH：
      topic = bid/pigeons/<pigeon_id>
    流程：
      1) 提取 pigeon_id
      2) 去抖（同一 pid 冷却期内跳过）
      3) 受限并发地在线程池跑同步爬虫抓最新出价明细
      4) 可选：从 ev.payload_preview 做超轻量字段观察（日志）
    """
    pid = int(m.group("pigeon"))

    # —— 仅用于观测：从 payload 预览里抠出关键词窗口（不做业务依赖）
    preview_fields = _parse_bid_preview(ev.payload_preview or "")

    # —— 去抖：同一 pid 冷却期内跳过
    now, last = time.time(), _last_run_at.get(pid, 0.0)
    if (now - last) < COOLDOWN_SEC:
        _log("debug", "debounced", pid=pid, since=round(now - last, 2), fields=preview_fields)
        return
    _last_run_at[pid] = now

    # —— 并发限流 + 在线程池执行同步爬虫
    async with _sema:
        bids = await asyncio.to_thread(_run_pids_crawler, pid)
        _log("info", "bids_fetched", pid=pid, count=(len(bids) if bids else None), fields=preview_fields)


# ╔══════════════════════════════════════════════════╗
# ║                   启动冷抓一遍                   ║
# ╚══════════════════════════════════════════════════╝
@on_startup
async def cold_start_once():
    """
    冷启动（只执行一次）：
      - 若 PIGEON_BOOTSTRAP_PIDS 提供，则并发受限地抓这批 pid
      - 否则且 USE_CURRENT_BOOTSTRAP=true，则通过 current 接口拿“当前 pid”再抓一次
      - 都没有就跳过
    """
    # 1) 优先解析显式配置的 pid 列表
    pids = _parse_bootstrap_pids(BOOTSTRAP_PIDS)

    # 2) 若没给且允许 fallback，则去 current 接口拿“当前 pid”
    if not pids and USE_CURRENT_BOOTSTRAP:
        try:
            pid = await asyncio.to_thread(_get_current_pid_blocking)
            if pid:
                pids = [pid]
                _log("info", "bootstrap_current_pid_ok", pid=pid)
            else:
                _log("warn", "bootstrap_current_pid_none")
        except Exception as e:
            _log("error", "bootstrap_current_pid_fail", err=repr(e))

    if not pids:
        _log("info", "bootstrap_skip", reason="no_pids")
        return

    _log("info", "bootstrap_start", pids=pids, concurrency=MAX_CONCURRENCY)

    async def _one(pid: int):
        async with _sema:
            try:
                await asyncio.to_thread(_run_pids_crawler, pid)
                _log("info", "bootstrap_bid_ok", pid=pid)
            except Exception as e:
                _log("error", "bootstrap_bid_fail", pid=pid, err=repr(e))

    await asyncio.gather(*(_one(pid) for pid in pids))
    _log("info", "bootstrap_done", total=len(pids))


# ╔══════════════════════════════════════════════════╗
# ║                 同步爬虫适配/调用                ║
# ╚══════════════════════════════════════════════════╝
def _run_pids_crawler(pigeon_id: int):
    """
    在线程中执行同步爬虫：用 pigeon_id 直查出价明细。
    兼容可能的历史命名（run_crawler / run_crawl / run / fetch_bids）。
    """
    _log("info", "crawler_start", pid=pigeon_id)
    with PidsPigeonsCrawler() as crawler:
        fn = _resolve_crawl_fn(crawler)
        bids = fn(pigeon_id)
    _log("info", "crawler_done", pid=pigeon_id, count=(len(bids) if bids else None))
    return bids

def _resolve_crawl_fn(crawler: Any) -> Callable[[int], Any]:
    """
    适配历史方法名，避免 AttributeError：
      优先顺序：run_crawler > run_crawl > run > fetch_bids
    """
    for name in ("run_crawler", "run_crawl", "run", "fetch_bids"):
        fn = getattr(crawler, name, None)
        if callable(fn):
            return fn
    raise AttributeError(
        "PidsPigeonsCrawler 缺少 run_crawler/run_crawl/run/fetch_bids 中任一方法"
    )


# ╔══════════════════════════════════════════════════╗
# ║                     小工具/日志                  ║
# ╚══════════════════════════════════════════════════╝
def _get_current_pid_blocking() -> Optional[int]:
    """同步拿‘当前鸽子 pid’（放到线程里调用）"""
    with CurrentPigeonsCrawler() as crawler:
        return crawler.get_current_pigeon_id()

def _parse_bid_preview(preview: str) -> Dict[str, str]:
    """从 payload 预览里抠关键词，仅用于日志观测（不做业务依赖）"""
    keys = ("bidid", "bid", "code")
    out: Dict[str, str] = {}
    for key in keys:
        i = preview.find(key)
        if i >= 0:
            snippet = preview[i : i + 24]
            out[key] = snippet.encode("unicode_escape").decode("utf-8")
    return out

def _parse_bootstrap_pids(env_val: str) -> List[int]:
    """解析 PIGEON_BOOTSTRAP_PIDS='101, 102,foo' → [101,102]"""
    if not env_val:
        return []
    out: List[int] = []
    for tok in env_val.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            out.append(int(tok))
        except ValueError:
            _log("warn", "bootstrap_pid_invalid", raw=tok)
    return out

def _log(level: str, event: str, **kw):
    """统一 JSON 日志"""
    print(json.dumps({"level": level, "event": event, **kw}, ensure_ascii=False))
