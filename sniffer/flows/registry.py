# flows/registry.py
from __future__ import annotations
import importlib
import json
import pkgutil
import re
from typing import Awaitable, Callable, List, Tuple

from sniffer.models import Event

RouteHandler = Callable[[Event, re.Match], Awaitable[None]]

_routes: List[Tuple[re.Pattern[str], RouteHandler]] = []

# ⭐ 新增：启动钩子列表
_startup_hooks: List[Callable[[], Awaitable[None]]] = []

def on_topic(pattern: str):
    rx = re.compile(pattern)
    def deco(fn: RouteHandler):
        _routes.append((rx, fn))
        return fn
    return deco

# ⭐ 新增：注册启动钩子的装饰器
def on_startup(fn: Callable[[], Awaitable[None]]):
    _startup_hooks.append(fn)
    return fn

async def topic_router(ev: Event):
    if ev.kind != "mqtt_publish" or not ev.topic:
        return
    for rx, fn in _routes:
        m = rx.match(ev.topic)
        if m:
            try:
                return await fn(ev, m)
            except Exception as e:
                print(json.dumps({"level":"error","event":"flow_failed","topic":ev.topic,"err":repr(e)}, ensure_ascii=False))

def autoload_flows():
    pkg_name = __package__  # 'flows'
    pkg = importlib.import_module(pkg_name)
    for _, name, ispkg in pkgutil.iter_modules(pkg.__path__):
        if name in ("registry", "base"):
            continue
        importlib.import_module(f"{pkg_name}.{name}")

# ⭐ 新增：由 main.py 调用，执行所有启动钩子
async def run_startup_hooks():
    for fn in _startup_hooks:
        try:
            await fn()
        except Exception as e:
            print(json.dumps({"level":"error","event":"startup_hook_failed","err":repr(e)}, ensure_ascii=False))
