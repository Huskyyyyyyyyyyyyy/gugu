# flows/__init__.py
from .registry import on_topic, on_startup, topic_router, autoload_flows, run_startup_hooks

# 导入时自动加载所有 flow（触发 @on_topic/@on_startup 注册）
autoload_flows()

__all__ = ["on_topic", "on_startup", "topic_router", "autoload_flows", "run_startup_hooks"]
