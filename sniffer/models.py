
# 数据模型
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

@dataclass
class Event:
    """
    统一事件对象（下游流程可依赖此模型，不关心上游细节）
    """
    ts: str                 # ISO UTC 时间戳
    kind: str               # "mqtt_publish" | "binary" | "ws_text"
    url: str                # 消息来源的 WebSocket URL
    topic: Optional[str] = None
    payload_preview: Optional[str] = None
    length: Optional[int] = None
    payload_b64: Optional[str] = None  # 如需持久化/回放可启用（默认不使用）
