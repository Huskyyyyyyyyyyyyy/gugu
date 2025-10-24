
# 事件工厂 EventFactory

from __future__ import annotations
import base64
from datetime import UTC, datetime
from typing import Any, Optional

from models import Event
from mqtt_codec import MqttCodec
from setting import MIN_BIN_LEN, TRIGGER_TEXT

class EventFactory:
    """
    将浏览器回传的 WS 原始消息（text/binary）转换为标准 Event。
    - 过滤 MQTT 心跳
    - 过滤长度过短的二进制包
    - 文本触发可配置
    """

    def __init__(self, trigger_text: bool = TRIGGER_TEXT, min_bin_len: int = MIN_BIN_LEN):
        self.trigger_text = trigger_text
        self.min_bin_len = min_bin_len

    def from_ws(self, m: dict[str, Any]) -> Optional[Event]:
        ts = datetime.now(UTC).isoformat()
        kind = m.get("kind")
        url = m.get("url", "")

        # 文本消息
        if kind == "text":
            if not self.trigger_text:
                return None
            preview = (m.get("data") or "")[:256]
            return Event(ts=ts, kind="ws_text", url=url, payload_preview=preview)

        # 二进制消息
        try:
            raw = base64.b64decode(m.get("data", ""))
        except Exception:
            return None

        if MqttCodec.is_mqtt_ping(raw) or len(raw) < self.min_bin_len:
            return None

        pub = MqttCodec.decode_mqtt_publish(raw)
        if pub:
            return Event(ts=ts, kind="mqtt_publish", url=url,
                         topic=pub["topic"], payload_preview=pub["payload_preview"])
        return Event(ts=ts, kind="binary", url=url, length=len(raw))
