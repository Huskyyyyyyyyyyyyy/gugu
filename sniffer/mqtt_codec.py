from __future__ import annotations
from typing import Optional, Dict


class MqttCodec:
    """
    提供 MQTT 报文的基础解析与心跳识别功能。
    """

    @staticmethod
    def _mqtt_varint(buf: bytes, i: int):
        """
        解析 MQTT 的 Remaining Length 可变长度字段。
        MQTT 报文头第二字节起为一个“可变长度编码”，用于表示剩余报文长度。

        参数：
            buf: 原始字节数据
            i: 当前读取起始位置

        返回：
            (value, new_index)
            - value: 剩余长度整数值（可能为 None 表示解析失败）
            - new_index: 读取结束的位置
        """
        mult, val, cnt = 1, 0, 0
        while True:
            # 若超过缓冲区或连续字节过多（MQTT 限定最多 4 字节），则失败
            if i >= len(buf) or cnt >= 4:
                return None, i
            b = buf[i];
            i += 1
            val += (b & 0x7F) * mult  # 取低 7 位作为数值
            if (b & 0x80) == 0:  # 若最高位未置位，表示结束
                break
            mult *= 128  # 每多一字节，进制乘以 128
            cnt += 1
        return val, i

    @staticmethod
    def _u16(buf: bytes, i: int):
        """
        从缓冲区读取一个 2 字节无符号整数（大端序，网络字节序）。

        参数：
            buf: 原始字节流
            i: 当前读取起点

        返回：
            (value, new_index)
            - value: 解析出的整数（None 表示超界失败）
            - new_index: 读取结束的位置
        """
        if i + 2 > len(buf):
            return None, i
        return (buf[i] << 8) | buf[i + 1], i + 2

    @staticmethod
    def _read_str(buf: bytes, i: int):
        """
        解析 MQTT 中的字符串字段。
        MQTT 字符串前 2 字节表示长度（大端序），后跟 UTF-8 编码数据。

        参数：
            buf: 原始字节数据
            i: 当前索引

        返回：
            (decoded_str, new_index)
            - decoded_str: 解码得到的字符串（若异常则返回 None）
            - new_index: 解析结束位置
        """
        ln, i = MqttCodec._u16(buf, i)  # 先读出长度字段
        if ln is None or i + ln > len(buf):
            return None, i
        try:
            # 解码 UTF-8 字符串
            return buf[i:i + ln].decode("utf-8", "replace"), i + ln
        except Exception:
            # 若解码出错，仍推进索引但返回 None
            return None, i + ln

    @staticmethod
    def is_mqtt_ping(raw: bytes) -> bool:
        """
        判断报文是否为 MQTT 的心跳包。
        - PINGREQ：0xC0 00
        - PINGRESP：0xD0 00

        返回：
            True 表示是心跳请求或响应；否则 False。
        """
        return len(raw) >= 2 and raw[1] == 0x00 and raw[0] in (0xC0, 0xD0)

    @staticmethod
    def decode_mqtt_publish(raw: bytes) -> Optional[Dict[str, str]]:
        """
        轻量解析 MQTT 的 PUBLISH 报文，返回主题与内容预览。

        返回：
            {
                "topic": str,               # 消息主题
                "payload_preview": str      # 载荷前 64 字符（UTF-8）
            }
            若解析失败则返回 None。

        步骤：
        1. 检查报文类型是否为 PUBLISH（固定头 0x3 << 4）
        2. 读取 Remaining Length（表示 variable header + payload 总长度）
        3. 读取主题字符串（Topic Name）
        4. 若 QoS > 0，则额外跳过 Packet Identifier（2 字节）
        5. 根据剩余长度计算 payload 区间
        6. 取 payload 前 64 字节作为可读预览
        """
        if not raw or ((raw[0] >> 4) & 0x0F) != 0x03:
            return None  # 不是 PUBLISH 报文

        flags = raw[0] & 0x0F
        qos = (flags >> 1) & 0x03  # QoS 位（第 1-2 位）

        # 解析可变长度 Remaining Length
        rem, idx = MqttCodec._mqtt_varint(raw, 1)
        if rem is None or idx + rem > len(raw):
            return None

        # 读取主题（Topic Name）
        topic, idx = MqttCodec._read_str(raw, idx)
        if topic is None:
            return None

        # 若 QoS > 0，则报文包含 Packet Identifier（2 字节）
        if qos > 0:
            pid, idx = MqttCodec._u16(raw, idx)
            if pid is None:
                return None

        # payload 起始位置
        payload_start = idx
        # 计算 payload 结束位置（1字节固定头 + 可变长度 + 剩余）
        payload_end = min(1 + (idx - 1) + rem, len(raw))
        payload = raw[payload_start:payload_end]

        # UTF-8 解码预览前 64 字节
        preview = payload[:64].decode("utf-8", "ignore").strip()
        return {"topic": topic, "payload_preview": preview}
