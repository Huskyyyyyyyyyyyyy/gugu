# ──────────────────────────────────────────────────────────────────────────────
# 模块用途：适配层（序列化）
# 说明：
#   - 将业务层的 BidRecord 列表转换为前端友好的 JSON 结构；
#   - 统一字段命名为 snake_case；
#   - 保留 results 以支持前端展示“内层历史记录”；
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import time
from decimal import Decimal
from typing import Dict, Any, List, Union
from mydataclass.record import BidRecord


# ---------- JSON 安全处理 ----------

JsonScalar = Union[str, int, float, bool, None]
JsonValue = Union[JsonScalar, Dict[str, "JsonValue"], List["JsonValue"]]


def _json_default(o):
    """json.dumps default，用于转 Decimal 等特殊类型"""
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _json_sanitize(v: Any) -> JsonValue:
    """递归清洗成可 JSON 序列化的结构"""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, dict):
        return {str(k): _json_sanitize(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_json_sanitize(i) for i in v]
    return str(v)


# ---------- 核心转换逻辑 ----------

def _one(r: BidRecord) -> Dict[str, Any]:
    """
    转换单条 BidRecord 为 JSON 友好的 dict。
    关键：保留 results，让前端能渲染历史记录。
    """
    user_code = r.user_code or ""
    results = _json_sanitize(r.results or {})

    return {
        "id": getattr(r, "id", None),
        "code": getattr(r, "code", None),
        "auction_id": getattr(r, "auction_id", None),
        "pigeon_id": getattr(r, "pigeon_id", None),
        "pigeon_code": getattr(r, "pigeon_code", None),
        "pigeon_name": getattr(r, "pigeon_name", None),

        "user_id": getattr(r, "user_id", None),
        "user_code": user_code,
        "user_nickname": getattr(r, "user_nickname", None) or getattr(r, "usernickname", None),
        "user_avatar": getattr(r, "user_avatar", None),

        "type": getattr(r, "type", None),
        "margin": _json_sanitize(getattr(r, "margin", None)),
        "quote": _json_sanitize(getattr(r, "quote", None)),
        "count": _json_sanitize(getattr(r, "count", 0)),
        "status": getattr(r, "status", None),
        "status_time": getattr(r, "status_time", None),

        "create_user_id": getattr(r, "create_user_id", None),
        "create_admin_id": getattr(r, "create_admin_id", None),
        "create_time": getattr(r, "create_time", None),
        "cancel_user_id": getattr(r, "cancel_user_id", None),
        "cancel_admin_id": getattr(r, "cancel_admin_id", None),

        # 保留完整 results（供前端按 user_code 分组）
        "results": results,

        # 当前用户的历史数组（冗余字段）
        "history": results.get(user_code, []),
    }


# ---------- 打包输出 ----------

def records_to_payload(records: List[BidRecord], current_info: dict) -> Dict[str, Any]:
    """
    将多条 BidRecord 打包为前端消费的统一结构。
    """
    return {
        "type": "pigeon/bids",
        "schema_version": "1.0",
        "ts": int(time.time() * 1000),
        "current_id": _json_sanitize(current_info or {}),
        "items": [_one(r) for r in records],
    }


def error_payload(message: str, code: str = "INTERNAL") -> Dict[str, Any]:
    """标准错误结构"""
    return {
        "type": "error",
        "schema_version": "1.0",
        "ts": int(time.time() * 1000),
        "error": {"code": code, "message": message},
    }
