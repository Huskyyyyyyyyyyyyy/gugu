# ──────────────────────────────────────────────────────────────────────────────
# 模块用途：适配层（序列化）
# 说明：
#   - 将业务层的 BidRecord 列表转换为前端友好的 JSON 结构；
#   - 不依赖框架和网络，仅做纯数据转换，便于单元测试与演进。
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import time
from decimal import Decimal
from typing import Dict, Any, List
from mydataclass.record import BidRecord

def _json_default(o):
    if isinstance(o, Decimal):
        # 你想保留小数：用 float；想保留原样：用 str
        return float(o)
    # 可以按需扩展其他类型，比如 datetime -> isoformat
    # if isinstance(o, datetime): return o.isoformat()


    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")



def _one(r: BidRecord) -> Dict[str, Any]:
    """
    将单条 BidRecord 转成前端友好的 dict。
    说明：
      - r.results 约定结构为 { user_code: [rows...] }，这里仅取该记录对应 user_code 的条目；
      - rows 内可能包含你在排序阶段注入的 _match_* / _agg_* / _match_spans 等辅助字段，直接透传。
    """
    uc = r.user_code or ""
    rows = (r.results or {}).get(uc, [])
    return {
        "code": getattr(r, "code", None),
        "userCode": uc,
        "userNickname": getattr(r, "user_nickname", None) or getattr(r, "usernickname", None),
        "count": getattr(r, "count", 0),
        "history": rows,
    }


def records_to_payload(records: List[BidRecord],current_info:dict) -> Dict[str, Any]:
    """
    将一批 BidRecord 打包为统一消息包（前端直接消费）。
    """
    return {
        "type": "pigeon/bids",
        "schema_version": "1.0",
        "ts": int(time.time() * 1000),
        "current_id": current_info,
        "items": [r for r in records],
    }


def error_payload(message: str, code: str = "INTERNAL") -> Dict[str, Any]:
    """
    统一错误消息格式；即便后端异常，也能给前端结构化错误。
    """
    return {
        "type": "error",
        "schema_version": "1.0",
        "ts": int(time.time() * 1000),
        "error": {"code": code, "message": message},
    }
