from __future__ import annotations

import time
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Dict, Any, List, Union, Optional, BinaryIO, Tuple

from io import BytesIO
from openpyxl import load_workbook

from tools.config_loader import load_config
from mydataclass.record import BidRecord

# =========================
# JSON 序列化与类型别名
# =========================

JsonScalar = Union[str, int, float, bool, None]
JsonValue = Union[JsonScalar, Dict[str, "JsonValue"], List["JsonValue"]]


def _json_default(o):
    """
    json.dumps 的 default 回调，可用于处理 Decimal 等类型。
    当前模块暂未直接调用 json.dumps，这里保留做兼容。
    """
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _json_sanitize(v: Any) -> JsonValue:
    """
    递归清洗对象，确保可被 JSON 序列化：
    - Decimal -> float
    - dataclass -> dict
    - 其他容器类型递归处理
    """
    if isinstance(v, Decimal):
        return float(v)
    if is_dataclass(v):
        return _json_sanitize(asdict(v))
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, dict):
        return {str(k): _json_sanitize(val) for k, val in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_json_sanitize(i) for i in v]
    return str(v)


# =========================
# Excel 加载与缓存
# =========================

XlsxSource = Union[str, bytes, BinaryIO]
# 路径字符串 -> (文件修改时间 mtime, {环号: 内容})
_XLSX_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def xlsx_cache_info() -> Dict[str, Any]:
    """返回当前缓存的 key 与时间戳（便于排障/观测）"""
    return {k: v[0] for k, v in _XLSX_CACHE.items()}


def clear_xlsx_cache(path: Optional[str] = None) -> None:
    """清除缓存：不传 path 则清空全部，传入具体路径仅清除对应项。"""
    if path is None:
        _XLSX_CACHE.clear()
    else:
        _XLSX_CACHE.pop(str(Path(path).resolve()), None)


def _locate_headers(ws, ring_header: str, content_header: str, search_rows: int = 10) -> Tuple[int, int, int]:
    """
    尝试在前 N 行内定位表头行与两列索引。
    返回: (header_row_idx, ring_col_idx, content_col_idx)
    若未能定位，默认 header_row=1，ring_col=1，content_col=2。
    """
    header_row, ring_col, content_col = 1, 1, 2
    for r in range(1, min(search_rows, ws.max_row) + 1):
        vals = [c.value for c in ws[r]]
        for i, v in enumerate(vals):
            if v == ring_header:
                ring_col = i + 1
            if v == content_header:
                content_col = i + 1
        if ring_col and content_col:
            header_row = r
            break
    return header_row, ring_col, content_col


def _xlsx_to_map_cached(xlsx_path: str, ring_header: str = "环号", content_header: str = "内容") -> Dict[str, Any]:
    """
    带缓存的 XLSX -> 映射读取。
    缓存 Key：文件绝对路径；缓存命中条件：mtime 相同。
    """
    p = Path(xlsx_path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"XLSX file not found: {p}")

    mtime = p.stat().st_mtime
    cache = _XLSX_CACHE.get(str(p))
    if cache and cache[0] == mtime:
        return cache[1]  # 命中缓存

    # 重新加载
    wb = load_workbook(filename=str(p), data_only=True, read_only=True)
    try:
        ws = wb.worksheets[0]
        header_row, ring_col, content_col = _locate_headers(ws, ring_header, content_header, search_rows=10)

        mapping: Dict[str, Any] = {}
        for r in range(header_row + 1, ws.max_row + 1):
            ring = ws.cell(r, ring_col).value
            val = ws.cell(r, content_col).value
            if ring is None:
                continue
            ring_str = str(ring).strip()
            if ring_str:
                mapping[ring_str] = val

        _XLSX_CACHE[str(p)] = (mtime, mapping)
        return mapping
    finally:
        wb.close()


# =========================
# 业务转换逻辑
# =========================

def _one(r: BidRecord) -> Dict[str, Any]:
    """
    转换单条 BidRecord 为 JSON 友好的 dict。
    关键：保留 results 让前端渲染历史记录；并冗余当前用户对应的 history。
    """
    user_code = r.user_code or ""
    results = _json_sanitize(getattr(r, "results", {}) or {})

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

        # 完整 results（供前端分组/展示）
        "results": results,

        # 冗余：当前 user_code 的历史数组
        "history": results.get(user_code, []),
    }


def _normalize_ring(s: Optional[str]) -> str:
    """
    轻量规范化环号：
    - 去空白
    - 全角/变体横线统一为 '-'
    - 转大写
    """
    if not s:
        return ""
    s = str(s).strip()
    s = (
        s.replace("－", "-")
         .replace("—", "-")
         .replace("–", "-")
         .replace("―", "-")
         .replace(" ", "")
    )
    return s.upper()


def records_to_payload(
    records: List[BidRecord],
    current_info: dict,
    *,
    xlsx: Optional[XlsxSource] = None,
    ring_header: str = "环号",
    content_header: str = "内容",
    target_field: str = "content",
    config_section: str = "context",
    config_file: str = "config/spider.yaml",
) -> Dict[str, Any]:
    """
    将一组 BidRecord + 当前拍品信息打包为 SSE 前端消费的 payload。
    - 如提供/配置了 Excel 表（含「环号」「内容」两列），则尝试按环号匹配并将内容写入 current[target_field]。
    - Excel 解析使用模块级缓存，按 path+mtime 命中。
    """
    enriched_current = dict(current_info or {})
    raw_footring = enriched_current.get("footring")
    footring = _normalize_ring(raw_footring)

    # 1) 若未显式传入 xlsx，尝试从 YAML 读取路径
    if xlsx is None:
        try:
            cfg = load_config(section=config_section, file_path=config_file)
            if isinstance(cfg, dict):
                xlsx_path = cfg.get("xlsx_path") or cfg.get("xlsx")
                if xlsx_path:
                    xlsx = str(Path(xlsx_path).expanduser().resolve())
                    # print(f"[DEBUG] 从配置加载 xlsx 路径: {xlsx}")
        except Exception as e:
            print(f"[DEBUG] YAML 加载错误: {e}")
            enriched_current.setdefault("_xlsx_warning", f"config_load_error: {e}")

    # 2) Excel 匹配：先用原值命中，再用规范化后命中（解决「环号格式不一致」）
    if xlsx and footring:
        try:
            mapping = _xlsx_to_map_cached(str(xlsx), ring_header=ring_header, content_header=content_header)
            val = mapping.get(raw_footring)
            if val is None:
                normalized_view = {_normalize_ring(k): v for k, v in mapping.items() if k}
                val = normalized_view.get(footring)

            if val is not None:
                enriched_current[target_field] = val
                print(f"[DEBUG] 匹配到内容: {val}")
            else:
                print(f"[DEBUG] 未匹配到环号: {footring}")
        except Exception as e:
            print(f"[DEBUG] Excel 读取/解析错误: {e}")
            enriched_current.setdefault("_xlsx_warning", str(e))
    else:
        print(f"[DEBUG] 未提供 xlsx 或 footring 为空: xlsx={xlsx}, footring={footring}")

    # 3) 打包输出
    return {
        "type": "pigeon/bids",
        "schema_version": "1.0",
        "ts": int(time.time() * 1000),
        "current_id": _json_sanitize(enriched_current),
        "items": [_one(r) for r in records],
    }


def error_payload(message: str, code: str = "INTERNAL") -> Dict[str, Any]:
    """
    统一错误结构（便于前端消费/展示）
    """
    return {
        "type": "error",
        "schema_version": "1.0",
        "ts": int(time.time() * 1000),
        "error": {"code": code, "message": message},
    }
