from typing import Dict, Any, Optional, List
from urllib.parse import urlsplit, urlunsplit


def _normalized_params(
    d: Dict[str, Any],
    *,
    keep_zero_keys: Optional[List[str]] = None,
    keep_empty_keys: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    剔除等价于“无限制”的参数（0 / 空串 / None / 仅空白），
    但允许通过 keep_zero_keys / keep_empty_keys 保留。
    """
    keep_zero = set(keep_zero_keys or [])
    keep_empty = set(keep_empty_keys or [])
    out: Dict[str, Any] = {}

    for k, v in d.items():
        if v is None:
            continue
        # bool 是 int 子类，优先判断
        if isinstance(v, bool):
            out[k] = v
            continue
        if isinstance(v, str):
            v2 = v.strip()
            if v2 == "" and k not in keep_empty:
                continue
            out[k] = v2
            continue
        if isinstance(v, (int, float)):
            if v == 0 and k not in keep_zero:
                continue
            out[k] = v
            continue
        out[k] = v

    return out


def _clean_url(url: str) -> str:
    """仅折叠 path 中的多余斜杠，保留原始是否以斜杠结尾的语义，不影响协议/域名/查询/fragment。"""
    parts = urlsplit(url)
    path = parts.path or ""
    trailing = path.endswith("/")
    # 折叠多余斜杠（保留根前导斜杠含义）
    collapsed = "/".join(filter(None, path.split("/")))
    clean_path = ("/" + collapsed) if path.startswith("/") else collapsed
    if trailing and clean_path and not clean_path.endswith("/"):
        clean_path += "/"
    return urlunsplit((parts.scheme, parts.netloc, clean_path, parts.query, parts.fragment))

