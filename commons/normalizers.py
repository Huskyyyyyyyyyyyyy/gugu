# -*- coding: utf-8 -*-  # 指定 UTF-8 编码
# commons/normalizers.py  # 文件路径说明
from __future__ import annotations  # 允许前向引用注解

"""
normalizers
-----------
通用“字段级转换 / 行级校验”函数库。  # 模块文档：说明用途与函数签名
转换函数：func(value) -> new_value；校验函数：func(row_dict) -> None（异常表示失败）。  # 签名约定
"""

from typing import Any, Optional  # 导入通用类型注解


def empty_to_none(x: Any) -> Any:  # 将空串转换为 None 的转换器
    """将空串（含全空白）转换为 None，其它值保持不变。"""  # 文档字符串说明
    return None if isinstance(x, str) and x.strip() == "" else x  # 逻辑：字符串且空白则 None，否则原样返回


def to_int_or_none(x: Any) -> Optional[int]:  # 将值转换为 int 或 None 的转换器
    """
    把值尽量强转为 int；空串/None/非法值返回 None：  # 行为说明
    - "123" -> 123
    - 123.0 -> 123
    - "" / "  " / None -> None
    - "abc" -> None
    """  # 示例说明
    try:
        return int(x) if x is not None and str(x).strip() != "" else None  # 非空则转 int，否则 None
    except Exception:
        return None  # 转换异常兜底返回 None


def ts_to_seconds(x: Any) -> Optional[int]:  # 时间戳统一为秒的转换器
    """
    将“可能为秒/毫秒/字符串”的时间戳统一为秒级 int：  # 行为说明
    - None/空串 -> None
    - 数值 >= 1e12 视为毫秒，除以 1000
    - 支持数字字符串
    """  # 规则说明
    if x is None or (isinstance(x, str) and x.strip() == ""):  # 空值处理
        return None  # 空返回 None
    try:
        val = float(x)  # 统一转为浮点以便判断
    except Exception:
        return None  # 非数字字符串返回 None
    if val >= 1_000_000_000_000:  # 粗略阈值判断：>=1e12 认为是毫秒
        val /= 1000.0  # 毫秒转秒
    return int(val)  # 转为整数秒返回


def ensure_end_ge_start(row: dict, start_key: str = "starttime", end_key: str = "endtime") -> None:  # 行级校验器
    """
    轻量业务约束：结束时间必须 >= 开始时间。  # 校验规则说明
    缺失或非 int 不校验；发现违反则抛 ValueError。  # 边界行为说明
    """  # 文档字符串
    st, et = row.get(start_key), row.get(end_key)  # 读取开始与结束时间
    if isinstance(st, int) and isinstance(et, int) and et < st:  # 当两者均为 int 且结束小于开始
        raise ValueError(f"{end_key}({et}) < {start_key}({st})")  # 抛出业务异常





def to_float_or_none(x: Any) -> Optional[float]:
    """将值转换为 float；空串/None/非法值返回 None。"""
    try:
        return float(x) if x is not None and str(x).strip() != "" else None
    except Exception:
        return None

def to_bool_or_none(x: Any) -> Optional[bool]:
    """
    将值转换为 bool；常见真值：True/1/"1"/"true"/"True"/"yes"/"y"
    常见假值：False/0/"0"/"false"/"False"/"no"/"n"
    其它或空返回 None。
    """
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    if s in {"1", "true", "yes", "y"}:
        return True
    if s in {"0", "false", "no", "n"}:
        return False
    return None

def strip_or_none(x: Any) -> Optional[str]:
    """
    去掉首尾空白，空字符串返回 None。
    """
    if x is None:
        return None
    if not isinstance(x, str):
        return str(x)
    s = x.strip()
    return s if s != "" else None

def to_none_if_negative(x: Any) -> Optional[int]:
    """
    转换为 int；若值为负（如 -1），返回 None。
    """
    try:
        v = int(x)
    except Exception:
        return None
    return None if v < 0 else v
