# -*- coding: utf-8 -*-
# commons/base_dataclasses.py
from __future__ import annotations

"""
BaseDataClass
-------------
为 dataclass 子类提供统一的构造/清洗/校验与序列化能力。
流程：字段映射 -> 默认值合并 -> 字段转换 -> 行级校验 -> 构造实例 -> 序列化。
"""

import json
import dataclasses
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Type,
    TypeVar,
    Union,
)

# ✅ 接入你自定义的日志管理器
from commons.base_logger import BaseLogger

# 模块级默认 logger（子类可覆盖 BaseDataClass.LOGGER 来定向到其他日志名/文件）
_DEFAULT_LOGGER = BaseLogger(name="BaseDataClass", to_file=True).logger

T = TypeVar("T", bound="BaseDataClass")
Converter = Callable[[Any], Any]
RowValidator = Callable[[Dict[str, Any]], None]


class BaseDataClass:
    """
    dataclass 子类的通用基类：构造、清洗、校验、序列化。
    子类可配置 DEFAULTS/FIELD_MAPPING/CONVERTERS/VALIDATORS/LOGGER 来定制行为。
    """

    DEFAULTS: ClassVar[Dict[str, Any]] = {}
    FIELD_MAPPING: ClassVar[Dict[str, str]] = {}
    CONVERTERS: ClassVar[Dict[str, Converter]] = {}
    VALIDATORS: ClassVar[List[RowValidator]] = []
    # ✅ 子类可覆盖：LOGGER = BaseLogger(name="YourModel").logger
    LOGGER: ClassVar[Any] = _DEFAULT_LOGGER

    # --------- 内部：拿到可用 logger（支持子类覆盖） ---------
    @classmethod
    def _logger(cls):
        return getattr(cls, "LOGGER", _DEFAULT_LOGGER) or _DEFAULT_LOGGER

    @classmethod
    def from_dict(
        cls: Type[T],
        data: Mapping[str, Any],
        *,
        strict: bool = False,
        log_errors: bool = True,
    ) -> T:
        """从单个字典构造实例：映射 -> 默认 -> 转换 -> 校验 -> 构造。"""
        logger = cls._logger()

        if not isinstance(data, Mapping):
            msg = f"from_dict 需要 Mapping，实际得到: {type(data).__name__}"
            if strict:
                raise TypeError(msg)
            if log_errors:
                logger.warning(msg)
            data = {}

        try:
            dc_names = {f.name for f in dataclasses.fields(cls)}
        except TypeError:
            raise TypeError(f"{cls.__name__} 必须是 dataclass")

        # 1) 字段映射（外键 -> 内部字段名）
        mapped: Dict[str, Any] = {}
        for k, v in data.items():
            internal = cls.FIELD_MAPPING.get(k, k)
            if internal in dc_names:
                mapped[internal] = v

        # 2) 默认值展开（支持 callable）
        defaults_expanded: Dict[str, Any] = {}
        for k, v in cls.DEFAULTS.items():
            defaults_expanded[k] = v() if callable(v) else v

        combined: Dict[str, Any] = {**defaults_expanded, **mapped}

        # 3) 字段级转换
        for key, fn in cls.CONVERTERS.items():
            if key in combined:
                try:
                    combined[key] = fn(combined[key])
                except Exception as e:
                    if strict:
                        raise
                    if log_errors:
                        logger.warning("字段转换失败 %s: %s; 值=%r", key, e, combined.get(key))

        # 4) 行级校验（异常即失败）
        for validate in cls.VALIDATORS:
            validate(combined)

        # 5) 构造 dataclass 实例（仅使用声明字段）
        slim = {k: v for k, v in combined.items() if k in dc_names}
        try:
            return cls(**slim)  # type: ignore[arg-type]
        except TypeError as e:
            if strict:
                raise
            if log_errors:
                logger.warning("构造实例失败: %s; 数据=%r", e, slim)
            # 保持原有语义：这里仍抛出，让 from_list 去兜底决定是否跳过
            raise

    @classmethod
    def from_list(
        cls: Type[T],
        data_list: Iterable[Mapping[str, Any]],
        *,
        yield_items: bool = False,
        strict: bool = False,
        log_errors: bool = True,
    ) -> Union[List[T], Iterator[T]]:
        """
        批量构造：支持生成器模式以降低峰值内存。
        非严格模式下，单条失败将被跳过并记录日志，不影响整体。
        """
        logger = cls._logger()

        # 基础迭代校验
        if not hasattr(data_list, "__iter__"):
            raise TypeError("from_list 需要可迭代对象（Iterable[Mapping]]）")

        def _iter() -> Iterator[T]:
            for idx, item in enumerate(data_list):
                if not isinstance(item, Mapping):
                    if strict:
                        raise TypeError(f"元素必须是 Mapping，实际为: {type(item).__name__}")
                    if log_errors:
                        logger.warning("跳过非 Mapping 元素: %r", item)
                    continue
                try:
                    yield cls.from_dict(item, strict=strict, log_errors=log_errors)
                except Exception as e:
                    if strict:
                        raise
                    if log_errors:
                        # 记录部分内容，避免日志过长
                        snippet = str(item)
                        snippet = snippet if len(snippet) <= 200 else (snippet[:200] + "…")
                        logger.warning("from_list 跳过第 %d 条失败项: %s; 片段=%s", idx, e, snippet)
                    continue

        return _iter() if yield_items else list(_iter())

    # ---------------- 序列化 ----------------
    def to_dict(self, *, drop_none: bool = False) -> Dict[str, Any]:
        """导出为 dict；drop_none=True 时剔除 None 字段。"""
        if not dataclasses.is_dataclass(self):
            raise TypeError(f"{type(self).__name__} 不是 dataclass，无法 asdict")
        d = dataclasses.asdict(self)
        return {k: v for k, v in d.items() if not (drop_none and v is None)}

    def to_json(self, *, ensure_ascii: bool = False, drop_none: bool = False) -> str:
        """导出 JSON 文本；ensure_ascii=False 保留中文；drop_none=True 剔除 None。"""
        return json.dumps(self.to_dict(drop_none=drop_none), ensure_ascii=ensure_ascii)
