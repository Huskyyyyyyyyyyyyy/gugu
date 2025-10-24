# -*- coding: utf-8 -*-
"""
BaseDataClass (优化版)
---------------------
为 dataclass 子类提供统一的构造/清洗/校验与序列化能力。
流程：字段映射 -> 默认值合并 -> 字段转换 -> 行级校验 -> 构造实例 -> 序列化。

本版本相对于原实现的改进点：
1) DEFAULTS 深拷贝：避免可变对象（list/dict/set）在不同实例间被共享。
2) 字段映射冲突检测：多外键映射到同一内部字段时记录一次警告，便于排障。
3) 转换/校验日志增强：携带异常类型与片段，定位更快。
4) from_list 可迭代性检查更严格：排除 str/bytes 的迭代陷阱；enumerate 从 1 开始，日志更友好。
5) 构造失败时补充：缺失字段、额外字段与数据片段。
6) to_dict 支持递归 drop_none（嵌套结构生效）。
7) to_json 支持 JSON_DEFAULT 钩子，便于处理 datetime/Decimal/Enum 等特殊类型。
8) 细节打磨：类型注解更精确（logging.Logger），注释与文档串联到使用规范。

使用建议：
- 子类必须使用 @dataclass 装饰；若业务允许，建议 frozen=True。
- DEFAULTS 中的可变对象请使用 lambda 返回，或依赖本类的 deepcopy 保护。
- CONVERTERS 建议为纯函数；若会就地修改，务必在注释中标注副作用。
- VALIDATORS 只抛错不改值（如需就地修正也可，但请在注释中明确用途）。
"""
from __future__ import annotations

import copy
import json
import logging
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
from collections.abc import Iterable as _IterableABC

# ✅ 接入你自定义的日志管理器
from commons.base_logger import BaseLogger

# 模块级默认 logger（子类可覆盖 BaseDataClass.LOGGER 来定向到其他日志名/文件）
_DEFAULT_LOGGER = BaseLogger(name="BaseDataClass", to_file=True).logger

T = TypeVar("T", bound="BaseDataClass")
Converter = Callable[[Any], Any]
RowValidator = Callable[[Dict[str, Any]], None]


class BaseDataClass:
    """dataclass 子类的通用基类：构造、清洗、校验、序列化。

    子类可配置以下类变量以定制行为：
    - DEFAULTS: Dict[str, Any]
        字段默认值；值为 callable 时在每次构造时调用；非 callable 将进行 deepcopy。
    - FIELD_MAPPING: Dict[str, str]
        外部字段名 -> 内部字段名 的映射；未声明则按原名匹配 dataclass 字段。
    - CONVERTERS: Dict[str, Converter]
        字段级转换器；在默认值合并后、校验前执行。
    - VALIDATORS: List[RowValidator]
        行级校验器；接收合并/转换后的 dict，抛出异常即视为校验失败。
    - LOGGER: logging.Logger
        日志器；可由子类覆盖，例如 LOGGER = BaseLogger(name="YourModel").logger。
    - JSON_DEFAULT: Optional[Callable[[Any], Any]]
        提供给 json.dumps 的 default 钩子，用于序列化非内建类型（datetime/Decimal/Enum 等）。

    典型用法：
        @dataclasses.dataclass
        class User(BaseDataClass):
            id: int
            name: str | None = None
            age: int | None = None

        User.FIELD_MAPPING = {"user_id": "id"}
        User.DEFAULTS = {"age": 0}
        User.CONVERTERS = {"id": int, "age": lambda x: int(x) if x is not None else None}
        User.VALIDATORS = [lambda row: (_ for _ in ()).throw(ValueError("age<0")) if row.get("age", 0) < 0 else None]

        u = User.from_dict({"user_id": "123", "name": "Alice"})
        lst = list(User.from_list([{"user_id": 1}, {"user_id": 2}], yield_items=True))
        d = u.to_dict(drop_none=True)
        js = u.to_json(ensure_ascii=False)
    """

    DEFAULTS: ClassVar[Dict[str, Any]] = {}
    FIELD_MAPPING: ClassVar[Dict[str, str]] = {}
    CONVERTERS: ClassVar[Dict[str, Converter]] = {}
    VALIDATORS: ClassVar[List[RowValidator]] = []

    # ✅ 子类可覆盖：LOGGER = BaseLogger(name="YourModel").logger
    LOGGER: ClassVar[logging.Logger] = _DEFAULT_LOGGER

    # ✅ JSON 特殊类型序列化钩子（可被子类覆盖）
    JSON_DEFAULT: ClassVar[Callable[[Any], Any] | None] = None

    # --------- 内部：拿到可用 logger（支持子类覆盖） ---------
    @classmethod
    def _logger(cls) -> logging.Logger:
        return getattr(cls, "LOGGER", _DEFAULT_LOGGER) or _DEFAULT_LOGGER

    # ---------------- 构造（单行） ----------------
    @classmethod
    def from_dict(
        cls: Type[T],
        data: Mapping[str, Any],
        *,
        strict: bool = False,
        log_errors: bool = True,
    ) -> T:
        """从单个字典构造实例：映射 -> 默认 -> 转换 -> 校验 -> 构造。

        参数：
            data: 外部输入行（Mapping）。若非 Mapping：
                  - strict=True 则 TypeError；
                  - 否则降级为空 dict，并记录警告（若 log_errors）。
            strict: True 则遇到任何异常直接抛出；False 则记录日志并尽量跳过。
            log_errors: 是否记录警告日志。
        """
        logger = cls._logger()

        if not isinstance(data, Mapping):
            msg = f"from_dict 需要 Mapping，实际得到: {type(data).__name__}"
            if strict:
                raise TypeError(msg)
            if log_errors:
                logger.warning(msg)
            data = {}

        # 确认 cls 是 dataclass 类型
        try:
            dc_names = {f.name for f in dataclasses.fields(cls)}
        except TypeError:
            raise TypeError(f"{cls.__name__} 必须使用 @dataclass 装饰")

        # 1) 字段映射（外键 -> 内部字段名），并检测冲突
        mapped: Dict[str, Any] = {}
        _seen_src: Dict[str, str] = {}
        for ext_key, val in data.items():
            internal = cls.FIELD_MAPPING.get(ext_key, ext_key)
            if internal in dc_names:
                if internal in mapped and log_errors:
                    logger.warning(
                        "字段映射冲突: %r 与 %r 都映射到 %r，后者覆盖前者",
                        _seen_src[internal], ext_key, internal,
                    )
                mapped[internal] = val
                _seen_src[internal] = ext_key

        # 2) 默认值展开（callable 执行；非 callable deepcopy 防止可变对象共享）
        defaults_expanded: Dict[str, Any] = {}
        for k, v in cls.DEFAULTS.items():
            defaults_expanded[k] = v() if callable(v) else copy.deepcopy(v)

        combined: Dict[str, Any] = {**defaults_expanded, **mapped}

        # 3) 字段级转换（逐字段；失败时按 strict/log_errors 策略处理）
        for key, fn in cls.CONVERTERS.items():
            if key in combined:
                try:
                    combined[key] = fn(combined[key])
                except Exception as e:
                    if strict:
                        raise
                    if log_errors:
                        snippet = repr(str(combined.get(key))[:120])
                        logger.warning(
                            "字段转换失败 %s (%s): %s; 值片段=%s",
                            key, type(e).__name__, e, snippet,
                        )

        # 4) 行级校验（异常即失败；日志包含 validator 名称/可读标识）
        for validate in cls.VALIDATORS:
            try:
                validate(combined)
            except Exception as e:
                if strict:
                    raise
                if log_errors:
                    vname = getattr(validate, "__name__", repr(validate))
                    snippet = repr(str(combined)[:200])
                    logger.warning("行级校验失败 (%s): %s; 数据片段=%s", vname, e, snippet)
                # 维持原语义：抛出让上层决定是否跳过
                raise

        # 5) 构造 dataclass 实例（仅使用声明字段）
        slim = {k: v for k, v in combined.items() if k in dc_names}
        try:
            return cls(**slim)  # type: ignore[arg-type]
        except TypeError as e:
            if strict:
                raise
            if log_errors:
                missing = [f.name for f in dataclasses.fields(cls) if f.name not in slim]
                extras = [k for k in combined.keys() if k not in dc_names]
                snippet = repr(str(slim)[:200])
                logger.warning(
                    "构造实例失败: %s; 缺失=%r; 额外=%r; 数据片段=%s",
                    e, missing, extras, snippet,
                )
            # 保持原有语义：这里仍抛出，让 from_list 去兜底决定是否跳过
            raise

    # ---------------- 批量构造 ----------------
    @classmethod
    def from_list(
        cls: Type[T],
        data_list: Iterable[Mapping[str, Any]],
        *,
        yield_items: bool = False,
        strict: bool = False,
        log_errors: bool = True,
    ) -> Union[List[T], Iterator[T]]:
        """批量构造：支持生成器模式以降低峰值内存。

        语义：
            - 非严格模式下，单条失败将被跳过并记录日志，不影响整体；
            - 严格模式下，遇到任意失败立即抛出异常；
            - yield_items=True 时返回生成器，可流式消费。
        """
        logger = cls._logger()

        # 基础迭代校验：拒绝 str/bytes 的“可迭代”陷阱
        if not isinstance(data_list, _IterableABC) or isinstance(data_list, (str, bytes)):
            raise TypeError("from_list 需要 Iterable[Mapping]，且不接受 str/bytes")

        def _iter() -> Iterator[T]:
            for idx, item in enumerate(data_list, start=1):
                if not isinstance(item, Mapping):
                    if strict:
                        raise TypeError(f"元素必须是 Mapping，实际为: {type(item).__name__}")
                    if log_errors:
                        logger.warning("跳过非 Mapping 元素(第 %d 条): %r", idx, item)
                    continue
                try:
                    yield cls.from_dict(item, strict=strict, log_errors=log_errors)
                except Exception as e:
                    if strict:
                        raise
                    if log_errors:
                        snippet = str(item)
                        snippet = snippet if len(snippet) <= 200 else (snippet[:200] + "…")
                        logger.warning("from_list 跳过第 %d 条失败项: %s; 片段=%s", idx, e, snippet)
                    continue

        return _iter() if yield_items else list(_iter())

    # ---------------- 序列化 ----------------
    def to_dict(self, *, drop_none: bool = False) -> Dict[str, Any]:
        """导出为 dict；
        - drop_none=False：等同 dataclasses.asdict(self)
        - drop_none=True：递归剔除 None（对嵌套 dict/list 生效）
        """
        if not dataclasses.is_dataclass(self):
            raise TypeError(f"{type(self).__name__} 不是 dataclass，无法 asdict")
        d = dataclasses.asdict(self)
        if not drop_none:
            return d

        def _strip_none(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {k: _strip_none(v) for k, v in obj.items() if v is not None}
            if isinstance(obj, list):
                return [_strip_none(x) for x in obj if x is not None]
            return obj

        return _strip_none(d)

    def to_json(
        self,
        *,
        ensure_ascii: bool = False,
        drop_none: bool = False,
        default: Callable[[Any], Any] | None = None,
    ) -> str:
        """导出 JSON 文本；
        - ensure_ascii=False 保留中文；
        - drop_none=True 递归剔除 None；
        - default：json 序列化 default 钩子；未提供时优先使用类属性 JSON_DEFAULT。
        """
        cls = type(self)
        return json.dumps(
            self.to_dict(drop_none=drop_none),
            ensure_ascii=ensure_ascii,
            default=default or cls.JSON_DEFAULT,
        )
