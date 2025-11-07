# mydataclass/bid_record.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, ClassVar, Dict, Any, Iterable, Iterator, List, Mapping, Type, TypeVar, Union
from collections import Counter
from collections.abc import Mapping as _MappingABC, Iterable as _IterableABC, Sequence as _SequenceABC

from commons.base_dataclasses import BaseDataClass
from commons.normalizers import (
    to_int_or_none,
    to_float_or_none,
    empty_to_none,
    ts_to_seconds,
)

T = TypeVar("T")

@dataclass(slots=True)
class BidRecord(BaseDataClass):
    # —— 核心字段 ——
    id: int
    code: str
    auction_id: int
    pigeon_id: int
    quote: float

    # 这两个改为可选，稍后由上层填充
    count: Optional[int] = None
    results: Optional[Dict[str, List[Dict[str, Any]]]] = None
    # 新增的拍卖统计字段
    auction_bid_count: Optional[int] = 0  # 本场拍得次数，默认 0
    auction_total_price: Optional[float] = 0.0  # 拍得总价，默认 0.0
    auction_highest_price: Optional[float] = 0.0  # 拍得最高价，默认 0.0
    auction_second_highest_price: Optional[float] = 0.0  # 拍得次高价，默认 0.0
    # —— 其他信息字段 ——
    pigeon_code: Optional[str] = None
    pigeon_name: Optional[str] = None
    user_id: Optional[int] = None
    user_code: Optional[str] = None
    user_nickname: Optional[str] = None
    match_score: Optional[float] = None
    user_avatar: Optional[str] = None
    type: Optional[str] = None
    margin: Optional[float] = None
    status: Optional[str] = None
    status_time: Optional[int] = None
    create_user_id: Optional[int] = None
    create_admin_id: Optional[int] = None
    create_time: Optional[int] = None
    cancel_user_id: Optional[int] = None
    cancel_admin_id: Optional[int] = None

    DEFAULTS: ClassVar[Dict[str, Any]] = {
        "pigeon_code": None,
        "pigeon_name": None,
        "user_id": None,
        "count": None,
        "user_code": None,
        "user_nickname": None,
        "user_avatar": None,
        "type": None,
        "margin": None,
        "status": None,
        "status_time": None,
        "create_user_id": None,
        "create_admin_id": None,
        "create_time": None,
        "cancel_user_id": None,
        "cancel_admin_id": None,
        "results": None,
        "match_score": None,  # 新增的默认字段
        "auction_bid_count": 0,  # 新增字段的默认值
        "auction_total_price": 0.0,  # 新增字段的默认值
        "auction_highest_price": 0.0,  # 新增字段的默认值
        "auction_second_highest_price": 0.0,  # 新增字段的默认值
    }

    FIELD_MAPPING: ClassVar[Dict[str, str]] = {
        "id": "id",
        "code": "code",
        "auctionid": "auction_id",
        "pigeonid": "pigeon_id",
        "pigeoncode": "pigeon_code",
        "pigeonname": "pigeon_name",
        "userid": "user_id",
        "usercode": "user_code",
        "usernickname": "user_nickname",
        "useravatar": "user_avatar",
        "type": "type",
        "quote": "quote",
        "margin": "margin",
        "status": "status",
        "statustime": "status_time",
        "createuserid": "create_user_id",
        "createadminid": "create_admin_id",
        "createtime": "create_time",
        "canceluserid": "cancel_user_id",
        "canceladminid": "cancel_admin_id",
        # 注：count / results 不从上游映射，后面程序注入
    }

    CONVERTERS: ClassVar[Dict[str, Any]] = {
        "id": to_int_or_none,
        "auction_id": to_int_or_none,
        "pigeon_id": to_int_or_none,
        "user_id": to_int_or_none,
        "create_user_id": to_int_or_none,
        "create_admin_id": to_int_or_none,
        "cancel_user_id": to_int_or_none,
        "cancel_admin_id": to_int_or_none,

        "quote": to_float_or_none,
        "margin": to_float_or_none,

        "create_time": ts_to_seconds,
        "status_time": ts_to_seconds,

        "code": empty_to_none,
        "pigeon_code": empty_to_none,
        "pigeon_name": empty_to_none,
        "user_code": empty_to_none,
        "user_nickname": empty_to_none,
        "user_avatar": empty_to_none,
        "type": empty_to_none,
        "status": empty_to_none,

        "match_score": to_float_or_none,  # 新增字段转换
        "auction_total_price": to_float_or_none,  # 新增字段转换
        "auction_highest_price": to_float_or_none,  # 新增字段转换
        "auction_second_highest_price": to_float_or_none,  # 新增字段转换

        # 让它保持结构化字典
    }

    @classmethod
    def from_list(
        cls: Type[T],
        data_list: Iterable[Mapping[str, Any]],
        *,
        yield_items: bool = False,
        strict: bool = False,
        log_errors: bool = True,
    ) -> Union[List[T], Iterator[T]]:
        logger = cls._logger()

        if not isinstance(data_list, _IterableABC) or isinstance(data_list, (str, bytes)):
            raise TypeError("from_list 需要 Iterable[Mapping]，且不接受 str/bytes")

        objs: List[T] = []
        for idx, item in enumerate(data_list, start=1):
            if not isinstance(item, _MappingABC):
                if strict:
                    raise TypeError(f"元素必须是 Mapping，实际为: {type(item).__name__}")
                if log_errors:
                    logger.warning("跳过非 Mapping 元素(第 %d 条): %r", idx, item)
                continue
            try:
                obj = cls.from_dict(item, strict=strict, log_errors=log_errors)
                objs.append(obj)
            except Exception as e:
                if strict:
                    raise
                if log_errors:
                    snippet = str(item)
                    snippet = snippet if len(snippet) <= 200 else (snippet[:200] + "…")
                    logger.warning("from_list 跳过第 %d 条失败项: %s; 片段=%s", idx, e, snippet)
                continue

        # 统计本批次的出价频次（同一 user_code 在本批数据中的出现次数）
        from collections import Counter
        counts = Counter(getattr(o, "user_code", None) for o in objs)
        for o in objs:
            uc = getattr(o, "user_code", None)
            o.count = int(counts.get(uc, 0))

        if yield_items:
            def _iter() -> Iterator[T]:
                for o in objs:
                    yield o
            return _iter()
        else:
            return objs
