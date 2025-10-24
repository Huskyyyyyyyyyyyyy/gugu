# mydataclass/bid_record.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, ClassVar, Dict, Any

from commons.base_dataclasses import BaseDataClass
from commons.normalizers import (
    to_int_or_none,
    to_float_or_none,
    empty_to_none,
    ts_to_seconds,
)

@dataclass(slots=True)
class BidRecord(BaseDataClass):
    """
    表示单条拍卖出价记录的数据模型。
    对应字段示例：
    {
        "id": 615399,
        "code": "JJ2510240F1ZXLZPVV280",
        "auctionid": 314,
        "pigeonid": 187862,
        "pigeoncode": "SG2510220F1RW26T5EP6O",
        "pigeonname": "【新奥林匹克索兰吉】直孙x【火山号】直女",
        "userid": 359602,
        "usercode": "GUGU007PGY350",
        "usernickname": "新用户57cb2",
        "useravatar": "",
        "type": "online",
        "quote": 1500,
        "margin": 500,
        "status": "running",
        "statustime": 1761292654,
        "createuserid": 359602,
        "createadminid": -1,
        "createtime": 1761292654,
        "canceluserid": -1,
        "canceladminid": -1
    }
    """

    # —— 核心字段 ——
    id: int
    code: str
    auction_id: int
    pigeon_id: int
    quote: float

    # —— 其他信息字段 ——
    pigeon_code: Optional[str] = None
    pigeon_name: Optional[str] = None
    user_id: Optional[int] = None
    user_code: Optional[str] = None
    user_nickname: Optional[str] = None
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

    # —— 默认值（全部 None） ——
    DEFAULTS: ClassVar[Dict[str, Any]] = {
        "pigeon_code": None,
        "pigeon_name": None,
        "user_id": None,
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
    }

    # —— 接口字段映射 ——
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
    }

    # —— 转换器 ——
    CONVERTERS: ClassVar[Dict[str, Any]] = {
        # 整型
        "id": to_int_or_none,
        "auction_id": to_int_or_none,
        "pigeon_id": to_int_or_none,
        "user_id": to_int_or_none,
        "create_user_id": to_int_or_none,
        "create_admin_id": to_int_or_none,
        "cancel_user_id": to_int_or_none,
        "cancel_admin_id": to_int_or_none,

        # 浮点
        "quote": to_float_or_none,
        "margin": to_float_or_none,

        # 时间戳
        "create_time": ts_to_seconds,
        "status_time": ts_to_seconds,

        # 字符串清洗
        "code": empty_to_none,
        "pigeon_code": empty_to_none,
        "pigeon_name": empty_to_none,
        "user_code": empty_to_none,
        "user_nickname": empty_to_none,
        "user_avatar": empty_to_none,
        "type": empty_to_none,
        "status": empty_to_none,
    }
