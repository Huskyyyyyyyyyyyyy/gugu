from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, ClassVar, Dict, Any, List

from commons.base_dataclasses import BaseDataClass
from commons.normalizers import (
    empty_to_none,
    to_int_or_none,
    to_float_or_none,   # 见下方补充
    to_bool_or_none,    # 见下方补充
    ts_to_seconds,
    ensure_end_ge_start,
)

@dataclass(slots=True)
class PigeonInfo(BaseDataClass):
    # —— 必填字段（强依赖主键/外键/核心业务名称）——
    id: int
    code: str
    auction_id: int
    name: str

    # —— 其它字段（可空，默认 None；由转换器做类型收敛）——
    auction_type: Optional[str] = None
    margin_ratio: Optional[float] = None
    section_id: Optional[int] = None
    ranking: Optional[int] = None
    competition_id: Optional[int] = None
    competition_name: Optional[str] = None
    match_id: Optional[int] = None
    match_name: Optional[str] = None
    gugu_pigeon_id: Optional[str] = None
    foot_ring: Optional[str] = None
    feather_color: Optional[str] = None
    matcher_name: Optional[str] = None
    start_price: Optional[float] = None
    image: Optional[str] = None
    sort: Optional[int] = None
    client_sort: Optional[int] = None
    is_current: Optional[bool] = None
    status: Optional[str] = None
    create_time: Optional[int] = None     # 秒级 UTC
    status_time: Optional[int] = None     # 秒级 UTC
    view_count: Optional[int] = None
    start_time: Optional[int] = None      # 秒级 UTC
    end_time: Optional[int] = None        # 秒级 UTC
    status_name: Optional[str] = None
    organizer_name: Optional[str] = None
    organizer_phone: Optional[str] = None
    order_status: Optional[str] = None
    order_status_name: Optional[str] = None
    is_watched: Optional[bool] = None
    remark: Optional[str] = None
    ws_remark: Optional[str] = None
    bid_id: Optional[int] = None
    quote: Optional[float] = None
    bid_type: Optional[str] = None
    bid_time: Optional[int] = None        # 若上游是时间戳/字符串，统一成秒
    bid_user_id: Optional[int] = None
    bid_user_code: Optional[str] = None
    bid_user_nickname: Optional[str] = None
    bid_user_avatar: Optional[str] = None
    bid_count: Optional[int] = None
    order_id: Optional[int] = None
    create_admin_id: Optional[int] = None
    specified_count: Optional[int] = None
    specified_sync: Optional[bool] = None

    # —— 默认值（用 None 表示“未知/缺失”，不要用 0/空串伪装“有值”）——
    DEFAULTS: ClassVar[Dict[str, Any]] = {
        # 必填字段不在这里给默认：缺就让它报错
        "auction_type": None,
        "margin_ratio": None,
        "section_id": None,
        "ranking": None,
        "competition_id": None,
        "competition_name": None,
        "match_id": None,
        "match_name": None,
        "gugu_pigeon_id": None,
        "foot_ring": None,
        "feather_color": None,
        "matcher_name": None,
        "start_price": None,
        "image": None,
        "sort": None,
        "client_sort": None,
        "is_current": None,
        "status": None,
        "create_time": None,
        "status_time": None,
        "view_count": None,
        "start_time": None,
        "end_time": None,
        "status_name": None,
        "organizer_name": None,
        "organizer_phone": None,
        "order_status": None,
        "order_status_name": None,
        "is_watched": None,
        "remark": None,
        "ws_remark": None,
        "bid_id": None,
        "quote": None,
        "bid_type": None,
        "bid_time": None,
        "bid_user_id": None,
        "bid_user_code": None,
        "bid_user_nickname": None,
        "bid_user_avatar": None,
        "bid_count": None,
        "order_id": None,
        "create_admin_id": None,
        "specified_count": None,
        "specified_sync": None,
    }

    # —— 字段映射（接口 -> 内部）。保持你原来的键位，便于无缝替换 ——
    FIELD_MAPPING: ClassVar[Dict[str, str]] = {
        "statusname": "status_name",
        "organizername": "organizer_name",
        "organizerphone": "organizer_phone",
        "orderstatus": "order_status",
        "orderstatusname": "order_status_name",
        "iswatched": "is_watched",
        "id": "id",
        "code": "code",
        "auctionid": "auction_id",
        "auctiontype": "auction_type",
        "marginratio": "margin_ratio",
        "sectionid": "section_id",
        "name": "name",
        "ranking": "ranking",
        "competitionid": "competition_id",
        "competitionname": "competition_name",
        "matchid": "match_id",
        "matchname": "match_name",
        "gugupigeonid": "gugu_pigeon_id",
        "footring": "foot_ring",
        "feathercolor": "feather_color",
        "matchername": "matcher_name",
        "startprice": "start_price",
        "image": "image",
        "sort": "sort",
        "clientsort": "client_sort",
        "iscurrent": "is_current",
        "remark": "remark",
        "wsremark": "ws_remark",
        "bidid": "bid_id",
        "quote": "quote",
        "bidtype": "bid_type",
        "bidtime": "bid_time",
        "biduserid": "bid_user_id",
        "bidusercode": "bid_user_code",
        "bidusernickname": "bid_user_nickname",
        "biduseravatar": "bid_user_avatar",
        "bidcount": "bid_count",
        "orderid": "order_id",
        "status": "status",
        "createadminid": "create_admin_id",
        "createtime": "create_time",
        "statustime": "status_time",
        "viewcount": "view_count",
        "specifiedcount": "specified_count",
        "specifiedsync": "specified_sync",
        "starttime": "start_time",
        "endtime": "end_time",
    }

    # —— 字段转换（清洗/强转/收敛）——
    CONVERTERS: ClassVar[Dict[str, Any]] = {
        # 主键/外键/整型
        "id": to_int_or_none,
        "auction_id": to_int_or_none,
        "section_id": to_int_or_none,
        "ranking": to_int_or_none,
        "competition_id": to_int_or_none,
        "match_id": to_int_or_none,
        "view_count": to_int_or_none,
        "bid_id": to_int_or_none,
        "bid_user_id": to_int_or_none,
        "bid_count": to_int_or_none,
        "order_id": to_int_or_none,
        "create_admin_id": to_int_or_none,
        "specified_count": to_int_or_none,
        "sort": to_int_or_none,
        "client_sort": to_int_or_none,

        # 字符串清洗
        "code": empty_to_none,
        "name": empty_to_none,
        "auction_type": empty_to_none,
        "competition_name": empty_to_none,
        "match_name": empty_to_none,
        "gugu_pigeon_id": empty_to_none,
        "foot_ring": empty_to_none,
        "feather_color": empty_to_none,
        "matcher_name": empty_to_none,
        "image": empty_to_none,
        "status": empty_to_none,
        "status_name": empty_to_none,
        "organizer_name": empty_to_none,
        "organizer_phone": empty_to_none,
        "order_status": empty_to_none,
        "order_status_name": empty_to_none,
        "remark": empty_to_none,
        "ws_remark": empty_to_none,
        "bid_type": empty_to_none,
        "bid_user_code": empty_to_none,
        "bid_user_nickname": empty_to_none,
        "bid_user_avatar": empty_to_none,

        # 数值/金额/比率
        "margin_ratio": to_float_or_none,
        "start_price": to_float_or_none,
        "quote": to_float_or_none,

        # 布尔
        "is_current": to_bool_or_none,
        "is_watched": to_bool_or_none,
        "specified_sync": to_bool_or_none,

        # 时间统一为秒（支持秒/毫秒/字符串）
        "create_time": ts_to_seconds,
        "status_time": ts_to_seconds,
        "start_time": ts_to_seconds,
        "end_time": ts_to_seconds,
        "bid_time": ts_to_seconds,
    }

    # # —— 行级校验 ——
    # VALIDATORS: ClassVar[List] = [
    #     ensure_end_ge_start,  # end_time >= start_time
    #     # 价格非负（需要的话可启用）
    #     lambda row: (_ for _ in ()).throw(ValueError("start_price < 0"))
    #     if (row.get("start_price") is not None and float(row["start_price"]) < 0)
    #     else None,
    #     # 比率在 [0, 1]（如果业务是百分数 0-1；若是 0-100 请改范围）
    #     lambda row: (_ for _ in ()).throw(ValueError("margin_ratio out of range [0,1]"))
    #     if (row.get("margin_ratio") is not None and not (0.0 <= float(row["margin_ratio"]) <= 1.0))
    #     else None,
    # ]
