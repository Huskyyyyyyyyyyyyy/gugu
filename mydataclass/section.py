from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, ClassVar, Dict, Any, List

from commons.base_dataclasses import BaseDataClass
from commons.normalizers import (
    empty_to_none,
    to_int_or_none,
    to_float_or_none,
    ts_to_seconds,
)

@dataclass(slots=True)
class SectionInfo(BaseDataClass):
    """
    表示某个公棚拍卖活动下的一个专场（section）。
    约定：
      - 内部字段统一为蛇形命名。
      - 时间统一为“秒级 UTC”的 int（或 None）。
      - 必填字段：id, auction_id, name；其余可空。
    """

    # —— 必填字段 ——
    id: int                      # 专场 ID（主键）
    auction_id: int              # 拍卖活动 ID（外键）
    name: str                    # 专场名称

    # —— 可选字段（缺失用 None 表示） ——
    auction_type: Optional[str] = None      # 拍卖类型，如 'live'
    organizer_name: Optional[str] = None    # 组织者名称
    organizer_phone: Optional[str] = None   # 组织者电话
    customerservice_phone: Optional[str] = None  # 客服电话
    match_id: Optional[int] = None          # 比赛 ID
    start_ranking: Optional[int] = None     # 起始排名
    end_ranking: Optional[int] = None       # 结束排名
    count: Optional[int] = None             # 鸽子数量
    sort_type: Optional[str] = None         # 排序方式（如 'asc'/'desc'）
    start_price: Optional[float] = None     # 起拍价
    sort: Optional[int] = None              # 排序字段
    create_admin_id: Optional[int] = None   # 创建人 ID
    create_time: Optional[int] = None       # 创建时间（秒级 UTC）
    status_name: Optional[str] = None       # 状态名（如：进行中/已结束）

    # —— 默认值（均用 None，避免把“缺失”伪装成“有效 0/空串”） ——
    DEFAULTS: ClassVar[Dict[str, Any]] = {
        "auction_type": None,
        "organizer_name": None,
        "organizer_phone": None,
        "customerservice_phone": None,
        "match_id": None,
        "start_ranking": None,
        "end_ranking": None,
        "count": None,
        "sort_type": None,
        "start_price": None,
        "sort": None,
        "create_admin_id": None,
        "create_time": None,
        "status_name": None,   # 如果你确实要默认“进行中”，这里可设为 "进行中"
    }

    # —— 字段映射（上游 -> 内部蛇形） ——
    FIELD_MAPPING: ClassVar[Dict[str, str]] = {
        "auctionid": "auction_id",
        "auctiontype": "auction_type",
        "organizername": "organizer_name",
        "organizerphone": "organizer_phone",
        "customerservicephone": "customerservice_phone",
        "matchid": "match_id",
        "startranking": "start_ranking",
        "endranking": "end_ranking",
        "count": "count",
        "sorttype": "sort_type",
        "startprice": "start_price",
        "sort": "sort",
        "createadminid": "create_admin_id",
        "createtime": "create_time",
        "statusname": "status_name",

        # 同名字段可省略；此处显式列出更直观
        "id": "id",
        "name": "name",
    }

    # —— 字段级转换（清洗/类型归一化） ——
    CONVERTERS: ClassVar[Dict[str, Any]] = {
        # 必填/整型
        "id": to_int_or_none,
        "auction_id": to_int_or_none,

        # 可选整型
        "match_id": to_int_or_none,
        "start_ranking": to_int_or_none,
        "end_ranking": to_int_or_none,
        "count": to_int_or_none,
        "sort": to_int_or_none,
        "create_admin_id": to_int_or_none,

        # 金额
        "start_price": to_float_or_none,

        # 文本清洗（空串 -> None）
        "name": empty_to_none,
        "auction_type": empty_to_none,
        "organizer_name": empty_to_none,
        "organizer_phone": empty_to_none,
        "customerservice_phone": empty_to_none,
        "sort_type": empty_to_none,
        "status_name": empty_to_none,

        # 时间统一为秒
        "create_time": ts_to_seconds,
    }

    # —— 行级校验（按需增删） ——
    VALIDATORS: ClassVar[List] = [
        # 起止排名逻辑
        lambda row: (_ for _ in ()).throw(ValueError("start_ranking > end_ranking"))
        if (isinstance(row.get("start_ranking"), int)
            and isinstance(row.get("end_ranking"), int)
            and row["start_ranking"] > row["end_ranking"])
        else None,

        # 数量非负
        lambda row: (_ for _ in ()).throw(ValueError("count < 0"))
        if (row.get("count") is not None and int(row["count"]) < 0)
        else None,

        # 起拍价非负
        lambda row: (_ for _ in ()).throw(ValueError("start_price < 0"))
        if (row.get("start_price") is not None and float(row["start_price"]) < 0)
        else None,

        # 排序方式校验（按你业务需要可放宽/去掉）
        lambda row: (_ for _ in ()).throw(ValueError("sort_type must be 'asc' or 'desc'"))
        if (row.get("sort_type") is not None and str(row["sort_type"]).lower() not in {"asc", "desc"})
        else None,
    ]
