# -*- coding: utf-8 -*-  # 指定 UTF-8 编码
# models/gongpeng.py  # 文件路径说明
from __future__ import annotations  # 允许前向注解

"""
拍卖公棚数据模型。 
特点：继承 BaseDataClass，支持字段映射/默认值/转换/校验/序列化。  # 特性列表
时间戳统一为“秒级 UTC”，字段尽量与上游接口保持一致。  # 关键约定
"""

from dataclasses import dataclass  # 引入 dataclass 装饰器
from typing import Optional, ClassVar, Dict, Any, List  # 导入类型注解

from commons.base_dataclasses import BaseDataClass  # 引入基类
from commons.normalizers import (  # 引入通用转换与校验函数
    empty_to_none,  # 空串 -> None
    to_int_or_none,  # 任意 -> int/None
    ts_to_seconds,  # 时间戳 -> 秒
    ensure_end_ge_start,  # 校验 end >= start
)


@dataclass(slots=True)  # 声明为 dataclass，并启用 slots 以降低内存占用
class GongpengInfo(BaseDataClass):  # 继承 BaseDataClass，获得构造/清洗/校验能力
    """
    公棚信息数据模型（派往网站抓取）。  # 类文档：说明来源与用途
    字段说明：  # 字段解释
      - id: 唯一标识（必填，来自上游接口；本类不自动生成）  # id 语义
      - name: 名称（必填）  # name 语义
      - organizername: 组织者名称（可空）  # organizername 语义
      - organizerphone: 组织者电话（可空）  # organizerphone 语义
      - customerservicephone: 客服电话（可空）  # customerservicephone 语义
      - starttime: 开始时间戳（秒级 UTC；输入可为毫秒/秒/字符串）  # starttime 语义
      - endtime: 结束时间戳（秒级 UTC）  # endtime 语义
      - statusname: 状态描述（如：预展中/进行中/已结束）（可空）  # statusname 语义
      - livestatusname: 直播状态描述（如：未开始/直播中/已结束）（可空）  # livestatusname 语义
    """  # 结束类文档

    id: int  # 拍卖活动唯一标识（必填）
    name: str  # 拍卖活动名称（必填）

    organizername: Optional[str] = None  # 组织者名称（可空）
    organizerphone: Optional[str] = None  # 组织者联系电话（可空）
    customerservicephone: Optional[str] = None  # 客服电话（可空）

    starttime: Optional[int] = None  # 开始时间（秒级 UTC，可空）
    endtime: Optional[int] = None  # 结束时间（秒级 UTC，可空）

    statusname: Optional[str] = None  # 状态中文描述（可空）
    livestatusname: Optional[str] = None  # 直播状态中文描述（可空）

    DEFAULTS: ClassVar[Dict[str, Any]] = {  # 默认值配置（类型与字段一致）
        "organizername": None,  # 默认无组织者名称
        "organizerphone": None,  # 默认无组织者电话
        "customerservicephone": None,  # 默认无客服电话
        "starttime": None,  # 默认无开始时间
        "endtime": None,  # 默认无结束时间
        "statusname": None,  # 默认无状态
        "livestatusname": None,  # 默认无直播状态
        # "id": lambda: ...  # 如需动态默认（不推荐），可使用 callable
    }  # 结束 DEFAULTS

    FIELD_MAPPING: ClassVar[Dict[str, str]] = {}  # 字段映射（外部 -> 内部），当前与接口一致留空

    CONVERTERS: ClassVar[Dict[str, Any]] = {  # 字段级转换器配置
        "id": to_int_or_none,  # 入参转 int/None（若最终 None 将在构造时报错）
        "name": empty_to_none,  # 空串 -> None（若最终 None 将在构造时报错）
        "organizername": empty_to_none,  # 清洗空串
        "organizerphone": empty_to_none,  # 清洗空串
        "customerservicephone": empty_to_none,  # 清洗空串
        "statusname": empty_to_none,  # 清洗空串
        "livestatusname": empty_to_none,  # 清洗空串
        "starttime": ts_to_seconds,  # 毫秒/秒/字符串 -> 秒级 int/None
        "endtime": ts_to_seconds,  # 同上
    }  # 结束 CONVERTERS

    VALIDATORS: ClassVar[List] = [  # 行级校验器列表
        ensure_end_ge_start,  # 校验 endtime >= starttime
    ]  # 结束 VALIDATORS
