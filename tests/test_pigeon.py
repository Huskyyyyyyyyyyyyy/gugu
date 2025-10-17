import pytest
from mydataclass.pigeon import PigeonInfo  # 按你项目的路径修改导入


def test_pigeoninfo_normal_case():
    """✅ 测试混合毫秒/秒/空串/布尔字符串的解析"""
    raw = {
        "id": "1001",
        "code": "GP-2025-001",
        "auctionid": "987",
        "name": "2025 秋季专场 - 一号",
        "starttime": 1760918400000,     # 毫秒
        "endtime": "1761004800",        # 秒字符串
        "startprice": "2000.00",
        "marginratio": "0.1",
        "iscurrent": "1",
        "iswatched": "false",
        "organizername": "XX 公棚",
        "organizerphone": "",
        "bidtime": "1760920000",
        "viewcount": "12",
    }

    obj = PigeonInfo.from_dict(raw)

    # 类型与清洗结果
    assert obj.id == 1001
    assert obj.auction_id == 987
    assert obj.margin_ratio == 0.1
    assert obj.start_price == 2000.0
    assert obj.is_current is True
    assert obj.is_watched is False
    assert obj.start_time == 1760918400
    assert obj.end_time == 1761004800
    assert obj.bid_time == 1760920000
    assert obj.organizer_phone is None
    assert obj.organizer_name == "XX 公棚"
    assert obj.view_count == 12

    # 输出转换
    d = obj.to_dict(drop_none=True)
    assert "organizer_phone" not in d
    j = obj.to_json(drop_none=True)
    assert '"organizer_phone"' not in j


def test_pigeoninfo_end_before_start():
    """❌ 结束时间早于开始时间应触发校验错误"""
    raw = {"id": "1002", "code": "X", "auctionid": "1", "name": "bad",
           "starttime": "1761004800", "endtime": "1760918400"}
    with pytest.raises(ValueError):
        PigeonInfo.from_dict(raw)


def test_pigeoninfo_negative_price_and_ratio_out_of_range():
    """❌ start_price<0 和 margin_ratio 超出 [0,1] 均应报错"""
    with pytest.raises(ValueError):
        PigeonInfo.from_dict({"id": "1", "code": "x", "auctionid": "1",
                              "name": "test", "startprice": "-5"})
    with pytest.raises(ValueError):
        PigeonInfo.from_dict({"id": "2", "code": "x", "auctionid": "1",
                              "name": "test", "marginratio": "2"})


def test_pigeoninfo_boolean_parsing():
    """✅ 布尔解析"""
    rows = [
        {"id": "1", "code": "A", "auctionid": "1", "name": "A", "iscurrent": "yes", "iswatched": "no"},
        {"id": "2", "code": "B", "auctionid": "1", "name": "B", "iscurrent": "true", "iswatched": "0"},
        {"id": "3", "code": "C", "auctionid": "1", "name": "C", "iscurrent": "n/a"},
    ]
    objs = PigeonInfo.from_list(rows)
    assert [o.is_current for o in objs] == [True, True, None]
    assert [o.is_watched for o in objs] == [False, False, None]


def test_pigeoninfo_missing_required_fields():
    """❌ 缺少必填字段（code、name）应直接报错"""
    with pytest.raises(TypeError):
        PigeonInfo.from_dict({"id": "1005", "auctionid": "1"})