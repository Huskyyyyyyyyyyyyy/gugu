import json
import pytest

from mydataclass.gongpeng import GongpengInfo


def test_gongpeng_from_dict_ms_and_s_and_empty():
    raw = {
        "id": "12345",
        "name": "2025 秋季精品赛鸽专场",
        "organizername": "XX 公棚",
        "organizerphone": "400-123-4567",
        "customerservicephone": "",         # -> None
        "starttime": 1760918400000,         # 毫秒 -> 秒
        "endtime": "1761004800",            # 秒（字符串）
        "statusname": "预展中",
        "livestatusname": "未开始",
        "extra": "ignored",                 # 未知字段应忽略
    }
    obj = GongpengInfo.from_dict(raw)
    assert obj.id == 12345
    assert obj.name == "2025 秋季精品赛鸽专场"
    assert obj.customerservicephone is None
    assert obj.starttime == 1760918400
    assert obj.endtime == 1761004800
    assert obj.statusname == "预展中"
    assert obj.livestatusname == "未开始"


def test_gongpeng_end_before_start_raises():
    raw = {"id": "1", "name": "X", "starttime": "1761004800", "endtime": "1760918400"}
    with pytest.raises(ValueError):
        GongpengInfo.from_dict(raw)


def test_gongpeng_missing_required_fields():
    # 缺少 name
    with pytest.raises(TypeError):
        GongpengInfo.from_dict({"id": "555"})

    # id 非法导致 None -> 构造缺必填
    with pytest.raises(TypeError):
        GongpengInfo.from_dict({"id": "not-int", "name": "Y"}, strict=True)


def test_gongpeng_from_json_top_level_list():
    payload = json.dumps([
        {"id": "99", "name": "第一项", "starttime": 1760918400000, "endtime": 1761004800000},
        {"id": "100", "name": "第二项"},
    ], ensure_ascii=False)
    obj = GongpengInfo.from_json(payload)
    assert obj.id == 99
    assert obj.starttime == 1760918400
    assert obj.endtime == 1761004800


def test_gongpeng_from_list_generator_and_list():
    rows = [
        {"id": "1", "name": "A"},
        {"id": "2", "name": "B"},
    ]
    # 生成器模式
    gen = GongpengInfo.from_list(rows, yield_items=True)
    got = list(gen)
    assert [x.id for x in got] == [1, 2]

    # 直接列表
    got2 = GongpengInfo.from_list(rows)
    assert [x.name for x in got2] == ["A", "B"]


def test_gongpeng_to_dict_and_to_json_drop_none():
    obj = GongpengInfo.from_dict({"id": "88", "name": "Z"})
    d_all = obj.to_dict()
    d_clean = obj.to_dict(drop_none=True)
    assert "organizername" in d_all and "organizername" not in d_clean  # None 被剔除

    j = obj.to_json(ensure_ascii=False, drop_none=True)
    assert '"organizername"' not in j
