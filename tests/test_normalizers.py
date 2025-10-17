import pytest

from commons.normalizers import empty_to_none, to_int_or_none, ts_to_seconds, ensure_end_ge_start


def test_empty_to_none():
    assert empty_to_none("") is None
    assert empty_to_none("   ") is None
    assert empty_to_none("x") == "x"
    assert empty_to_none(0) == 0
    assert empty_to_none(None) is None  # 保持 None


@pytest.mark.parametrize("value,expected", [
    ("123", 123),
    (123.0, 123),
    ("0010", 10),
    ("", None),
    ("  ", None),
    (None, None),
    ("abc", None),
])
def test_to_int_or_none(value, expected):
    assert to_int_or_none(value) == expected


@pytest.mark.parametrize("value,expected", [
    (1_760_918_400, 1_760_918_400),                 # 秒
    ("1760918400", 1_760_918_400),                  # 秒字符串
    (1_760_918_400_000, 1_760_918_400),             # 毫秒
    ("1760918400000", 1_760_918_400),               # 毫秒字符串
    ("", None),
    (None, None),
    ("abc", None),
])
def test_ts_to_seconds(value, expected):
    assert ts_to_seconds(value) == expected


def test_ensure_end_ge_start_ok():
    row = {"starttime": 100, "endtime": 200}
    ensure_end_ge_start(row)  # 不应抛异常


def test_ensure_end_ge_start_violates():
    row = {"starttime": 300, "endtime": 200}
    with pytest.raises(ValueError):
        ensure_end_ge_start(row)
