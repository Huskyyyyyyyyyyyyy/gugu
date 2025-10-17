import pytest
from gugu2.dao.gongpeng_dao import GongpengDAO
from gugu2.mydataclass.gongpeng import GongpengInfo
import time

@pytest.fixture(scope="module")
def db_config():
    return {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'gugu_test'
    }

@pytest.fixture(scope="module")
def dao(db_config):
    dao = GongpengDAO(db_config)
    dao.ensure_table()
    yield dao
    # 测试结束后可加清理代码

def test_crud_operations(dao):
    test_id = 99999

    # 1. 插入
    info = GongpengInfo(
        id=test_id,
        name="测试拍卖",
        organizername="组织者A",
        organizerphone="123456789",
        customerservicephone="987654321",
        starttime=int(time.time()),
        endtime=int(time.time()) + 3600,
        statusname="准备中",
        livestatusname="未开始",
        pigeonname="和平鸽",
        pigeoncode="PG123456",
        # pigeonimage=[]  # 空列表，符合调整后定义
    )
    dao.insert_gongpeng(info)

    # 2. 查询
    got = dao.get_by_id(test_id)
    assert got is not None
    assert got.id == info.id
    assert got.name == info.name

    # 3. 更新
    info.name = "测试拍卖_更新"
    dao.update_gongpeng(info)
    got2 = dao.get_by_id(test_id)
    assert got2.name == "测试拍卖_更新"

    # 4. 删除
    dao.delete_by_id(test_id)
    got3 = dao.get_by_id(test_id)
    assert got3 is None
