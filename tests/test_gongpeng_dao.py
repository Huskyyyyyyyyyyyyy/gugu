import pytest
from gugu2.dao.gongpeng_dao import GongpengDAO

@pytest.fixture(scope="module")
def db_config():
    # 你的测试数据库配置，改成你本地环境
    return {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'gugu_test'
    }

@pytest.fixture(scope="module")
def gongpeng_dao(db_config):
    dao = GongpengDAO(db_config)
    return dao

def test_ensure_table(gongpeng_dao):
    # 测试确保表存在，调用方法不抛异常
    try:
        gongpeng_dao.ensure_table()
    except Exception as e:
        pytest.fail(f"ensure_table 抛出了异常: {e}")

    # 可选：简单验证表是否存在
    with gongpeng_dao.db.connection_ctx() as conn:
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES LIKE 'gongpeng_info';")
        result = cursor.fetchone()
        cursor.close()
        assert result is not None, "gongpeng_info 表不存在"
