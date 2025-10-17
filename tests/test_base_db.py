import pytest
from commons.base_db import BaseDB  # 假设你的 BaseDB 文件叫 base_db.py
import mysql.connector


# 使用 fixture 创建共享实例
@pytest.fixture(scope="module")
def db_instance():
    # 创建 BaseDB 实例
    db = BaseDB(
        host='localhost',
        port=3306,
        user='root',
        password='root',     # 替换为你本地的密码
        database='gugu_test'
    )
    return db


def test_connection_pool_created(db_instance):
    """测试是否成功创建连接池"""
    assert db_instance.connection_pool is not None or db_instance.use_pool is False


def test_get_connection(db_instance):
    """测试获取连接"""
    conn = db_instance.get_connection()
    # assert isinstance(conn, mysql.connector.connection.MySQLConnection)
    assert conn.is_connected()
    db_instance.close_connection(conn)


def test_get_cached_connection(db_instance):
    """测试缓存连接获取"""
    conn = db_instance.get_cached_connection()
    assert conn is not None
    assert conn.is_connected()


def test_connection_ctx(db_instance):
    """测试上下文管理器是否能成功打开和关闭连接"""
    with db_instance.connection_ctx() as conn:
        assert conn.is_connected()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
        cursor.close()


def test_close_connection_safe(db_instance):
    """测试关闭空连接是否报错"""
    db_instance.close_connection(None)  # 不应抛出异常

    # 模拟关闭未连接对象（构造假连接）
    class DummyConn:
        def close(self): raise mysql.connector.Error("关闭失败")
    db_instance.close_connection(DummyConn())  # 应该被 logger 捕捉
