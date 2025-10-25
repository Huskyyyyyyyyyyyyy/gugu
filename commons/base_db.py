import mysql.connector
from mysql.connector import pooling, Error
from commons.base_logger import BaseLogger
from contextlib import contextmanager
from tools.config_loader import load_config
from tools.retry_on_exception import retry_on_exception


class BaseDB:
    """
    数据库连接基类，提供连接池支持、日志记录、连接管理和上下文管理功能
    """

    _instance = None  # 单例实例变量
    _lock = False     # 用于线程安全控制（若将来使用线程安全机制时使用）


    def __new__(cls, *args, **kwargs):
        # 实现单例模式：仅创建一个实例
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, **kwargs):
        config_path: str = r"config\db_config.yaml"
        config_section: str = "mysqlconfig"
        # 初始化只执行一次（单例下防止重复执行）
        if not hasattr(self, 'initialized'):
            # 初始化日志记录器
            self.logger = BaseLogger(name='DB', to_file=True)

            # 设置数据库连接参数
        # if kwargs is not None:
        #     self.host = kwargs.pop("host")
        #     self.port = kwargs.pop("port")
        #     self.user = kwargs.pop("user")
        #     self.password = kwargs.pop("password")
        #     self.database = kwargs.pop("database")
        #     self.pool_size = kwargs.pop("pool_size")

            cfg = load_config(config_section, config_path)
            self.host = cfg.get('host')
            self.port = cfg.get('port')
            self.user = cfg.get('user')
            self.password = cfg.get('password')
            self.database = cfg.get('database')
            self.pool_size = cfg.get('pool_size')

            # 初始化连接状态
            self.connection_pool = None         # 连接池对象
            self._connection = None             # 单个连接（非连接池模式下使用）
            self.use_pool = True                # 是否启用连接池

            # 创建连接池
            self._initialize_connection_pool()

            # 标记为已初始化，防止重复 init
            self.initialized = True

    def _initialize_connection_pool(self):
        """
        尝试创建连接池，失败则退回到普通连接模式
        """
        try:
            # 创建 MySQL 连接池
            self.connection_pool = pooling.MySQLConnectionPool(
                pool_name="mypool",
                pool_size=self.pool_size,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.logger.log_info('成功创建数据库连接池')
            self.use_pool = True  # 启用连接池模式
        except Error as e:
            # 连接池创建失败，记录错误并尝试使用普通连接
            self.logger.log_error(f'连接池创建失败，降级为普通连接: {e}')
            self.use_pool = False
            self.connection_pool = None

    def _connect_direct(self):
        """
        创建一个普通数据库连接（不使用连接池）
        """
        try:
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                autocommit=True
            )
            self.logger.log_info('成功创建普通数据库连接')
            return conn
        except Error as e:
            self.logger.log_error(f'普通数据库连接失败: {e}', exc_info=True)
            return None

    def get_connection(self):
        """
        获取数据库连接，根据是否启用连接池决定方式
        """
        try:
            if self.use_pool and self.connection_pool:
                # 从连接池中获取连接
                conn = self.connection_pool.get_connection()
                self.logger.log_info('成功从连接池获取数据库连接')
                return conn
            else:
                # 使用普通连接
                return self._connect_direct()
        except Error as e:
            self.logger.log_error(f'获取数据库连接失败: {e}', exc_info=True)
            return None

    def get_cached_connection(self):
        """
        获取缓存连接（懒加载），避免重复创建
        """
        if self._connection is None or not self._connection.is_connected():
            self._connection = self.get_connection()
        return self._connection

    def close_connection(self, conn):
        """
        安全关闭数据库连接，支持连接池连接和普通连接
        """
        if conn:
            try:
                # 如果连接来自连接池，使用 pool_release() 释放回池
                if hasattr(conn, 'pool_release'):
                    conn.pool_release()
                else:
                    conn.close()
                self.logger.log_info('数据库连接已成功关闭')
            except Error as e:
                self.logger.log_error(f'关闭数据库连接时发生错误: {e}', exc_info=True)
    @retry_on_exception()
    @contextmanager
    def connection_ctx(self):
        """
        上下文管理器：自动获取并释放数据库连接
        用法：
            with db.connection_ctx() as conn:
                cursor = conn.cursor()
                ...
        """
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.close_connection(conn)
