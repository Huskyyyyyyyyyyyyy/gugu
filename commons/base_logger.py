import inspect
import logging
import os
from logging.handlers import TimedRotatingFileHandler


class BaseLogger:
    """
    基础日志类：
    - 控制台 + 按天轮转文件输出
    - 自动推断调用类名作为 logger 名
    - 统一格式化输出（含时间、文件名、函数、线程）
    """

    def __init__(
        self,
        name: str | None = None,
        level: int = logging.INFO,
        to_file: bool = False,
        file_path: str | None = None,
        file_level: int = logging.ERROR,
    ):
        """
        初始化日志系统。

        :param name: logger 名称（默认取调用者类名）
        :param level: 控制台日志级别（默认 INFO）
        :param to_file: 是否启用文件日志
        :param file_path: 日志文件路径（可选）
        :param file_level: 文件日志的最低级别（默认 ERROR，仅错误写入）
        """
        # 自动确定 logger 名称
        if name is None:
            name = self._get_caller_class_name() or self.__class__.__name__

        # 创建 logger 实例
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.propagate = False  # 防止重复输出

        # 若尚未配置 handler，防止重复添加
        if not self.logger.handlers:
            # 通用格式
            formatter = logging.Formatter(
                "%(asctime)s | %(name)s | %(levelname)s | "
                "[%(filename)s:%(lineno)d %(funcName)s] | %(threadName)s | %(message)s"
            )

            # 控制台 handler
            ch = logging.StreamHandler()
            ch.setLevel(level)
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

            # 文件 handler（可选）
            if to_file:
                # 若未指定路径，默认 logs/xxx.log
                if file_path is None:
                    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    log_dir = os.path.join(project_root, "logs")
                    os.makedirs(log_dir, exist_ok=True)
                    file_path = os.path.join(log_dir, f"{self.logger.name}.log")

                fh = TimedRotatingFileHandler(
                    filename=file_path,
                    when="midnight",  # 每天轮转
                    interval=1,
                    backupCount=7,  # 保留 7 天
                    encoding="utf-8",
                )
                fh.setLevel(file_level)
                fh.setFormatter(formatter)
                self.logger.addHandler(fh)

    # ------------------ 内部方法 ------------------

    def _get_caller_class_name(self) -> str | None:
        """
        获取调用者类名（跳过 BaseLogger 自身）。
        例如：MySpider(BaseLogger) -> 自动命名为 'MySpider'
        """
        for frame_record in inspect.stack():
            instance = frame_record.frame.f_locals.get("self")
            if instance and instance.__class__ != self.__class__:
                return instance.__class__.__name__
        return None

    # ------------------ 对外日志接口 ------------------

    def log_info(self, message: str, exc_info: bool = False):
        """记录 INFO 日志"""
        self.logger.info(message, exc_info=exc_info)

    def log_warning(self, message: str, exc_info: bool = False):
        """记录 WARNING 日志"""
        self.logger.warning(message, exc_info=exc_info)

    def log_error(self, message: str, exc_info: bool = True):
        """记录 ERROR 日志（默认包含异常堆栈）"""
        self.logger.error(message, exc_info=exc_info)

    def log_debug(self, message: str, exc_info: bool = False):
        """记录 DEBUG 日志"""
        self.logger.debug(message, exc_info=exc_info)
# class LogMixin:
#     """让任意类都能拥有 log_info / log_error 方法"""
#     def __init__(self, *args, **kwargs):
#         self.logger = BaseLogger(name=self.__class__.__name__, to_file=True)
#         super().__init__(*args, **kwargs)
#
#     def log_info(self, msg): self.logger.log_info(msg)
#     def log_error(self, msg): self.logger.log_error(msg)
#     def log_warning(self, msg): self.logger.log_warning(msg)
#     def log_debug(self, msg): self.logger.log_debug(msg)