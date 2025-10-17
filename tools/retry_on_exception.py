import time
import functools
import mysql.connector

def retry_on_exception(retries=3, delay=1, exceptions=(mysql.connector.Error,)):
    """
    通用重试装饰器：遇到指定异常时，自动重试
    :param retries: 最大重试次数
    :param delay: 每次重试间隔（秒）
    :param exceptions: 哪些异常触发重试
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    print(f"[第{attempt}次重试] {func.__name__} 出现异常: {e}")
                    if attempt == retries:
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator
