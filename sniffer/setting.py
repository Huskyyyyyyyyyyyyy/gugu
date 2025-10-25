#  配置 / Settings
from tools.config_loader import load_config
import os

TARGET_URL   = load_config("TARGET_URL",r"config\spider.yaml")
HEADLESS     = os.getenv("HEADLESS", "True").lower() == "true"        # True/False
BROWSER      = os.getenv("BROWSER", "edge").lower()                   # edge | chrome | chromium
QUEUE_CAP    = int(os.getenv("QUEUE_CAP", "1024"))                    # 入队上限（满则丢最旧）
TRIGGER_TEXT = os.getenv("TRIGGER_TEXT", "True").lower() == "true"    # 文本消息是否也触发
MIN_BIN_LEN  = int(os.getenv("MIN_BIN_LEN", "10"))                    # 过滤长度 < MIN_BIN_LEN 的二进制消息

