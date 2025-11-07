# main.py
"""
=========================================
主程序入口
=========================================

功能说明：
  - 启动浏览器 (Playwright) 并注入 WebSocket Hook
  - 启动 Trigger（触发器），异步接收并解析 WebSocket 消息
  - 自动加载 flows 中的所有业务流程
  - 将触发的 Event 分发给对应的 flow 处理
  - 支持 Ctrl+C 优雅退出
"""

from __future__ import annotations
import asyncio
import contextlib
import json
import signal
from pigeon_socket.sse_runner import (
    start_sse_background,
    stop_sse_background,
)

# —— 基础模块导入 ——
from browser import run_browser         # 浏览器管理：启动 + 注入 JS Hook
from trigger import Trigger             # 核心触发器：处理 WS 消息队列
from models import Event                # 统一事件模型
from setting import QUEUE_CAP, TRIGGER_TEXT, MIN_BIN_LEN  # 配置参数


# —— flows 包自动加载所有业务流程并注册到路由中 ——
# flows/__init__.py 会调用 autoload_flows() 自动导入所有 flow 文件，
# 每个 flow 里通过 @on_topic(...) 注册自己的正则匹配和处理逻辑。
from sniffer.flows import topic_router, run_startup_hooks  # 中央路由器，负责分发事件到对应 flow


# ╔══════════════════════════════════════════════════╗
# ║            调试 handler：打印所有事件内容        ║
# ╚══════════════════════════════════════════════════╝
async def print_handler(ev: Event):
    """
    调试用 handler：打印所有事件内容（包含 ws_text / mqtt_publish 等）。
    在生产环境中可以移除或替换为日志系统。
    """
    print(json.dumps({
        "ts": ev.ts,                 # 时间戳
        "kind": ev.kind,             # 事件类型（mqtt_publish / binary / ws_text）
        "url": ev.url,               # 来源的 WebSocket URL
        "topic": ev.topic,           # MQTT Topic（若存在）
        "preview": ev.payload_preview,  # Payload 前 64 字符
        "length": ev.length          # 二进制包长度（若是 binary）
    }, ensure_ascii=False))


# ╔══════════════════════════════════════════════════╗
# ║                    主协程 main()                ║
# ╚══════════════════════════════════════════════════╝
async def main():
    """
    主入口：
      1. 设置退出信号 (SIGINT / SIGTERM)
      2. 创建并启动 Trigger 消费协程
      3. 启动浏览器并注入 WebSocket hook
      4. 等待触发器接收事件并自动分发给 flows
    """
    # —— 用于通知“停止运行”的事件对象（Ctrl+C 时触发）
    stop_evt = asyncio.Event()
    loop = asyncio.get_running_loop()

    # —— 信号回调函数：收到 SIGINT/SIGTERM 时设置 stop_evt
    def _stop(*_):
        stop_evt.set()

    # 注册信号监听，支持优雅关闭
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # Windows 下可能不支持
            loop.add_signal_handler(sig, _stop)

    # —— 初始化触发器 Trigger
    # queue_cap: 最大队列容量，超出则丢最旧；trigger_text: 是否触发文本消息
    trigger = Trigger(queue_cap=QUEUE_CAP, trigger_text=TRIGGER_TEXT, min_bin_len=MIN_BIN_LEN)

    # 注册两个 handler：
    #   1. print_handler：打印所有事件（调试用）
    #   2. topic_router：flows 路由器（负责将事件分发到具体业务 flow）
    # trigger.on(print_handler)
    trigger.on(topic_router)

    # 启动触发器的消费者协程（用于异步处理队列中的 WS 消息）
    await trigger.start(n_workers=4)
    #  冷启动：这里统一执行所有 flows 注册的 on_startup 钩子（包括 current→pid→bids）
    await run_startup_hooks()

    #  启动 SSE（后台协程，不阻塞主流程）
    sse_handle = await start_sse_background(host="0.0.0.0", port=8001)

    try:
        # —— 浏览器抓取主循环
        await run_browser(trigger, stop_evt)
    finally:
        # 关闭 SSE
        await stop_sse_background(sse_handle)
    try:
        # —— 启动浏览器并进入主循环
        # run_browser 内部：
        #   - 启动 Playwright 浏览器
        #   - 注入 JS Hook 脚本
        #   - 拦截所有 WebSocket “收到的消息”
        #   - 将消息回调到 Python 侧 (trigger.push_raw)
        await run_browser(trigger, stop_evt)

    finally:
        # —— 程序退出时：关闭触发器、结束消费者协程
        await trigger.stop()
        print(json.dumps({"msg": "bye"}, ensure_ascii=False))



if __name__ == "__main__":
    """
    同步入口点：
      - asyncio.run(main()) 启动事件循环
      - 捕获 KeyboardInterrupt，确保安全退出
    """
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
