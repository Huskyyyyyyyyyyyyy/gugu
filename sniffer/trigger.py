
#     触发器主类 Trigger
from __future__ import annotations
import asyncio
from typing import Awaitable, Callable, List

from drop_head_queue import DropHeadQueue
from event_factory import EventFactory
from handler_bus import HandlerBus
from models import Event
from setting import QUEUE_CAP, TRIGGER_TEXT, MIN_BIN_LEN

# 事件处理函数签名：接受一个 Event，异步执行，不返回值
Handler = Callable[[Event], Awaitable[None]]

class Trigger:
    """
    WebSocket 触发器：
      - 对外暴露推送原始消息的入口（push_raw），由浏览器回调/JS 调用
      - 内部使用 DropHeadQueue 做异步缓冲，解决入队速度 > 消费速度时的背压问题
      - 启动多个 worker 并发消费；每个 worker：
          1) 从队列取出原始消息
          2) 用 EventFactory 将原始消息转成业务事件（Event）
          3) 通过 HandlerBus 将事件广播给已注册的 handler
      - 支持停止（stop）：向队列投递哨兵对象使 worker 结束循环

         该类不关心网络层/WS 本身，只负责“消息入队 → 事件产出 → 分发”这一链路。
    """

    def __init__(self,
                 queue_cap: int = QUEUE_CAP,
                 trigger_text: bool = TRIGGER_TEXT,
                 min_bin_len: int = MIN_BIN_LEN):
        """
        参数：
          queue_cap     : 队列容量上限，超出将触发 DropHeadQueue 的“丢头”策略
          trigger_text  : 传入 EventFactory，用于控制是否对文本类消息触发事件
          min_bin_len   : 传入 EventFactory，用于二进制消息最小长度阈值（过滤心跳/噪声）
        """
        # 用于削峰填谷的有界队列，丢弃最早元素以保证最新消息可进入（避免积压）
        self._queue = DropHeadQueue(queue_cap)

        # 事件总线：负责维护 handler 列表并在 emit 时逐一调用
        self._bus = HandlerBus()

        # 事件工厂：从“WS 原始消息 dict”生成“领域事件 Event”
        self._factory = EventFactory(trigger_text=trigger_text, min_bin_len=min_bin_len)

        # 保存所有消费者任务句柄，便于 stop 时统一收敛
        self._workers: List[asyncio.Task] = []

    # === 对外接口 ===

    def on(self, handler: Handler) -> None:
        """注册事件处理器（订阅者）。后续当有 Event 产出时将回调该处理器。"""
        self._bus.add(handler)

    async def start(self, n_workers: int = 2) -> None:
        """
        启动 n_workers 个异步消费者。
        每个 worker 循环从队列拉取消息，转换为 Event，最后经 HandlerBus 分发。
        """
        for _ in range(n_workers):
            # 创建并记录 worker 任务；不 await，使其后台运行
            self._workers.append(asyncio.create_task(self._worker()))

    async def stop(self) -> None:
        """
        停止所有 worker：
          - 为每个 worker 投递一个“哨兵对象”（_sentinel=True），通知退出
          - 等待所有 worker 任务结束（gather）
        说明：这里不强制等待队列清空；如需“处理完为止”，可先 await self._queue.join()
        """
        for _ in self._workers:
            # 逐个投递哨兵，让每个 worker 都能在下一次 get() 时感知到退出
            await self._queue.put({"_sentinel": True})
        # 回收所有任务；return_exceptions=True 保证任何一个任务失败也不影响回收过程
        await asyncio.gather(*self._workers, return_exceptions=True)

    async def push_raw(self, msg: dict) -> None:
        """
        供浏览器回调/JS 调用：推入一条“原始消息”（通常是从 WS 收到的结构化 dict）。
        入队可能因队列满而触发丢头策略（由 DropHeadQueue 决定）。
        """
        await self._queue.put(msg)

    # === 内部 ===

    async def _worker(self) -> None:
        """
        单个消费者的主循环：
          - 从队列取出一条消息
          - 若为哨兵，则退出循环、结束任务
          - 否则用 EventFactory 试图生成 Event（可能返回 None 表示忽略）
          - 若有 Event，交给 HandlerBus 广播给已注册的 handler
          - 最后对队列调用 task_done()，通知该消息处理完毕（配合 join 使用）
        """
        while True:
            m = await self._queue.get()
            try:
                # 收到停止哨兵 → 退出循环
                if m.get("_sentinel"):
                    return

                # 原始消息 → 领域事件；可能因过滤规则不满足而返回 None
                ev = self._factory.from_ws(m)
                if ev:
                    # 交给总线分发（可能并发调用多个 handler）
                    await self._bus.emit(ev)
            finally:
                # 无论是否成功处理，都标记该条队列元素已完成
                self._queue.task_done()
