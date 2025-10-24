# run_current.py
import asyncio
import contextlib
import json
import signal
from datetime import UTC, datetime
from pprint import pprint

from crawlers.pids_crawler import PidsPigeonsCrawler
from sniffer.ws_trigger import Trigger, run_browser, QUEUE_CAP, TRIGGER_TEXT
from crawlers.current_crawler import CurrentPigeonsCrawler
from flows.flows import my_flow


async def cold_start() -> None:
    """程序启动即执行一次：current -> id -> bids"""
    def _do():
        with CurrentPigeonsCrawler() as crawler:
            pid = crawler.get_current_pigeon_id()
            if pid is None:
                print("[Startup] 未获取到当前鸽子 ID，后续等待触发器事件。")
                return
            with PidsPigeonsCrawler() as crawler:
                pids = crawler.run_crawl(pid)
                pprint(pids)


    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _do)



async def main():
    stop_evt = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _stop(*_):
        stop_evt.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _stop)

    # 1) 先跑一遍（解决“收不到第一个 id”的问题）
    await cold_start()

    # 2) 再启动触发器：后续都按 topic 的 pid 处理
    trigger = Trigger(queue_cap=QUEUE_CAP, trigger_text=TRIGGER_TEXT)
    trigger.on(my_flow)  # flows.py 不改

    await trigger.start(n_workers=4)
    try:
        await run_browser(trigger, stop_evt)
    finally:
        await trigger.stop()
        print(json.dumps({"msg": "bye", "ts": datetime.now(UTC).isoformat()}, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
