# ────────────────────────────────────────────────────────────────
# 模块用途：启动 FastAPI SSE 服务器，推送拍卖实时数据
# 说明：
#   - 不再主动触发爬虫，只监听总线 bus；
#   - flow 层每次抓完数据后调用 bus.publish()；
#   - SSE 客户端接入后立即推送最新快照，再持续等待更新；
#   - 自动心跳保持连接；
#   - 任何异常都返回 event:error JSON 事件，不再出现“非 JSON”错误。
# ────────────────────────────────────────────────────────────────

from __future__ import annotations
import asyncio
import json
import os
from decimal import Decimal
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# —— 项目内导入 —— #
from pigeon_socket.bus import bus
from pigeon_socket.adapters.bidrecord_payload import error_payload

# 可通过环境变量调整心跳间隔（毫秒）
DEFAULT_INTERVAL_MS = int(os.getenv("PIGEON_SSE_INTERVAL_MS", "500"))

# ────────────────────────────────────────────────────────────────
# JSON 序列化兜底函数：处理 Decimal / datetime / 其他类型
# ────────────────────────────────────────────────────────────────
def _json_default(o):
    if isinstance(o, Decimal):
        return float(o)
    try:
        import datetime
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()
    except Exception:
        pass
    return str(o)  # 最终兜底：转字符串，永不抛错


# ────────────────────────────────────────────────────────────────
# 构建 FastAPI 实例
# ────────────────────────────────────────────────────────────────
def build_sse_app() -> FastAPI:
    app = FastAPI(title="Pigeon SSE Server", version="1.0.0")

    # 允许跨域（前端在不同端口时必须加）
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    @app.get("/healthz")
    async def _h():
        """健康检查：用于存活探测"""
        return {"ok": True}

    @app.post("/api/trigger")
    async def _t():
        """
        调试接口：返回当前快照。
        不触发抓取，仅用于前端排查。
        """
        snap = bus.peek()
        if snap is None:
            return JSONResponse(error_payload("no snapshot yet"), status_code=503)
        return JSONResponse(snap)

    @app.get("/sse/pigeon")
    async def _sse(request: Request):
        """
        SSE 主入口：
          - 首次连接 → 立即推送当前快照；
          - 后续等待 bus 更新 → 推送；
          - 超时则发 keep-alive。
        """
        try:
            q = request.query_params
            try:
                interval_ms = int(q.get("interval_ms", DEFAULT_INTERVAL_MS))
            except ValueError:
                interval_ms = DEFAULT_INTERVAL_MS
            interval_ms = max(50, interval_ms)

            async def gen():
                # 1️⃣ 初次推送当前快照
                snap = bus.peek()
                if snap is not None:
                    yield f"event: bids\ndata: {json.dumps(snap, ensure_ascii=False, default=_json_default)}\n\n".encode("utf-8")

                # 2️⃣ 等待更新循环
                while True:
                    if await request.is_disconnected():
                        break

                    snap = await bus.wait_update(timeout=15.0)
                    if snap is None:
                        # 发送心跳保持连接
                        yield b": keep-alive\n\n"
                        continue

                    yield f"event: bids\ndata: {json.dumps(snap, ensure_ascii=False, default=_json_default)}\n\n".encode("utf-8")

            headers = {
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Content-Encoding": "identity",
            }
            return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)

        except Exception as e:
            # ❗任意异常 → 返回单次 error 事件（JSON）
            def one_error():
                err = error_payload(f"setup error: {e}")
                yield f"event: error\ndata: {json.dumps(err, ensure_ascii=False, default=_json_default)}\n\n".encode("utf-8")

            headers = {
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Content-Encoding": "identity",
            }
            return StreamingResponse(one_error(), media_type="text/event-stream", headers=headers)

    return app


# ────────────────────────────────────────────────────────────────
# 启动与停止：可供主程序 trigger_main 调用
# ────────────────────────────────────────────────────────────────
import uvicorn
from dataclasses import dataclass

@dataclass
class SseServerHandle:
    server: uvicorn.Server
    task: asyncio.Task


async def start_sse_background(host: str = "0.0.0.0", port: int = 8000) -> SseServerHandle:
    """
    后台启动 SSE 服务。
    - 不阻塞主线程；
    - 自动捕获端口占用；
    - 返回句柄，供主程序 stop。
    """
    app = build_sse_app()
    config = uvicorn.Config(app=app, host=host, port=port, loop="asyncio", log_level="info")
    server = uvicorn.Server(config)

    async def _serve():
        try:
            await server.serve()
        except SystemExit:
            print(f"[SSE] Port {port} already in use, please change or stop other process.")
        except Exception as e:
            print(f"[SSE] Exception: {e}")

    task = asyncio.create_task(_serve(), name=f"pigeon-sse:{port}")
    await asyncio.sleep(0.1)
    print(f"[SSE] Server started on http://{host}:{port}/sse/pigeon")
    return SseServerHandle(server, task)


async def stop_sse_background(handle: SseServerHandle):
    """优雅关闭 SSE 服务"""
    if not handle:
        return
    handle.server.should_exit = True
    handle.task.cancel()
    with asyncio.suppress(asyncio.CancelledError):
        await handle.task
