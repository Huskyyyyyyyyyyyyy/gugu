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
from contextlib import suppress
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles

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

    # ========== Static files mounting ==========
    # 前端目录挂到 /static
    static_dir = Path(r"D:\pareedo\gugu\gugu2\front")
    if not static_dir.exists():
        # 如果路径不存在，打印提示（便于调试）
        print(f"[SSE] static dir not found: {static_dir}")
    else:
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
        print(f"[SSE] static files served at /static from {static_dir}")

    # 根路径重定向到 index.html（方便在浏览器直接访问 http://<host>:<port>/ ）
    @app.get("/")
    async def _index():
        # 如果没有 index.html，则返回一个简单提示 JSON
        index_file = static_dir / "index.html"
        if index_file.exists():
            return RedirectResponse(url="/static/index.html")
        return {"msg": "static index not found, visit /static to list files."}
    # ===========================================

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
        snap = bus.peek()
        if snap is None:
            return JSONResponse(error_payload("no snapshot yet"), status_code=503)
        return JSONResponse(snap)

    @app.get("/sse/pigeon")
    async def _sse(request: Request):
        try:
            q = request.query_params
            try:
                interval_ms = int(q.get("interval_ms", DEFAULT_INTERVAL_MS))
            except ValueError:
                interval_ms = DEFAULT_INTERVAL_MS
            interval_ms = max(50, interval_ms)

            async def gen():
                snap = bus.peek()
                if snap is not None:
                    yield f"event: bids\ndata: {json.dumps(snap, ensure_ascii=False, default=_json_default)}\n\n".encode("utf-8")

                while True:
                    if await request.is_disconnected():
                        break

                    snap = await bus.wait_update(timeout=15.0)
                    if snap is None:
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
    ip = "127.0.0.1" if host in ("0.0.0.0", "localhost") else host
    print(f"[SSE] SSE server started: http://{ip}:{port}/sse/pigeon")
    print(f"[SSE] Frontend address:  http://{ip}:{port}/static/index.html")

    return SseServerHandle(server, task)


async def stop_sse_background(handle):
    """关闭 SSE 服务"""
    if not handle:
        return
    handle.server.should_exit = True
    handle.task.cancel()
    with suppress(asyncio.CancelledError):
        await handle.task
