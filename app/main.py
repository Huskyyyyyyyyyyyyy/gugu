from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import asyncio
import uvicorn
from tools.config_loader import load_config
from dao.pigeon_dao import PigeonDao


app = FastAPI()

# 模板目录
templates = Jinja2Templates(directory="app/templates")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """首页路由，返回渲染后的 index.html"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.websocket("/ws/data")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket 连接端点
    1. 接收连接
    2. 循环读取数据（这里用测试数据）
    3. 每3秒推送数据给客户端
    4. 处理断开连接异常
    """
    await websocket.accept()
    try:
        # 这里你加载了配置并实例化 DAO，但示例数据没用 DAO，后续可以改成真实查询
        mysqlconfig = load_config('mysqlconfig', 'config\\db_config.yaml')
        pigeon_dao = PigeonDao(**mysqlconfig)
        data = [{
            "foot_ring": "TEST123",
            "start_price": 100,
            "bid_count": 5,
            "bid_time": "2025-07-23 12:00:00",
            "feather_color": "灰",
            "matcher_name": "张三",
            "status_name": "在拍"
            }, {
            "foot_ring": "TEST123555",
            "start_price": 100,
            "bid_count": 5,
            "bid_time": "2025-07-23 12:00:00",
            "feather_color": "灰花",
            "matcher_name": "李四",
            "status_name": "结拍"
            }]

        idx = 0
        while True:
            await websocket.send_json({"data": [data[idx]]})
            print(f"发送数据给客户端: {data[idx]}")
            idx = (idx + 1) % len(data)  # 循环切换索引
            await asyncio.sleep(3)

    except WebSocketDisconnect:
        print("客户端断开连接")


if __name__ == "__main__":
    # 本地调试用localhost或127.0.0.1都行，端口8000
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
