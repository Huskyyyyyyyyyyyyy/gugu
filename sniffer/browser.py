# ╔══════════════════════════════════════════════════╗
# ║                   浏览器集成                     ║
# ╚══════════════════════════════════════════════════╝
from __future__ import annotations
import json
from datetime import UTC, datetime

from setting import BROWSER, HEADLESS, TARGET_URL

HOOK_SCRIPT = r"""
(() => {
  if (window.__ws_in_hook) return;
  window.__ws_in_hook = true;
  const Orig = window.WebSocket;
  function b64(buf){ let s='',u=new Uint8Array(buf); for (let i=0;i<u.length;i++) s+=String.fromCharCode(u[i]); return btoa(s); }
  function PatchedWebSocket(url, protocols){
    const ws = protocols ? new Orig(url, protocols) : new Orig(url);
    ws.addEventListener('message', ev=>{
      try{
        if (typeof ev.data==='string') {
          window.py_on_ws && window.py_on_ws({url, kind:'text', data: ev.data});
        } else if (ev.data instanceof ArrayBuffer) {
          window.py_on_ws && window.py_on_ws({url, kind:'binary', data: b64(ev.data)});
        } else if (ev.data && ev.data.constructor && ev.data.constructor.name==='Blob'){
          const r=new FileReader();
          r.onload=()=>window.py_on_ws && window.py_on_ws({url, kind:'binary', data: b64(r.result)});
          r.readAsArrayBuffer(ev.data);
        }
      }catch(e){}
    });
    return ws;
  }
  PatchedWebSocket.prototype = Orig.prototype;
  Object.defineProperties(PatchedWebSocket, {
    CONNECTING: { get: () => Orig.CONNECTING },
    OPEN:       { get: () => Orig.OPEN },
    CLOSING:    { get: () => Orig.CLOSING },
    CLOSED:     { get: () => Orig.CLOSED },
  });
  const nativeToString = Function.prototype.toString;
  PatchedWebSocket.toString = nativeToString.bind(Orig);
  PatchedWebSocket.prototype.constructor = PatchedWebSocket;
  window.WebSocket = PatchedWebSocket;
})();
"""

async def run_browser(trigger, stop_evt) -> None:
    """
    启动浏览器，注入 Hook，把 JS 收到的 WS 消息转交给 Trigger。
    """
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        # 浏览器选择（含 fallback）
        try:
            if BROWSER == "edge":
                browser = await p.chromium.launch(headless=HEADLESS, channel="msedge")
            elif BROWSER == "chrome":
                browser = await p.chromium.launch(headless=HEADLESS, channel="chrome")
            else:
                raise ValueError
        except Exception:
            browser = await p.chromium.launch(headless=HEADLESS)

        context = await browser.new_context()
        await context.add_init_script(HOOK_SCRIPT)   # 最早注入
        page = await context.new_page()

        # JS → Python 桥：浏览器收到的 WS 消息推入 Trigger
        async def py_on_ws(m: dict):
            m["recv_ts"] = datetime.now(UTC).isoformat()
            await trigger.push_raw(m)

        await page.expose_function("py_on_ws", py_on_ws)

        print(json.dumps({"msg": "open", "url": TARGET_URL}, ensure_ascii=False))
        await page.goto(TARGET_URL, wait_until="domcontentloaded")

        try:
            while not stop_evt.is_set():
                await page.wait_for_timeout(500)
        finally:
            await browser.close()
