import asyncio
import os
import secrets
import time
import httpx
import logging
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("X4G")

app = FastAPI(title="X4G Lite", docs_url=None, redoc_url=None)

UUID = os.environ.get("VLESS_UUID") or secrets.token_urlsafe(16)
PROTOCOLS = [p.strip() for p in os.environ.get("PROTOCOLS", "vless-ws,xhttp-packet-up").split(",") if p.strip()]
PORT = int(os.environ.get("PORT", 8000))
HOST = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "localhost")

stats = {"total_bytes": 0, "total_requests": 0, "start_time": time.time()}
connections = {}
http_client = None

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_IDS = {int(x) for x in os.environ.get("TELEGRAM_ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
_api_client = None
_bot_running = False
_poll_task = None

@app.on_event("startup")
async def startup():
    global http_client, _api_client, _bot_running, _poll_task
    http_client = httpx.AsyncClient(limits=httpx.Limits(max_connections=500), timeout=30.0)
    _api_client = httpx.AsyncClient(timeout=40.0)
    if BOT_TOKEN and ADMIN_IDS:
        _bot_running = True
        _poll_task = asyncio.create_task(_poll_loop())
        logger.info("Telegram bot started")
    logger.info(f"X4G Lite started, UUID={UUID}, protocols={PROTOCOLS}")

@app.on_event("shutdown")
async def shutdown():
    global http_client, _api_client, _bot_running, _poll_task
    _bot_running = False
    if _poll_task:
        _poll_task.cancel()
        try:
            await _poll_task
        except:
            pass
    if http_client:
        await http_client.aclose()
    if _api_client:
        await _api_client.aclose()

@app.get("/health")
async def health():
    return {"status": "ok", "connections": len(connections)}

@app.get("/stats")
async def get_stats():
    return {
        "total_bytes": stats["total_bytes"],
        "total_requests": stats["total_requests"],
        "connections": len(connections),
        "uptime_sec": int(time.time() - stats["start_time"])
    }

_HOP = {"connection","keep-alive","proxy-authenticate","proxy-authorization",
        "te","trailers","transfer-encoding","upgrade","content-encoding","content-length"}

@app.api_route("/proxy/{target_url:path}", methods=["GET","POST","PUT","DELETE","PATCH","HEAD","OPTIONS"])
async def http_proxy(target_url: str, request: Request):
    if not target_url.startswith("http"):
        target_url = "https://" + target_url
    try:
        body = await request.body()
        headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP and k.lower() != "host"}
        resp = await http_client.request(method=request.method, url=target_url, headers=headers, content=body)
        stats["total_bytes"] += len(resp.content)
        stats["total_requests"] += 1
        return Response(content=resp.content, status_code=resp.status_code,
                        headers={k: v for k, v in resp.headers.items() if k.lower() not in _HOP})
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Proxy error: {exc}")

from relay_vless import websocket_tunnel
app.add_api_websocket_route("/ws/{uuid}", websocket_tunnel)

from xhttp_siz10 import router as xhttp_router
app.include_router(xhttp_router)

async def _call(method, **params):
    if _api_client is None:
        return None
    try:
        r = await _api_client.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=params, timeout=40)
        data = r.json()
        if not data.get("ok"):
            logger.warning(f"Telegram API {method} failed: {data}")
        return data
    except Exception as e:
        logger.warning(f"Telegram API {method} error: {e}")
        return None

async def _send(chat_id, text):
    await _call("sendMessage", chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True)

def _is_admin(chat_id):
    return chat_id in ADMIN_IDS

async def _handle_message(msg):
    chat_id = msg.get("chat", {}).get("id")
    text = (msg.get("text") or "").strip()
    if chat_id is None or not _is_admin(chat_id):
        return
    if text in ("/start", "/help"):
        await _send(chat_id, "🤖 X4G Lite\n/config → نمایش لینک‌های اتصال\n/stats → آمار مصرف")
    elif text == "/config":
        lines = []
        for p in PROTOCOLS:
            if p == "vless-ws":
                lines.append(f"VLESS+WS:\nvless://{UUID}@{HOST}:443?encryption=none&security=tls&type=ws&host={HOST}&path=/ws/{UUID}&sni={HOST}&fp=chrome&alpn=http/1.1#X4G")
            elif p.startswith("xhttp-"):
                mode = p.replace("xhttp-", "")
                path = f"/xhttp-siz10/{mode}/{UUID}"
                lines.append(f"XHTTP ({mode}):\nvless://{UUID}@{HOST}:443?encryption=none&security=tls&type=xhttp&mode={mode}&host={HOST}&path={path}&sni={HOST}&fp=chrome&alpn=h2,http/1.1#X4G")
        if not lines:
            lines = ["هیچ پروتکلی فعال نیست."]
        await _send(chat_id, "\n\n".join(lines))
    elif text == "/stats":
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(f"http://localhost:{PORT}/stats")
                data = r.json()
            msg = (f"📊 آمار مصرف:\n"
                   f"کل ترافیک: {data['total_bytes']//1024//1024} MB\n"
                   f"تعداد درخواست‌ها: {data['total_requests']}\n"
                   f"اتصالات زنده: {data['connections']}\n"
                   f"آپ‌تایم: {data['uptime_sec']//3600}h {(data['uptime_sec']%3600)//60}m")
            await _send(chat_id, msg)
        except Exception:
            await _send(chat_id, "خطا در دریافت آمار.")
    else:
        await _send(chat_id, "دستور نامعتبر. از /config یا /stats استفاده کنید.")

async def _poll_loop():
    offset = 0
    while _bot_running:
        try:
            res = await _call("getUpdates", offset=offset, timeout=30, allowed_updates=["message"])
            if res and res.get("ok"):
                for upd in res.get("result", []):
                    offset = upd["update_id"] + 1
                    if "message" in upd:
                        await _handle_message(upd["message"])
            else:
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Poll loop error: {e}")
            await asyncio.sleep(3)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info", workers=1)
