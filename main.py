import asyncio
import os
import secrets
import socket
import time
import httpx
import logging
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("X4G")

app = FastAPI(title="X4G Lite - XHTTP Only", docs_url=None, redoc_url=None)

PORT = int(os.environ.get("PORT", 8000))
UUID = os.environ.get("VLESS_UUID") or secrets.token_urlsafe(16)

def get_host() -> str:
    return os.environ.get("RAILWAY_PUBLIC_DOMAIN") or os.environ.get("RAILWAY_STATIC_URL") or "localhost"

HOST = get_host()

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
    global http_client, _api_client, _bot_running, _poll_task, HOST
    HOST = get_host()
    if not BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN is required but not set. Exiting.")
        raise RuntimeError("TELEGRAM_BOT_TOKEN not set")
    if not ADMIN_IDS:
        logger.error("TELEGRAM_ADMIN_IDS is required but not set. Exiting.")
        raise RuntimeError("TELEGRAM_ADMIN_IDS not set")
    http_client = httpx.AsyncClient(limits=httpx.Limits(max_connections=500), timeout=30.0)
    _api_client = httpx.AsyncClient(timeout=40.0)
    _bot_running = True
    _poll_task = asyncio.create_task(_poll_loop())
    logger.info(f"X4G Lite (XHTTP Only) started, UUID={UUID}, host={HOST}")

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

RELAY_BUF = 256 * 1024
TCP_CONNECT_TIMEOUT = 10.0

def _ws_client_ip(ws: WebSocket) -> str:
    fwd = ws.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    real_ip = ws.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return ws.client.host if ws.client else "نامشخص"

def _tune_socket(writer):
    sock = writer.transport.get_extra_info("socket")
    if not sock:
        return
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2 * 1024 * 1024)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2 * 1024 * 1024)
    except OSError:
        pass

async def parse_vless_header(chunk: bytes):
    if len(chunk) < 24:
        raise ValueError("chunk too small")
    pos = 1
    pos += 16
    addon_len = chunk[pos]
    pos += 1 + addon_len
    command = chunk[pos]
    pos += 1
    port = int.from_bytes(chunk[pos:pos+2], "big")
    pos += 2
    addr_type = chunk[pos]
    pos += 1
    if addr_type == 1:
        address = ".".join(str(b) for b in chunk[pos:pos+4])
        pos += 4
    elif addr_type == 2:
        dlen = chunk[pos]
        pos += 1
        address = chunk[pos:pos+dlen].decode("utf-8", errors="ignore")
        pos += dlen
    elif addr_type == 3:
        ab = chunk[pos:pos+16]
        pos += 16
        address = ":".join(f"{ab[i]:02x}{ab[i+1]:02x}" for i in range(0, 16, 2))
    else:
        raise ValueError(f"unknown addr type: {addr_type}")
    return command, address, port, chunk[pos:]

async def relay_ws_to_tcp(ws, writer, conn_id):
    try:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                break
            data = msg.get("bytes") or (msg.get("text") or "").encode()
            if not data:
                continue
            stats["total_requests"] += 1
            stats["total_bytes"] += len(data)
            if conn_id in connections:
                connections[conn_id]["bytes"] += len(data)
            writer.write(data)
            if writer.transport.get_write_buffer_size() > RELAY_BUF:
                await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.write_eof()
        except Exception:
            pass

async def relay_tcp_to_ws(ws, reader, conn_id):
    first = True
    try:
        while True:
            data = await reader.read(RELAY_BUF)
            if not data:
                break
            stats["total_requests"] += 1
            stats["total_bytes"] += len(data)
            if conn_id in connections:
                connections[conn_id]["bytes"] += len(data)
            payload = (b"\x00\x00" + data) if first else data
            first = False
            await ws.send_bytes(payload)
    except Exception:
        pass

@app.websocket("/ws/{uuid}")
async def websocket_tunnel(ws: WebSocket, uuid: str):
    if uuid != UUID:
        await ws.close(code=1008, reason="invalid uuid")
        return
    await ws.accept()
    ip = _ws_client_ip(ws)
    conn_id = secrets.token_urlsafe(6)
    connections[conn_id] = {
        "ip": ip,
        "uuid": uuid,
        "connected_at": time.time(),
        "bytes": 0
    }
    logger.info(f"WS connected {conn_id} from {ip}")
    writer = None
    try:
        first_msg = await asyncio.wait_for(ws.receive(), timeout=15.0)
        if first_msg["type"] == "websocket.disconnect":
            return
        first_chunk = first_msg.get("bytes") or (first_msg.get("text") or "").encode()
        if not first_chunk:
            return
        command, address, port, payload = await parse_vless_header(first_chunk)
        stats["total_requests"] += 1
        stats["total_bytes"] += len(first_chunk)
        connections[conn_id]["bytes"] += len(first_chunk)
        logger.info(f"WS {conn_id} -> {address}:{port}")
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(address, port),
            timeout=TCP_CONNECT_TIMEOUT
        )
        _tune_socket(writer)
        if payload:
            writer.write(payload)
            await writer.drain()
        done, pending = await asyncio.wait(
            {
                asyncio.create_task(relay_ws_to_tcp(ws, writer, conn_id)),
                asyncio.create_task(relay_tcp_to_ws(ws, reader, conn_id))
            },
            return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
    except WebSocketDisconnect:
        pass
    except asyncio.TimeoutError:
        logger.warning(f"WS {conn_id} timeout")
    except Exception as exc:
        logger.error(f"WS {conn_id} error: {exc}")
    finally:
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        connections.pop(conn_id, None)
        logger.info(f"WS {conn_id} closed")

from xhttp_siz10 import router as xhttp_router
app.include_router(xhttp_router)

# ربات تلگرام (دقیقاً مثل فایل اصلی)
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
        await _send(chat_id, "🤖 X4G Lite\n/config → نمایش لینک اتصال\n/stats → آمار مصرف")
    elif text == "/config":
        link = f"XHTTP://{UUID}@{HOST}:443?type=xhttp-siz10&mode=stream-up&host={HOST}&fp=chrome#X4G-XHTTP"
        await _send(chat_id, f"🔗 لینک اتصال:\n<code>{link}</code>")
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
        await _send(chat_id, "دستور نامعتبر.")

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

@app.get("/xhttp")
async def xhttp_link():
    link = f"XHTTP://{UUID}@{HOST}:443?type=xhttp-siz10&mode=stream-up&host={HOST}&fp=chrome#X4G-XHTTP"
    return {"link": link}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info", workers=1)
