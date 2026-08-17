import asyncio
import os
import secrets
import socket
import time
import httpx
import logging
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
import uvicorn

# ── راه‌اندازی لاگر ──────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("X4G")

# ── برنامه FastAPI ──────────────────────────────────────────────────────────
app = FastAPI(title="X4G Lite", docs_url=None, redoc_url=None)

# ── متغیرهای محیطی ──────────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", 8000))
UUID = os.environ.get("VLESS_UUID") or secrets.token_urlsafe(16)

def get_host() -> str:
    return os.environ.get("RAILWAY_PUBLIC_DOMAIN") or os.environ.get("RAILWAY_STATIC_URL") or "localhost"

HOST = get_host()

# ── آمار و اتصالات ──────────────────────────────────────────────────────────
stats = {"total_bytes": 0, "total_requests": 0, "start_time": time.time(), "total_errors": 0}
connections = {}
http_client = None

# ── ربات تلگرام ─────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_IDS = {int(x) for x in os.environ.get("TELEGRAM_ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
_api_client = None
_bot_running = False
_poll_task = None

# ── داده‌های لینک (برای VLESS) ─────────────────────────────────────────────
LINKS = {}                 # در این دمو خالی است، اما ساختار نگه داشته می‌شود
LINKS_LOCK = asyncio.Lock()
hourly_traffic = {}
error_logs = []

# ── توابع کمکی برای سیستم لینک ─────────────────────────────────────────────
def is_link_allowed(link):
    return True

def is_ip_allowed(link, uuid, ip):
    return True

async def save_state():
    pass

def log_activity(action, message, level="info"):
    logger.log(getattr(logging, level.upper()), f"{action}: {message}")

def now_ir():
    return datetime.now()

# ── توابع مربوط به WebSocket/VLESS (برگرفته از relay_vless.py) ────────────
RELAY_BUF = 256 * 1024   # 256 KB buffer

def _ws_client_ip(ws: WebSocket) -> str:
    fwd = ws.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    real_ip = ws.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return ws.client.host if ws.client else "نامشخص"

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

async def check_and_use(uid: str, n: int) -> bool:
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            return False
        if not is_link_allowed(link):
            return False
        stats["total_bytes"] += n
        hourly_traffic[now_ir().strftime("%H:00")] = hourly_traffic.get(now_ir().strftime("%H:00"), 0) + n
    return True

async def relay_ws_to_tcp(ws: WebSocket, writer: asyncio.StreamWriter, conn_id: str, uid: str):
    try:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                logger.info(f"🔄 relay_ws_to_tcp: WebSocket disconnected for {conn_id}")
                break
            data = msg.get("bytes") or (msg.get("text") or "").encode()
            if not data:
                continue
            if not await check_and_use(uid, len(data)):
                logger.warning(f"🔄 relay_ws_to_tcp: quota exceeded for {conn_id}")
                await ws.close(code=1008, reason="quota/disabled/unknown")
                break
            stats["total_requests"] += 1
            if conn_id in connections:
                connections[conn_id]["bytes"] += len(data)
            writer.write(data)
            if writer.transport.get_write_buffer_size() > RELAY_BUF:
                await writer.drain()
    except (WebSocketDisconnect, Exception) as e:
        logger.error(f"🔄 relay_ws_to_tcp error for {conn_id}: {e}")
    finally:
        try:
            writer.write_eof()
        except Exception:
            pass

async def relay_tcp_to_ws(ws: WebSocket, reader: asyncio.StreamReader, conn_id: str, uid: str):
    first = True
    try:
        while True:
            data = await reader.read(RELAY_BUF)
            if not data:
                logger.info(f"🔄 relay_tcp_to_ws: no more data from target for {conn_id}")
                break
            if not await check_and_use(uid, len(data)):
                logger.warning(f"🔄 relay_tcp_to_ws: quota exceeded for {conn_id}")
                await ws.close(code=1008, reason="quota/disabled/unknown")
                break
            if conn_id in connections:
                connections[conn_id]["bytes"] += len(data)
            payload = (b"\x00\x00" + data) if first else data
            first = False
            await ws.send_bytes(payload)
    except Exception as e:
        logger.error(f"🔄 relay_tcp_to_ws error for {conn_id}: {e}")

async def websocket_tunnel(ws: WebSocket, uuid: str):
    await ws.accept()

    async with LINKS_LOCK:
        link = LINKS.get(uuid)

    if not is_link_allowed(link):
        logger.warning(f"🚫 WS rejected uuid={uuid[:8]}… (not allowed)")
        await ws.close(code=1008, reason="not authorized")
        return

    ip = _ws_client_ip(ws)

    if not is_ip_allowed(link, uuid, ip):
        logger.warning(f"🚫 WS rejected uuid={uuid[:8]}… ip={ip} (ip limit reached)")
        log_activity("connection", f"اتصال {ip} به کانفیگ رد شد (محدودیت تعداد آی‌پی)", "warn")
        await ws.close(code=1008, reason="ip limit reached")
        return

    conn_id = secrets.token_urlsafe(6)
    connections[conn_id] = {
        "uuid": uuid,
        "ip": ip,
        "transport": "vless-ws",
        "connected_at": datetime.now().isoformat(),
        "bytes": 0,
    }
    logger.info(f"✅ WS [{conn_id}] uuid={uuid[:8]}… ip={ip} total={len(connections)}")
    log_activity("connection", f"اتصال جدید از {ip}", "info")
    writer = None

    try:
        first_msg = await asyncio.wait_for(ws.receive(), timeout=15.0)
        if first_msg["type"] == "websocket.disconnect":
            logger.info(f"WS [{conn_id}] disconnected before sending header")
            return
        first_chunk = first_msg.get("bytes") or (first_msg.get("text") or "").encode()
        if not first_chunk:
            logger.warning(f"WS [{conn_id}] empty first chunk")
            return

        logger.info(f"WS [{conn_id}] parsing VLESS header (first {len(first_chunk)} bytes)")
        command, address, port, payload = await parse_vless_header(first_chunk)
        logger.info(f"WS [{conn_id}] parsed → {address}:{port}")

        if not await check_and_use(uuid, len(first_chunk)):
            logger.warning(f"WS [{conn_id}] quota check failed on first chunk")
            await ws.close(code=1008, reason="quota/disabled")
            return

        stats["total_requests"] += 1
        if conn_id in connections:
            connections[conn_id]["bytes"] += len(first_chunk)

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(address, port),
            timeout=10.0
        )
        sock = writer.transport.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        logger.info(f"WS [{conn_id}] TCP connected to {address}:{port}")

        if payload:
            writer.write(payload)
            await writer.drain()
            logger.info(f"WS [{conn_id}] wrote payload of {len(payload)} bytes")

        done, pending = await asyncio.wait(
            {
                asyncio.create_task(relay_ws_to_tcp(ws, writer, conn_id, uuid)),
                asyncio.create_task(relay_tcp_to_ws(ws, reader, conn_id, uuid)),
            },
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        asyncio.create_task(save_state())
        logger.info(f"WS [{conn_id}] relay tasks finished")

    except WebSocketDisconnect:
        logger.info(f"WS [{conn_id}] WebSocket disconnected")
    except asyncio.TimeoutError:
        stats["total_errors"] += 1
        error_logs.append({"error": "connection timeout", "time": datetime.now().isoformat()})
        logger.error(f"WS [{conn_id}] timeout")
    except Exception as exc:
        stats["total_errors"] += 1
        error_logs.append({"error": str(exc), "time": datetime.now().isoformat()})
        logger.error(f"WS [{conn_id}] error: {exc}")
    finally:
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        connections.pop(conn_id, None)
        logger.info(f"🔌 WS closed [{conn_id}] total={len(connections)}")

# ── رویدادهای startup/shutdown ──────────────────────────────────────────
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
    logger.info(f"X4G Lite started, UUID={UUID}, host={HOST}")

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

# ── مسیرهای عمومی ────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "connections": len(connections)}

@app.get("/stats")
async def get_stats():
    return {
        "total_bytes": stats["total_bytes"],
        "total_requests": stats["total_requests"],
        "connections": len(connections),
        "uptime_sec": int(time.time() - stats["start_time"]),
        "total_errors": stats.get("total_errors", 0)
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

# ── WebSocket ──────────────────────────────────────────────────────────────
@app.websocket("/ws/{uuid}")
async def websocket_endpoint(ws: WebSocket, uuid: str):
    await websocket_tunnel(ws, uuid)

# ── ربات تلگرام ────────────────────────────────────────────────────────────
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
        current_host = get_host()
        current_uuid = UUID
        link = f"vless://{current_uuid}@{current_host}:443?encryption=none&security=tls&type=ws&host={current_host}&path=/ws/{current_uuid}&sni={current_host}&fp=chrome&alpn=h2%2Chttp%2F1.1#X4G-Mahdi"
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

@app.get("/xhttp")  # این مسیر فقط برای نمایش لینک (اختیاری) باقی می‌ماند
async def xhttp_link():
    link = f"vless://{UUID}@{HOST}:443?encryption=none&security=tls&type=ws&host={HOST}&path=/ws/{UUID}&sni={HOST}&fp=chrome#X4G-WS"
    return {"link": link}

# ── اجرای اصلی ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info", workers=1)
