import asyncio
import os
import secrets
import socket
import time
import httpx
import logging
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, APIRouter
from fastapi.responses import JSONResponse, Response, StreamingResponse
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
stats = {"total_bytes": 0, "total_requests": 0, "start_time": time.time()}
connections = {}
http_client = None

# ── ربات تلگرام ─────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_IDS = {int(x) for x in os.environ.get("TELEGRAM_ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
_api_client = None
_bot_running = False
_poll_task = None

# ── متغیرهای مورد نیاز برای XHTTP ──────────────────────────────────────────
LINKS = {}                 # استاب (در این دمو خالی)
LINKS_LOCK = asyncio.Lock()
hourly_traffic = {}
error_logs = []
_reaper_started = False

def is_link_allowed(link):
    return True

def is_ip_allowed(link, uuid, ip):
    return True

async def save_state():
    pass

async def throttle(uuid, nbytes):
    pass

async def check_and_use(uuid, nbytes):
    return True

# ── توابع کمکی برای XHTTP ──────────────────────────────────────────────────
XHTTP_BUF = 512 * 1024
DOWNLINK_QUEUE_MAX = 512
SESSION_IDLE_TIMEOUT = 30
REAPER_INTERVAL = 10
TCP_CONNECT_TIMEOUT = 10.0

SOCK_BUF_SIZE = 2 * 1024 * 1024
FLOW_MIN_HW = 256 * 1024
FLOW_MAX_HW = 16 * 1024 * 1024
FLOW_START_HW = 2 * 1024 * 1024
FLOW_FAST_DRAIN_MS = 2.0
FLOW_SLOW_DRAIN_MS = 25.0

QUOTA_MIN_BATCH = 32 * 1024
QUOTA_MAX_BATCH = 1 * 1024 * 1024
QUOTA_START_BATCH = 64 * 1024
QUOTA_CHECK_INTERVAL = 0.2

PACKET_UP_HIGH_WATER = 2 * 1024 * 1024

xhttp_sessions = {}
XHTTP_LOCK = asyncio.Lock()

FINGERPRINTS = {
    "chrome": {
        "content-type": "application/grpc",
        "cache-control": "no-cache, no-store",
        "x-accel-buffering": "no",
        "server": "cloudflare",
    },
    "plain": {
        "content-type": "application/octet-stream",
        "cache-control": "no-store",
        "x-accel-buffering": "no",
    },
}
DEFAULT_FINGERPRINT = "chrome"

def _resp_headers(fp: str) -> dict:
    return dict(FINGERPRINTS.get(fp, FINGERPRINTS[DEFAULT_FINGERPRINT]))

def _tune_socket(writer: asyncio.StreamWriter):
    sock = writer.transport.get_extra_info("socket")
    if not sock:
        return
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCK_BUF_SIZE)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCK_BUF_SIZE)
    except OSError:
        pass

class _QuotaGate:
    __slots__ = ("uuid", "pending", "last_check", "ok", "batch_bytes", "rate_ewma")
    def __init__(self, uuid: str):
        self.uuid = uuid
        self.pending = 0
        self.last_check = time.monotonic()
        self.ok = True
        self.batch_bytes = QUOTA_START_BATCH
        self.rate_ewma = 0.0

    async def add(self, nbytes: int) -> bool:
        if not self.ok:
            return False
        self.pending += nbytes
        now = time.monotonic()
        elapsed = now - self.last_check
        if self.pending >= self.batch_bytes or elapsed >= QUOTA_CHECK_INTERVAL:
            flush, self.pending = self.pending, 0
            if elapsed > 0:
                inst_rate = flush / elapsed
                self.rate_ewma = inst_rate if self.rate_ewma == 0 else (0.7 * self.rate_ewma + 0.3 * inst_rate)
                target = int(self.rate_ewma * QUOTA_CHECK_INTERVAL)
                self.batch_bytes = max(QUOTA_MIN_BATCH, min(QUOTA_MAX_BATCH, target or QUOTA_MIN_BATCH))
            self.last_check = now
            self.ok = await check_and_use(self.uuid, flush)
            return self.ok
        return True

    async def flush(self) -> bool:
        if self.pending:
            flush, self.pending = self.pending, 0
            self.ok = self.ok and await check_and_use(self.uuid, flush)
        return self.ok

class _AdaptiveFlow:
    __slots__ = ("high_water", "last_drain_ms")
    def __init__(self):
        self.high_water = FLOW_START_HW
        self.last_drain_ms = 0.0

    def should_drain(self, buf_size: int) -> bool:
        return buf_size > self.high_water

    async def drain(self, writer: asyncio.StreamWriter):
        t0 = time.monotonic()
        await writer.drain()
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.last_drain_ms = elapsed_ms
        if elapsed_ms < FLOW_FAST_DRAIN_MS:
            self.high_water = min(FLOW_MAX_HW, int(self.high_water * 1.5) + 65536)
        elif elapsed_ms > FLOW_SLOW_DRAIN_MS:
            self.high_water = max(FLOW_MIN_HW, self.high_water // 2)

def _req_client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return request.client.host if request.client else "نامشخص"

async def _open_tcp_from_header(first_chunk: bytes):
    command, address, port, payload = await parse_vless_header(first_chunk)
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(address, port), timeout=TCP_CONNECT_TIMEOUT
    )
    _tune_socket(writer)
    if payload:
        writer.write(payload)
        await writer.drain()
    return reader, writer, address, port

async def _check_link(uuid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uuid)
    if not is_link_allowed(link):
        raise HTTPException(status_code=403, detail="not authorized")

async def _get_or_create_session(uuid: str, mode: str, session_id: str, ip: str = "نامشخص") -> dict:
    async with XHTTP_LOCK:
        sess = xhttp_sessions.get(session_id)
        if sess is not None:
            sess["last_seen"] = time.time()
            return sess

        async with LINKS_LOCK:
            link = LINKS.get(uuid)
        if not is_ip_allowed(link, uuid, ip):
            logger.warning(f"🚫 XHTTP[{mode}] rejected uuid={uuid[:8]} ip={ip} (ip limit reached)")
            raise HTTPException(status_code=403, detail="ip limit reached")

        conn_id = secrets.token_urlsafe(6)
        connections[conn_id] = {
            "uuid": uuid,
            "ip": ip,
            "connected_at": datetime.now().isoformat(),
            "bytes": 0,
            "transport": f"xhttp-{mode}",
        }
        sess = {
            "uuid": uuid, "mode": mode, "writer": None,
            "downlink_task": None, "uplink_task": None,
            "down_q": asyncio.Queue(maxsize=DOWNLINK_QUEUE_MAX),
            "last_seen": time.time(),
            "conn_id": conn_id, "tcp_open": False, "closed": False,
            "seq_buf": {}, "next_seq": 0,
            "gate": None,
            "flow": None,
        }
        xhttp_sessions[session_id] = sess
        logger.info(f"new XHTTP[{mode}] session [{session_id[:8]}] uuid={uuid[:8]} ip={ip}")
        return sess

async def _teardown(session_id: str):
    async with XHTTP_LOCK:
        sess = xhttp_sessions.pop(session_id, None)
    if not sess:
        return
    sess["closed"] = True
    for t in ("uplink_task", "downlink_task"):
        task = sess.get(t)
        if task:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
    writer = sess.get("writer")
    if writer:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
    connections.pop(sess.get("conn_id"), None)
    dq = sess.get("down_q")
    if dq:
        try:
            dq.put_nowait(None)
        except Exception:
            pass
    logger.info(f"closed XHTTP[{sess.get('mode')}] [{session_id[:8]}] total={len(xhttp_sessions)}")

async def _reaper():
    while True:
        await asyncio.sleep(REAPER_INTERVAL)
        now = time.time()
        async with XHTTP_LOCK:
            stale = [sid for sid, s in xhttp_sessions.items()
                     if now - s["last_seen"] > SESSION_IDLE_TIMEOUT and not s.get("tcp_open")]
        for sid in stale:
            await _teardown(sid)

def ensure_reaper():
    global _reaper_started
    if not _reaper_started:
        asyncio.create_task(_reaper())
        _reaper_started = True

async def _pump_tcp_to_queue(session_id: str, uuid: str, reader: asyncio.StreamReader, down_q: asyncio.Queue):
    first = True
    gate = _QuotaGate(uuid)
    try:
        while True:
            data = await reader.read(XHTTP_BUF)
            if not data:
                break
            if not await gate.add(len(data)):
                break
            await throttle(uuid, len(data))
            async with XHTTP_LOCK:
                sess = xhttp_sessions.get(session_id)
            if sess:
                c = connections.get(sess["conn_id"])
                if c:
                    c["bytes"] += len(data)
            payload = (b"\x00\x00" + data) if first else data
            first = False
            await down_q.put(payload)
    except (asyncio.CancelledError, Exception):
        pass
    finally:
        await gate.flush()
        await _teardown(session_id)

async def _open_tcp_for_session(session_id: str, uuid: str, sess: dict, first_chunk: bytes):
    reader, writer, address, port = await _open_tcp_from_header(first_chunk)
    logger.info(f"connect XHTTP[{sess['mode']}] [{session_id[:8]}] -> {address}:{port}")
    sess["writer"] = writer
    sess["tcp_open"] = True
    sess["downlink_task"] = asyncio.create_task(
        _pump_tcp_to_queue(session_id, uuid, reader, sess["down_q"])
    )
    asyncio.create_task(save_state())

def _downstream_gen(sess: dict):
    async def gen():
        try:
            while True:
                chunk = await sess["down_q"].get()
                if chunk is None:
                    break
                sess["last_seen"] = time.time()
                yield chunk
        finally:
            pass
    return gen()

# ── تعریف روتر XHTTP ──────────────────────────────────────────────────────
router = APIRouter()

@router.get("/xhttp-siz10/{mode}/{uuid}/{session_id}")
async def xhttp_downlink(mode: str, uuid: str, session_id: str, request: Request):
    ensure_reaper()
    if mode not in ("packet-up", "stream-up"):
        raise HTTPException(status_code=404, detail="unknown mode")
    await _check_link(uuid)
    fp = request.query_params.get("fp", DEFAULT_FINGERPRINT)
    sess = await _get_or_create_session(uuid, mode, session_id, _req_client_ip(request))
    if sess.get("closed"):
        raise HTTPException(status_code=404, detail="session closed")
    headers = _resp_headers(fp)
    return StreamingResponse(_downstream_gen(sess), headers=headers, media_type=headers["content-type"])

@router.post("/xhttp-siz10/packet-up/{uuid}/{session_id}/{seq}")
async def packet_up_upload(uuid: str, session_id: str, seq: int, request: Request):
    ensure_reaper()
    sess = await _get_or_create_session(uuid, "packet-up", session_id, _req_client_ip(request))
    if sess.get("closed"):
        raise HTTPException(status_code=404, detail="session closed")
    sess["last_seen"] = time.time()
    body = await request.body()
    if not body:
        return {"ok": True}
    if not await check_and_use(uuid, len(body)):
        await _teardown(session_id)
        raise HTTPException(status_code=403, detail="quota/disabled/unknown")
    await throttle(uuid, len(body))
    stats["total_requests"] += 1
    connections[sess["conn_id"]]["bytes"] += len(body)
    try:
        if sess["writer"] is None:
            if seq != 0:
                sess["seq_buf"][seq] = body
                return {"ok": True, "buffered": True}
            await _open_tcp_for_session(session_id, uuid, sess, body)
            nxt = 1
            while nxt in sess["seq_buf"]:
                pending = sess["seq_buf"].pop(nxt)
                sess["writer"].write(pending)
                nxt += 1
            sess["next_seq"] = nxt
            return {"ok": True, "connected": True}
        if seq == sess["next_seq"]:
            sess["writer"].write(body)
            sess["next_seq"] += 1
            while sess["next_seq"] in sess["seq_buf"]:
                pending = sess["seq_buf"].pop(sess["next_seq"])
                sess["writer"].write(pending)
                sess["next_seq"] += 1
        else:
            sess["seq_buf"][seq] = body
        if sess["writer"].transport.get_write_buffer_size() > PACKET_UP_HIGH_WATER:
            await sess["writer"].drain()
    except Exception as exc:
        error_logs.append({"error": str(exc), "time": datetime.now().isoformat()})
        await _teardown(session_id)
        raise HTTPException(status_code=502, detail="write failed")
    return {"ok": True}

@router.post("/xhttp-siz10/stream-up/{uuid}/{session_id}")
async def stream_up_upload(uuid: str, session_id: str, request: Request):
    ensure_reaper()
    sess = await _get_or_create_session(uuid, "stream-up", session_id, _req_client_ip(request))
    if sess.get("closed"):
        raise HTTPException(status_code=404, detail="session closed")
    gate = sess.get("gate")
    if gate is None:
        gate = _QuotaGate(uuid)
        sess["gate"] = gate
    flow = sess.get("flow")
    if flow is None:
        flow = _AdaptiveFlow()
        sess["flow"] = flow
    conn = connections[sess["conn_id"]]
    writer = sess["writer"]
    try:
        async for chunk in request.stream():
            if not chunk:
                continue
            sess["last_seen"] = time.time()
            if not await gate.add(len(chunk)):
                raise HTTPException(status_code=403, detail="quota/disabled/unknown")
            await throttle(uuid, len(chunk))
            stats["total_requests"] += 1
            conn["bytes"] += len(chunk)
            if writer is None:
                await _open_tcp_for_session(session_id, uuid, sess, chunk)
                writer = sess["writer"]
                continue
            writer.write(chunk)
            if flow.should_drain(writer.transport.get_write_buffer_size()):
                await flow.drain(writer)
    except HTTPException:
        await gate.flush()
        await _teardown(session_id)
        raise
    except Exception as exc:
        error_logs.append({"error": str(exc), "time": datetime.now().isoformat()})
        await gate.flush()
        await _teardown(session_id)
        raise HTTPException(status_code=502, detail="stream error")
    await gate.flush()
    return {"ok": True}

# ── اتصال روتر به برنامه ──────────────────────────────────────────────────
app.include_router(router)

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

# ── WebSocket ──────────────────────────────────────────────────────────────
RELAY_BUF = 256 * 1024

def _ws_client_ip(ws: WebSocket) -> str:
    fwd = ws.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    real_ip = ws.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return ws.client.host if ws.client else "نامشخص"

def _tune_socket_ws(writer):
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
        _tune_socket_ws(writer)
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
        link = f"vless://{UUID}@{HOST}:443?encryption=none&security=tls&type=xhttp&host={HOST}&path=/xhttp-siz10/{UUID}&mode=auto&sni={HOST}&fp=chrome&alpn=h2%2Chttp%2F1.1#X4G-Mahdi"
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

@app.get("/xhttp")
async def xhttp_link():
    link = f"XHTTP://{UUID}@{HOST}:443?type=xhttp-siz10&mode=stream-up&host={HOST}&fp=chrome#X4G-XHTTP"
    return {"link": link}

# ── اجرای اصلی ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info", workers=1)
