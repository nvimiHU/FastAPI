import asyncio
import secrets
import socket
import time
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from main import UUID, stats, connections, logger

router = APIRouter()

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

def _tune_socket(writer):
    sock = writer.transport.get_extra_info("socket")
    if not sock:
        return
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCK_BUF_SIZE)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCK_BUF_SIZE)
    except OSError:
        pass

class _AdaptiveFlow:
    __slots__ = ("high_water", "last_drain_ms")
    def __init__(self):
        self.high_water = FLOW_START_HW
        self.last_drain_ms = 0.0
    def should_drain(self, buf_size: int) -> bool:
        return buf_size > self.high_water
    async def drain(self, writer):
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
    from relay_vless import parse_vless_header
    command, address, port, payload = await parse_vless_header(first_chunk)
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(address, port), timeout=TCP_CONNECT_TIMEOUT
    )
    _tune_socket(writer)
    if payload:
        writer.write(payload)
        await writer.drain()
    return reader, writer, address, port

async def _get_or_create_session(uuid: str, mode: str, session_id: str, ip: str = "نامشخص") -> dict:
    async with XHTTP_LOCK:
        sess = xhttp_sessions.get(session_id)
        if sess is not None:
            sess["last_seen"] = time.time()
            return sess
        if uuid != UUID:
            raise HTTPException(status_code=403, detail="invalid uuid")
        conn_id = secrets.token_urlsafe(6)
        connections[conn_id] = {
            "uuid": uuid,
            "ip": ip,
            "connected_at": time.time(),
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
            "flow": _AdaptiveFlow(),
        }
        xhttp_sessions[session_id] = sess
        logger.info(f"new XHTTP[{mode}] session {session_id[:8]} from {ip}")
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
            except:
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
    logger.info(f"closed XHTTP[{sess.get('mode')}] {session_id[:8]}")

async def _reaper():
    while True:
        await asyncio.sleep(REAPER_INTERVAL)
        now = time.time()
        async with XHTTP_LOCK:
            stale = [sid for sid, s in xhttp_sessions.items()
                     if now - s["last_seen"] > SESSION_IDLE_TIMEOUT and not s.get("tcp_open")]
        for sid in stale:
            await _teardown(sid)

_reaper_started = False

def ensure_reaper():
    global _reaper_started
    if not _reaper_started:
        asyncio.create_task(_reaper())
        _reaper_started = True

async def _pump_tcp_to_queue(session_id: str, uuid: str, reader, down_q):
    first = True
    try:
        while True:
            data = await reader.read(XHTTP_BUF)
            if not data:
                break
            stats["total_requests"] += 1
            stats["total_bytes"] += len(data)
            async with XHTTP_LOCK:
                sess = xhttp_sessions.get(session_id)
            if sess:
                c = connections.get(sess["conn_id"])
                if c:
                    c["bytes"] += len(data)
            payload = (b"\x00\x00" + data) if first else data
            first = False
            await down_q.put(payload)
    except:
        pass
    finally:
        await _teardown(session_id)

async def _open_tcp_for_session(session_id: str, uuid: str, sess: dict, first_chunk: bytes):
    reader, writer, address, port = await _open_tcp_from_header(first_chunk)
    logger.info(f"XHTTP[{sess['mode']}] {session_id[:8]} -> {address}:{port}")
    sess["writer"] = writer
    sess["tcp_open"] = True
    sess["downlink_task"] = asyncio.create_task(
        _pump_tcp_to_queue(session_id, uuid, reader, sess["down_q"])
    )

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

@router.get("/xhttp-siz10/{mode}/{uuid}/{session_id}")
async def xhttp_downlink(mode: str, uuid: str, session_id: str, request: Request):
    ensure_reaper()
    if mode not in ("packet-up", "stream-up"):
        raise HTTPException(status_code=404, detail="unknown mode")
    if uuid != UUID:
        raise HTTPException(status_code=403, detail="invalid uuid")
    fp = request.query_params.get("fp", DEFAULT_FINGERPRINT)
    sess = await _get_or_create_session(uuid, mode, session_id, _req_client_ip(request))
    if sess.get("closed"):
        raise HTTPException(status_code=404, detail="session closed")
    headers = _resp_headers(fp)
    return StreamingResponse(_downstream_gen(sess), headers=headers, media_type=headers["content-type"])

@router.post("/xhttp-siz10/packet-up/{uuid}/{session_id}/{seq}")
async def packet_up_upload(uuid: str, session_id: str, seq: int, request: Request):
    ensure_reaper()
    if uuid != UUID:
        raise HTTPException(status_code=403, detail="invalid uuid")
    sess = await _get_or_create_session(uuid, "packet-up", session_id, _req_client_ip(request))
    if sess.get("closed"):
        raise HTTPException(status_code=404, detail="session closed")
    sess["last_seen"] = time.time()
    body = await request.body()
    if not body:
        return {"ok": True}
    stats["total_requests"] += 1
    stats["total_bytes"] += len(body)
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
        logger.error(f"packet-up error: {exc}")
        await _teardown(session_id)
        raise HTTPException(status_code=502, detail="write failed")
    return {"ok": True}

@router.post("/xhttp-siz10/stream-up/{uuid}/{session_id}")
async def stream_up_upload(uuid: str, session_id: str, request: Request):
    ensure_reaper()
    if uuid != UUID:
        raise HTTPException(status_code=403, detail="invalid uuid")
    sess = await _get_or_create_session(uuid, "stream-up", session_id, _req_client_ip(request))
    if sess.get("closed"):
        raise HTTPException(status_code=404, detail="session closed")
    flow = sess["flow"]
    conn = connections[sess["conn_id"]]
    writer = sess["writer"]
    try:
        async for chunk in request.stream():
            if not chunk:
                continue
            sess["last_seen"] = time.time()
            stats["total_requests"] += 1
            stats["total_bytes"] += len(chunk)
            conn["bytes"] += len(chunk)
            if writer is None:
                await _open_tcp_for_session(session_id, uuid, sess, chunk)
                writer = sess["writer"]
                continue
            writer.write(chunk)
            if flow.should_drain(writer.transport.get_write_buffer_size()):
                await flow.drain(writer)
    except Exception as exc:
        logger.error(f"stream-up error: {exc}")
        await _teardown(session_id)
        raise HTTPException(status_code=502, detail="stream error")
    return {"ok": True}
