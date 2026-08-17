import asyncio
import secrets
import socket
from fastapi import WebSocket, WebSocketDisconnect
from main import UUID, stats, connections, logger

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
