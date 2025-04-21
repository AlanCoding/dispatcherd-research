import asyncio
import os
import pickle
import socket
import struct
from typing import Any, Tuple


def create_socketpair() -> Tuple[socket.socket, socket.socket]:
    parent_sock, child_sock = socket.socketpair()
    parent_sock.setblocking(False)
    child_sock.setblocking(True)
    return parent_sock, child_sock


def send_pickled(sock: socket.socket, obj: Any) -> None:
    payload = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    header = struct.pack(">I", len(payload))
    sock.sendall(header + payload)


async def asend_pickled(sock: socket.socket, obj: Any, timeout: float = 1.0) -> None:
    payload = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    header = struct.pack(">I", len(payload))
    data = header + payload

    loop = asyncio.get_running_loop()
    try:
        await asyncio.wait_for(loop.sock_sendall(sock, data), timeout)
    except asyncio.TimeoutError:
        raise RuntimeError("Timeout sending message")


def read_exact(fd: int, length: int) -> bytes:
    buf = bytearray()
    while len(buf) < length:
        chunk = os.read(fd, length - len(buf))
        if not chunk:
            raise EOFError("unexpected EOF")
        buf += chunk
    return bytes(buf)


def read_pickled(fd: int) -> Any:
    header = read_exact(fd, 4)
    (length,) = struct.unpack(">I", header)
    payload = read_exact(fd, length)
    return pickle.loads(payload)


async def read_pickled_async(reader: asyncio.StreamReader) -> Any:
    header = await reader.readexactly(4)
    (length,) = struct.unpack(">I", header)
    payload = await reader.readexactly(length)
    return pickle.loads(payload)
