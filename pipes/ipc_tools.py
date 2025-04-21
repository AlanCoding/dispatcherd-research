# ipc_tools.py
import socket
import asyncio
from typing import Tuple

def create_socketpair() -> Tuple[socket.socket, socket.socket]:
    """
    Create a pair of connected sockets.
    Returns (parent_sock, child_sock).
    """
    parent_sock, child_sock = socket.socketpair()
    parent_sock.setblocking(False)
    child_sock.setblocking(True)
    return parent_sock, child_sock

async def send_message(writer: asyncio.StreamWriter, message: str) -> None:
    """
    Send a newline-terminated message.
    """
    data = message.encode() + b'\n'
    writer.write(data)
    await writer.drain()

async def read_message(reader: asyncio.StreamReader) -> str:
    """
    Read a newline-terminated message.
    """
    line: bytes = await reader.readline()
    return line.decode().strip()
