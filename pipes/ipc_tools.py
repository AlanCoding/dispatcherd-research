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

async def send_message(sock: socket.socket, message: str, timeout: float = 1.0) -> None:
    """
    Send a newline-terminated message to the child over a raw socket.
    Uses asyncio's sock_sendall with timeout.
    """
    loop = asyncio.get_running_loop()
    data = message.encode() + b'\n'
    try:
        await asyncio.wait_for(loop.sock_sendall(sock, data), timeout)
    except asyncio.TimeoutError:
        raise RuntimeError("Timed out trying to send message to child")
