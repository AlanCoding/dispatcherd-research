# ipc_tools.py
import socket
import asyncio
from multiprocessing.connection import Connection
from typing import Tuple, Any


def create_socketpair() -> Tuple[socket.socket, socket.socket]:
    """
    Create a pair of connected sockets.
    Returns (parent_sock, child_sock).
    """
    parent_sock, child_sock = socket.socketpair()
    parent_sock.setblocking(False)
    child_sock.setblocking(True)
    return parent_sock, child_sock


async def send_pickle(conn: Connection, obj: Any, timeout: float = 1.0) -> None:
    """
    Send a Python object using Connection.send(), in a background thread.
    """
    loop = asyncio.get_running_loop()
    try:
        await asyncio.wait_for(loop.run_in_executor(None, conn.send, obj), timeout)
    except asyncio.TimeoutError:
        raise RuntimeError("Timed out sending object to child")
