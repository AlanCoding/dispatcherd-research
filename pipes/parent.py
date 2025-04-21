# parent.py
import asyncio
import multiprocessing
from multiprocessing.connection import Connection
import socket
import os
import sys
from dataclasses import dataclass
from typing import List

from ipc_tools import create_socketpair, send_pickle


@dataclass
class ChildHandle:
    process: multiprocessing.Process
    command_conn: Connection
    id: int


def child_entry(command_fd: int, reply_fd: int, child_id: int) -> None:
    cmd_conn = Connection(command_fd)
    reply_sock = socket.socket(fileno=reply_fd)

    while True:
        try:
            msg = cmd_conn.recv()
        except EOFError:
            break

        print(f"child-{child_id}: received {msg!r}, replying...", file=sys.stderr)
        if msg == "exit":
            break

        response = f"child-{child_id}: got '{msg}'\n"
        encoded = response.encode()
        assert len(encoded) <= 4096, "Reply too large for atomic write"

        try:
            reply_sock.sendall(encoded)
        except (BlockingIOError, BrokenPipeError) as e:
            print(f"[child-{child_id}] failed to send reply: {e}", file=sys.stderr)
            os._exit(1)

    print(f"child-{child_id}: exiting", file=sys.stderr)
    reply_sock.close()
    os._exit(0)


async def run() -> None:
    num_children = 3
    children: List[ChildHandle] = []

    # Shared reply socket
    reply_parent_sock, reply_child_sock = create_socketpair()
    reply_parent_sock.setblocking(False)

    # Wrap reply socket in StreamReader
    loop = asyncio.get_running_loop()
    reply_reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reply_reader)
    transport, _ = await loop.connect_accepted_socket(lambda: protocol, reply_parent_sock)

    socks = []
    for i in range(num_children):
        cmd_parent_sock, cmd_child_sock = create_socketpair()
        cmd_parent_sock.setblocking(False)

        p = multiprocessing.Process(
            target=child_entry,
            args=(cmd_child_sock.fileno(), reply_child_sock.fileno(), i)
        )
        p.start()

        cmd_child_sock.close()
        cmd_conn = Connection(cmd_parent_sock.fileno())
        children.append(ChildHandle(
            process=p,
            command_conn=cmd_conn,
            id=i
        ))
        socks.append(cmd_parent_sock)

    reply_child_sock.close()

    # Send commands to each child
    for child in children:
        await send_pickle(child.command_conn, f"Hello from parent to child-{child.id}")

    # Read responses
    for _ in range(num_children):
        response = await reply_reader.readline()
        print("Parent received:", response.decode().strip())

    # Send shutdown signals
    for child in children:
        await send_pickle(child.command_conn, "exit")
        child.command_conn.close()

    transport.close()
    reply_parent_sock.close()

    for child in children:
        child.process.join()


if __name__ == "__main__":
    asyncio.run(run())
