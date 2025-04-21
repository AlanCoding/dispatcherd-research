# parent.py
import asyncio
import multiprocessing
import socket
import os
from dataclasses import dataclass
from typing import List, Tuple

from ipc_tools import create_socketpair, send_message, read_message

@dataclass
class ChildHandle:
    process: multiprocessing.Process
    writer: asyncio.StreamWriter
    id: int


def child_entry(command_fd: int, reply_fd: int, child_id: int) -> None:
    """
    Subprocess function. Reads commands from parent and replies.
    """
    import sys

    command_sock = socket.socket(fileno=command_fd)
    reply_sock = socket.socket(fileno=reply_fd)

    command_file = command_sock.makefile('rb', buffering=0)

    while True:
        line: bytes = command_file.readline()
        if not line:
            print('child got blank line')
            break

        msg = line.decode().strip()
        if msg == "exit":
            print(f"child-{child_id}: exiting", file=sys.stderr)
            break

        response = f"child-{child_id}: got '{msg}'\n"
        print(f"child-{child_id}: received {msg!r}, replying...", file=sys.stderr)
        encoded = response.encode()
        assert len(encoded) <= 4096, "Reply too large for atomic write"

        reply_sock.sendall(encoded)

    command_file.close()
    command_sock.close()
    reply_sock.close()
    os._exit(0)


async def run() -> None:
    num_children = 3
    children: List[ChildHandle] = []

    # Shared reply socket
    reply_parent_sock, reply_child_sock = create_socketpair()
    reply_parent_sock.setblocking(False)

    # wrap reply socket in asyncio reader
    loop = asyncio.get_running_loop()
    reply_reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reply_reader)
    transport, _ = await loop.connect_accepted_socket(lambda: protocol, reply_parent_sock)

    # Launch children
    for i in range(num_children):
        cmd_parent_sock, cmd_child_sock = create_socketpair()

        p = multiprocessing.Process(
            target=child_entry,
            args=(cmd_child_sock.fileno(), reply_child_sock.fileno(), i)
        )
        p.start()

        cmd_child_sock.close()
        writer = (await asyncio.open_unix_connection(sock=cmd_parent_sock))[1]

        children.append(ChildHandle(process=p, writer=writer, id=i))

    reply_child_sock.close()

    # Send a message to each child
    for child in children:
        await send_message(child.writer, f"Hello from parent to child-{child.id}")

    # Read N replies (1 per child)
    for _ in range(num_children):
        response = await read_message(reply_reader)
        print("Parent received:", response)

    # Shut down each child
    for child in children:
        await send_message(child.writer, "exit")
        child.writer.close()
        await child.writer.wait_closed()

    transport.close()
    reply_parent_sock.close()

    for child in children:
        child.process.join()


if __name__ == "__main__":
    asyncio.run(run())
