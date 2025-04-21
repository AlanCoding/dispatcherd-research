# parent.py
import asyncio
import multiprocessing
import socket
import os
from dataclasses import dataclass
from typing import List

from ipc_tools import create_socketpair, send_message

@dataclass
class ChildHandle:
    process: multiprocessing.Process
    send_sock: socket.socket
    id: int

def child_entry(command_fd: int, reply_fd: int, child_id: int) -> None:
    import sys

    command_sock = socket.socket(fileno=command_fd)
    reply_sock = socket.socket(fileno=reply_fd)

    command_file = command_sock.makefile('rb', buffering=0)

    while True:
        line: bytes = command_file.readline()
        if not line:
            break

        msg = line.decode().strip()
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

    # Wrap shared socket in a StreamReader
    loop = asyncio.get_running_loop()
    reply_reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reply_reader)
    transport, _ = await loop.connect_accepted_socket(lambda: protocol, reply_parent_sock)

    for i in range(num_children):
        cmd_parent_sock, cmd_child_sock = create_socketpair()
        cmd_parent_sock.setblocking(False)

        p = multiprocessing.Process(
            target=child_entry,
            args=(cmd_child_sock.fileno(), reply_child_sock.fileno(), i)
        )
        p.start()

        cmd_child_sock.close()
        children.append(ChildHandle(process=p, send_sock=cmd_parent_sock, id=i))

    reply_child_sock.close()

    # Send message to each child
    for child in children:
        await send_message(child.send_sock, f"Hello from parent to child-{child.id}")

    # Read replies from shared socket
    for _ in range(num_children):
        response = await reply_reader.readline()
        print("Parent received:", response.decode().strip())

    # Send shutdown message
    for child in children:
        await send_message(child.send_sock, "exit")
        child.send_sock.close()

    # Close reply stream
    transport.close()
    reply_parent_sock.close()

    for child in children:
        child.process.join()

if __name__ == "__main__":
    asyncio.run(run())
