import asyncio
import multiprocessing
import os
import socket
import sys
from dataclasses import dataclass
from typing import List

from ipc_tools import (
    asend_pickled,
    create_socketpair,
    read_pickled,
    read_pickled_async,
    send_pickled,
)


@dataclass
class ChildHandle:
    process: multiprocessing.Process
    command_sock: socket.socket
    id: int


def child_entry(command_fd: int, reply_fd: int, child_id: int) -> None:
    reply_sock = socket.socket(fileno=reply_fd)
    reply_sock.setblocking(True)

    while True:
        try:
            msg = read_pickled(command_fd)
        except Exception as e:
            print(f"child-{child_id}: read failed: {e}", file=sys.stderr)
            break

        print(f"child-{child_id}: received {msg[:100]}", file=sys.stderr)
        if msg == "exit":
            break

        try:
            send_pickled(reply_sock, f"child-{child_id}: got len={len(msg)} msg:'{msg}'")
        except Exception as e:
            print(f"child-{child_id}: failed to send reply: {e}", file=sys.stderr)
            os._exit(1)

    print(f"child-{child_id}: exiting", file=sys.stderr)
    reply_sock.close()
    os._exit(0)


async def run() -> None:
    num_children = 3
    children: List[ChildHandle] = []

    reply_parent_sock, reply_child_sock = create_socketpair()
    reply_parent_sock.setblocking(False)

    loop = asyncio.get_running_loop()
    reply_reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reply_reader)
    transport, _ = await loop.connect_accepted_socket(
        lambda: protocol, reply_parent_sock
    )

    for i in range(num_children):
        cmd_parent_sock, cmd_child_sock = create_socketpair()
        cmd_parent_sock.setblocking(False)

        p = multiprocessing.Process(
            target=child_entry,
            args=(cmd_child_sock.fileno(), reply_child_sock.fileno(), i),
        )
        p.start()
        cmd_child_sock.close()

        children.append(ChildHandle(process=p, command_sock=cmd_parent_sock, id=i))

    # Keep reply_child_sock open until children are done

    for child in children:
        await asend_pickled(
            child.command_sock, f"Hello from parent to child-{child.id}"# + "X" * 312992
        )

    for _ in range(num_children):
        try:
            response = await read_pickled_async(reply_reader)
            print("Parent received:", response[:100])
        except EOFError as e:
            print("Parent read error:", e)

    for child in children:
        await asend_pickled(child.command_sock, "exit")
        child.command_sock.close()

    # Now it's safe to close the child end of the reply socket
    reply_child_sock.close()

    transport.close()
    reply_parent_sock.close()

    for child in children:
        child.process.join()


if __name__ == "__main__":
    asyncio.run(run())
