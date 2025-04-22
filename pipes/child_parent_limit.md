# Child-to-Parent Message Size Limits in Shared Reply Socket

## Background

This project uses a single shared socket for replies sent from multiple subprocesses (children) back to a parent process. Messages are encoded with a 4-byte big-endian length prefix and pickled payload. To maintain atomicity and protocol integrity, all replies are sent via `sock.send(...)`.

## The Issue

When multiple subprocesses write to the **same** reply socket concurrently, **POSIX only guarantees atomicity for messages up to `PIPE_BUF` bytes** (typically 4096 bytes).

If a child sends a message larger than that, the operating system may interleave parts of messages from different children, resulting in corrupted or undecodable responses in the parent.

## Reproduction Steps

You can reproduce a failed case by forcing a child to send a large response:

```python
reply_str = "A" * 313000
send_pickled(reply_sock, reply_str)
```

This triggers a partial write like:

```
child-2: failed to send reply: Partial send: only 73088 of 313061 bytes sent
child-1: failed to send reply: Partial send: only 292352 of 313061 bytes sent
```

This happens because `sock.send(data)` does **not** retry. It only performs a single system call:

- If the socket buffer is full or partially filled, only part of `data` may be written.
- No lock or coordination exists between writers.

## Resolution

To preserve protocol integrity:

- `send_pickled()` now asserts that the number of bytes actually sent matches the total size of the message.
- **Any message larger than what the kernel is willing to accept atomically will raise an exception.**

This ensures we fail early rather than corrupt the parent’s message stream.

## Why Not Use `sendall()` Here?

Because we’re intentionally relying on atomicity for protocol safety:

- `sendall()` would retry partial writes silently, but only for a **single writer**.
- In a shared socket with multiple writers, `sendall()` does **not** prevent another process from interleaving.

## Recommendation

If your messages may exceed 4 KiB after pickling:

- **Do not share a reply socket.**
- Give each child its own dedicated socket and read from them using separate asyncio tasks in the parent.

For most use cases where child replies are modest in size (status updates, logs, ack messages), the current shared socket design is robust and efficient.

## Related Info

Check your system socket buffer size:

```python
import socket
parent_sock, child_sock = socket.socketpair()
print(parent_sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF))
```

This returns something like `212992` (bytes), but that does **not** override `PIPE_BUF` atomicity rules.

Check `PIPE_BUF` explicitly:

```bash
getconf PIPE_BUF /dev/stdout
```

Returns:

```
4096
```

This is the hard upper bound for atomic multi-writer safety.
