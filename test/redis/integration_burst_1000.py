#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Integration test: burst delivery (N messages) end-to-end.

Flow:
- start listener subscribed to burst topic
- start sender, send N messages quickly (at-least-once)
- assert listener receives exactly N within timeout

Requires Redis. Auto-starts Redis via Docker if needed.
"""

import os
import sys
import time
import socket
import atexit
import subprocess
import datetime
import threading
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_PATH.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from python import liner  # noqa: E402


REDIS_HOST = "127.0.0.1"
REDIS_PORT = int(os.environ.get("LINER_TEST_REDIS_PORT", "16379"))


def _log(msg: str):
    now = datetime.datetime.now()
    ts = now.strftime("%Y-%m-%d %H:%M:%S") + f".{int(now.microsecond / 1000):03d}"
    print(f"[{ts}] {msg}", flush=True)


def _wait_until(pred, timeout_s: float, sleep_s: float = 0.05, what: str = "condition"):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if pred():
            return
        time.sleep(sleep_s)
    raise TimeoutError(f"timeout waiting for {what}")


def _redis_cmd(*args: str):
    def enc_bulk(s: bytes) -> bytes:
        return b"$" + str(len(s)).encode("ascii") + b"\r\n" + s + b"\r\n"

    payload = b"*" + str(len(args)).encode("ascii") + b"\r\n"
    for a in args:
        payload += enc_bulk(a.encode("utf-8"))

    with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=2.0) as sock:
        sock.sendall(payload)

        def read_line() -> bytes:
            buf = b""
            while not buf.endswith(b"\r\n"):
                chunk = sock.recv(1)
                if not chunk:
                    raise ConnectionError("unexpected EOF from redis")
                buf += chunk
            return buf[:-2]

        def read_exact(n: int) -> bytes:
            buf = b""
            while len(buf) < n:
                chunk = sock.recv(n - len(buf))
                if not chunk:
                    raise ConnectionError("unexpected EOF from redis")
                buf += chunk
            return buf

        first = read_exact(1)
        if first == b"+":
            return read_line().decode("utf-8", errors="replace")
        if first == b":":
            return int(read_line())
        if first == b"$":
            n = int(read_line())
            if n == -1:
                return None
            data = read_exact(n)
            _ = read_exact(2)
            return data.decode("utf-8", errors="replace")
        if first == b"-":
            raise RuntimeError("redis error: " + read_line().decode("utf-8", errors="replace"))
        raise RuntimeError(f"unknown redis reply prefix: {first!r}")


def _docker(*cmd: str) -> str:
    return (
        subprocess.check_output(["docker", *cmd], stderr=subprocess.STDOUT)
        .decode("utf-8", errors="replace")
        .strip()
    )


def _ensure_redis():
    try:
        if _redis_cmd("PING") == "PONG":
            return
    except Exception:
        pass

    redis_container = os.environ.get("LINER_TEST_REDIS_CONTAINER", "liner-test-redis")
    try:
        _docker("rm", "-f", redis_container)
    except Exception:
        pass

    _docker(
        "run",
        "--rm",
        "-d",
        "--name",
        redis_container,
        "-p",
        f"{REDIS_PORT}:6379",
        "redis:7-alpine",
    )

    def _cleanup():
        try:
            _docker("rm", "-f", redis_container)
        except Exception:
            pass

    atexit.register(_cleanup)

    def _ping_ok() -> bool:
        try:
            return _redis_cmd("PING") == "PONG"
        except Exception:
            return False

    _wait_until(_ping_ok, timeout_s=15.0, what="redis PING")


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return int(port)


def _ensure_release_lib():
    lib_path = PROJECT_ROOT / "target" / "release" / "libliner_broker.so"
    if lib_path.exists():
        return lib_path
    subprocess.run(["cargo", "build", "--release"], cwd=str(PROJECT_ROOT), check=True)
    if not lib_path.exists():
        raise RuntimeError(f"release library not found at {lib_path}")
    return lib_path


def main() -> int:
    liner.loadLib(str(_ensure_release_lib()))
    _ensure_redis()
    redis_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/"

    N = int(os.environ.get("LINER_TEST_BURST_N", "1000"))

    sender_name, sender_topic = "sender_it_burst", "topic_sender_it_burst"
    listener_name, listener_topic = "listener_it_burst", "topic_listener_it_burst"
    burst_topic = "topic_burst"

    sender_addr = f"localhost:{_free_port()}"
    listener_addr = f"localhost:{_free_port()}"

    # Clean sender state best-effort.
    s = liner.Client(sender_name, sender_topic, sender_addr, redis_url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    # Listener.
    lock = threading.Lock()
    seen = set()
    done = threading.Event()

    l = liner.Client(listener_name, listener_topic, listener_addr, redis_url)

    def on_recv(_to: str, _from: str, data: bytes):
        nonlocal seen
        # payload is "i:<num>"
        with lock:
            seen.add(data)
            if len(seen) >= N:
                done.set()

    assert l.run(on_recv), "listener failed to run"
    assert l.subscribe(burst_topic), "subscribe failed"

    assert s.run(lambda _to, _from, _data: None), "sender failed to run"
    s.refresh_address_topic(burst_topic)

    _log(f"[sender] burst send N={N}")
    for i in range(N):
        payload = f"i:{i}".encode("utf-8")
        assert s.send_to(burst_topic, payload, True)

    _wait_until(lambda: done.is_set(), timeout_s=25.0, what="receive burst")

    with lock:
        assert len(seen) == N, f"expected {N}, got {len(seen)}"

    l.close()
    s.close()
    _log("OK integration_burst_1000")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

