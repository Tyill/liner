#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Integration test: concurrent subscribe/unsubscribe while messages are being sent.

Goal: ensure no deadlocks/crashes, and delivery continues.

Flow:
- start listener
- in one thread: toggle subscribe/unsubscribe on a topic for a while
- start sender and spam messages to that topic
- assert test finishes and listener received at least some messages

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
PROJECT_ROOT = MODULE_PATH.parent
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

    sender_name, sender_topic = "sender_it_toggle", "topic_sender_it_toggle"
    listener_name, listener_topic = "listener_it_toggle", "topic_listener_it_toggle"
    topic = "topic_toggle"

    sender_addr = f"localhost:{_free_port()}"
    listener_addr = f"localhost:{_free_port()}"

    # Clean sender state best-effort.
    s = liner.Client(sender_name, sender_topic, sender_addr, redis_url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    recv_lock = threading.Lock()
    recv_count = 0

    l = liner.Client(listener_name, listener_topic, listener_addr, redis_url)

    def on_recv(_to: str, _from: str, _data: bytes):
        nonlocal recv_count
        with recv_lock:
            recv_count += 1

    assert l.run(on_recv), "listener failed to run"

    stop = threading.Event()

    def toggler():
        # Toggle for ~3 seconds.
        end = time.time() + 3.0
        on = False
        while time.time() < end:
            if on:
                l.unsubscribe(topic)
            else:
                l.subscribe(topic)
            on = not on
            time.sleep(0.03)
        stop.set()

    t = threading.Thread(target=toggler, daemon=True)
    t.start()

    assert s.run(lambda _to, _from, _data: None), "sender failed to run"
    s.refresh_address_topic(topic)

    sent = 0
    while not stop.is_set():
        s.send_to(topic, b"x", True)
        sent += 1
        if sent % 100 == 0:
            s.refresh_address_topic(topic)

    t.join(timeout=2.0)
    time.sleep(0.6)

    with recv_lock:
        assert recv_count > 0, "expected to receive at least some messages"

    l.close()
    s.close()
    _log(f"OK integration_concurrent_sub_unsub recv={recv_count} sent~={sent}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

