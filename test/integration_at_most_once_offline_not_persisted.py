#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Integration test: at_most_once offline send should not be persisted/delivered later.

Flow:
- start sender
- register listener mapping in Redis but keep listener offline
- sender refreshes routing and sends with at_least_once_delivery=False
- assert redis pending queue length stays 0 (or key missing)
- start listener and assert no delivery occurs

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

    sender_name, sender_topic = "sender_it_amo", "topic_sender_it_amo"
    listener_name, listener_topic = "listener_it_amo", "topic_listener_it_amo"
    sender_addr = f"localhost:{_free_port()}"
    listener_addr = f"localhost:{_free_port()}"

    s = liner.Client(sender_name, sender_topic, sender_addr, redis_url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    # Register listener mapping but don't start it.
    _redis_cmd("DEL", f"lnr_topic:{listener_topic}:addr")
    _redis_cmd("HSET", f"lnr_topic:{listener_topic}:addr", listener_addr, listener_name)

    assert s.run(lambda _to, _from, _data: None), "sender failed to run"
    s.refresh_address_topic(listener_topic)

    payload = b"at_most_once"
    _log("[sender] send while listener offline with at_least_once_delivery=False")
    assert s.send_to(listener_topic, payload, False), "send_to failed"

    # If a connection key exists, ensure its message list is empty.
    conn_key_str = _redis_cmd(
        "GET", f"lnr_connection:{sender_name}:{sender_topic}:{listener_name}:key"
    )
    if conn_key_str:
        conn_key = int(conn_key_str)
        list_key = f"lnr_connection:{conn_key}:messages"

        def _empty():
            try:
                return int(_redis_cmd("LLEN", list_key)) == 0
            except Exception:
                return True

        _wait_until(_empty, timeout_s=5.0, what="redis list empty for at_most_once")

    # Start listener and ensure no delivery occurs.
    got = threading.Event()

    l = liner.Client(listener_name, listener_topic, listener_addr, redis_url)

    def on_recv(_to: str, _from: str, data: bytes):
        _log(f"[listener] unexpected recv {data!r}")
        got.set()

    assert l.run(on_recv), "listener failed to run"
    time.sleep(3.0)
    assert not got.is_set(), "unexpected delivery for at_most_once offline send"

    l.close()
    s.close()
    _log("OK integration_at_most_once_offline_not_persisted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

