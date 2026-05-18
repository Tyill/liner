#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import sys
import time
import socket
import atexit
import subprocess
import datetime

module_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, str(Path(module_path).resolve().parent.parent.parent))

from python import liner


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
    """
    Minimal Redis client via RESP over TCP.
    """

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
    return subprocess.check_output(["docker", *cmd], stderr=subprocess.STDOUT).decode("utf-8", errors="replace").strip()


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


if __name__ == "__main__":
    liner.loadLib(str(Path(module_path).resolve().parent.parent.parent / "target/release/libliner_broker.so"))
    _ensure_redis()

    redis_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/"

    sender_name, sender_topic = "sender_simple", "topic_sender_simple"
    listener_name, listener_topic = "listener_simple", "topic_listener_simple"

    sender_addr = f"localhost:{_free_port()}"
    listener_addr = f"localhost:{_free_port()}"

    # Clean previous sender state (messages/listeners mapping).
    s = liner.Client(sender_name, sender_topic, sender_addr, redis_url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    # Register listener mapping in redis but don't start listener process yet.
    _redis_cmd("DEL", f"lnr_topic:{listener_topic}:addr")
    _redis_cmd("HSET", f"lnr_topic:{listener_topic}:addr", listener_addr, listener_name)

    ok = s.run(lambda _to, _from, _data: None)
    assert ok, "sender failed to run"
    s.refresh_address_topic(listener_topic)

    payload = b"offline_simple"
    _log(f"[sender] send_to {listener_topic} while offline payload={payload!r}")
    assert s.send_to(listener_topic, payload, True), "send_to failed"

    # Connection key is deterministic.
    conn_key_str = _redis_cmd("GET", f"lnr_connection:{sender_name}:{sender_topic}:{listener_name}:key")
    assert conn_key_str, "missing connection key"
    conn_key = int(conn_key_str)
    list_key = f"lnr_connection:{conn_key}:messages"

    _wait_until(lambda: int(_redis_cmd("LLEN", list_key)) > 0, timeout_s=8.0, what="redis persisted message")
    pending = int(_redis_cmd("LLEN", list_key))
    _log(f"[redis] conn_key={conn_key} pending={pending}")

    # Start listener and expect delivery (sender retries connect every ~10s).
    got = {"data": None}

    def on_recv(_to: str, _from: str, data: bytes):
        got["data"] = data
        _log(f"[listener] recv from={_from} data={data!r}")

    l = liner.Client(listener_name, listener_topic, listener_addr, redis_url)
    ok = l.run(on_recv)
    assert ok, "listener failed to run"

    _wait_until(lambda: got["data"] is not None, timeout_s=25.0, what="listener receive after offline")
    assert got["data"] == payload, f"unexpected payload: {got['data']!r}"

    _wait_until(lambda: int(_redis_cmd("LLEN", list_key)) == 0, timeout_s=25.0, what="redis queue drain")
    _log("OK offline_delivery_simple")

    l.close()
    s.close()

