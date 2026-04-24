#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Integration test: "repeat last" for offline reply.

Scenario:
- client2 sends to topic1 while client1 is offline
- client2 closes
- client1 starts, receives, and replies to topic2 (echo)
- client2 starts again and should receive the reply that was sent while it was offline

Requires Redis. Will auto-start Redis via Docker if needed.
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

    client1_name, topic1 = "client1_it_rl", "topic1_it_rl"
    client2_name, topic2 = "client2_it_rl", "topic2_it_rl"

    addr1 = f"localhost:{_free_port()}"
    addr2 = f"localhost:{_free_port()}"

    # Clean mapping and stored messages best-effort.
    c2 = liner.Client(client2_name, topic2, addr2, redis_url)
    c2.clear_stored_messages()
    c2.clear_addresses_of_topic()

    # Prepare mapping for both topics.
    _redis_cmd("DEL", f"lnr_topic:{topic1}:addr")
    _redis_cmd("HSET", f"lnr_topic:{topic1}:addr", addr1, client1_name)
    _redis_cmd("DEL", f"lnr_topic:{topic2}:addr")
    _redis_cmd("HSET", f"lnr_topic:{topic2}:addr", addr2, client2_name)

    # Start client2, refresh routing, send to topic1, ensure it's persisted, then close client2.
    assert c2.run(lambda _to, _from, _data: None), "client2 failed to run"
    c2.refresh_address_topic(topic1)
    payload = b"repeat_last"
    _log(f"[client2] send_to {topic1} while client1 offline payload={payload!r}")
    assert c2.send_to(topic1, payload, True), "send_to failed"

    # Wait until message is persisted for client1 (since client1 is offline).
    conn12 = _redis_cmd("GET", f"lnr_connection:{client2_name}:{topic2}:{client1_name}:key")
    assert conn12, "missing connection key c2->c1"
    conn12 = int(conn12)
    list12 = f"lnr_connection:{conn12}:messages"
    _wait_until(lambda: int(_redis_cmd("LLEN", list12)) > 0, timeout_s=8.0, what="persisted c2->c1")

    c2.close()
    time.sleep(0.5)

    # Start client1 (echo server) in-process so it uses the same redis_url/port.
    c1 = liner.Client(client1_name, topic1, addr1, redis_url)

    def echo_cb(_to: str, from_: str, data_: bytes):
        # Echo back to the sender topic.
        c1.send_to(from_, data_, True)

    assert c1.run(echo_cb), "client1 failed to run"

    # Restart client2; it should deliver the stored message to client1, and get the echo back.
    got = {"data": None}
    ev = threading.Event()

    c2b = liner.Client(client2_name, topic2, addr2, redis_url)

    def on_recv(_to: str, _from: str, data: bytes):
        got["data"] = data
        _log(f"[client2] recv reply data={data!r}")
        ev.set()

    assert c2b.run(on_recv), "client2 restart failed to run"
    c2b.refresh_address_topic(topic1)
    _wait_until(lambda: ev.is_set(), timeout_s=25.0, what="client2 receive reply")
    assert got["data"] == payload, f"unexpected reply payload: {got['data']!r}"

    # Eventually the original offline queue should drain.
    _wait_until(lambda: int(_redis_cmd("LLEN", list12)) == 0, timeout_s=25.0, what="drain c2->c1")
    _log("OK integration_repeat_last_reply")
    c2b.close()
    c1.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

