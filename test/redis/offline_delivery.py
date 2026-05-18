#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import sys
import time
import threading
import socket
import atexit
import datetime

module_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, str(Path(module_path).resolve().parent.parent.parent))

from python import liner


REDIS_HOST = "127.0.0.1"
REDIS_PORT = int(os.environ.get("LINER_TEST_REDIS_PORT", "16379"))


def _redis_cmd(*args: str):
    """
    Minimal Redis client via RESP over TCP.
    Assumes Redis listens on localhost:6379 (same as used by redis://localhost/).
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
            _ = read_exact(2)  # \r\n
            return data.decode("utf-8", errors="replace")
        if first == b"-":
            raise RuntimeError("redis error: " + read_line().decode("utf-8", errors="replace"))
        raise RuntimeError(f"unknown redis reply prefix: {first!r}")


def _wait_until(pred, timeout_s: float, sleep_s: float = 0.02, what: str = "condition"):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if pred():
            return
        time.sleep(sleep_s)
    raise TimeoutError(f"timeout waiting for {what}")


def _mk_msg(i: int) -> bytes:
    return f"msg-{i:06d}".encode("utf-8")

def _log(msg: str):
    # Local time with millisecond precision: YYYY-MM-DD HH:MM:SS.mmm
    now = datetime.datetime.now()
    ts = now.strftime("%Y-%m-%d %H:%M:%S") + f".{int(now.microsecond / 1000):03d}"
    print(f"[{ts}] {msg}", flush=True)


if __name__ == "__main__":
    liner.loadLib(str(Path(module_path).resolve().parent.parent.parent / "target/release/libliner_broker.so"))

    # Ensure Redis is available for the test. Prefer running an isolated instance via Docker.
    redis_container = os.environ.get("LINER_TEST_REDIS_CONTAINER", "liner-test-redis")
    redis_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/"

    def _docker(*cmd: str):
        return subprocess.check_output(["docker", *cmd], stderr=subprocess.STDOUT).decode("utf-8", errors="replace").strip()

    def _ensure_redis():
        try:
            _redis_cmd("PING")
            return
        except Exception:
            pass

        # Cleanup if container already exists.
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

    import subprocess  # kept local to avoid top-level import ordering surprises

    _ensure_redis()

    c1_name, c1_topic, c1_addr = "client1", "topic1", "localhost:2255"
    c2_name, c2_topic, c2_addr = "client2", "topic2", "localhost:2256"

    # Clean previous state for these clients/topics.
    c1 = liner.Client(c1_name, c1_topic, c1_addr, redis_url)
    c1.clear_addresses_of_topic()
    c1.clear_stored_messages()

    c2 = liner.Client(c2_name, c2_topic, c2_addr, redis_url)
    c2.clear_addresses_of_topic()
    c2.clear_stored_messages()

    recv_lock = threading.Lock()
    recv_while_online: list[bytes] = []
    recv_after_reconnect: list[bytes] = []
    sent_by_c1: list[bytes] = []

    def c2_cb_online(_to: str, _from: str, data: bytes):
        with recv_lock:
            recv_while_online.append(data)
        _log(f"[client2 online] recv from={_from} data={data!r}")

    ok = c1.run(lambda _to, _from, _data: None)
    assert ok, "client1 failed to run"
    ok = c2.run(c2_cb_online)
    assert ok, "client2 failed to run"

    # Phase 1: online delivery, every second, numbered messages.
    n_online = 5
    for i in range(1, n_online + 1):
        payload = _mk_msg(i)
        sent_by_c1.append(payload)
        _log(f"[client1] send -> {c2_topic} data={payload!r}")
        assert c1.send_to(c2_topic, payload, True), f"send_to failed for {payload!r}"
        _wait_until(lambda: len(recv_while_online) >= i, timeout_s=3.0, what=f"client2 receive msg {i}")
        time.sleep(1.0)

    with recv_lock:
        assert recv_while_online == [_mk_msg(i) for i in range(1, n_online + 1)], (
            f"client2 got unexpected messages online: {recv_while_online!r}"
        )

    # Phase 2: disconnect client2.
    _log("[test] close client2")
    c2.close()
    del c2

    # Give sender thread time to notice broken stream / reconnect loop.
    time.sleep(0.5)

    # Phase 3: client1 sends a few messages while client2 offline, then client1 also disconnects.
    n_offline_before_c1_off = 3
    for j in range(1, n_offline_before_c1_off + 1):
        i = n_online + j
        payload = _mk_msg(i)
        sent_by_c1.append(payload)
        _log(f"[client1] send -> {c2_topic} data={payload!r} (client2 offline)")
        assert c1.send_to(c2_topic, payload, True), f"send_to failed for {payload!r}"
        time.sleep(1.0)

    # Allow sender to fail-connect and flush unsent to Redis before stopping client1.
    time.sleep(2.0)
    _log("[test] close client1")
    c1.close()
    del c1

    # Phase 4: client1 reconnects (still with client2 offline) and sends a few more messages.
    time.sleep(0.5)
    c1 = liner.Client(c1_name, c1_topic, c1_addr, redis_url)
    ok = c1.run(lambda _to, _from, _data: None)
    assert ok, "client1 failed to run after reconnect"

    n_offline_after_c1_on = 2
    for j in range(1, n_offline_after_c1_on + 1):
        i = n_online + n_offline_before_c1_off + j
        payload = _mk_msg(i)
        sent_by_c1.append(payload)
        _log(f"[client1] send -> {c2_topic} data={payload!r} (client2 offline, after c1 restart)")
        assert c1.send_to(c2_topic, payload, True), f"send_to failed for {payload!r}"
        time.sleep(1.0)

    # Allow sender to fail-connect and flush unsent to Redis.
    time.sleep(2.0)

    # Determine connection key (unique numeric id) for sender->listener pair.
    conn_key_str = _redis_cmd("GET", f"lnr_connection:{c1_name}:{c1_topic}:{c2_name}:key")
    assert conn_key_str is not None and conn_key_str != "", "missing connection key in Redis"
    conn_key = int(conn_key_str)

    pending_len = int(_redis_cmd("LLEN", f"lnr_connection:{conn_key}:messages"))
    assert pending_len >= 1, "expected some pending messages in Redis"
    _log(f"[redis] conn_key={conn_key} pending_before_reconnect={pending_len}")

    # Phase 5: reconnect client2, expect it to receive everything sent while it was offline.
    c2 = liner.Client(c2_name, c2_topic, c2_addr, redis_url)

    def c2_cb_reconnected(_to: str, _from: str, data: bytes):
        with recv_lock:
            recv_after_reconnect.append(data)
        _log(f"[client2 reconnected] recv from={_from} data={data!r}")

    ok = c2.run(c2_cb_reconnected)
    assert ok, "client2 failed to run after reconnect"

    expected_total = n_online + n_offline_before_c1_off + n_offline_after_c1_on
    expected_all = [_mk_msg(i) for i in range(1, expected_total + 1)]

    # Wait until we receive the whole offline tail (both batches).
    n_offline_total = n_offline_before_c1_off + n_offline_after_c1_on
    _wait_until(
        lambda: len(recv_after_reconnect) >= n_offline_total,
        timeout_s=15.0,
        what="client2 receive messages after reconnect",
    )

    with recv_lock:
        got_online = recv_while_online[:]
        got_reconn = recv_after_reconnect[:]

    # Strict end-to-end check: everything client1 sent must be received by client2 across both phases.
    assert sent_by_c1 == expected_all, f"internal: sent list mismatch, sent={sent_by_c1!r}"
    assert got_online + got_reconn == expected_all, (
        "client2 did not receive full ordered stream.\n"
        f"got_online={got_online!r}\n"
        f"got_reconn={got_reconn!r}\n"
        f"expected={expected_all!r}"
    )

    # Pending list in Redis should be drained after successful reconnect+send.
    _wait_until(
        lambda: int(_redis_cmd("LLEN", f"lnr_connection:{conn_key}:messages")) == 0,
        timeout_s=10.0,
        what="redis pending queue drain",
    )
    _log("[redis] pending_after_reconnect=0")

    c2.close()
    c1.close()

    _log("OK offline_delivery")
