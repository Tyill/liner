# -*- coding: utf-8 -*-
"""Shared helpers for Redis-backed integration tests."""

from __future__ import annotations

import atexit
import datetime
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_PATH.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

REDIS_HOST = "127.0.0.1"
REDIS_PORT = int(os.environ.get("LINER_TEST_REDIS_PORT", "16379"))
REDIS_URL = os.environ.get("LINER_TEST_REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/")


def log(msg: str) -> None:
    now = datetime.datetime.now()
    ts = now.strftime("%Y-%m-%d %H:%M:%S") + f".{int(now.microsecond / 1000):03d}"
    print(f"[{ts}] {msg}", flush=True)


def wait_until(pred, timeout_s: float, sleep_s: float = 0.05, what: str = "condition") -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if pred():
            return
        time.sleep(sleep_s)
    raise TimeoutError(f"timeout waiting for {what}")


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return int(port)


def redis_cmd(*args: str):
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
        if first == b"*":
            count = int(read_line())
            items = []
            for _ in range(count):
                p = read_exact(1)
                if p == b"+":
                    items.append(read_line().decode("utf-8", errors="replace"))
                elif p == b":":
                    items.append(int(read_line()))
                elif p == b"$":
                    n = int(read_line())
                    if n == -1:
                        items.append(None)
                    else:
                        data = read_exact(n)
                        _ = read_exact(2)
                        items.append(data.decode("utf-8", errors="replace"))
                else:
                    raise RuntimeError(f"unknown redis array element prefix: {p!r}")
            return items
        if first == b"-":
            raise RuntimeError("redis error: " + read_line().decode("utf-8", errors="replace"))
        raise RuntimeError(f"unknown redis reply prefix: {first!r}")


def docker(*cmd: str) -> str:
    return (
        subprocess.check_output(["docker", *cmd], stderr=subprocess.STDOUT)
        .decode("utf-8", errors="replace")
        .strip()
    )


def flush_liner_keys() -> None:
    """Drop ``lnr_*`` keys so tests do not cross-contaminate on a shared Redis."""
    keys = redis_cmd("KEYS", "lnr_*")
    if keys:
        redis_cmd("DEL", *keys)


def ensure_redis() -> None:
    try:
        if redis_cmd("PING") == "PONG":
            if os.environ.get("LINER_TEST_REDIS_NO_FLUSH", "") != "1":
                flush_liner_keys()
            return
    except Exception:
        pass

    redis_container = os.environ.get("LINER_TEST_REDIS_CONTAINER", "liner-test-redis")
    try:
        docker("rm", "-f", redis_container)
    except Exception:
        pass

    docker(
        "run",
        "--rm",
        "-d",
        "--name",
        redis_container,
        "-p",
        f"{REDIS_PORT}:6379",
        "redis:7-alpine",
    )

    def _cleanup() -> None:
        try:
            docker("rm", "-f", redis_container)
        except Exception:
            pass

    atexit.register(_cleanup)

    wait_until(lambda: redis_cmd("PING") == "PONG", timeout_s=15.0, what="redis PING")


def ensure_release_lib() -> Path:
    target_base = (
        Path(os.environ["CARGO_TARGET_DIR"]) if os.environ.get("CARGO_TARGET_DIR") else PROJECT_ROOT / "target"
    )
    lib_path = target_base / "release" / "libliner_broker.so"
    deps_lib = target_base / "release" / "deps" / "libliner_broker.so"
    if not lib_path.exists() and deps_lib.exists():
        shutil.copy2(deps_lib, lib_path)
    if lib_path.exists():
        return lib_path
    subprocess.run(["cargo", "build", "--release"], cwd=str(PROJECT_ROOT), check=True)
    if deps_lib.exists() and not lib_path.exists():
        shutil.copy2(deps_lib, lib_path)
    if not lib_path.exists():
        raise RuntimeError(f"release library not found at {lib_path}")
    return lib_path


_log = log
_wait_until = wait_until
_free_port = free_port
_redis_cmd = redis_cmd
_ensure_redis = ensure_redis
_ensure_release_lib = ensure_release_lib
