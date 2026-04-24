#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import time
import subprocess
import threading
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_PATH.parent
sys.path.insert(0, str(PROJECT_ROOT))

from python import liner  # noqa: E402


def ensure_release_lib():
    lib_path = PROJECT_ROOT / "target" / "release" / "libliner_broker.so"
    if lib_path.exists():
        return lib_path
    subprocess.run(
        ["cargo", "build", "--release"],
        cwd=str(PROJECT_ROOT),
        check=True,
    )
    if not lib_path.exists():
        raise RuntimeError(f"release library not found at {lib_path}")
    return lib_path


def spawn_client1(*extra_args: str):
    client_process = str((MODULE_PATH / "client_process.py").resolve())
    return subprocess.Popen(
        [
            client_process,
            "--client-name=client1",
            "--client-topic=topic1",
            "--client-addr=localhost:2255",
            *extra_args,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def main() -> int:
    # Requires a local Redis at redis://localhost/
    lib_path = ensure_release_lib()
    liner.loadLib(str(lib_path))

    got = threading.Event()
    recv_count = 0
    lock = threading.Lock()

    h2 = liner.Client("client2", "topic2", "localhost:2256", "redis://localhost/")

    def rcb(to: str, from_: str, data_: bytes):
        nonlocal recv_count
        with lock:
            recv_count += 1
        got.set()

    assert h2.run(rcb)

    # Phase 1: subscribed => should echo back.
    c1 = spawn_client1("--subscr-topic=topic_sub")
    try:
        time.sleep(1.5)
        got.clear()
        assert h2.send_to("topic_sub", b"one", True)
        assert got.wait(timeout=3.0), "expected echo while subscribed"
    finally:
        try:
            c1.kill()
        except Exception:
            pass
        try:
            c1.wait(timeout=2)
        except Exception:
            pass

    time.sleep(0.8)
    with lock:
        baseline = recv_count

    # Phase 2: unsubscribed => should NOT echo back.
    c1 = spawn_client1("--unsubscr-topic=topic_sub")
    try:
        time.sleep(1.5)
        # Ensure client2 refreshes topic routing state after listener changes subscription.
        h2.refresh_address_topic("topic_sub")
        got.clear()
        assert h2.send_to("topic_sub", b"two", True)
        if got.wait(timeout=2.0):
            out = ""
            if c1.stdout:
                try:
                    out = c1.stdout.read()
                except Exception:
                    out = ""
            raise AssertionError(
                f"unexpected echo after unsubscribe; client1 output:\n{out}"
            )
    finally:
        try:
            c1.kill()
        except Exception:
            pass
        try:
            c1.wait(timeout=2)
        except Exception:
            pass

    h2.close()

    with lock:
        assert recv_count == baseline, f"expected no new echoes, got {recv_count - baseline}"
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

