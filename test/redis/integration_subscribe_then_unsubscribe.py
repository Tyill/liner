#!/usr/bin/python3
# -*- coding: utf-8 -*-

import subprocess
import sys
import threading
import time
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_PATH.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODULE_PATH))

from python import liner  # noqa: E402

from _support import REDIS_URL, _ensure_redis, _ensure_release_lib, _free_port  # noqa: E402


def spawn_client1(client_addr: str, *extra_args: str):
    client_process = str((MODULE_PATH / "client_process.py").resolve())
    return subprocess.Popen(
        [
            sys.executable,
            client_process,
            "--client-name=client1",
            "--client-topic=topic1",
            "--client-addr",
            client_addr,
            f"--redis-url={REDIS_URL}",
            *extra_args,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def main() -> int:
    liner.loadLib(str(_ensure_release_lib()))
    _ensure_redis()

    p1 = _free_port()
    p2 = _free_port()
    addr1 = f"127.0.0.1:{p1}"
    addr2 = f"127.0.0.1:{p2}"

    got = threading.Event()
    recv_count = 0
    lock = threading.Lock()

    h2 = liner.Client("client2", "topic2", addr2, REDIS_URL)

    def rcb(to: str, from_: str, data_: bytes):
        nonlocal recv_count
        with lock:
            recv_count += 1
        got.set()

    assert h2.run(rcb)

    c1 = spawn_client1(addr1, "--subscr-topic=topic_sub")
    try:
        time.sleep(2.5)
        got.clear()
        h2.refresh_address_topic("topic_sub")
        assert h2.send_to("topic_sub", b"one", True)
        assert got.wait(timeout=3.0), "expected echo while subscribed"
    finally:
        # Free the port before the unsub helper binds the same address.
        try:
            c1.terminate()
        except Exception:
            pass
        try:
            c1.wait(timeout=2)
        except Exception:
            try:
                c1.kill()
            except Exception:
                pass
            try:
                c1.wait(timeout=1)
            except Exception:
                pass
        unsub_proc = spawn_client1(addr1, "--unsubscr-topic=topic_sub")
        try:
            time.sleep(1.0)
        finally:
            try:
                unsub_proc.terminate()
            except Exception:
                pass
            try:
                unsub_proc.wait(timeout=2)
            except Exception:
                try:
                    unsub_proc.kill()
                except Exception:
                    pass

    time.sleep(0.8)
    with lock:
        baseline = recv_count

    c1 = spawn_client1(addr1, "--unsubscr-topic=topic_sub")
    try:
        time.sleep(2.5)
        h2.refresh_address_topic("topic_sub")
        got.clear()
        assert not h2.send_to("topic_sub", b"two", True), (
            "send_to should fail: no subscribers on topic after unsubscribe"
        )
        time.sleep(0.5)
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
