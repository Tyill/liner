#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Burst delivery (N messages) using a shared SQLite store."""

import os
import sys
import threading
import time
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_PATH.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from python import liner  # noqa: E402

from _support import ensure_release_lib, free_port, log, temp_shared_db, wait_until  # noqa: E402


def main() -> int:
    liner.loadLib(str(ensure_release_lib()))
    db_path, cleanup = temp_shared_db()
    try:
        N = int(os.environ.get("LINER_TEST_BURST_N", "1000"))

        sender_name, sender_topic = "sender_it_burst", "topic_sender_it_burst"
        listener_name, listener_topic = "listener_it_burst", "topic_listener_it_burst"
        burst_topic = "topic_burst"

        sender_addr = f"127.0.0.1:{free_port()}"
        listener_addr = f"127.0.0.1:{free_port()}"

        s = liner.Client.new_sqlite(sender_name, sender_topic, sender_addr, db_path, "")
        s.clear_stored_messages()
        s.clear_addresses_of_topic()

        lock = threading.Lock()
        seen: set[bytes] = set()
        done = threading.Event()

        l = liner.Client.new_sqlite(listener_name, listener_topic, listener_addr, db_path, "")

        def on_recv(_to: str, _from: str, data: bytes):
            with lock:
                seen.add(data)
                if len(seen) >= N:
                    done.set()

        assert l.run(on_recv), "listener failed to run"
        assert l.subscribe(burst_topic), "subscribe failed"

        assert s.run(lambda _to, _from, _data: None), "sender failed to run"
        s.refresh_address_topic(burst_topic)

        log(f"[sender] burst send N={N}")
        for i in range(N):
            payload = f"i:{i}".encode("utf-8")
            assert s.send_to(burst_topic, payload, True)

        wait_until(lambda: done.is_set(), timeout_s=25.0, what="receive burst")

        with lock:
            assert len(seen) == N, f"expected {N}, got {len(seen)}"

        l.close()
        s.close()
        log("OK integration_burst_1000 (sqlite)")
        return 0
    finally:
        cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
