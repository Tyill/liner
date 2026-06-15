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



sys.path.insert(0, str(MODULE_PATH))

from _support import (  # noqa: E402
    REDIS_URL,
    _ensure_redis,
    _ensure_release_lib,
    _free_port,
    _log,
    _redis_cmd,
    _wait_until,
)

def main() -> int:
    liner.loadLib(str(_ensure_release_lib()))
    _ensure_redis()
    redis_url = REDIS_URL

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

