#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Integration test: listener should not re-deliver already delivered messages after restart.

Flow:
- start sender and listener
- send exactly one message
- wait listener receives it (count=1), then stop listener
- start listener again with same identity
- wait a bit and assert receive count did not increase (no duplicate delivery)

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

    sender_name, sender_topic = "sender_it_dedup", "topic_sender_it_dedup"
    listener_name, listener_topic = "listener_it_dedup", "topic_listener_it_dedup"

    sender_addr = f"localhost:{_free_port()}"
    listener_addr = f"localhost:{_free_port()}"

    # Clean sender state best-effort.
    s = liner.Client(sender_name, sender_topic, sender_addr, redis_url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    # Ensure mapping exists.
    _redis_cmd("DEL", f"lnr_topic:{listener_topic}:addr")
    _redis_cmd("HSET", f"lnr_topic:{listener_topic}:addr", listener_addr, listener_name)

    assert s.run(lambda _to, _from, _data: None), "sender failed to run"
    s.refresh_address_topic(listener_topic)

    recv_lock = threading.Lock()
    recv_count = 0
    first = threading.Event()

    def mk_listener():
        nonlocal recv_count
        l = liner.Client(listener_name, listener_topic, listener_addr, redis_url)

        def on_recv(_to: str, _from: str, data: bytes):
            nonlocal recv_count
            with recv_lock:
                recv_count += 1
            _log(f"[listener] recv {data!r} count={recv_count}")
            first.set()

        assert l.run(on_recv), "listener failed to run"
        return l

    l = mk_listener()

    payload = b"dedup"
    _log("[sender] send_to once")
    assert s.send_to(listener_topic, payload, True), "send_to failed"

    _wait_until(lambda: first.is_set(), timeout_s=10.0, what="first receive")

    # Stop listener and restart; should not re-deliver the same message.
    l.close()
    time.sleep(0.8)
    first.clear()
    l2 = mk_listener()

    # Wait a bit: no new messages expected.
    time.sleep(3.0)
    with recv_lock:
        assert recv_count == 1, f"expected no duplicate delivery, got {recv_count}"

    l2.close()
    s.close()
    _log("OK integration_no_duplicate_on_listener_restart")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

