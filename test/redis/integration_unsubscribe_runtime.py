#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Integration test: unsubscribe at runtime should stop receiving.

Flow:
- start listener, subscribe to "topic_sub_rt"
- start sender, send => listener receives (count=1)
- call listener.unsubscribe("topic_sub_rt")
- sender sends again => listener should NOT receive (count stays 1)

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

    sender_name, sender_topic = "sender_it_unsub", "topic_sender_it_unsub"
    listener_name, listener_topic = "listener_it_unsub", "topic_listener_it_unsub"
    sub_topic = "topic_sub_rt"

    sender_addr = f"localhost:{_free_port()}"
    listener_addr = f"localhost:{_free_port()}"

    # Clean sender state best-effort.
    s = liner.Client(sender_name, sender_topic, sender_addr, redis_url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    # Listener.
    recv_lock = threading.Lock()
    recv_count = 0
    got_first = threading.Event()

    l = liner.Client(listener_name, listener_topic, listener_addr, redis_url)

    def on_recv(_to: str, _from: str, data: bytes):
        nonlocal recv_count
        with recv_lock:
            recv_count += 1
        _log(f"[listener] recv {data!r} count={recv_count}")
        got_first.set()

    assert l.run(on_recv), "listener failed to run"
    assert l.subscribe(sub_topic), "subscribe failed"

    # Sender routing.
    assert s.run(lambda _to, _from, _data: None), "sender failed to run"
    s.refresh_address_topic(sub_topic)

    _log("[sender] send #1")
    assert s.send_to(sub_topic, b"one", True), "send_to #1 failed"
    _wait_until(lambda: got_first.is_set(), timeout_s=10.0, what="first receive")

    _log("[listener] unsubscribe runtime")
    assert l.unsubscribe(sub_topic), "unsubscribe failed"

    with recv_lock:
        baseline = recv_count
    got_first.clear()

    # Send again; should not be received.
    s.refresh_address_topic(sub_topic)
    _log("[sender] send #2 after unsubscribe")
    assert not s.send_to(sub_topic, b"two", True), "send_to should fail: no subscribers on topic"
    time.sleep(0.5)
    with recv_lock:
        assert recv_count == baseline, f"unexpected receive after unsubscribe; delta={recv_count - baseline}"

    l.close()
    s.close()
    _log("OK integration_unsubscribe_runtime")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

