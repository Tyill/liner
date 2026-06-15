#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Integration test: concurrent subscribe/unsubscribe while messages are being sent.

Goal: ensure no deadlocks/crashes, and delivery continues.

Flow:
- start listener
- in one thread: toggle subscribe/unsubscribe on a topic for a while
- start sender and spam messages to that topic
- assert test finishes and listener received at least some messages

Requires Redis. Auto-starts Redis via Docker if needed.
"""

import sys
import threading
import time
import uuid
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

    suffix = uuid.uuid4().hex[:8]
    sender_name, sender_topic = f"sender_it_toggle_{suffix}", f"topic_sender_it_toggle_{suffix}"
    listener_name, listener_topic = f"listener_it_toggle_{suffix}", f"topic_listener_it_toggle_{suffix}"
    topic = f"topic_toggle_{suffix}"

    sender_addr = f"127.0.0.1:{_free_port()}"
    listener_addr = f"127.0.0.1:{_free_port()}"

    # Clean sender state best-effort.
    s = liner.Client(sender_name, sender_topic, sender_addr, redis_url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    recv_lock = threading.Lock()
    recv_count = 0

    l = liner.Client(listener_name, listener_topic, listener_addr, redis_url)

    def on_recv(_to: str, _from: str, _data: bytes):
        nonlocal recv_count
        with recv_lock:
            recv_count += 1

    assert l.run(on_recv), "listener failed to run"
    l.subscribe(topic)
    time.sleep(0.2)
    assert s.run(lambda _to, _from, _data: None), "sender failed to run"
    s.refresh_address_topic(topic)

    stop = threading.Event()

    def toggler():
        # Toggle for ~3 seconds. Start subscribed (see l.subscribe above).
        end = time.time() + 3.0
        on = True
        while time.time() < end:
            if on:
                l.unsubscribe(topic)
            else:
                l.subscribe(topic)
            on = not on
            time.sleep(0.03)
        stop.set()

    t = threading.Thread(target=toggler, daemon=True)
    t.start()

    sent = 0
    while not stop.is_set():
        s.send_to(topic, b"x", True)
        sent += 1
        if sent % 100 == 0:
            s.refresh_address_topic(topic)

    t.join(timeout=2.0)
    time.sleep(0.6)

    with recv_lock:
        assert recv_count > 0, "expected to receive at least some messages"

    l.close()
    s.close()
    _log(f"OK integration_concurrent_sub_unsub recv={recv_count} sent~={sent}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

