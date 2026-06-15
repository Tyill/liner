#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Integration test: at-least-once offline delivery.

It starts a sender client, sends to a listener topic while the listener is offline,
asserts the payload is persisted in Redis, then starts the listener and asserts:
- payload is delivered
- Redis pending queue is drained

The test will auto-start Redis via Docker if needed (same approach as offline_delivery_simple.py).
"""

import os
import sys
import time
import socket
import atexit
import subprocess
import datetime
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

    sender_name, sender_topic = "sender_it", "topic_sender_it"
    listener_name, listener_topic = "listener_it", "topic_listener_it"
    sender_addr = f"localhost:{_free_port()}"
    listener_addr = f"localhost:{_free_port()}"

    # Clean sender state (best-effort).
    s = liner.Client(sender_name, sender_topic, sender_addr, redis_url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    # Register listener mapping in redis but keep the listener offline.
    _redis_cmd("DEL", f"lnr_topic:{listener_topic}:addr")
    _redis_cmd("HSET", f"lnr_topic:{listener_topic}:addr", listener_addr, listener_name)

    assert s.run(lambda _to, _from, _data: None), "sender failed to run"
    s.refresh_address_topic(listener_topic)

    payload = b"offline_it"
    _log(f"[sender] send_to while offline payload={payload!r}")
    assert s.send_to(listener_topic, payload, True), "send_to failed"

    conn_key_str = _redis_cmd(
        "GET", f"lnr_connection:{sender_name}:{sender_topic}:{listener_name}:key"
    )
    assert conn_key_str, "missing connection key"
    conn_key = int(conn_key_str)
    list_key = f"lnr_connection:{conn_key}:messages"

    _wait_until(
        lambda: int(_redis_cmd("LLEN", list_key)) > 0,
        timeout_s=8.0,
        what="redis persisted message",
    )

    got = {"data": None}

    def on_recv(_to: str, _from: str, data: bytes):
        got["data"] = data
        _log(f"[listener] recv data={data!r}")

    l = liner.Client(listener_name, listener_topic, listener_addr, redis_url)
    assert l.run(on_recv), "listener failed to run"

    _wait_until(lambda: got["data"] is not None, timeout_s=25.0, what="listener receive")
    assert got["data"] == payload, f"unexpected payload: {got['data']!r}"

    _wait_until(lambda: int(_redis_cmd("LLEN", list_key)) == 0, timeout_s=25.0, what="redis drain")
    _log("OK integration_offline_delivery_at_least_once")

    l.close()
    s.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

