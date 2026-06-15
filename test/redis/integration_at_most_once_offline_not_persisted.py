#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Integration test: at_most_once offline send should not be persisted/delivered later.

Flow:
- start sender
- register listener mapping in Redis but keep listener offline
- sender refreshes routing and sends with at_least_once_delivery=False
- assert redis pending queue length stays 0 (or key missing)
- start listener and assert no delivery occurs

Requires Redis. Auto-starts Redis via Docker if needed.
"""

import sys
import threading
import time
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

    sender_name, sender_topic = "sender_it_amo", "topic_sender_it_amo"
    listener_name, listener_topic = "listener_it_amo", "topic_listener_it_amo"
    sender_addr = f"localhost:{_free_port()}"
    listener_addr = f"localhost:{_free_port()}"

    s = liner.Client(sender_name, sender_topic, sender_addr, redis_url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    # Register listener mapping but don't start it.
    _redis_cmd("DEL", f"lnr_topic:{listener_topic}:addr")
    _redis_cmd("HSET", f"lnr_topic:{listener_topic}:addr", listener_addr, listener_name)

    assert s.run(lambda _to, _from, _data: None), "sender failed to run"
    s.refresh_address_topic(listener_topic)

    payload = b"at_most_once"
    _log("[sender] send while listener offline with at_least_once_delivery=False")
    assert s.send_to(listener_topic, payload, False), "send_to failed"

    # If a connection key exists, ensure its message list is empty.
    conn_key_str = _redis_cmd(
        "GET", f"lnr_connection:{sender_name}:{sender_topic}:{listener_name}:key"
    )
    if conn_key_str:
        conn_key = int(conn_key_str)
        list_key = f"lnr_connection:{conn_key}:messages"

        def _empty():
            try:
                return int(_redis_cmd("LLEN", list_key)) == 0
            except Exception:
                return True

        _wait_until(_empty, timeout_s=5.0, what="redis list empty for at_most_once")

    # Stop sender before listener comes online so nothing is in flight.
    s.close()

    got = threading.Event()

    l = liner.Client(listener_name, listener_topic, listener_addr, redis_url)

    def on_recv(_to: str, _from: str, data: bytes):
        _log(f"[listener] unexpected recv {data!r}")
        got.set()

    assert l.run(on_recv), "listener failed to run"
    time.sleep(3.0)
    assert not got.is_set(), "unexpected delivery for at_most_once offline send"

    l.close()
    _log("OK integration_at_most_once_offline_not_persisted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

