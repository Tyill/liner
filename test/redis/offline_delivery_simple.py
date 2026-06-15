#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import time
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_PATH.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODULE_PATH))

from python import liner  # noqa: E402

from _support import (  # noqa: E402
    REDIS_URL,
    _ensure_redis,
    _ensure_release_lib,
    _free_port,
    _log,
    _redis_cmd,
    _wait_until,
)


if __name__ == "__main__":
    liner.loadLib(str(_ensure_release_lib()))
    _ensure_redis()

    sender_name, sender_topic = "sender_simple", "topic_sender_simple"
    listener_name, listener_topic = "listener_simple", "topic_listener_simple"

    sender_addr = f"localhost:{_free_port()}"
    listener_addr = f"localhost:{_free_port()}"

    s = liner.Client(sender_name, sender_topic, sender_addr, REDIS_URL)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    _redis_cmd("DEL", f"lnr_topic:{listener_topic}:addr")
    _redis_cmd("HSET", f"lnr_topic:{listener_topic}:addr", listener_addr, listener_name)

    assert s.run(lambda _to, _from, _data: None), "sender failed to run"
    s.refresh_address_topic(listener_topic)

    payload = b"offline_simple"
    _log(f"[sender] send_to {listener_topic} while offline payload={payload!r}")
    assert s.send_to(listener_topic, payload, True), "send_to failed"

    conn_key_str = _redis_cmd("GET", f"lnr_connection:{sender_name}:{sender_topic}:{listener_name}:key")
    assert conn_key_str, "missing connection key"
    conn_key = int(conn_key_str)
    list_key = f"lnr_connection:{conn_key}:messages"

    _wait_until(lambda: int(_redis_cmd("LLEN", list_key)) > 0, timeout_s=8.0, what="redis persisted message")
    pending = int(_redis_cmd("LLEN", list_key))
    _log(f"[redis] conn_key={conn_key} pending={pending}")

    got = {"data": None}

    def on_recv(_to: str, _from: str, data: bytes):
        got["data"] = data
        _log(f"[listener] recv from={_from} data={data!r}")

    l = liner.Client(listener_name, listener_topic, listener_addr, REDIS_URL)
    assert l.run(on_recv), "listener failed to run"

    _wait_until(lambda: got["data"] is not None, timeout_s=25.0, what="listener receive after offline")
    assert got["data"] == payload, f"unexpected payload: {got['data']!r}"

    _wait_until(lambda: int(_redis_cmd("LLEN", list_key)) == 0, timeout_s=25.0, what="redis queue drain")
    _log("OK offline_delivery_simple")

    l.close()
    s.close()
