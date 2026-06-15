#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import threading
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


def _conn_key(sender_name: str, sender_topic: str, listener_name: str) -> int:
    s = _redis_cmd("GET", f"lnr_connection:{sender_name}:{sender_topic}:{listener_name}:key")
    assert s, "missing connection key"
    return int(s)


def _pending_len(conn_key: int) -> int:
    return int(_redis_cmd("LLEN", f"lnr_connection:{conn_key}:messages"))


def _mk_payload(i: int) -> bytes:
    return f"msg-{i:06d}".encode("utf-8")


def test_offline_batch_100(redis_url: str):
    sender_name, sender_topic = "batch_sender", "topic_batch_sender"
    listener_name, listener_topic = "batch_listener", "topic_batch_listener"

    sender_addr = f"localhost:{_free_port()}"
    listener_addr = f"localhost:{_free_port()}"

    s = liner.Client(sender_name, sender_topic, sender_addr, redis_url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    _redis_cmd("DEL", f"lnr_topic:{listener_topic}:addr")
    _redis_cmd("HSET", f"lnr_topic:{listener_topic}:addr", listener_addr, listener_name)

    assert s.run(lambda _to, _from, _data: None)
    s.refresh_address_topic(listener_topic)

    n = 100
    for i in range(1, n + 1):
        assert s.send_to(listener_topic, _mk_payload(i), True)

    ck = _conn_key(sender_name, sender_topic, listener_name)
    _wait_until(lambda: _pending_len(ck) >= n, timeout_s=15.0, what="redis pending batch")
    _log(f"[batch] persisted pending={_pending_len(ck)}")

    got: list[bytes] = []
    got_lock = threading.Lock()

    def on_recv(_to: str, _from: str, data: bytes):
        with got_lock:
            got.append(data)

    l = liner.Client(listener_name, listener_topic, listener_addr, redis_url)
    assert l.run(on_recv)

    _wait_until(lambda: (len(got) >= n), timeout_s=40.0, what="batch receive all")
    with got_lock:
        assert got[:n] == [_mk_payload(i) for i in range(1, n + 1)], "batch order mismatch"
    _wait_until(lambda: _pending_len(ck) == 0, timeout_s=40.0, what="batch redis drain")

    l.close()
    s.close()
    _log("OK test_offline_batch_100")


def test_compressed_large_payload(redis_url: str):
    sender_name, sender_topic = "comp_sender", "topic_comp_sender"
    listener_name, listener_topic = "comp_listener", "topic_comp_listener"

    sender_addr = f"localhost:{_free_port()}"
    listener_addr = f"localhost:{_free_port()}"

    s = liner.Client(sender_name, sender_topic, sender_addr, redis_url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    _redis_cmd("DEL", f"lnr_topic:{listener_topic}:addr")
    _redis_cmd("HSET", f"lnr_topic:{listener_topic}:addr", listener_addr, listener_name)

    assert s.run(lambda _to, _from, _data: None)
    s.refresh_address_topic(listener_topic)

    # > MIN_SIZE_DATA_FOR_COMPRESS_BYTE (currently 1MiB). Use 2MiB.
    payload = (b"abcd" * (2 * 1024 * 1024 // 4))
    assert len(payload) >= 2 * 1024 * 1024

    assert s.send_to(listener_topic, payload, True)

    ck = _conn_key(sender_name, sender_topic, listener_name)
    _wait_until(lambda: _pending_len(ck) >= 1, timeout_s=10.0, what="redis pending comp")

    got = {"data": None}

    def on_recv(_to: str, _from: str, data: bytes):
        got["data"] = data

    l = liner.Client(listener_name, listener_topic, listener_addr, redis_url)
    assert l.run(on_recv)

    _wait_until(lambda: got["data"] is not None, timeout_s=40.0, what="receive compressed payload")
    assert got["data"] == payload, "payload mismatch after compress/decompress"
    _wait_until(lambda: _pending_len(ck) == 0, timeout_s=40.0, what="comp redis drain")

    l.close()
    s.close()
    _log("OK test_compressed_large_payload")


def test_send_all_two_listeners_offline(redis_url: str):
    sender_name, sender_topic = "all_sender", "topic_all_sender"

    l1_name = "all_l1"
    l2_name = "all_l2"

    sender_addr = f"localhost:{_free_port()}"
    l1_addr = f"localhost:{_free_port()}"
    l2_addr = f"localhost:{_free_port()}"

    s = liner.Client(sender_name, sender_topic, sender_addr, redis_url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()

    # Both listeners share same topic (receiver topic). Sender will send_all to this topic.
    recv_topic = "topic_all_recv"
    _redis_cmd("DEL", f"lnr_topic:{recv_topic}:addr")
    _redis_cmd("HSET", f"lnr_topic:{recv_topic}:addr", l1_addr, l1_name)
    _redis_cmd("HSET", f"lnr_topic:{recv_topic}:addr", l2_addr, l2_name)

    assert s.run(lambda _to, _from, _data: None)
    s.refresh_address_topic(recv_topic)

    payload = b"send_all_offline"
    assert s.send_all(recv_topic, payload, True)

    ck1 = _conn_key(sender_name, sender_topic, l1_name)
    ck2 = _conn_key(sender_name, sender_topic, l2_name)
    _wait_until(lambda: _pending_len(ck1) >= 1, timeout_s=10.0, what="redis pending l1")
    _wait_until(lambda: _pending_len(ck2) >= 1, timeout_s=10.0, what="redis pending l2")

    got1 = {"data": None}
    got2 = {"data": None}

    l1 = liner.Client(l1_name, recv_topic, l1_addr, redis_url)
    assert l1.run(lambda _to, _from, data: got1.__setitem__("data", data))
    l2 = liner.Client(l2_name, recv_topic, l2_addr, redis_url)
    assert l2.run(lambda _to, _from, data: got2.__setitem__("data", data))

    _wait_until(lambda: got1["data"] is not None and got2["data"] is not None, timeout_s=40.0, what="both listeners receive")
    assert got1["data"] == payload
    assert got2["data"] == payload

    _wait_until(lambda: _pending_len(ck1) == 0, timeout_s=40.0, what="l1 redis drain")
    _wait_until(lambda: _pending_len(ck2) == 0, timeout_s=40.0, what="l2 redis drain")

    l1.close()
    l2.close()
    s.close()
    _log("OK test_send_all_two_listeners_offline")


if __name__ == "__main__":
    liner.loadLib(str(_ensure_release_lib()))
    _ensure_redis()

    test_offline_batch_100(REDIS_URL)
    test_compressed_large_payload(REDIS_URL)
    test_send_all_two_listeners_offline(REDIS_URL)
    _log("OK offline_delivery_more")

