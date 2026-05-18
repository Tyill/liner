#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import threading
import time
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_PATH.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from python import liner  # noqa: E402

from _support import (  # noqa: E402
    ensure_release_lib,
    free_port,
    get_connection_key,
    log,
    register_peer_catalog,
    pending_messages_count,
    postgres_session,
    wait_until,
)


def _mk_payload(i: int) -> bytes:
    return f"msg-{i:06d}".encode("utf-8")


def test_offline_batch_100(url: str) -> None:
    sender_name, sender_topic = "batch_sender", "topic_batch_sender"
    listener_name, listener_topic = "batch_listener", "topic_batch_listener"

    sender_addr = f"127.0.0.1:{free_port()}"
    listener_addr = f"127.0.0.1:{free_port()}"

    register_peer_catalog(url, [(listener_topic, listener_addr, listener_name)])
    s = liner.Client.new_postgres(sender_name, sender_topic, sender_addr, url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()
    assert s.run(lambda _to, _from, _data: None)
    s.refresh_address_topic(listener_topic)

    n = 100
    for i in range(1, n + 1):
        assert s.send_to(listener_topic, _mk_payload(i), True)

    got: list[bytes] = []
    got_lock = threading.Lock()

    def on_recv(_to: str, _from: str, data: bytes):
        with got_lock:
            got.append(data)

    l = liner.Client.new_postgres(listener_name, listener_topic, listener_addr, url)
    assert l.run(on_recv)

    wait_until(lambda: len(got) >= n, timeout_s=40.0, what="batch receive all")
    with got_lock:
        assert got[:n] == [_mk_payload(i) for i in range(1, n + 1)], "batch order mismatch"

    l.close()
    s.close()
    time.sleep(0.2)
    ck = get_connection_key(url, sender_name, sender_topic, listener_name)
    if ck is not None:
        wait_until(
            lambda: pending_messages_count(url, ck) == 0,
            timeout_s=40.0,
            what="batch postgres drain",
        )
    log("OK test_offline_batch_100 (postgres)")


def test_compressed_large_payload(url: str) -> None:
    sender_name, sender_topic = "comp_sender", "topic_comp_sender"
    listener_name, listener_topic = "comp_listener", "topic_comp_listener"

    sender_addr = f"127.0.0.1:{free_port()}"
    listener_addr = f"127.0.0.1:{free_port()}"

    register_peer_catalog(url, [(listener_topic, listener_addr, listener_name)])
    s = liner.Client.new_postgres(sender_name, sender_topic, sender_addr, url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()
    assert s.run(lambda _to, _from, _data: None)
    s.refresh_address_topic(listener_topic)

    payload = b"abcd" * (2 * 1024 * 1024 // 4)
    assert len(payload) >= 2 * 1024 * 1024

    assert s.send_to(listener_topic, payload, True)

    got = {"data": None}

    def on_recv(_to: str, _from: str, data: bytes):
        got["data"] = data

    l = liner.Client.new_postgres(listener_name, listener_topic, listener_addr, url)
    assert l.run(on_recv)

    wait_until(lambda: got["data"] is not None, timeout_s=40.0, what="receive compressed payload")
    assert got["data"] == payload, "payload mismatch after compress/decompress"

    l.close()
    s.close()
    time.sleep(0.2)
    ck = get_connection_key(url, sender_name, sender_topic, listener_name)
    if ck is not None:
        wait_until(
            lambda: pending_messages_count(url, ck) == 0,
            timeout_s=40.0,
            what="comp postgres drain",
        )
    log("OK test_compressed_large_payload (postgres)")


def test_send_all_two_listeners_offline(url: str) -> None:
    sender_name, sender_topic = "all_sender", "topic_all_sender"

    l1_name = "all_l1"
    l2_name = "all_l2"

    sender_addr = f"127.0.0.1:{free_port()}"
    l1_addr = f"127.0.0.1:{free_port()}"
    l2_addr = f"127.0.0.1:{free_port()}"

    recv_topic = "topic_all_recv"
    register_peer_catalog(url, [(recv_topic, l1_addr, l1_name), (recv_topic, l2_addr, l2_name)])

    s = liner.Client.new_postgres(sender_name, sender_topic, sender_addr, url)
    s.clear_stored_messages()
    s.clear_addresses_of_topic()
    assert s.run(lambda _to, _from, _data: None)
    s.refresh_address_topic(recv_topic)

    payload = b"send_all_offline"
    assert s.send_all(recv_topic, payload, True)

    got1 = {"data": None}
    got2 = {"data": None}

    l1 = liner.Client.new_postgres(l1_name, recv_topic, l1_addr, url)
    assert l1.run(lambda _to, _from, data: got1.__setitem__("data", data))
    l2 = liner.Client.new_postgres(l2_name, recv_topic, l2_addr, url)
    assert l2.run(lambda _to, _from, data: got2.__setitem__("data", data))

    wait_until(
        lambda: got1["data"] is not None and got2["data"] is not None,
        timeout_s=40.0,
        what="both listeners receive",
    )
    assert got1["data"] == payload
    assert got2["data"] == payload

    l1.close()
    l2.close()
    s.close()
    time.sleep(0.2)
    ck1 = get_connection_key(url, sender_name, sender_topic, l1_name)
    ck2 = get_connection_key(url, sender_name, sender_topic, l2_name)
    if ck1 is not None:
        wait_until(lambda: pending_messages_count(url, ck1) == 0, timeout_s=40.0, what="l1 postgres drain")
    if ck2 is not None:
        wait_until(lambda: pending_messages_count(url, ck2) == 0, timeout_s=40.0, what="l2 postgres drain")
    log("OK test_send_all_two_listeners_offline (postgres)")


def main() -> int:
    liner.loadLib(str(ensure_release_lib()))
    with postgres_session() as url:
        test_offline_batch_100(url)
        test_compressed_large_payload(url)
        test_send_all_two_listeners_offline(url)
        log("OK offline_delivery_more (postgres)")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
