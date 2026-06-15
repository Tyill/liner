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


def _mk_msg(i: int) -> bytes:
    return f"msg-{i:06d}".encode("utf-8")


if __name__ == "__main__":
    liner.loadLib(str(_ensure_release_lib()))
    _ensure_redis()

    c1_name, c1_topic = "client1", "topic1"
    c2_name, c2_topic = "client2", "topic2"
    c1_addr = f"localhost:{_free_port()}"
    c2_addr = f"localhost:{_free_port()}"

    # Clean previous state for these clients/topics.
    c1 = liner.Client(c1_name, c1_topic, c1_addr, REDIS_URL)
    c1.clear_addresses_of_topic()
    c1.clear_stored_messages()

    c2 = liner.Client(c2_name, c2_topic, c2_addr, REDIS_URL)
    c2.clear_addresses_of_topic()
    c2.clear_stored_messages()

    recv_lock = threading.Lock()
    recv_while_online: list[bytes] = []
    recv_after_reconnect: list[bytes] = []
    sent_by_c1: list[bytes] = []

    def c2_cb_online(_to: str, _from: str, data: bytes):
        with recv_lock:
            recv_while_online.append(data)
        _log(f"[client2 online] recv from={_from} data={data!r}")

    ok = c1.run(lambda _to, _from, _data: None)
    assert ok, "client1 failed to run"
    ok = c2.run(c2_cb_online)
    assert ok, "client2 failed to run"

    # Phase 1: online delivery, every second, numbered messages.
    n_online = 5
    for i in range(1, n_online + 1):
        payload = _mk_msg(i)
        sent_by_c1.append(payload)
        _log(f"[client1] send -> {c2_topic} data={payload!r}")
        assert c1.send_to(c2_topic, payload, True), f"send_to failed for {payload!r}"
        _wait_until(lambda: len(recv_while_online) >= i, timeout_s=3.0, what=f"client2 receive msg {i}")
        time.sleep(1.0)

    with recv_lock:
        assert recv_while_online == [_mk_msg(i) for i in range(1, n_online + 1)], (
            f"client2 got unexpected messages online: {recv_while_online!r}"
        )

    # Phase 2: disconnect client2.
    _log("[test] close client2")
    c2.close()
    del c2

    # Give sender thread time to notice broken stream / reconnect loop.
    time.sleep(0.5)

    # Phase 3: client1 sends a few messages while client2 offline, then client1 also disconnects.
    n_offline_before_c1_off = 3
    for j in range(1, n_offline_before_c1_off + 1):
        i = n_online + j
        payload = _mk_msg(i)
        sent_by_c1.append(payload)
        _log(f"[client1] send -> {c2_topic} data={payload!r} (client2 offline)")
        assert c1.send_to(c2_topic, payload, True), f"send_to failed for {payload!r}"
        time.sleep(1.0)

    # Allow sender to fail-connect and flush unsent to Redis before stopping client1.
    time.sleep(2.0)
    _log("[test] close client1")
    c1.close()
    del c1

    # Phase 4: client1 reconnects (still with client2 offline) and sends a few more messages.
    time.sleep(0.5)
    c1 = liner.Client(c1_name, c1_topic, c1_addr, REDIS_URL)
    ok = c1.run(lambda _to, _from, _data: None)
    assert ok, "client1 failed to run after reconnect"

    n_offline_after_c1_on = 2
    for j in range(1, n_offline_after_c1_on + 1):
        i = n_online + n_offline_before_c1_off + j
        payload = _mk_msg(i)
        sent_by_c1.append(payload)
        _log(f"[client1] send -> {c2_topic} data={payload!r} (client2 offline, after c1 restart)")
        assert c1.send_to(c2_topic, payload, True), f"send_to failed for {payload!r}"
        time.sleep(1.0)

    # Allow sender to fail-connect and flush unsent to Redis.
    time.sleep(2.0)

    # Determine connection key (unique numeric id) for sender->listener pair.
    conn_key_str = _redis_cmd("GET", f"lnr_connection:{c1_name}:{c1_topic}:{c2_name}:key")
    assert conn_key_str is not None and conn_key_str != "", "missing connection key in Redis"
    conn_key = int(conn_key_str)

    pending_len = int(_redis_cmd("LLEN", f"lnr_connection:{conn_key}:messages"))
    assert pending_len >= 1, "expected some pending messages in Redis"
    _log(f"[redis] conn_key={conn_key} pending_before_reconnect={pending_len}")

    # Phase 5: reconnect client2, expect it to receive everything sent while it was offline.
    c2 = liner.Client(c2_name, c2_topic, c2_addr, REDIS_URL)

    def c2_cb_reconnected(_to: str, _from: str, data: bytes):
        with recv_lock:
            recv_after_reconnect.append(data)
        _log(f"[client2 reconnected] recv from={_from} data={data!r}")

    ok = c2.run(c2_cb_reconnected)
    assert ok, "client2 failed to run after reconnect"

    expected_total = n_online + n_offline_before_c1_off + n_offline_after_c1_on
    expected_all = [_mk_msg(i) for i in range(1, expected_total + 1)]

    # Wait until we receive the whole offline tail (both batches).
    n_offline_total = n_offline_before_c1_off + n_offline_after_c1_on
    _wait_until(
        lambda: len(recv_after_reconnect) >= n_offline_total,
        timeout_s=15.0,
        what="client2 receive messages after reconnect",
    )

    with recv_lock:
        got_online = recv_while_online[:]
        got_reconn = recv_after_reconnect[:]

    # Strict end-to-end check: everything client1 sent must be received by client2 across both phases.
    assert sent_by_c1 == expected_all, f"internal: sent list mismatch, sent={sent_by_c1!r}"
    assert got_online + got_reconn == expected_all, (
        "client2 did not receive full ordered stream.\n"
        f"got_online={got_online!r}\n"
        f"got_reconn={got_reconn!r}\n"
        f"expected={expected_all!r}"
    )

    # Pending list in Redis should be drained after successful reconnect+send.
    _wait_until(
        lambda: int(_redis_cmd("LLEN", f"lnr_connection:{conn_key}:messages")) == 0,
        timeout_s=10.0,
        what="redis pending queue drain",
    )
    _log("[redis] pending_after_reconnect=0")

    c2.close()
    c1.close()

    _log("OK offline_delivery")
