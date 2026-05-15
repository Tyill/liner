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
    pending_messages_count,
    temp_shared_db,
    wait_until,
)


def _mk_msg(i: int) -> bytes:
    return f"msg-{i:06d}".encode("utf-8")


def main() -> int:
    liner.loadLib(str(ensure_release_lib()))
    db_path, cleanup = temp_shared_db()
    try:
        c1_addr = f"127.0.0.1:{free_port()}"
        c2_addr = f"127.0.0.1:{free_port()}"
        c1_name, c1_topic = "client1", "topic1"
        c2_name, c2_topic = "client2", "topic2"

        c1 = liner.Client.new_sqlite(c1_name, c1_topic, c1_addr, db_path, "")
        c1.clear_addresses_of_topic()
        c1.clear_stored_messages()

        c2 = liner.Client.new_sqlite(c2_name, c2_topic, c2_addr, db_path, "")
        c2.clear_addresses_of_topic()
        c2.clear_stored_messages()

        recv_lock = threading.Lock()
        recv_while_online: list[bytes] = []
        recv_after_reconnect: list[bytes] = []
        sent_by_c1: list[bytes] = []

        def c2_cb_online(_to: str, _from: str, data: bytes):
            with recv_lock:
                recv_while_online.append(data)
            log(f"[client2 online] recv from={_from} data={data!r}")

        assert c1.run(lambda _to, _from, _data: None)
        assert c2.run(c2_cb_online)

        n_online = 5
        for i in range(1, n_online + 1):
            payload = _mk_msg(i)
            sent_by_c1.append(payload)
            log(f"[client1] send -> {c2_topic} data={payload!r}")
            assert c1.send_to(c2_topic, payload, True)
            wait_until(lambda: len(recv_while_online) >= i, timeout_s=3.0, what=f"client2 receive msg {i}")
            time.sleep(1.0)

        with recv_lock:
            assert recv_while_online == [_mk_msg(i) for i in range(1, n_online + 1)], (
                f"client2 got unexpected messages online: {recv_while_online!r}"
            )

        log("[test] close client2")
        c2.close()
        del c2

        time.sleep(0.5)

        n_offline_before_c1_off = 3
        for j in range(1, n_offline_before_c1_off + 1):
            i = n_online + j
            payload = _mk_msg(i)
            sent_by_c1.append(payload)
            log(f"[client1] send -> {c2_topic} data={payload!r} (client2 offline)")
            assert c1.send_to(c2_topic, payload, True)
            time.sleep(1.0)

        time.sleep(2.0)
        log("[test] close client1")
        c1.close()
        del c1

        time.sleep(0.5)
        c1 = liner.Client.new_sqlite(c1_name, c1_topic, c1_addr, db_path, "")
        assert c1.run(lambda _to, _from, _data: None)

        n_offline_after_c1_on = 2
        for j in range(1, n_offline_after_c1_on + 1):
            i = n_online + n_offline_before_c1_off + j
            payload = _mk_msg(i)
            sent_by_c1.append(payload)
            log(f"[client1] send -> {c2_topic} data={payload!r} (client2 offline, after c1 restart)")
            assert c1.send_to(c2_topic, payload, True)
            time.sleep(1.0)

        time.sleep(2.0)

        c2 = liner.Client.new_sqlite(c2_name, c2_topic, c2_addr, db_path, "")

        def c2_cb_reconnected(_to: str, _from: str, data: bytes):
            with recv_lock:
                recv_after_reconnect.append(data)
            log(f"[client2 reconnected] recv from={_from} data={data!r}")

        assert c2.run(c2_cb_reconnected)

        expected_total = n_online + n_offline_before_c1_off + n_offline_after_c1_on
        expected_all = [_mk_msg(i) for i in range(1, expected_total + 1)]
        n_offline_total = n_offline_before_c1_off + n_offline_after_c1_on
        wait_until(
            lambda: len(recv_after_reconnect) >= n_offline_total,
            timeout_s=15.0,
            what="client2 receive messages after reconnect",
        )

        with recv_lock:
            got_online = recv_while_online[:]
            got_reconn = recv_after_reconnect[:]

        assert sent_by_c1 == expected_all, f"internal: sent list mismatch, sent={sent_by_c1!r}"
        assert got_online + got_reconn == expected_all, (
            "client2 did not receive full ordered stream.\n"
            f"got_online={got_online!r}\n"
            f"got_reconn={got_reconn!r}\n"
            f"expected={expected_all!r}"
        )

        c2.close()
        c1.close()
        time.sleep(0.25)
        ck = get_connection_key(db_path, c1_name, c1_topic, c2_name)
        if ck is not None:
            wait_until(
                lambda: pending_messages_count(db_path, ck) == 0,
                timeout_s=20.0,
                what="sqlite pending queue drain",
            )
        log("[sqlite] pending_after_reconnect=0")

        log("OK offline_delivery (sqlite)")
        return 0
    finally:
        cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
