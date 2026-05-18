#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Listener restart must not re-deliver — PostgreSQL."""

import sys
import threading
import time
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_PATH.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from python import liner  # noqa: E402

from _support import ensure_release_lib, free_port, log, register_peer_catalog, postgres_session, wait_until  # noqa: E402


def main() -> int:
    liner.loadLib(str(ensure_release_lib()))
    with postgres_session() as url:
        sender_name, sender_topic = "sender_it_dedup", "topic_sender_it_dedup"
        listener_name, listener_topic = "listener_it_dedup", "topic_listener_it_dedup"

        sender_addr = f"127.0.0.1:{free_port()}"
        listener_addr = f"127.0.0.1:{free_port()}"

        register_peer_catalog(url, [(listener_topic, listener_addr, listener_name)])
        s = liner.Client.new_postgres(sender_name, sender_topic, sender_addr, url)
        s.clear_stored_messages()
        s.clear_addresses_of_topic()

        assert s.run(lambda _to, _from, _data: None), "sender failed to run"
        s.refresh_address_topic(listener_topic)

        recv_lock = threading.Lock()
        recv_count = 0
        first = threading.Event()

        def mk_listener():
            l = liner.Client.new_postgres(listener_name, listener_topic, listener_addr, url)

            def on_recv(_to: str, _from: str, data: bytes):
                nonlocal recv_count
                with recv_lock:
                    recv_count += 1
                log(f"[listener] recv {data!r} count={recv_count}")
                first.set()

            assert l.run(on_recv), "listener failed to run"
            return l

        l = mk_listener()

        payload = b"dedup"
        log("[sender] send_to once")
        assert s.send_to(listener_topic, payload, True), "send_to failed"

        wait_until(lambda: first.is_set(), timeout_s=10.0, what="first receive")

        l.close()
        time.sleep(0.8)
        first.clear()
        l2 = mk_listener()

        time.sleep(3.0)
        with recv_lock:
            assert recv_count == 1, f"expected no duplicate delivery, got {recv_count}"

        l2.close()
        s.close()
        log("OK integration_no_duplicate_on_listener_restart (postgres)")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
