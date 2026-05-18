#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Concurrent subscribe/unsubscribe while sending — shared PostgreSQL."""

import sys
import threading
import time
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_PATH.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from python import liner  # noqa: E402

from _support import ensure_release_lib, free_port, log, postgres_session, wait_until  # noqa: E402


def main() -> int:
    liner.loadLib(str(ensure_release_lib()))
    with postgres_session() as url:
        sender_name, sender_topic = "sender_it_toggle", "topic_sender_it_toggle"
        listener_name, listener_topic = "listener_it_toggle", "topic_listener_it_toggle"
        topic = "topic_toggle"

        sender_addr = f"127.0.0.1:{free_port()}"
        listener_addr = f"127.0.0.1:{free_port()}"

        s = liner.Client.new_postgres(sender_name, sender_topic, sender_addr, url)
        s.clear_stored_messages()
        s.clear_addresses_of_topic()

        recv_lock = threading.Lock()
        recv_count = 0

        l = liner.Client.new_postgres(listener_name, listener_topic, listener_addr, url)

        def on_recv(_to: str, _from: str, _data: bytes):
            nonlocal recv_count
            with recv_lock:
                recv_count += 1

        assert l.run(on_recv), "listener failed to run"

        stop = threading.Event()

        def toggler():
            end = time.time() + 3.0
            on = False
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

        assert s.run(lambda _to, _from, _data: None), "sender failed to run"
        s.refresh_address_topic(topic)

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
        log(f"OK integration_concurrent_sub_unsub (postgres) recv={recv_count} sent~={sent}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
