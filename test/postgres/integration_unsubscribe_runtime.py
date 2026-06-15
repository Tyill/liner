#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Runtime unsubscribe stops new deliveries — PostgreSQL."""

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
        sender_name, sender_topic = "sender_it_unsub", "topic_sender_it_unsub"
        listener_name, listener_topic = "listener_it_unsub", "topic_listener_it_unsub"
        sub_topic = "topic_sub_rt"

        sender_addr = f"127.0.0.1:{free_port()}"
        listener_addr = f"127.0.0.1:{free_port()}"

        s = liner.Client.new_postgres(sender_name, sender_topic, sender_addr, url)
        s.clear_stored_messages()
        s.clear_addresses_of_topic()

        recv_lock = threading.Lock()
        recv_count = 0
        got_first = threading.Event()

        l = liner.Client.new_postgres(listener_name, listener_topic, listener_addr, url)

        def on_recv(_to: str, _from: str, data: bytes):
            nonlocal recv_count
            with recv_lock:
                recv_count += 1
            log(f"[listener] recv {data!r} count={recv_count}")
            got_first.set()

        assert l.run(on_recv), "listener failed to run"
        assert l.subscribe(sub_topic), "subscribe failed"

        assert s.run(lambda _to, _from, _data: None), "sender failed to run"
        s.refresh_address_topic(sub_topic)

        log("[sender] send #1")
        assert s.send_to(sub_topic, b"one", True), "send_to #1 failed"
        wait_until(lambda: got_first.is_set(), timeout_s=10.0, what="first receive")

        log("[listener] unsubscribe runtime")
        assert l.unsubscribe(sub_topic), "unsubscribe failed"

        with recv_lock:
            baseline = recv_count
        got_first.clear()

        s.refresh_address_topic(sub_topic)
        log("[sender] send #2 after unsubscribe")
        assert not s.send_to(sub_topic, b"two", True), "send_to should fail: no subscribers on topic"
        time.sleep(0.5)
        with recv_lock:
            assert recv_count == baseline, f"unexpected receive after unsubscribe; delta={recv_count - baseline}"

        l.close()
        s.close()
        log("OK integration_unsubscribe_runtime (postgres)")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
