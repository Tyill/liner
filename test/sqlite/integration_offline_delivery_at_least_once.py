#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""At-least-once offline delivery — SQLite (catalog via ``receivers_json``)."""

import sys
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
    peer_catalog_json,
    pending_messages_count,
    temp_shared_db,
    wait_until,
)


def main() -> int:
    liner.loadLib(str(ensure_release_lib()))
    db_path, cleanup = temp_shared_db()
    try:
        sender_name, sender_topic = "sender_it", "topic_sender_it"
        listener_name, listener_topic = "listener_it", "topic_listener_it"
        sender_addr = f"127.0.0.1:{free_port()}"
        listener_addr = f"127.0.0.1:{free_port()}"

        s = liner.Client.new_sqlite(
            sender_name,
            sender_topic,
            sender_addr,
            db_path,
            peer_catalog_json([(listener_topic, listener_addr, listener_name)]),
        )
        s.clear_stored_messages()
        s.clear_addresses_of_topic()

        assert s.run(lambda _to, _from, _data: None), "sender failed to run"
        s.refresh_address_topic(listener_topic)

        payload = b"offline_it"
        log(f"[sender] send_to while offline payload={payload!r}")
        assert s.send_to(listener_topic, payload, True), "send_to failed"

        got = {"data": None}

        def on_recv(_to: str, _from: str, data: bytes):
            got["data"] = data
            log(f"[listener] recv data={data!r}")

        l = liner.Client.new_sqlite(listener_name, listener_topic, listener_addr, db_path, "")
        assert l.run(on_recv), "listener failed to run"

        wait_until(lambda: got["data"] is not None, timeout_s=25.0, what="listener receive")
        assert got["data"] == payload, f"unexpected payload: {got['data']!r}"

        l.close()
        s.close()
        time.sleep(0.25)
        ck = get_connection_key(db_path, sender_name, sender_topic, listener_name)
        if ck is not None:
            wait_until(
                lambda: pending_messages_count(db_path, ck) == 0,
                timeout_s=25.0,
                what="sqlite drain",
            )
        log("OK integration_offline_delivery_at_least_once (sqlite)")

        return 0
    finally:
        cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
