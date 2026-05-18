#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""At-most-once offline send must not persist — PostgreSQL."""

import sys
import threading
import time
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_PATH.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from python import liner  # noqa: E402

from _support import ensure_release_lib, free_port, log, register_peer_catalog, postgres_session  # noqa: E402


def main() -> int:
    liner.loadLib(str(ensure_release_lib()))
    with postgres_session() as url:
        sender_name, sender_topic = "sender_it_amo", "topic_sender_it_amo"
        listener_name, listener_topic = "listener_it_amo", "topic_listener_it_amo"
        sender_addr = f"127.0.0.1:{free_port()}"
        listener_addr = f"127.0.0.1:{free_port()}"

        register_peer_catalog(url, [(listener_topic, listener_addr, listener_name)])
        s = liner.Client.new_postgres(sender_name, sender_topic, sender_addr, url)
        s.clear_stored_messages()
        s.clear_addresses_of_topic()

        assert s.run(lambda _to, _from, _data: None), "sender failed to run"
        s.refresh_address_topic(listener_topic)

        payload = b"at_most_once"
        log("[sender] send while listener offline with at_least_once_delivery=False")
        assert s.send_to(listener_topic, payload, False), "send_to failed"

        got = threading.Event()

        l = liner.Client.new_postgres(listener_name, listener_topic, listener_addr, url)

        def on_recv(_to: str, _from: str, data: bytes):
            log(f"[listener] unexpected recv {data!r}")
            got.set()

        assert l.run(on_recv), "listener failed to run"
        time.sleep(3.0)
        assert not got.is_set(), "unexpected delivery for at_most_once offline send"

        l.close()
        s.close()
        log("OK integration_at_most_once_offline_not_persisted (postgres)")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
