#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Offline send then reply after peer comes online — PostgreSQL."""

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
    log,
    register_peer_catalog,
    postgres_session,
    wait_until,
)


def main() -> int:
    liner.loadLib(str(ensure_release_lib()))
    with postgres_session() as url:
        client1_name, topic1 = "client1_it_rl", "topic1_it_rl"
        client2_name, topic2 = "client2_it_rl", "topic2_it_rl"

        addr1 = f"127.0.0.1:{free_port()}"
        addr2 = f"127.0.0.1:{free_port()}"

        register_peer_catalog(url, [(topic1, addr1, client1_name)])
        c2 = liner.Client.new_postgres(client2_name, topic2, addr2, url)
        c2.clear_stored_messages()
        c2.clear_addresses_of_topic()

        assert c2.run(lambda _to, _from, _data: None), "client2 failed to run"
        c2.refresh_address_topic(topic1)
        payload = b"repeat_last"
        log(f"[client2] send_to {topic1} while client1 offline payload={payload!r}")
        assert c2.send_to(topic1, payload, True), "send_to failed"

        c2.close()
        time.sleep(0.5)

        c1 = liner.Client.new_postgres(client1_name, topic1, addr1, url)

        def echo_cb(_to: str, from_: str, data_: bytes):
            c1.send_to(from_, data_, True)

        assert c1.run(echo_cb), "client1 failed to run"

        got = {"data": None}
        ev = threading.Event()

        c2b = liner.Client.new_postgres(client2_name, topic2, addr2, url)

        def on_recv(_to: str, _from: str, data: bytes):
            got["data"] = data
            log(f"[client2] recv reply data={data!r}")
            ev.set()

        assert c2b.run(on_recv), "client2 restart failed to run"
        c2b.refresh_address_topic(topic1)
        wait_until(lambda: ev.is_set(), timeout_s=25.0, what="client2 receive reply")
        assert got["data"] == payload, f"unexpected reply payload: {got['data']!r}"

        c2b.close()
        c1.close()
        log("OK integration_repeat_last_reply (postgres)")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
