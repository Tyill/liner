#!/usr/bin/python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from python import liner  # noqa: E402


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-name", required=True)
    parser.add_argument("--client-topic", required=True)
    parser.add_argument("--client-addr", required=True)
    parser.add_argument("--postgres-url", required=True, help="Shared PostgreSQL URL (LINER_TEST_POSTGRES_URL)")
    parser.add_argument("--subscr-topic", default="")
    parser.add_argument("--unsubscr-topic", default="")

    args = parser.parse_args()

    lib = ROOT / "target" / "release" / "libliner_broker.so"
    liner.loadLib(str(lib))

    h = liner.Client.new_postgres(
        args.client_name,
        args.client_topic,
        args.client_addr,
        args.postgres_url,
    )
    h.clear_addresses_of_topic()
    h.clear_stored_messages()

    def receive_cback1(to: str, from_: str, data_: bytes):
        print(f"{args.client_name} receive_from {from_}, data: {data_}")
        h.send_to(from_, bytearray(data_), True)

    if args.subscr_topic:
        h.subscribe(args.subscr_topic)
    if args.unsubscr_topic:
        h.unsubscribe(args.unsubscr_topic)

    if not h.run(receive_cback1):
        raise SystemExit("liner run() failed")

    loop = asyncio.new_event_loop()
    loop.run_forever()
