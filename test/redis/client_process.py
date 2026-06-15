#!/usr/bin/python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from python import liner  # noqa: E402

DEFAULT_REDIS_URL = os.environ.get("LINER_TEST_REDIS_URL", "redis://127.0.0.1:6379/")


def release_lib() -> Path:
    target_base = Path(os.environ["CARGO_TARGET_DIR"]) if os.environ.get("CARGO_TARGET_DIR") else ROOT / "target"
    lib_path = target_base / "release" / "libliner_broker.so"
    deps_lib = target_base / "release" / "deps" / "libliner_broker.so"
    if not lib_path.exists() and deps_lib.exists():
        shutil.copy2(deps_lib, lib_path)
    return lib_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-name", required=True)
    parser.add_argument("--client-topic", required=True)
    parser.add_argument("--client-addr", required=True)
    parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    parser.add_argument("--subscr-topic", default="")
    parser.add_argument("--unsubscr-topic", default="")

    args = parser.parse_args()

    liner.loadLib(str(release_lib()))

    h = liner.Client(args.client_name, args.client_topic, args.client_addr, args.redis_url)
    h.clear_addresses_of_topic()
    h.clear_stored_messages()

    def receive_cback1(to: str, from_: str, data_: bytes):
        print(f"{args.client_name} receive_from {from_}, data: {data_}")
        h.send_to(from_, bytearray(data_), True)

    if args.subscr_topic:
        h.subscribe(args.subscr_topic)

    if not h.run(receive_cback1):
        raise SystemExit("liner run() failed")

    if args.unsubscr_topic:
        h.unsubscribe(args.unsubscr_topic)

    loop = asyncio.new_event_loop()
    loop.run_forever()
