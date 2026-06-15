#!/usr/bin/python3
# -*- coding: utf-8 -*-

import subprocess
import sys
import threading
import time
import uuid
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
    _wait_until,
)


def main() -> int:
    liner.loadLib(str(_ensure_release_lib()))
    _ensure_redis()

    suffix = uuid.uuid4().hex[:8]
    topic_sub = f"topic_sub_{suffix}"
    client1_name = f"client1_{suffix}"
    client2_name = f"client2_{suffix}"
    topic1 = f"topic1_{suffix}"
    topic2 = f"topic2_{suffix}"
    addr1 = f"127.0.0.1:{_free_port()}"
    addr2 = f"127.0.0.1:{_free_port()}"

    client_process = str((MODULE_PATH / "client_process.py").resolve())

    c1 = subprocess.Popen(
        [
            sys.executable,
            client_process,
            f"--client-name={client1_name}",
            f"--client-topic={topic1}",
            f"--client-addr={addr1}",
            f"--subscr-topic={topic_sub}",
            f"--redis-url={REDIS_URL}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        got = threading.Event()
        received: dict = {}

        h2 = liner.Client(client2_name, topic2, addr2, REDIS_URL)
        h2.clear_addresses_of_topic()
        h2.clear_stored_messages()

        def rcb(to: str, from_: str, data_: bytes):
            received["to"] = to
            received["from"] = from_
            received["data"] = data_
            got.set()

        assert h2.run(rcb)

        _wait_until(
            lambda: h2.refresh_address_topic(topic_sub),
            timeout_s=15.0,
            what=f"subscriber on {topic_sub}",
        )

        payload = b"smoke"
        assert h2.send_to(topic_sub, payload, True)

        if not got.wait(timeout=3.0):
            out = ""
            if c1.stdout:
                try:
                    out = c1.stdout.read()
                except Exception:
                    out = ""
            raise AssertionError(f"timeout waiting for echo; client1 output:\n{out}")

        assert received["to"] == topic2
        assert received["from"] == topic1
        assert received["data"] == payload

        h2.close()
        return 0
    finally:
        try:
            c1.kill()
        except Exception:
            pass
        try:
            c1.wait(timeout=2)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
