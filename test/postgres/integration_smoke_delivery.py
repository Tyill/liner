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

from python import liner  # noqa: E402

from _support import ensure_release_lib, free_port, postgres_session  # noqa: E402


def main() -> int:
    lib_path = ensure_release_lib()
    liner.loadLib(str(lib_path))

    with postgres_session() as url:
        suffix = uuid.uuid4().hex[:8]
        topic_sub = f"topic_sub_{suffix}"
        client1_name = f"client1_{suffix}"
        client2_name = f"client2_{suffix}"
        topic1 = f"topic1_{suffix}"
        topic2 = f"topic2_{suffix}"
        p1 = free_port()
        p2 = free_port()
        addr1 = f"127.0.0.1:{p1}"
        addr2 = f"127.0.0.1:{p2}"

        client_process = str((MODULE_PATH / "client_process.py").resolve())

        c1 = subprocess.Popen(
            [
                sys.executable,
                client_process,
                f"--client-name={client1_name}",
                f"--client-topic={topic1}",
                f"--client-addr={addr1}",
                f"--subscr-topic={topic_sub}",
                f"--postgres-url={url}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            time.sleep(3.5)

            got = threading.Event()
            received: dict = {}

            h2 = liner.Client.new_postgres(client2_name, topic2, addr2, url)

            def rcb(to: str, from_: str, data_: bytes):
                received["to"] = to
                received["from"] = from_
                received["data"] = data_
                got.set()

            assert h2.run(rcb)

            payload = b"smoke"
            h2.refresh_address_topic(topic_sub)
            assert h2.send_to(topic_sub, payload, True)

            if not got.wait(timeout=3.0):
                raise AssertionError("timeout waiting for echo (see client_process logs if redirected)")

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
