# -*- coding: utf-8 -*-
"""Shared helpers for SQLite-backed integration tests (one shared DB file per scenario).

Avoid opening the same DB with Python sqlite3 while a liner Client on that path is still open
(SIGBUS risk). Use peer_catalog_json for seeding; use get_connection_key / pending_messages_count
only after all such clients are closed.
"""

from __future__ import annotations

import datetime
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_PATH.parent.parent


def peer_catalog_json(peers: list[tuple[str, str, str]]) -> str:
    """``receivers_json`` for :meth:`liner.Client.new_sqlite` — each tuple is ``(peer_topic, listen_addr, client_name)``."""
    return json.dumps(
        [{"topic": t, "addr": a, "client_name": n} for (t, a, n) in peers],
        separators=(",", ":"),
    )


def log(msg: str) -> None:
    now = datetime.datetime.now()
    ts = now.strftime("%Y-%m-%d %H:%M:%S") + f".{int(now.microsecond / 1000):03d}"
    print(f"[{ts}] {msg}", flush=True)


def wait_until(pred, timeout_s: float, sleep_s: float = 0.05, what: str = "condition") -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if pred():
            return
        time.sleep(sleep_s)
    raise TimeoutError(f"timeout waiting for {what}")


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return int(port)


def ensure_release_lib() -> Path:
    target_base = Path(os.environ["CARGO_TARGET_DIR"]) if os.environ.get("CARGO_TARGET_DIR") else PROJECT_ROOT / "target"
    lib_path = target_base / "release" / "libliner_broker.so"
    deps_lib = target_base / "release" / "deps" / "libliner_broker.so"
    if not lib_path.exists() and deps_lib.exists():
        shutil.copy2(deps_lib, lib_path)
    if lib_path.exists():
        return lib_path
    subprocess.run(["cargo", "build", "--release"], cwd=str(PROJECT_ROOT), check=True)
    if deps_lib.exists() and not lib_path.exists():
        shutil.copy2(deps_lib, lib_path)
    if not lib_path.exists():
        raise RuntimeError(f"release library not found at {lib_path}")
    return lib_path


def composite(sender_name: str, sender_topic: str, listener_name: str) -> str:
    return f"{sender_name}:{sender_topic}:{listener_name}"


def _connect_reader(db_path: str) -> sqlite3.Connection:
    """Short-lived reads; prefer **no** liner clients open on ``db_path`` (avoids WAL reader races)."""
    return sqlite3.connect(db_path, timeout=30.0, isolation_level=None)


def get_connection_key(db_path: str, sender_name: str, sender_topic: str, listener_name: str) -> int | None:
    comp = composite(sender_name, sender_topic, listener_name)
    con = _connect_reader(db_path)
    try:
        cur = con.execute(
            "SELECT connection_key FROM conn_key_map WHERE composite = ?",
            (comp,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else None
    finally:
        con.close()


def pending_messages_count(db_path: str, connection_key: int) -> int:
    con = _connect_reader(db_path)
    try:
        cur = con.execute(
            "SELECT COUNT(*) FROM conn_messages WHERE connection_key = ?",
            (connection_key,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        con.close()


def temp_shared_db(prefix: str = "liner_sqlite_it_") -> tuple[str, callable]:
    d = tempfile.mkdtemp(prefix=prefix)
    db_path = str(Path(d) / "shared.sqlite")

    def cleanup() -> None:
        shutil.rmtree(d, ignore_errors=True)

    return db_path, cleanup
