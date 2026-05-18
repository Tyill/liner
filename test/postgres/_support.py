# -*- coding: utf-8 -*-
"""Shared helpers for PostgreSQL-backed integration tests (one shared database URL per scenario).

Requires:
  - ``LINER_TEST_POSTGRES_URL`` (libpq URL, e.g. ``postgresql://user:pass@127.0.0.1/liner_test``)
  - ``liner_broker`` built with ``cargo build --release --features postgres``
  - ``psycopg2`` (``pip install psycopg2-binary``) for direct catalog / queue inspection
"""

from __future__ import annotations

import datetime
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

MODULE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_PATH.parent.parent

try:
    import psycopg2
except ImportError:  # pragma: no cover - optional at import time
    psycopg2 = None  # type: ignore[assignment]


def require_postgres_url() -> str:
    url = os.environ.get("LINER_TEST_POSTGRES_URL", "").strip()
    if not url:
        print(
            "LINER_TEST_POSTGRES_URL is not set (e.g. postgresql://user:pass@127.0.0.1/liner_test)",
            file=sys.stderr,
        )
        sys.exit(2)
    return url


def _connect(url: str):
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is required for postgres integration tests (pip install psycopg2-binary)")
    return psycopg2.connect(url)


# Keep in sync with `SCHEMA` in `src/store/postgres.rs`.
_POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS seq (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    v INTEGER NOT NULL
);
INSERT INTO seq (id, v) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS topic_addr (
    topic TEXT NOT NULL,
    addr TEXT NOT NULL,
    client_name TEXT NOT NULL,
    PRIMARY KEY (topic, addr)
);

CREATE TABLE IF NOT EXISTS sender_listener (
    sender_key TEXT NOT NULL,
    addr TEXT NOT NULL,
    listener_topic TEXT NOT NULL,
    PRIMARY KEY (sender_key, addr)
);

CREATE TABLE IF NOT EXISTS conn_key_map (
    composite TEXT PRIMARY KEY,
    connection_key INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS topic_key (
    topic TEXT PRIMARY KEY,
    k INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS conn_sender (
    connection_key INTEGER PRIMARY KEY,
    sender_topic TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS conn_mess_number (
    connection_key INTEGER PRIMARY KEY,
    v BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS conn_messages (
    id BIGSERIAL PRIMARY KEY,
    connection_key INTEGER NOT NULL,
    payload BYTEA NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_conn_messages_ck
    ON conn_messages(connection_key, id);
"""


_TRUNCATE_SQL = """
TRUNCATE TABLE conn_messages, conn_mess_number, conn_sender, topic_key,
    conn_key_map, sender_listener, topic_addr;
UPDATE seq SET v = 0 WHERE id = 1;
"""


def reset_tables(url: str) -> None:
    con = _connect(url)
    try:
        con.autocommit = True
        with con.cursor() as cur:
            try:
                cur.execute(_TRUNCATE_SQL)
            except psycopg2.errors.UndefinedTable:
                con.rollback()
                con.autocommit = True
                cur.execute(_POSTGRES_SCHEMA)
                cur.execute(_TRUNCATE_SQL)
    finally:
        con.close()


@contextmanager
def postgres_session() -> Iterator[str]:
    """Reset liner tables before and after a test scenario."""
    url = require_postgres_url()
    reset_tables(url)
    try:
        yield url
    finally:
        reset_tables(url)


def register_peer_catalog(url: str, peers: list[tuple[str, str, str]]) -> None:
    """Insert peer rows into ``topic_addr`` (shared DB; listener may still be offline)."""
    con = _connect(url)
    try:
        con.autocommit = True
        with con.cursor() as cur:
            for topic, addr, client_name in peers:
                cur.execute(
                    """
                    INSERT INTO topic_addr (topic, addr, client_name) VALUES (%s, %s, %s)
                    ON CONFLICT (topic, addr) DO UPDATE SET client_name = EXCLUDED.client_name
                    """,
                    (topic, addr, client_name),
                )
    finally:
        con.close()


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
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return int(port)


def ensure_release_lib() -> Path:
    lib_path = PROJECT_ROOT / "target" / "release" / "libliner_broker.so"
    if lib_path.exists():
        return lib_path
    subprocess.run(
        ["cargo", "build", "--release", "--features", "postgres"],
        cwd=str(PROJECT_ROOT),
        check=True,
    )
    if not lib_path.exists():
        raise RuntimeError(f"release library not found at {lib_path}")
    return lib_path


def composite(sender_name: str, sender_topic: str, listener_name: str) -> str:
    return f"{sender_name}:{sender_topic}:{listener_name}"


def get_connection_key(url: str, sender_name: str, sender_topic: str, listener_name: str) -> int | None:
    comp = composite(sender_name, sender_topic, listener_name)
    con = _connect(url)
    try:
        with con.cursor() as cur:
            cur.execute(
                "SELECT connection_key FROM conn_key_map WHERE composite = %s",
                (comp,),
            )
            row = cur.fetchone()
            return int(row[0]) if row else None
    finally:
        con.close()


def pending_messages_count(url: str, connection_key: int) -> int:
    con = _connect(url)
    try:
        with con.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM conn_messages WHERE connection_key = %s",
                (connection_key,),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        con.close()
