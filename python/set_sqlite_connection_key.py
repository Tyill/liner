#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Set SQLite connection_key rows for isolated multi-peer liner deployments.

See docs/using-sqlite.md (Manual connection_key alignment).

Example — listener DB (client receives from server on wire key 2):
  python3 set_sqlite_connection_key.py client2.sqlite --side listener \\
    --conn-key 2 --topic topic_server1 --unique-name server1

Example — sender DB (server sends to client2 on wire key 2):
  python3 set_sqlite_connection_key.py server1.sqlite --side sender \\
    --conn-key 2 --self-name server1 --self-topic topic_server1 \\
    --topic topic_client --unique-name client2
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def composite(sender_name: str, sender_topic: str, listener_name: str) -> str:
    return f"{sender_name}:{sender_topic}:{listener_name}"


def bump_seq(cur: sqlite3.Cursor, conn_key: int) -> None:
    cur.execute("INSERT OR IGNORE INTO seq (id, v) VALUES (1, 0)")
    cur.execute(
        "UPDATE seq SET v = MAX(v, ?) WHERE id = 1",
        (conn_key,),
    )


def apply_sender(
    cur: sqlite3.Cursor,
    self_name: str,
    self_topic: str,
    peer_name: str,
    conn_key: int,
    *,
    replace_key_owner: bool,
) -> None:
    comp = composite(self_name, self_topic, peer_name)
    if replace_key_owner:
        cur.execute(
            "DELETE FROM conn_key_map WHERE connection_key = ? AND composite != ?",
            (conn_key, comp),
        )
    cur.execute(
        "INSERT OR REPLACE INTO conn_key_map (composite, connection_key) VALUES (?, ?)",
        (comp, conn_key),
    )
    cur.execute(
        "INSERT OR REPLACE INTO conn_sender (connection_key, sender_topic) VALUES (?, ?)",
        (conn_key, self_topic),
    )
    bump_seq(cur, conn_key)
    print(f"sender: conn_key_map {comp!r} -> {conn_key}")
    print(f"sender: conn_sender ({conn_key}, {self_topic!r})")


def apply_listener(
    cur: sqlite3.Cursor,
    sender_topic: str,
    conn_key: int,
    *,
    replace_key_owner: bool,
) -> None:
    if replace_key_owner:
        cur.execute(
            "DELETE FROM conn_sender WHERE connection_key = ? AND sender_topic != ?",
            (conn_key, sender_topic),
        )
    cur.execute(
        "INSERT OR REPLACE INTO conn_sender (connection_key, sender_topic) VALUES (?, ?)",
        (conn_key, sender_topic),
    )
    bump_seq(cur, conn_key)
    print(f"listener: conn_sender ({conn_key}, {sender_topic!r})")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Set conn_key_map / conn_sender for liner isolated SQLite files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("db_path", help="Path to the .sqlite file for this process")
    p.add_argument(
        "--conn-key",
        type=int,
        required=True,
        help="Wire connection_key to assign (e.g. 1, 2)",
    )
    p.add_argument(
        "--topic",
        required=True,
        help="Listener DB: remote sender source_topic (callback 'from'). "
        "Ignored for --side sender (use --self-topic for conn_sender on sender file).",
    )
    p.add_argument(
        "--unique-name",
        required=True,
        help="Peer unique_name (client_name in catalog / receivers_json)",
    )
    p.add_argument(
        "--self-name",
        help="This file owner's unique_name (required for --side sender or both)",
    )
    p.add_argument(
        "--self-topic",
        help="This file owner's source_topic (required for --side sender or both)",
    )
    p.add_argument(
        "--side",
        choices=("listener", "sender", "both"),
        default="listener",
        help="listener: conn_sender only; sender: conn_key_map + conn_sender; both: both",
    )
    p.add_argument(
        "--no-replace-key",
        action="store_true",
        help="Do not delete other rows that already use this connection_key "
        "(may fail with UNIQUE constraint on conn_key_map)",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    db_path = Path(args.db_path)
    if not db_path.is_file():
        print(f"error: not a file: {db_path}", file=sys.stderr)
        return 1
    if args.conn_key < 1:
        print("error: --conn-key must be >= 1", file=sys.stderr)
        return 1

    need_self = args.side in ("sender", "both")
    if need_self and (not args.self_name or not args.self_topic):
        print(
            "error: --self-name and --self-topic are required for --side sender or both",
            file=sys.stderr,
        )
        return 1

    replace = not args.no_replace_key
    con = sqlite3.connect(str(db_path), timeout=30.0)
    try:
        cur = con.cursor()
        cur.execute("PRAGMA foreign_keys = ON")
        if args.side in ("listener", "both"):
            apply_listener(
                cur,
                args.topic,
                args.conn_key,
                replace_key_owner=replace,
            )
        if args.side in ("sender", "both"):
            apply_sender(
                cur,
                args.self_name,
                args.self_topic,
                args.unique_name,
                args.conn_key,
                replace_key_owner=replace,
            )
        con.commit()
    except sqlite3.Error as e:
        con.rollback()
        print(f"error: {e}", file=sys.stderr)
        return 1
    finally:
        con.close()

    print(f"OK {db_path} (close liner clients on this file before run)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
