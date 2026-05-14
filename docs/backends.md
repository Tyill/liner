# Store backends (Redis and SQLite)

Both backends implement the same internal `Store` contract so clients, listeners, and senders behave the same at the messaging layer.

## Choosing a backend

- **Redis** — shared in-memory/disk persistence, multiple hosts, good for distributed deployments. Pass a **Redis URL** (for example `redis://127.0.0.1/`).
- **SQLite** — single file, no separate server process, good for embedded or single-machine tests. Pass a **filesystem path** to the database file.

Use **`lnr_new_client_redis` / `lnr_new_client_sqlite`** (C) or **`Client::new_redis` / `Client::new_sqlite`** (Rust) consistently for a given deployment.

## `unique_name`

The `unique_name` string identifies this client instance in the store (together with topics and addresses). All peers that should share routing and offline queues must use the **same Redis URL or SQLite path** and compatible topic/address data; each **running client** still needs a **distinct** `unique_name` (and typically its own TCP `localhost` binding).

## Redis

- Connection uses the official `redis` crate: **`Client::open`** then **`get_connection`** during `Redis::new`. If the server is down, **`Client::new_*` returns `None`** / C **`NULL`**.
- Further operations may fail with **`DbError`** messages originating from Redis (timeouts, connection drops, etc.). Those surface as failed API calls and stderr logs, not necessarily as process exit.
- **Key prefix, `clear_*` behavior, server version (≥ 6.2 recommended):** [operations-redis-sqlite.md](operations-redis-sqlite.md).

## SQLite

- Opening uses **`rusqlite`** with the **bundled** SQLite (see `Cargo.toml`).
- On open, the library sets **`PRAGMA journal_mode=WAL`**, **`PRAGMA busy_timeout`** to **5000 ms**, and **`foreign_keys=ON`**.
- **`SQLITE_BUSY`**: SQLite waits up to the busy timeout for locks. If a lock is still not available, the operation fails with an error string from `rusqlite` (surfaced as **`DbError`**). This is **not** a separate timeout for “connection lifetime”; it applies per database operation that needs a lock.
- Multiple processes or threads opening the **same file** can contend for the database lock; design workloads accordingly (few writers, or serialize access).
- **Operations (backup, WAL files, `clear_*` on SQLite):** see [operations-redis-sqlite.md](operations-redis-sqlite.md).
- **Memory / message size limits, compression:** see [capacity-and-limits.md](capacity-and-limits.md).

## Mixed deployments

Do **not** point one client at Redis and another at SQLite for the same logical mesh unless you deliberately want two isolated systems. They do not share data.

**Security posture (no TLS, trust boundaries):** [security-defaults.md](security-defaults.md).

## Related

- [store-startup-failure-semantics.md](store-startup-failure-semantics.md) — client vs listener/sender when opening the store at `run` time.
- [routing-and-store-layout.md](routing-and-store-layout.md) — Redis key names and SQLite tables (for debugging and backups).
- [operations-redis-sqlite.md](operations-redis-sqlite.md) — prefix policy, `clear_*`, Redis 6.2+, SQLite WAL backup.
