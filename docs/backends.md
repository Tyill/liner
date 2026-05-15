# Store backends (Redis and SQLite)

Both backends implement the same internal `Store` contract so clients, listeners, and senders behave the same at the messaging layer.

## Choosing a backend

- **Redis** — shared in-memory/disk persistence, multiple hosts, good for distributed deployments. Pass a **Redis URL** (for example `redis://127.0.0.1/`).
- **SQLite** — single file, no separate server process, good for embedded or single-machine tests. Pass a **filesystem path** to the database file.

Use **`lnr_new_client_redis` / `lnr_new_client_sqlite`** (C) or **`Client::new_redis` / `Client::new_sqlite`** (Rust) consistently for a given deployment.

## Isolated SQLite (one file per process)

In many deployments **each process has its own SQLite file**. The directory tables (`topic_addr`, `topic_key`) are then **local**; they are **not** shared the way a single **Redis** URL is. A sender with an empty local catalog cannot resolve another peer’s topic until that catalog is filled—either from a **previous run** on the same file, manual `INSERT`, or the optional **`receivers_json`** argument on **`Client::new_sqlite` / `lnr_new_client_sqlite`**.

- **`receivers_json`:** UTF-8 JSON **array** of objects `{ "topic", "addr", "client_name" }` — typically **one entry per peer** (your own topic does not need a row). **`NULL` / `""` / whitespace / `[]`** are valid and mean **no seeding** (use whatever is already in the file). A **non-empty** array requires those three fields on every object; parse errors return **`NULL` / `None`** and log to stderr. Seeding **upserts** by primary key (same topic / same `(topic, addr)` updates in place). For isolated empty DBs the implementation assigns wire **`topic_key.k = 1`** for every seeded peer topic, **`INSERT OR IGNORE`** the same for **your** `source_topic`, and the first **`connection_key` = 1** (not in JSON). A legacy **`topic_key`** property in JSON is **ignored**.
- **`topic_key` on the wire** must match the value in the **`topic_key`** table for that topic on the **listener** process. With catalog seeding from `receivers_json` on empty isolated files, that value is **1** for seeded topics. Inspect with SQLite `SELECT k FROM topic_key WHERE topic = ?` on the peer’s file, or Redis **`GET lnr_topic:{topic}:key`** when using Redis.
- **End-to-end example** (two temp DB files, catalog written to a JSON file, second client seeds from that file): see unit test **`isolated_sqlite_two_clients_via_receivers_json_catalog_file`** in [`src/client.rs`](../src/client.rs).
- **`at_least_once_delivery`:** with **one SQLite file per process**, listener acknowledgements land in the **receiver’s** file while the sender polls **`conn_mess_number` in its own file**—they do not cross over. Prefer **`at_least_once_delivery == false`** for `send_to` / `send_all` in that layout, or use a **shared** DB file / Redis if you need cross-process at-least-once. Details: [using-sqlite.md](using-sqlite.md) (*Isolated files and `at_least_once_delivery`*), [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md).

Typical steps: (1) peer A **`run`** on its DB and topic; (2) read **`bound_listen_addr`** (or your known bind string) and **`unique_name`**; (3) write one JSON array entry (`topic`, `addr`, `client_name`) to a file or config; (4) peer B **`new_sqlite(..., receivers_json)`** with that JSON, then **`run`**; (5) B **`send_to`** A’s topic.

## `unique_name`

The `unique_name` string identifies this client instance in the store (together with topics and addresses). With **Redis**, peers that should share routing and offline queues use the **same Redis URL** and compatible topic/address data. With **SQLite**, processes that share **one database file** on a host see the same catalog; **separate files** do not—use **`receivers_json`** (or out-of-band SQL) to align `topic_key` and addresses (see *Isolated SQLite* above). Each **running client** still needs a **distinct** `unique_name` (and typically its own TCP `localhost` binding).

## Redis

- Connection uses the official `redis` crate: **`Client::open`** then **`get_connection`** during `Redis::new`. If the server is down, **`Client::new_*` returns `None`** / C **`NULL`**.
- Further operations may fail with **`DbError`** messages originating from Redis (timeouts, connection drops, etc.). Those surface as failed API calls and stderr logs, not necessarily as process exit.
- **Key prefix, `clear_*` behavior, server version (≥ 6.2 recommended):** [operations-redis-sqlite.md](operations-redis-sqlite.md).

## SQLite

**Step-by-step usage (Rust/C, `receivers_json`, catalog export, link to unit test):** [using-sqlite.md](using-sqlite.md).

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
