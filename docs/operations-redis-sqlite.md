# Operations: Redis, SQLite, and PostgreSQL

Companion to the key/table catalog in [routing-and-store-layout.md](routing-and-store-layout.md). Here: **prefix discipline**, what **`clear_*`** touches, **Redis server compatibility**, **SQLite backup** with **WAL**, and **PostgreSQL** maintenance notes.

---

## Redis: `lnr_*` prefix

All broker-owned keys and the global counter use the **`lnr_`** prefix (for example `lnr_topic:…`, `lnr_connection:…`, `lnr_unique_key`). That keeps them visually distinct from unrelated keys in the same logical Redis database.

**Operational recommendation:** run liner workloads in a **dedicated Redis logical database** (`SELECT` / URL path like `redis://host/3`) or a dedicated instance, so maintenance (`KEYS`, `FLUSHDB`, monitoring) does not collide with other applications. The library does **not** add an extra configurable prefix beyond `lnr_`.

**Discovery:** prefer **`SCAN`** with pattern `lnr_*` over **`KEYS`** in production.

---

## Redis: what `clear_*` does and does **not** do

Both APIs are only valid when the **client is not running** (see [using-the-api.md](using-the-api.md)).

### `clear_addresses_of_topic`

**Removes** the **directory of bind addresses** for this client’s **source topic only**:

| Backend | Effect |
|---------|--------|
| **Redis** | **`DEL lnr_topic:{source_topic}:addr`** — the whole hash for that topic string. |
| **SQLite / PostgreSQL** | **`DELETE FROM topic_addr WHERE topic = ?`** with `topic = source_topic`. |

**Does not remove:** topic integer keys (`lnr_topic:{topic}:key` / `topic_key` table), `lnr_unique_key`, any `lnr_connection:*` keys, `lnr_sender:*:listener`, offline queues, or **other topics’** `…:addr` entries.

After this call, other clients may still have **cached** old addresses until they **`refresh_address_topic`** or reconnect.

### `clear_stored_messages`

**Removes offline queues and ack cursors** for listeners recorded for **this sender identity** (`unique_name` + `source_topic`), and drops the sender’s listener map.

| Backend | Effect |
|---------|--------|
| **Redis** | Reads **`lnr_sender:{unique}:{source_topic}:listener`**. For each `(addr, listener_topic)`, resolves **`connection_key`**, then **`DEL lnr_connection:{id}:messages`** and **`DEL lnr_connection:{id}:mess_number`**. Finally **`DEL`** the **`lnr_sender:…:listener`** hash. |
| **SQLite / PostgreSQL** | Same flow via **`sender_listener`** → **`connection_key`** → **`DELETE FROM conn_messages`** and **`DELETE FROM conn_mess_number`** for those keys, then **`DELETE FROM sender_listener`** for this **`sender_key`**. |

**Does not remove:**

- **Redis:** `lnr_connection:{composite}:key` (string composite → id), `lnr_connection:{id}:sender`, `lnr_topic:*:addr`, `lnr_topic:*:key`, `lnr_unique_key`, other clients’ `lnr_sender:*` hashes, or queues for **connection keys** not reachable from this sender’s listener list (for example after manual key edits).
- **SQLite:** rows in **`conn_key_map`**, **`conn_sender`**, **`topic_key`**, **`topic_addr`**, or **`seq`**.

So **`clear_stored_messages`** is **not** a full “wipe all liner state from the server”; it clears **persisted message queues and last-ack numbers** tied to this sender’s saved listener set, plus that listener map.

---

## Redis: server version compatibility

The Rust dependency is **`redis = "0.26.1"`** (see `Cargo.toml`). The broker uses common commands (`GET`, `SET`, `HSET`, `HGETALL`, `DEL`, `INCR`, `RPUSH`, `LLEN`, `LRANGE`, …).

**Important:** draining an offline queue uses **`LPOP` with a count** (`load_messages_for_sender`). That form is supported from **Redis 6.2** onward.

**Practical guidance:** use **Redis ≥ 6.2**. Older servers may fail that code path. Newer Redis versions (7.x) are generally fine; the crate does not pin a maximum Redis version—validate in your environment.

---

## SQLite: single file, WAL, backup

### Files on disk

- **Main DB path** — the path argument to `Client::new_sqlite` / `lnr_new_client_sqlite`. Optional **`receivers_json`** (last C parameter / last Rust argument) seeds **`topic_addr`** and **`topic_key`** (wire key **1** per peer and for your `source_topic`), plus **`conn_sender`** / **`conn_key_map`** for peers; see [backends.md](backends.md) (*Isolated SQLite*).
- **WAL mode** — SQLite may create **`<db>-wal`** and **`<db>-shm`** next to the main file while connections are open. This is normal.

The library sets **`journal_mode=WAL`** and **`busy_timeout`** (see [backends.md](backends.md)) on open.

### Why stop clients (or quiesce) before copying

Multiple processes (**client + listener + sender** each open the file, and you may run several clients) hold connections and may be writing. A **naive file copy** of the main `.sqlite` file while writers are active can produce an **inconsistent** backup when WAL is enabled.

**Conservative backup procedure:**

1. **Stop all processes** that have the database file open (all liner clients using that path, and any `sqlite3` shells).
2. Copy the **main file** and, if they exist, **`-wal`** and **`-shm`** together, **or** copy only after WAL has been fully checkpointed into the main file (next bullet).
3. Alternatively, with the app **paused** and a single maintenance connection, run **`PRAGMA wal_checkpoint(TRUNCATE)`** so the WAL is merged and truncated, then copy **only** the main database file.

**Online** options (no full stop) include SQLite’s **backup API** (`sqlite3_backup_*` / `.backup` in the CLI) used correctly with WAL—prefer official SQLite documentation for your tool; liner does not expose a built-in backup API.

### Restore

Restore the **main** file (and matching **`-wal`/`-shm`** if you backed them up as a set) to a path clients use, with **compatible schema** (same major liner version that created the tables). Mixed copies (main from one moment, WAL from another) risk corruption.

---

## PostgreSQL: shared database, backup

### Connection URL

The store string is a **libpq URL** passed to **`Client::new_postgres`** / **`lnr_new_client_postgres`** (requires **`--features postgres`**). All cooperating liner processes for one mesh should use the **same URL** (dedicated database per deployment is recommended).

Schema is created automatically on first open (`CREATE TABLE IF NOT EXISTS …`). There is no separate migration tool in the library.

### Backup and restore

Use standard PostgreSQL tools for your environment:

- **Logical backup:** `pg_dump` / `pg_restore` of the database that holds liner tables.
- **Physical backup:** follow your operator’s policy (WAL archiving, snapshots, managed-service backups).

**Quiesce or stop liner clients** on that URL before a **consistent** logical dump if you need a point-in-time that matches in-flight offline queues. Running clients may hold rows in **`conn_messages`** and update **`topic_addr`** during the dump.

After restore, ensure the **same schema version** (tables created by the liner version you run). Mixed liner versions against one database are untested.

### `clear_*` on PostgreSQL

Same SQL effects as SQLite (see tables above): **`clear_addresses_of_topic`** deletes **`topic_addr`** rows for this client’s **`source_topic`** only; **`clear_stored_messages`** clears queues and ack numbers for this sender’s saved listeners. Neither wipes the whole database.

---

## Related

- [routing-and-store-layout.md](routing-and-store-layout.md) — full key/table reference.  
- [backends.md](backends.md) — choosing backend, `SQLITE_BUSY`, bundled SQLite, optional PostgreSQL feature.
- [using-postgres.md](using-postgres.md) — PostgreSQL client API and tests.  
- [using-the-api.md](using-the-api.md) — when `clear_*` is allowed (`run` must be off).
