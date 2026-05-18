# Using liner with PostgreSQL

This guide is for integrators who run the broker with **PostgreSQL** instead of Redis or SQLite: how to **build** the crate, create clients, share one database between processes, and run the Python integration harness.

**Also read:** [backends.md](backends.md) (backend choice), [routing-and-store-layout.md](routing-and-store-layout.md) (tables — same layout as SQLite), [operations-redis-sqlite.md](operations-redis-sqlite.md) (PostgreSQL backup and `clear_*`), [errors-and-logging.md](errors-and-logging.md) (`NULL` / `None`, stderr), [bindings.md](bindings.md) (C / Python over `include/liner.h`).

---

## When PostgreSQL fits

- **Shared catalog** — all cooperating processes use the **same connection URL** (like **Redis**, unlike per-process SQLite files).
- **Durable SQL storage** — routing, offline queues, and ack cursors live in **relational tables** (`topic_addr`, `conn_messages`, …).
- **Same messaging API** as other backends: `run`, `send_to`, `subscribe`, TCP between peers.

**Not a fit** when you want **zero server process** and a single portable file — use **SQLite**. When you already standardize on **Redis** and only need an in-memory-style shared store, **Redis** remains the default build (no extra feature).

There is **no `receivers_json`** on `new_postgres` / `lnr_new_client_postgres`. Peers discover each other after **`run`** registers `topic_addr`, or via **`refresh_address_topic`** on senders (same model as Redis).

---

## Build and link

PostgreSQL support is **optional** in `Cargo.toml`:

```bash
cargo build --release --features postgres
```

The implementation uses the Rust **`postgres`** crate with **`NoTls`** (plain TCP to the server). TLS requires a different connector setup and is **not** wired in the stock build — see [security-defaults.md](security-defaults.md).

**C / Python:** the shared library must be built **with** `--features postgres` or symbols such as **`lnr_new_client_postgres`** will be missing at link/load time.

---

## Connection URL

Pass a **libpq** connection string, for example:

```text
postgresql://user:password@127.0.0.1:5432/liner
```

Rules match other backends: non-empty **`unique_name`**, **`topic`**, and **`localhost`** (bind address). If the server is down or the URL is wrong, **`Client::new_postgres`** returns **`None`** / C **`NULL`** (usually without a detailed message on the creation path — check stderr and connectivity).

On first connect, the store runs **`CREATE TABLE IF NOT EXISTS …`** for the liner schema and sets **`lock_timeout`** to **5s** for contended rows.

---

## Shared database model

| Topic | PostgreSQL behavior |
|--------|---------------------|
| **Catalog** | Rows in **`topic_addr`** (topic → bind address → `unique_name`) are visible to **all** clients on the same URL. |
| **Starting a mesh** | Typical order: peer A **`run`** (registers its topic); peer B **`run`**; B **`refresh_address_topic(A's topic)`** then **`send_to`**. |
| **`at_least_once_delivery`** | With **one shared URL**, listener acks in **`conn_mess_number`** are visible to the sender on the same DB — same as Redis / **shared SQLite file**. |
| **Multi-peer** | **One-to-many** / **many-to-many** on a **single URL** is supported without JSON seed files (contrast with **isolated SQLite files** in [using-sqlite.md](using-sqlite.md)). |

Internal **`seed_receivers`** exists on the `Store` trait for parity with SQLite (used in unit tests and store-level APIs). The **public client constructors do not take catalog JSON** for PostgreSQL.

---

## Creating a client

### Rust

```rust
#[cfg(feature = "postgres")]
use liner_broker::Client;

let mut c = Client::new_postgres(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "postgresql://user:pass@127.0.0.1/liner",
)
.expect("open postgres store");

c.clear_stored_messages();
c.clear_addresses_of_topic();
assert!(c.run(|_to, _from, _data| { /* … */ }));
```

**`liner_broker::Liner::new_postgres`** wraps the C constructor (panics on null handle — prefer **`Client`** for `Option`).

### C (`include/liner.h`)

Requires a build with feature **`postgres`**:

```c
lnr_hClient c = lnr_new_client_postgres(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "postgresql://user:pass@127.0.0.1/liner"
);
if (!c) { /* invalid args or store open failure */ }

BOOL ok = lnr_run(c, my_receive_cb, my_udata);
```

### Python (`python/liner.py`)

```python
liner.loadLib("target/release/libliner_broker.so")
c = liner.Client.new_postgres(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "postgresql://user:pass@127.0.0.1/liner",
)
```

If the `.so` was built without **`postgres`**, `new_postgres` raises that the symbol is missing.

After **`run`**, use **`bound_listen_addr()`** and **`unique_name()`** when configuring peers (catalog rows are written by registration, not JSON).

---

## Operator queries

PostgreSQL uses the **same table names** as SQLite (see [routing-and-store-layout.md](routing-and-store-layout.md)). Examples:

```sql
SELECT * FROM topic_addr WHERE topic = 'my_topic';
SELECT COUNT(*) FROM conn_messages WHERE connection_key = 1;
SELECT v FROM conn_mess_number WHERE connection_key = 1;
```

Integration tests that inspect the DB use **`LINER_TEST_POSTGRES_URL`** and **`psycopg2`** — see [debug-and-tests.md](debug-and-tests.md).

---

## Two peers (minimal flow)

1. Choose one database and URL; create role/database in PostgreSQL.
2. Build: `cargo build --release --features postgres`.
3. Process **A**: `new_postgres(...)`, `run` on topic `topic_a` (port `0` if you want an ephemeral port).
4. Process **B**: `new_postgres(...)` with the **same URL**, `run` on `topic_b`.
5. **B**: `refresh_address_topic("topic_a")`, then `send_to("topic_a", payload, true)` (or `false` per your delivery policy).
6. **A** receives on its callback; reply with `send_to` on B’s topic after `refresh_address_topic("topic_b")` if needed.

Reference: integration test **`shared_postgres_two_clients_send_to`** in [`src/client.rs`](../src/client.rs); Python mirror under **`test/postgres/`**.

---

## Tests

From the repository root:

```bash
export LINER_TEST_POSTGRES_URL='postgresql://user:pass@127.0.0.1/liner_test'
cargo build --release --features postgres
pip install psycopg2-binary
python3 test/postgres/run_integration.py
```

Rust unit tests under `store::postgres` use the same environment variable.

---

## Related

- [backends.md](backends.md) — compare Redis, SQLite, PostgreSQL.  
- [store-startup-failure-semantics.md](store-startup-failure-semantics.md) — listener/sender store open at `run`.  
- [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) — queues and `connection_key`.  
- [debug-and-tests.md](debug-and-tests.md) — `test/postgres/` harness.
