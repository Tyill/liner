# Using liner with Redis

This guide is for integrators who run the broker with **Redis** as the shared store: how to **build** the crate (default), create clients, share one Redis URL between processes, and run the Python integration harness.

**Also read:** [backends.md](backends.md) (backend choice), [routing-and-store-layout.md](routing-and-store-layout.md) (Redis key layout), [operations-redis-sqlite.md](operations-redis-sqlite.md) (`lnr_*` keys, `clear_*`, server version), [errors-and-logging.md](errors-and-logging.md) (`NULL` / `None`, stderr), [bindings.md](bindings.md) (C / Python over `include/liner.h`).

---

## When Redis fits

- **Shared catalog** — all cooperating processes use the **same Redis URL** (like **PostgreSQL**; unlike per-process SQLite files).
- **Low-latency, in-memory-first** store — good default when you already run Redis for other services.
- **Same messaging API** as other backends: `run`, `send_to`, `subscribe`, TCP between peers.

**Not a fit** when you need a **single portable file** with no server process — use **SQLite**. When you want **durable SQL** and ops tooling on tables — consider **PostgreSQL** ([using-postgres.md](using-postgres.md)).

There is **no `receivers_json`** on Redis clients. Peers discover each other after **`run`** registers `lnr_topic:{topic}:addr`; the **internal channel** keeps address caches in sync at runtime (see [using-the-api.md](using-the-api.md)).

---

## Build and link

Redis is included in the **default** build — no extra Cargo feature:

```bash
cargo build --release
```

The implementation uses the **`redis`** crate: **`Client::open`** on your URL, then **`get_connection`** when the store is created.

**C / Python:** the stock **`libliner_broker.so`** / `.dll` already exports **`lnr_new_client_redis`**.

---

## Connection URL

Pass a **Redis connection URL**, for example:

```text
redis://127.0.0.1/
redis://127.0.0.1:6379/3
```

Use a **dedicated logical database** (`/N` in the URL or `SELECT N`) or a dedicated instance so liner keys (`lnr_*`) do not collide with unrelated apps — see [operations-redis-sqlite.md](operations-redis-sqlite.md).

Rules match other backends: non-empty **`unique_name`**, **`topic`**, and **`localhost`** (bind address). If Redis is down or the URL is wrong, **`Client::new_redis`** returns **`None`** / C **`NULL`** (check stderr).

**Server version:** Redis **≥ 6.2** is recommended for the commands liner uses ([operations-redis-sqlite.md](operations-redis-sqlite.md)).

---

## Shared store model

| Topic | Redis behavior |
|--------|----------------|
| **Catalog** | Hash **`lnr_topic:{topic}:addr`** (address → `unique_name`) is visible to **all** clients on the same URL. |
| **Starting a mesh** | Typical order: peer A **`run`**, peer B **`run`**; then **`send_to`** (address cache updates via the internal channel). **`refresh_address_topic`** is optional—see [using-the-api.md](using-the-api.md). |
| **`at_least_once_delivery`** | With **one shared URL**, listener acks in **`lnr_connection:{id}:mess_number`** are visible to the sender on the same Redis — use **`true`** when you need offline persistence. |
| **Multi-peer** | **One-to-many** / **many-to-many** on a **single URL** work without JSON seed files (contrast with **isolated SQLite files** in [using-sqlite.md](using-sqlite.md)). |

Wire **`topic_key`** integers live under **`lnr_topic:{topic}:key`**. **`connection_key`** values are allocated from **`lnr_unique_key`** (see [routing-and-store-layout.md](routing-and-store-layout.md)).

---

## Creating a client

### Rust

```rust
use liner_broker::Client;

let mut c = Client::new_redis(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "redis://127.0.0.1/",
)
.expect("open redis store");

// `Client::new(...)` is an alias for `new_redis`.

c.clear_stored_messages();
c.clear_addresses_of_topic();
assert!(c.run(|_to, _from, _data| { /* … */ }));
```

**`liner_broker::Liner::new`** wraps the legacy C constructor (same Redis URL).

### C (`include/liner.h`)

```c
lnr_hClient c = lnr_new_client_redis(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "redis://127.0.0.1/"
);
if (!c) { /* invalid args or Redis unreachable */ }

BOOL ok = lnr_run(c, my_receive_cb, my_udata);
```

**`lnr_new_client`** (deprecated) calls **`lnr_new_client_redis`** with the same arguments.

### Python (`python/liner.py`)

```python
import liner

liner.loadLib("target/release/libliner_broker.so")
c = liner.Client(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "redis://127.0.0.1/",
)
```

Redis is the default constructor on **`Client(...)`** (not a separate `new_redis` classmethod).

After **`run`**, use **`bound_listen_addr()`** and **`unique_name()`** when documenting peers; catalog rows are written by **`regist_topic`**, not JSON.

---

## Operator inspection

Examples (replace topic / ids as needed):

```bash
redis-cli HGETALL lnr_topic:my_topic:addr
redis-cli GET lnr_topic:my_topic:key
redis-cli GET lnr_unique_key
```

Prefer **`SCAN`** with pattern `lnr_*` over **`KEYS`** in production. Full key catalog: [routing-and-store-layout.md](routing-and-store-layout.md).

---

## Two peers (minimal flow)

1. Start Redis (local or remote); pick one URL for all processes.
2. Build: `cargo build --release`.
3. Process **A**: `new_redis(...)`, `run` on topic `topic_a` (port `0` for an ephemeral bind port).
4. Process **B**: `new_redis(...)` with the **same URL**, `run` on `topic_b`.
5. **B**: `refresh_address_topic("topic_a")`, then `send_to("topic_a", payload, true)` (or `false` per policy).
6. **A** receives on its callback; reply with `send_to` on B’s topic after `refresh_address_topic("topic_b")` if needed.

Benchmark reference: **`bench_pair_sendto_redis`** in [`benchmark/`](../benchmark/).

---

## Tests

From the repository root:

```bash
cargo build --release
python3 test/redis/run_integration.py
```

Many scripts auto-start Redis via **Docker** on port **16379** if nothing is listening. Optional env:

```bash
LINER_TEST_REDIS_PORT=16379 LINER_TEST_REDIS_CONTAINER=liner-test-redis \
  python3 test/redis/run_integration.py --only offline,burst
```

`--list`, `--only`, and `--continue-on-fail` are supported. Details: [debug-and-tests.md](debug-and-tests.md).

---

## Related

- [backends.md](backends.md) — compare Redis, SQLite, PostgreSQL.  
- [using-sqlite.md](using-sqlite.md) — isolated files and `receivers_json`.  
- [using-postgres.md](using-postgres.md) — SQL shared URL.  
- [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) — queues and `connection_key`.  
- [debug-and-tests.md](debug-and-tests.md) — `test/redis/` harness.
