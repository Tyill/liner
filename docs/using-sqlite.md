# Using liner with SQLite

This guide is for integrators who run the broker with a **SQLite file** instead of Redis: how to create clients, when to use **`receivers_json`**, how **seeded wire keys** relate to routing, and how to mirror the **reference unit test** in your own code.

**Also read:** [backends.md](backends.md) (backend choice, isolated DB model), [operations-redis-sqlite.md](operations-redis-sqlite.md) (WAL, backup, `clear_*`), [errors-and-logging.md](errors-and-logging.md) (`NULL` / `None`, stderr), [bindings.md](bindings.md) (C / Python over `include/liner.h`).

---

## When SQLite fits

- **Single file**, no Redis server — embedded tests, laptops, appliances.
- **Same messaging API** as Redis: `run`, `send_to`, `subscribe`, TCP between peers.
- **Catalog** (which topic listens on which address, and the integer **topic key** used on the wire) lives in SQLite tables `topic_addr` and `topic_key` (see [routing-and-store-layout.md](routing-and-store-layout.md)).

**Caveat:** if **each process has its own `.sqlite` file**, those catalogs are **not** shared. A sender’s empty DB does not know a peer’s topic until you **seed** the catalog (`receivers_json`, previous run on the same file, or manual SQL). Redis avoids that by sharing one URL.

**Isolated pair (empty DBs):** the first logical channel uses wire **`connection_key` = 1** (documented as **token id** for this model). `receivers_json` does **not** carry that id; list **only peers** (their `topic` / `addr` / `client_name`). Seeding inserts **`conn_sender(1, peer_topic)`** and **`conn_key_map`** for those rows, and **`INSERT OR IGNORE topic_key(your_source_topic, 1)`** so your listener’s wire key exists without a self row in JSON. **Wire `topic_key.k = 1`** for every seeded peer topic as well (not passed in JSON).

**One shared SQLite file** (same path for cooperating processes): pass **`receivers_json` empty** (`""` / `[]`); peers register into the same store and `conn_sender` / keys stay consistent without catalog JSON.

---

## Creating a client

### Rust

Use **`liner_broker::Client`** (full control, `send_to` with delivery flag) or **`liner_broker::Liner`** (Rust `Box` callback, same as Redis examples). The crate README still shows **`Liner::new`** for Redis.

**`Liner::new_sqlite`** takes the same five logical arguments as the C API (`receivers_json` may be `""`).

```rust
use liner_broker::Liner;

let mut c = Liner::new_sqlite(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "/path/to/db.sqlite",
    "", // or a JSON catalog string for isolated DBs
);
c.clear_stored_messages();
c.clear_addresses_of_topic();
assert!(c.run(Box::new(|_to, _from, _data| { /* … */ })));
```

After a successful **`run`**, on **`Liner`** you can call:

- **`bound_listen_addr()`** → `Option<String>` — real bind address if you used port **`0`** (for catalog export).
- **`unique_name()`** → `String` — value peers must put in JSON **`client_name`**.

The same accessors exist on **`Client`** (`bound_listen_addr` / `unique_name` return `Option<&str>` / `&str`).

### C (`include/liner.h`)

```c
lnr_hClient c = lnr_new_client_sqlite(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "/path/to/db.sqlite",
    NULL   /* or "[]" or a UTF-8 JSON array string */
);
if (!c) { /* invalid args, open failure, or bad non-empty JSON */ }

BOOL ok = lnr_run(c, my_receive_cb, my_udata);
```

- **`receivers_json == NULL`**, empty string, whitespace-only, or JSON **`[]`** — **not an error**; no seeding (database may already contain rows from an earlier session).
- Invalid **non-empty** JSON → **`NULL`** client, message on stderr.

---

## `receivers_json` catalog format

UTF-8 JSON: a **single array** of objects. Each object must have these three fields:

| Field | Meaning |
|--------|---------|
| **`topic`** | The peer’s **registered topic name** (their `source_topic` / what you pass to `send_to` to reach them). It is also the **`from`** string when that peer sends to you. (Optional legacy: a row equal to **your** `source_topic` is ignored for `conn_sender`; your wire `topic_key` is ensured by the seeder.) |
| **`addr`** | Remote listener TCP address, e.g. `127.0.0.1:54321` — must match how the peer bound (after `run`, often use **`bound_listen_addr()`** if you used port `0`). |
| **`client_name`** | Remote **`unique_name`** (what the listener registered in `topic_addr`). |

**Not in JSON:** wire **`topic_key`** and per-channel **`connection_key`** — for isolated empty DBs the implementation seeds **`topic_key.k = 1`** for every peer catalog topic, the same for **your** `source_topic`, and **`conn_sender`** / **`conn_key_map`** for the first channel as documented above.

Repeated `(topic, addr)` or same **`topic`** with a new row: seeding **upserts** (last row wins for duplicate topics in one batch). See [backends.md](backends.md) (*Isolated SQLite*). A legacy **`topic_key`** property in JSON objects is **ignored** by the parser.

Example (one peer):

```json
[
  {
    "topic": "peer_topic",
    "addr": "127.0.0.1:2256",
    "client_name": "peer_unique"
  }
]
```

**Redis note:** `Store::seed_receivers` on the Redis backend is a **no-op**; this JSON path is for **SQLite** isolated files.

---

## Reference walkthrough (same steps as the unit test)

The repository includes an end-to-end test that exercises **two different SQLite files**, a **catalog JSON file**, and **`send_to`**. Use it as the canonical recipe.

- **Location:** [`src/client.rs`](../src/client.rs), test function **`isolated_sqlite_two_clients_via_receivers_json_catalog_file`**.
- **Run:** `cargo test isolated_sqlite_two_clients_via_receivers_json_catalog_file`

**Steps (mirror of the test):**

1. **Receiver (client A)**  
   - Pick a dedicated DB path, e.g. `…/a.sqlite`.  
   - `Client::new_sqlite("unique_a_iso", "topic_iso_a", "127.0.0.1:0", path_a, "")` — empty `receivers_json`.  
   - `run` with a receive callback that recognizes the payload you will send (the test checks for `b"ping"`).

2. **Export catalog after A is running**  
   - `listen = client_a.bound_listen_addr().expect("…")` — real host:port after bind.  
   - Build JSON: one object with `"topic": "topic_iso_a"`, `"addr": listen`, `"client_name": client_a.unique_name()` (no `topic_key`; seeding assigns wire key **1**).  
   - Write string to a file (the test uses `catalog.json` under a temp directory).

3. **Sender (client B)**  
   - Different DB path `…/b.sqlite`, different **`unique_name`**, different source topic (e.g. `"topic_iso_b"`).  
   - Read the file into a string and call `Client::new_sqlite(..., &catalog_json)`.  
   - `run` (the test uses a no-op receive callback for B).

4. **Send**  
   - B calls `send_to("topic_iso_a", payload, at_least_once)` in a retry loop until it returns `true` (TCP and routing may need a few tries).

5. **Assert delivery**  
   - A’s callback should observe the payload within your timeout.

The test uses `std::env::temp_dir()` for paths and removes the directory at the end; your app would use real paths or configuration.

---

## Shared SQLite file vs isolated files

| Model | How catalog is shared |
|--------|------------------------|
| **One `.sqlite` file** opened by cooperating processes (same host, locking discipline) | `regist_topic` / `run` updates the same `topic_addr` / `topic_key`; peers see each other without JSON if they share that file. Prefer **empty** `receivers_json`. |
| **One file per process** (isolated empty DBs, single counterparty) | Use **`receivers_json`** so each DB lists the peer’s **`topic`**, **`addr`**, **`client_name`**. First wire **`connection_key`** is **1**; seeded **`topic_key.k`** is **1** for those topics; seeding writes **`conn_sender`** for the callback **`from`**. |

Do not mix **Redis** and **SQLite** for one logical mesh unless you intend two isolated systems ([backends.md](backends.md)).

---

## Related

- [using-the-api.md](using-the-api.md) — `run` order, threading, `send_to` pitfalls.  
- [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md) — building `libliner_broker` and linking C/C++.
