# Using liner with SQLite

This guide is for integrators who run the broker with a **SQLite file** instead of Redis: how to create clients, when to use **`receivers_json`**, how **seeded wire keys** relate to routing, and how to mirror the **reference unit test** in your own code.

**Also read:** [backends.md](backends.md) (backend choice, isolated DB model), [operations-redis-sqlite.md](operations-redis-sqlite.md) (WAL, backup, `clear_*`), [errors-and-logging.md](errors-and-logging.md) (`NULL` / `None`, stderr), [bindings.md](bindings.md) (C / Python over `include/liner.h`).

---

## When SQLite fits

- **Single file**, no Redis server — embedded tests, laptops, appliances.
- **Same messaging API** as Redis: `run`, `send_to`, `subscribe`, TCP between peers.
- **Catalog** (which topic listens on which address, and the integer **topic key** used on the wire) lives in SQLite tables `topic_addr` and `topic_key` (see [routing-and-store-layout.md](routing-and-store-layout.md)).

**Caveat:** if **each process has its own `.sqlite` file**, those catalogs are **not** shared. A sender’s empty DB does not know a peer’s topic until you **seed** the catalog (`receivers_json`, previous run on the same file, or manual SQL). Redis avoids that by sharing one URL.

**Isolated files and `at_least_once_delivery`:** listener acks are written to **that process’s** SQLite file (`conn_mess_number`), while the sender refreshes acks from **its** file. With **different DB paths per peer**, those views do **not** merge—keep **`at_least_once_delivery` false** (`lnr_send_to` / `send_to` / `send_all` last argument, Rust `false`) so the stack does not retain unacked at-least-once traffic in RAM waiting for a store update that never arrives. For real at-least-once across processes, use **one shared SQLite path** (same as one Redis URL) or Redis.

**Isolated pair (empty DBs):** the first logical channel uses wire **`connection_key` = 1** (documented as **token id** for this model). `receivers_json` does **not** carry that id; list **only peers** (their `topic` / `addr` / `client_name`). Seeding inserts **`conn_sender(1, peer_topic)`** and **`conn_key_map`** for those rows, and **`INSERT OR IGNORE topic_key(your_source_topic, 1)`** so your listener’s wire key exists without a self row in JSON. **Wire `topic_key.k = 1`** for every seeded peer topic as well (not passed in JSON).

**One shared SQLite file** (same path for cooperating processes): pass **`receivers_json` empty** (`""` / `[]`); peers register into the same store and `conn_sender` / keys stay consistent without catalog JSON.

### Isolated DBs: one-to-one only (multi-peer limitation)

If **each process has its own `.sqlite` file**, the catalog seeding path is designed for **one remote peer per process** in `receivers_json` — a classic **one-to-one** link (A lists only B, B lists only A). That is the layout covered by the reference test **`isolated_sqlite_two_clients_via_receivers_json_catalog_file`**.

**Not supported out of the box** on isolated empty files (without manual store edits):

- **One sender, many listeners** — e.g. `send_all` to a topic with several `(addr, client_name)` rows in the sender’s `receivers_json`.
- **Many senders, one listener** — several server entries in the listener’s `receivers_json`.
- **Several peers in one `receivers_json` array** on a single process when those peers need distinct wire channels.

**Why:** seeding writes wire **`connection_key = 1`** for every peer row, but table **`conn_key_map`** also has **`UNIQUE(connection_key)`** (see [routing-and-store-layout.md](routing-and-store-layout.md)). Only one composite can own key **1**; additional peers may lose their `conn_key_map` row or get dynamic keys **2**, **3**, … when sending, while listener DBs still only have **`conn_sender(1, …)`** from seed. The sender and receiver files do not reconcile those ids automatically.

**Typical symptoms:** stderr `couldn't get_sender_topic, conn_key N, err Query returned no rows`; receive callback **`from`** empty or wrong while **`to`** looks fine.

**What works instead:**

| Goal | Approach |
|------|----------|
| Many-to-many, `send_all`, several senders | **One shared `.sqlite` path** for all cooperating processes (same idea as one Redis URL), **empty** `receivers_json`, or use **Redis**. |
| Stay on isolated files, one logical peer | **One-to-one** seeding only; use **`at_least_once_delivery == false`**. |
| Advanced / unsupported | Manually align store tables (below) in **each** process’s file, or use **`python/set_sqlite_connection_key.py`**. |

The library does **not** assign distinct seeded `connection_key` values **1, 2, …** per peer in JSON today; do not rely on multi-row `receivers_json` for isolated fan-out or fan-in unless you maintain the tables yourself.

#### Manual `connection_key` alignment (operator)

Use this only when you accept **one `.sqlite` file per process** and need **more than one peer** (e.g. one server, two listeners). Every process must agree on the **same integer on the wire** for a given sender→listener pair.

**Safety**

- **Stop** all `liner` clients that have the file open (no `run()` on that path). Editing under WAL while a client is running can crash the process; see [test/sqlite/_support.py](../test/sqlite/_support.py).
- Use **`at_least_once_delivery == false`** for cross-peer sends on isolated files.
- After edits, restart peers (or create clients again on the same paths).

**Names (do not confuse)**

| Name | Meaning |
|------|---------|
| **`self` `unique_name` / `source_topic`** | The process that **owns** this `.sqlite` file (`Client::new_sqlite` arguments). |
| **Peer `unique_name`** | Remote `client_name` in `topic_addr` / JSON (`client1`, `server1`, …). |
| **Peer `topic`** | On the **listener** file: remote sender’s **`source_topic`** (string in callback **`from`**, e.g. `topic_server1`). On the **sender** file: the **listener topic** you pass to `send_to` / `send_all` (e.g. `topic_client`). |
| **`connection_key`** | Integer in the message header; must match on **sender** `conn_key_map` and **listener** `conn_sender` for that link. |

**Composite string (sender file only):**

```text
{self_unique_name}:{self_source_topic}:{peer_unique_name}
```

Example: `server1:topic_server1:client2`.

---

**1. Sender database** (process that calls `send_to` / `send_all`)

| Table | Action |
|-------|--------|
| **`conn_key_map`** | `INSERT OR REPLACE` one row: `(composite, connection_key)` for each listener peer. **Only one row per `connection_key`** (`UNIQUE`); assigning key `2` to `client2` may require deleting another composite that already used `2`. |
| **`conn_sender`** | Optional before first send; on send the library sets `(connection_key, self_source_topic)`. You may pre-set `(N, your_source_topic)` for consistency. |
| **`seq`** | Set `v` to **≥** the largest `connection_key` you use in `conn_key_map` and `conn_sender`: `UPDATE seq SET v = MAX(v, ?) WHERE id = 1`. Otherwise the next dynamic `get_connection_key_for_sender` can collide. |

Example SQL (server `server1.sqlite`, send to `client2` with wire key **2**):

```sql
INSERT OR REPLACE INTO conn_key_map (composite, connection_key)
  VALUES ('server1:topic_server1:client2', 2);
INSERT OR REPLACE INTO conn_sender (connection_key, sender_topic)
  VALUES (2, 'topic_server1');
UPDATE seq SET v = MAX(v, 2) WHERE id = 1;
```

**Do not** need to edit `topic_addr` / `topic_key` if they were already seeded or registered by `run` — only keys for the **channel**.

---

**2. Listener database** (process that receives)

| Table | Action |
|-------|--------|
| **`conn_sender`** | `INSERT OR REPLACE (connection_key, sender_topic)` where **`sender_topic`** is the **remote sender’s `source_topic`** (callback **`from`**), not your own topic. One row per wire key you accept from that sender. **Primary key on `connection_key`**: two different senders cannot share the same key on one file unless you only need one `from` mapping. |
| **`conn_key_map`** | Not required if this process **only receives** and does not send back on that channel. |
| **`seq`** | Same bump as above if you insert high keys. |

Example SQL (client `client2.sqlite`, expect server `topic_server1` on wire key **2**):

```sql
INSERT OR REPLACE INTO conn_sender (connection_key, sender_topic)
  VALUES (2, 'topic_server1');
UPDATE seq SET v = MAX(v, 2) WHERE id = 1;
```

---

**3. Agreement pattern (one server, two listeners)**

| Peer | Sender `conn_key_map` | Listener `conn_sender` |
|------|------------------------|-------------------------|
| `client1` | `…:client1` → **1** | `(1, 'topic_server1')` |
| `client2` | `…:client2` → **2** | `(2, 'topic_server1')` |

Run the helper **once per file**: `--side listener` on each client DB, `--side sender` on the server DB (`--topic` is only for listener rows; sender uses `--self-topic` for `conn_sender`).

**Helper script** (from repo root):

```bash
# Listener file: remote server topic + server unique_name, wire key 2
python3 python/set_sqlite_connection_key.py client2.sqlite \
  --side listener --conn-key 2 \
  --topic topic_server1 --unique-name server1

# Sender file: peer listener topic + peer unique_name, same wire key
python3 python/set_sqlite_connection_key.py server1.sqlite \
  --side sender --conn-key 2 \
  --self-name server1 --self-topic topic_server1 \
  --topic topic_client --unique-name client2
```

---

**4. Client replies to the server (reverse direction)**

When a **client** must **`send_to` the server’s topic** (e.g. `topic_server1`) on isolated files, roles swap: the **client file is the sender**, the **server file is the listener**. You need the same kind of edits on **both** databases for a new wire key **K** (not necessarily the same integer as server→client for that peer).

| Process | Role on reply | What to set |
|---------|----------------|-------------|
| **`clientN.sqlite`** | sender | `conn_key_map`: `clientN:topic_client:server1` → **K**; bump **`seq`** |
| **`server1.sqlite`** | listener | `conn_sender`: **(K, `topic_client`)** so callback **`from`** is the client’s `source_topic`; bump **`seq`** |

**`--topic` on the server (listener) run** must be **`topic_client`**, not `topic_server1`.

Example: server→`client2` uses wire key **2** (section 3); client2→server uses a **different** key **K = 12** on both files to avoid overloading **`conn_sender(2, …)`** on the server (that file may already use key **2** for outbound `conn_key_map` to `client2`, and **`conn_sender`** has only one row per `connection_key`).

```bash
# client2 sends to server
python3 python/set_sqlite_connection_key.py client2.sqlite \
  --side sender --conn-key 12 \
  --self-name client2 --self-topic topic_client \
  --topic topic_server1 --unique-name server1

# server receives from client2
python3 python/set_sqlite_connection_key.py server1.sqlite \
  --side listener --conn-key 12 \
  --topic topic_client --unique-name client2
```

Then `client2` calls `send_to("topic_server1", payload, false)` (isolated files → **`at_least_once_delivery` false** on replies too unless you share one DB).

**One-to-one** isolated pair (only A and B, each seeds the other in `receivers_json`): both directions often use wire key **1** on each file (`A:topic_a:b` and `B:topic_b:a` are different composites, so `conn_key_map` does not collide). **One server, many clients** plus replies: plan **separate keys** for inbound vs outbound on the **server** file when the same integer would collide in **`conn_sender`**.

---

## Creating a client

### Rust

Use **`liner_broker::Client`** or **`liner_broker::Liner`**; both take **`at_least_once_delivery`** on **`send_to`** / **`send_all`**. The crate README still shows **`Liner::new`** for Redis.

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
   - B calls `send_to("topic_iso_a", payload, false)` in a retry loop until it returns `true` (TCP and routing may need a few tries). Use **`false`** here because A and B use **different** SQLite files; **`true`** would wait on acks in B’s DB that only A’s listener updates (see *Isolated files and `at_least_once_delivery`* above).

5. **Assert delivery**  
   - A’s callback should observe the payload within your timeout.

The test uses `std::env::temp_dir()` for paths and removes the directory at the end; your app would use real paths or configuration.

---

## Shared SQLite file vs isolated files

| Model | How catalog is shared |
|--------|------------------------|
| **One `.sqlite` file** opened by cooperating processes (same host, locking discipline) | `regist_topic` / `run` updates the same `topic_addr` / `topic_key`; peers see each other without JSON if they share that file. Prefer **empty** `receivers_json`. |
| **One file per process** (isolated empty DBs) | **One-to-one only:** each DB should list **at most one** remote peer in **`receivers_json`** (see *Isolated DBs: one-to-one only* above). First wire **`connection_key`** is **1**; seeded **`topic_key.k`** is **1**; seeding writes **`conn_sender`** for **`from`**. Use **`at_least_once_delivery == false`**. For **one-to-many / many-to-one**, use a **shared** SQLite path or Redis — not multiple isolated files with multi-row JSON. |

Do not mix **Redis** and **SQLite** for one logical mesh unless you intend two isolated systems ([backends.md](backends.md)).

---

## Related

- [using-the-api.md](using-the-api.md) — `run` order, threading, `send_to` pitfalls.  
- [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md) — building `libliner_broker` and linking C/C++.
