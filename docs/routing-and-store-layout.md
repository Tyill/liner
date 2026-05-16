# Routing and store layout (operators)

For **topic / delivery / error behavior** without store internals, see [behavior-topics-delivery-and-errors.md](behavior-topics-delivery-and-errors.md).

This document is for **people debugging production**: what gets written to **Redis** or **SQLite**, how **topics** map to **TCP addresses**, and how **connection keys** relate to **offline queues**. The behavior is shared between backends unless noted.

## Concepts

| Concept | Meaning |
|--------|---------|
| **`unique_name`** | Stable string identity of a **client instance** (per process). Used in store keys and in the composite id for a sender–listener channel. |
| **`source_topic`** | The client’s **current topic** (the one passed at construction / `set_source_topic`). |
| **`localhost` (bind string)** | The **TCP listen address** string (for example `127.0.0.1:2255`). Stored as the **field name** in the topic directory (Redis hash field / SQLite `topic_addr.addr`). |
| **Topic (string)** | Logical name peers use (`send_to("other_topic", …)`). |
| **Listener address** | A **reachable TCP endpoint** of another client, taken from the store when sending. |

## How routing works (high level)

1. **Directory: topic → TCP addresses**  
   When a client registers a topic (`regist_topic`), the store records a mapping: **topic name → (this client’s bind address → this client’s `unique_name`)**.  
   Senders resolve a destination topic with **`get_addresses_of_topic`**, then open **TCP** to those addresses. Multiple addresses under one topic mean multiple replicas; the Rust client **round-robins** across them (`last_send_index`).

2. **Who is on the other end**  
   For each listener address, the sender looks up **`get_listener_unique_name(destination_topic, addr)`** so it knows the **listener’s `unique_name`**. That name is part of the **composite** used to allocate a stable **`connection_key`** (integer) for that sender–listener pair.

3. **After a successful TCP connect**  
   The sender persists **`save_listener_for_sender(addr, listener_topic)`** so that after restart it can **`get_listeners_of_sender`** and reconnect to the same peers.

4. **Stale address cache**  
   The client caches addresses per topic in memory. If a peer re-registers on a **new** port, callers may need **`refresh_address_topic`** so the cache is refreshed from the store (see [using-the-api.md](using-the-api.md)).

5. **SQLite ordering**  
   Addresses for a topic are read **`ORDER BY addr ASC`**, which affects round-robin order. Redis uses **`HGETALL`** order (do not rely on a specific order for operations).

---

## Redis key reference

All keys use the literal prefix **`lnr_`**. Placeholders:

- `{topic}` — topic string  
- `{localhost}` — bind string used as hash **field**  
- `{unique}` — client `unique_name`  
- `{source_topic}` — sender’s topic  
- `{listener_name}` — **listener’s** `unique_name` (from the topic directory)  
- `{composite}` — string `"{unique}:{source_topic}:{listener_name}"` (three components, colon-separated)  
- `{connection_key}` — decimal string of integer id  
- `{sender_key}` — string `"{unique}:{source_topic}"` (two components)

| Key / pattern | Type | Purpose |
|---------------|------|---------|
| `lnr_topic:{topic}:addr` | **HASH** field → value | Field name = **`localhost`** bind string of a registrant; value = that client’s **`unique_name`**. Directory of who listens under `topic`. |
| `lnr_topic:{topic}:key` | **STRING** (int) | Stable small integer **topic key** for wire encoding / subscriptions. |
| `lnr_unique_key` | **STRING** (counter) | Global **`INCR`** source for new numeric ids (`connection_key`, `topic` key). |
| `lnr_connection:{composite}:key` | **STRING** (int) | Maps **`{unique}:{source_topic}:{listener_name}`** → **`connection_key`**. |
| `lnr_connection:{connection_key}:sender` | **STRING** | Sender’s **source topic** string for that logical channel. |
| `lnr_connection:{connection_key}:mess_number` | **STRING** (uint) | Last **acknowledged** message number for offline / dedup (see [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md)). |
| `lnr_connection:{connection_key}:messages` | **LIST** (binary blobs) | FIFO queue of encoded messages waiting for that connection. **`RPUSH`** / **`LPOP`** style drain when loading. |
| `lnr_sender:{sender_key}:listener` | **HASH** field → value | Field = **listener TCP address** string; value = **listener topic** string. Reconnect hints for this sender identity. |

### Redis maintenance notes

- **`clear_addresses_of_topic`** / **`clear_stored_messages`**: summary here; full **what is and is not deleted**, prefix policy, and **Redis ≥ 6.2** note → [operations-redis-sqlite.md](operations-redis-sqlite.md).

---

## SQLite schema (operator view)

Single file database; **WAL** and **`busy_timeout`** are set on open (see [backends.md](backends.md)). Tables:

| Table | Role |
|-------|------|
| **`seq`** | Single row `id = 1`, column **`v`**: monotonic counter for new **`connection_key`** values (`UPDATE … RETURN` pattern via `SELECT` after increment). |
| **`topic_addr`** | Rows `(topic, addr, client_name)` — same semantics as Redis `lnr_topic:{topic}:addr`: **`addr`** is the bind string, **`client_name`** is `unique_name`. **Primary key `(topic, addr)`**. |
| **`topic_key`** | `(topic, k)` — integer **topic key** per topic name. |
| **`conn_key_map`** | `(composite, connection_key)` where **`composite`** = `"{unique}:{source_topic}:{listener_name}"`. **`connection_key` is UNIQUE** in this table (only one composite may reference a given integer). Isolated seeding tries to assign key **1** to every peer row; with **several peers in one `receivers_json`**, later rows can evict earlier composites or force dynamic keys on send — see [using-sqlite.md](using-sqlite.md) (*Isolated DBs: one-to-one only*). |
| **`conn_sender`** | `(connection_key, sender_topic)` — maps wire **`connection_key`** to the sender’s topic for receive callback **`from`**. **Primary key** on **`connection_key`** (one topic per key on that process). |
| **`conn_mess_number`** | `(connection_key, v)` — last ack message number (same role as Redis `mess_number`). |
| **`sender_listener`** | `(sender_key, addr, listener_topic)` where **`sender_key`** = `"{unique}:{source_topic}"`. Same as Redis `lnr_sender:…:listener`. |
| **`conn_messages`** | `(id, connection_key, payload)` with **`AUTOINCREMENT id`**, index **`(connection_key, id)`**. Queue of encoded blobs; **FIFO** by ascending **`id`**. |

### SQLite maintenance notes

- **`clear_*` scope** and **backup / WAL** procedures → [operations-redis-sqlite.md](operations-redis-sqlite.md).

---

## Quick “where do I look?”

| Symptom | Redis | SQLite |
|---------|-------|--------|
| No addresses for topic `T` | `HGETALL lnr_topic:T:addr` | `SELECT * FROM topic_addr WHERE topic = 'T';` |
| Offline queue stuck | `LLEN lnr_connection:{id}:messages` | `SELECT COUNT(*) FROM conn_messages WHERE connection_key = ?;` |
| Dedup / ack cursor | `GET lnr_connection:{id}:mess_number` | `SELECT v FROM conn_mess_number WHERE connection_key = ?;` |
| Wrong peer / stale port | Check field names in `…:addr` match current bind strings | Same in **`topic_addr.addr`** |

---

## Related

- [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) — `connection_key`, `number_mess`, reconnect timing.  
- [backends.md](backends.md) — choosing Redis vs SQLite.  
- [using-the-api.md](using-the-api.md) — `refresh_address_topic`, `clear_*` only when not running.
- [operations-redis-sqlite.md](operations-redis-sqlite.md) — `clear_*` details, Redis version, SQLite backup.
