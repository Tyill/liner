# Using the API (lifecycle, threading, pitfalls)

This guide describes how to create a client, start and stop it, send and receive messages, and use the additive helpers introduced in crate **1.4.0** (last-error codes, advertise address, introspection, log hook, and runtime size limits).

Related documents:

- [errors-and-logging.md](errors-and-logging.md) — error codes, stderr, log hook, status callback vs sync returns
- [store-startup-failure-semantics.md](store-startup-failure-semantics.md) — what happens when `run` fails after TCP bind
- [capacity-and-limits.md](capacity-and-limits.md) — message size and compression thresholds
- [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) — at-least-once delivery and message numbers

---

## Lifecycle (typical order)

Use this sequence for a normal integration:

1. **Create** a client with store parameters and local identity: `unique_name`, initial source `topic`, TCP **bind** string (`localhost`), and a Redis URL, SQLite path, or PostgreSQL URL (depending on backend).
2. Optionally call **`set_advertise_addr`** / `lnr_set_advertise_addr` **before** `run` if peers should connect to a different address than the bind string (for example when you bind `0.0.0.0` or an ephemeral port but want to publish `127.0.0.1` or a public/VPN address).
3. Optionally call **`subscribe` / `unsubscribe`** before `run`. Subscriptions are queued and applied when the listener starts.
4. Optionally register a **status callback** (`lnr_set_status_cb` / `Client::set_status_cb` / Python `set_status_callback`) for peer events and background sender/listener errors. You may set or clear it before or after `run`.
5. Optionally install a **process-global log hook** once (`lnr_set_log_cb` / Python `set_log_callback`) if you do not want `print_error!` lines on stderr.
6. Optionally tune **runtime size limits** (`lnr_set_max_message_size`, `lnr_set_compress_threshold`) before `run`. Prefer setting them early so the first frames already use the intended caps.
7. Call **`run`** (C: `lnr_run`) to bind TCP, register in the store catalog, and start the internal listener and sender loops.
   - Until `run` succeeds, **`send_to` / `send_all`** fail with **`LNR_ERR_NOT_RUNNING`**.
   - Calling **`run` again while already running** is idempotent: it returns **`true`** and sets **`LNR_OK`**.
   - If TCP bind/resolve fails → **`false`** and **`LNR_ERR_BIND`**.
   - If catalog registration fails → **`false`** and **`LNR_ERR_STORE`**.
   - If listener startup fails **after** a successful bind/registration → **`false`** and **`LNR_ERR_STARTUP`** (no process panic). See [store-startup-failure-semantics.md](store-startup-failure-semantics.md).
8. Send and receive according to your binding’s thread-safety rules (see **Threading** below).
9. Optionally call **`stop`** / `lnr_stop` to unregister from the store and join background threads **without** destroying the client handle. After `stop` you may call **`clear_*`** and **`run` again** on the same handle.
   - **`bound_listen_addr`** keeps the last successful bind address (useful for diagnostics).
   - **`published_addr`** is cleared (`NULL` / `None`) because the client is no longer in the store catalog.
10. **Destroy** the client when finished (C: `lnr_delete_client`). Drop / delete also call `stop` if the client is still running.

---

## Threading

- The Rust **`Client`** serializes API calls with an internal **`Mutex`**. Concurrent calls from multiple threads are safe at the Rust level, but you must still avoid deadlocks: do not call back into the same client from inside a receive or status callback if that path already holds the client lock in your integration.
- After **`run`**, the **listener** and **sender** tasks share the client’s store handle (`Arc<Mutex<dyn Store>>`). They do **not** open a second independent store connection at startup.
- **C / Python / other FFI:** treat a given **`lnr_hClient`** as **single-threaded** unless you add your own synchronization around the binding. Even though Rust serializes entry points, language wrappers (Python GIL, C++ wrappers, etc.) may not be safe to share across threads without care.

---

## TCP bind vs advertise address

These are two different strings:

| Concept | API | Meaning |
|---------|-----|---------|
| **Bind** | Constructor `localhost` | Where this process listens (`ToSocketAddrs`), e.g. `127.0.0.1:2255` or `0.0.0.0:0`. |
| **Advertise / published** | Optional `set_advertise_addr` → catalog | Address peers should dial. Written to the store and exposed as `published_addr` while registered. |

Rules:

- If resolution or TCP **bind** fails, **`run`** returns **`false`** and **`LNR_ERR_BIND`**.
- Call **`set_advertise_addr`** only while the client is **not** running. While running it returns **`false`** with **`LNR_ERR_ALREADY_RUNNING`**.
- Pass `NULL` / `""` / `None` to clear advertise and fall back to publishing the bound address (legacy behavior).
- An advertise string that does not parse as a socket address returns **`LNR_ERR_INVALID_ARG`**.
- If advertise uses port **`0`**, **`run`** rewrites that port from the actual ephemeral bound port so peers never see `host:0` in the catalog.
- Without advertise, the catalog receives the bound listen address (or the original bind string if the OS did not report a local address).

**Getters**

| Getter | While running | After `stop` |
|--------|---------------|--------------|
| `unique_name` | Always available | Always available |
| `topic` | Constructor source topic | Same |
| `bind_addr` | Constructor TCP bind string | Same (not rewritten by ephemeral bind) |
| `advertise_addr` | Configured advertise, if set | Same (independent of catalog) |
| `bound_listen_addr` | Actual local bind address after successful `run` | **Kept** (last successful bind) |
| `published_addr` | String currently registered in the store | **Cleared** (`NULL` / `None`) |

---

## Last error code

Every synchronous API call that can fail sets a per-client **last error code**:

- C: `lnr_last_error_code`
- Rust: `Client::last_error` → `ErrorCode`
- Python: `last_error_code`

Successful calls clear it to **`LNR_OK`**. Detail text is available via **`lnr_last_error_message`** / `Client::last_error_message` (bare string), plus **stderr** or the process-global **log hook**.

Full code table and per-function outcomes: [errors-and-logging.md](errors-and-logging.md).

---

## Introspection

### List addresses for a topic

**`lnr_list_addresses` / `Client::list_addresses` / Python `list_addresses`** reads the topic directory from the **store** (not only the in-memory send cache) and returns rows of `(addr, unique_name)`.

- **C:** invokes your `lnr_addr_cb` once per row. An empty topic is still **success** with **zero** callbacks.
- **Rust / Python:** return an empty list on success when nothing is registered.
- On database errors: C returns `FALSE`, Rust/Python return `None` / raise according to binding, and last error is **`LNR_ERR_STORE`**.
- On success the client also refreshes its in-memory address cache for that topic (same idea as a successful `refresh_address_topic`).

### Pending offline messages

**`lnr_pending_count` / `Client::pending_count` / Python `pending_count`** sums offline queued blobs for **this sender identity** in the store.

- Returns **`0`** when there is nothing pending.
- Returns **`-1`** (C) or **`None`** (Rust) on store error; check `lnr_last_error_code`.
- Depth can **lag** while at-least-once payloads still sit in the sender’s in-memory queues. Typical moments when the store catches up: after a peer is lost, or after **`stop`** (sender teardown flushes to the store).

**`lnr_pending_by_peer` / `Client::pending_by_peer`** walks the same routes and reports **per peer** `(addr, topic, unique_name, count)`. Sum of counts matches `pending_count` when both succeed.

### In-memory send queue

**`lnr_send_queue_depth` / `Client::send_queue_depth`** sums messages waiting in the sender’s **RAM** worklists (what `max_send_queue` / `LNR_ERR_BUSY` constrain). Returns **`0`** if the client is not running. This is **not** the store offline depth.

**`lnr_send_queue_depth_by_peer`** reports `(addr, count)` for known send routes. Sum of counts matches `send_queue_depth` for those routes.

### Subscriptions and related topics

- **`lnr_list_subscriptions`**: app-facing subscriptions (excludes `__#internal_channel`).
- **`lnr_list_related_topics`**: topics this client has sent to, subscribed to, or refreshed — same set as the status peer-event filter.

Both are in-memory only (no store round-trip); empty → success with zero callbacks.

---

## Runtime limits

Process-global tunables:

| Tunable | Default | Setters / getters |
|---------|---------|-------------------|
| Max framed TCP message size | 1 GiB | `lnr_set_max_message_size` / `lnr_get_max_message_size` |
| Min payload size before zstd is attempted | 1 MiB | `lnr_set_compress_threshold` / `lnr_get_compress_threshold` |
| Max in-memory sender messages **per peer** | unlimited (`0`) | `lnr_set_max_send_queue` / `lnr_get_max_send_queue` |
| Stream availability poll interval | 10 s | `lnr_set_stream_check_timeout_ms` / getter |
| Bytestream would-block wait | 10 s | `lnr_set_would_block_timeout_ms` / getter |

- Size / timeout setters reject **`0`** (`FALSE`), except **`max_send_queue`** where **`0` means unlimited**.
- Prefer setting values **before** `run`. Changing later is allowed, but only **new** frames / waits / enqueues see the new values.
- When `max_send_queue > 0` and a peer’s in-memory worklist is full, `send_to` / `send_all` return **`FALSE`** + **`LNR_ERR_BUSY`** (and may emit **`LNR_SENDER_BUSY`**). Other peers are unaffected.
- Details: [capacity-and-limits.md](capacity-and-limits.md).

---

## Topics and addresses

- You cannot **`send_to` / `send_all` / `subscribe` / `unsubscribe`** to your **own** source topic. Those calls fail (`LNR_ERR_SELF_TOPIC`) and log.
- **`send_to` / `send_all`** need known addresses for the destination topic. The client caches addresses from the store (see **Internal channel** below).
- If no addresses exist for a topic, send fails with **`LNR_ERR_NO_ADDR`** (and a “not found addr for topic …” log line).
- You cannot **`subscribe` / `unsubscribe`** the reserved internal topic **`__#internal_channel`** via the public API (`LNR_ERR_INTERNAL_TOPIC`). The library subscribes to it automatically on **`run`**.

### Internal channel (`__#internal_channel`)

Every running client is subscribed to the reserved topic **`__#internal_channel`**. The broker uses it for **control events only**. Those events are **not** delivered to your receive callback.

| Event | When | Effect on other clients |
|-------|------|-------------------------|
| `client_connected` | after **`run`** | refresh address cache for the peer’s **source topic** and for the internal channel |
| `client_disconnected` | on client teardown / **`stop`** | same refresh from the store |
| `subscribed` | after **`subscribe`** while running | refresh cache for the **subscribed topic** |
| `unsubscribed` | after **`unsubscribe`** while running | refresh cache for that topic (no subscribers left ⇒ **`send_to`** fails after refresh) |

Payloads are JSON, for example: `{"event":"subscribed","client":"peer_name","topic":"foo"}`.

**Typical mesh (happy path):** a producer does **not** need **`refresh_address_topic`** before every **`send_to`** when peers **`run`** or **`subscribe`** while the producer is already running — peers learn each other’s routes via these events.

**When `refresh_address_topic` may still be needed:**

1. **Subscribe before `run`** — `subscribe` queues locally and registers in the store, but no `subscribed` event is emitted until the client is running. Other peers learn the route on the first **`send_to`** if their cache is empty (lookup from the store), or after an explicit **`refresh_address_topic`**.
2. **Race** — a peer just subscribed; the internal event has not arrived yet. The first **`send_to`** may fail; retry shortly or call **`refresh_address_topic`**.
3. **Stale cache** — a peer re-registered on a **new port** without a clean disconnect sequence. Call **`refresh_address_topic(topic)`** to force a reload from the store.
4. **Producer was not running** when the peer registered — no internal events were processed; refresh or send (an empty cache loads from the store on first lookup).

**`refresh_address_topic`** is the explicit force-reload. It **removes** a cached topic when the store currently has no addresses for it.

---

## Offline / persistence flags

C functions **`lnr_send_to`** and **`lnr_send_all`** take **`at_least_once_delivery`**.

- When `TRUE`, the stack may persist messages for offline delivery depending on topic and connection state.
- When `FALSE`, behavior is best-effort.

Rust **`Client`** and **`Liner`** expose the same flag on **`send_to`** / **`send_all`**.

If peers use **different SQLite files** (no shared store), pass **`false`** for cross-peer sends — see [using-sqlite.md](using-sqlite.md) (*Isolated files and `at_least_once_delivery`*). For persistence rules, reconnect timing, and per-message **`number_mess`** deduplication, see [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md).

To inspect how many offline blobs this sender currently has in the store, use **`pending_count`** (see **Introspection** above).

---

## Clearing state

**`clear_stored_messages`** and **`clear_addresses_of_topic`** are allowed only when the client is **not** running:

- before the first successful `run`, or
- after **`stop`**, or
- after the client has been torn down.

If called while running, they return failure with **`LNR_ERR_CLEAR_WHILE_RUNNING`** and log. For which Redis keys / SQLite rows are affected, see [operations-redis-sqlite.md](operations-redis-sqlite.md).

---

## Callbacks (receive path)

The receive callback receives **pointers into transient buffers** that are valid only for the duration of the callback. **Copy** topic names and payload bytes if you need them after returning.

---

## Status / background-error callback

Additive API:

- C: **`lnr_set_status_cb`**
- Rust: **`Client::set_status_cb`** / **`Liner::set_status_callback`**
- Python: **`set_status_callback`**

Pass a null / empty callback to clear. Safe before or after **`run`**.

C signature:

```c
void (*)(int kind, const char* topic, const char* peer, const char* message, lnr_uData);
```

All string pointers are valid **only during the call**.

| Kind | Meaning |
|------|---------|
| `LNR_PEER_CONNECTED` (1) | Peer `run` / internal `client_connected` |
| `LNR_PEER_DISCONNECTED` (2) | Peer teardown / `client_disconnected` |
| `LNR_PEER_SUBSCRIBED` (3) | Peer `subscribe` |
| `LNR_PEER_UNSUBSCRIBED` (4) | Peer `unsubscribe` |
| `LNR_SENDER_ROUTE_LOST` (5) | **Sender:** TCP connect fail or stream close |
| `LNR_SENDER_STORE_ERROR` (6) | **Sender:** background store error (reconnect / persist) |
| `LNR_SENDER_SEND_ERROR` (7) | **Sender:** write/flush failure after an accepted send |
| `LNR_LISTENER_STORE_ERROR` (8) | **Listener:** background store error (ack / lookup) |

**Related-topic filter (peer kinds only):** `LNR_PEER_*` events are delivered only for topics this client has previously **sent to**, **subscribed to**, or **refreshed** via `refresh_address_topic`. The internal channel still fans out control events to all peers for cache refresh; the filter applies only to the user status callback. Local sender/listener error kinds are not filtered that way.

**Threading:** status callbacks may run on **listener** or **sender** background threads. Use the same caution as for the receive callback — do not re-enter the same client without care.

Synchronous API failures still return **`false` / `NULL`** and may log to **stderr** (or the log hook). They are **not** redirected exclusively into the status callback.

---

## Process-global log hook

By default, `print_error!` writes to stderr. You can install one process-wide sink:

- C: `lnr_set_log_cb(lnr_log_cb cb, lnr_uData)` — pass `cb == NULL` to restore stderr
- Rust: `set_log_cb` / `Liner::set_log_callback`
- Python: `set_log_callback`

The hook is **not** per-client. A later `set` replaces the previous callback. See [errors-and-logging.md](errors-and-logging.md).

---

## Checklist for integrators

1. Verify **store connectivity** before relying on `run` (creating a client already opens the store once).
2. After **`run`**, expect **stderr** or the log hook for non-fatal store issues during steady operation.
3. Treat **`LNR_ERR_STARTUP`** on `run` as a recoverable setup failure, not a process abort ([store-startup-failure-semantics.md](store-startup-failure-semantics.md)).
4. If you bind on `0.0.0.0` or port `0`, set **`advertise`** to a reachable host (and optionally port `0` for rewrite) so peers dial the right address.
5. For SQLite on a shared file, expect **`SQLITE_BUSY`** under contention; tune workload or timeout at the SQLite/OS level if needed ([backends.md](backends.md)).
6. Lower **`lnr_set_max_message_size`** before `run` if the default 1 GiB framed-message cap is too high for your threat model.
