# Using the API (lifecycle, threading, pitfalls)

## Lifecycle (typical order)

1. **Create** a client with store parameters and local identity (`unique_name`, initial `topic`, `localhost` bind address, Redis URL or SQLite path).
2. Optionally call **`set_advertise_addr`** / `lnr_set_advertise_addr` before `run` so the store catalog lists a reachable address (e.g. when binding `0.0.0.0` or an ephemeral port).
3. Optionally call **`subscribe` / `unsubscribe`** before `run` (subscriptions are queued and applied when the listener starts).
4. Optionally call **`lnr_set_status_cb`** / `Client::set_status_cb` / `Liner::set_status_callback` (before or after `run`) for peer and background-error notifications. Optionally call **`lnr_set_log_cb`** once per process to redirect `print_error!` away from stderr.
5. Optionally tune **`lnr_set_max_message_size`** / **`lnr_set_compress_threshold`** before `run`.
6. Call **`run`** (C: `lnr_run`) to start the internal listener and sender loops. Until then, **`send_to` / `send_all`** return failure (`LNR_ERR_NOT_RUNNING`). Calling **`run` again while already running** returns **`true`** and sets **`LNR_OK`** (idempotent). Listener startup failure after bind returns **`false`** + **`LNR_ERR_STARTUP`** (no panic).
7. Send and receive on the **same thread or different threads** only according to your binding’s thread-safety rules (see below).
8. Optionally **`stop`** / `lnr_stop` to unregister and join threads without destroying the handle — then **`clear_*`** and a fresh **`run`** are allowed. **`bound_listen_addr`** remains (last bind); **`published_addr`** is cleared (`NULL`) because the client is no longer in the store catalog.
9. **Destroy** the client (C: `lnr_delete_client`) when finished (`Drop` / delete also stop if still running).

## Threading

- The Rust **`Client`** is guarded by an internal **`Mutex`**. Concurrent calls from multiple threads are serialized; avoid deadlocking by not calling back into the same client from inside a callback if that callback is invoked with the lock held (depends on your integration).
- After **`run`**, background **listener** and **sender** tasks own their own store handles (`open_store_mutex`) and event loops; they do not replace the client’s main `db` instance.
- **C / Python / other FFI**: assume **single-threaded use of a given `lnr_hClient`** unless you add your own synchronization. The Rust side will serialize if you call through from multiple threads, but your language bindings may not be safe across threads without care.

## TCP bind vs advertise

- **`localhost` (constructor)** is the **bind** string (`ToSocketAddrs`, e.g. `127.0.0.1:2255` or `0.0.0.0:0`). If resolution or **bind** fails, **`run`** returns **`false`** and sets **`LNR_ERR_BIND`**.
- **`set_advertise_addr(addr)`** (before `run` only): optional address written to the store catalog for peers to connect to. `NULL` / `""` / `None` clears it. While running → **`false`** + **`LNR_ERR_ALREADY_RUNNING`**. Invalid address → **`LNR_ERR_INVALID_ARG`**.
- If advertise uses port **`0`**, **`run`** rewrites that port from the actual bound ephemeral port.
- Without advertise, the catalog gets the bound address (same as before).
- Getters: **`unique_name`**, **`bound_listen_addr`** (kept after `stop`), **`published_addr`** (catalog string while registered; `NULL` after `stop`).

## Last error code

Sync failures still return **`false` / `NULL`** and log to **stderr** (or the process-global **log hook**). Additionally, **`lnr_last_error_code`** / `Client::last_error` / Python **`last_error_code`** returns a stable `LNR_OK` / `LNR_ERR_*` code for the last sync call on that client (see [errors-and-logging.md](errors-and-logging.md)). There is **no** public error-message getter.

## Introspection

- **`lnr_list_addresses` / `Client::list_addresses`**: topic directory from the store as `(addr, unique_name)` rows. Empty topic ⇒ success with zero callbacks / empty `Vec`. DB errors ⇒ `false` / `None` + `LNR_ERR_STORE`. Also refreshes the in-memory address cache like a successful refresh.
- **`lnr_pending_count` / `Client::pending_count`**: sum of offline queued blobs for this sender identity (`0` if none; `-1` / `None` on error). Depth may lag until the sender flushes in-memory at-least-once queues (e.g. after peer loss or `stop`).

## Runtime limits

Process-global **`lnr_set_max_message_size`** / **`lnr_set_compress_threshold`** (and getters). `0` is rejected (`FALSE`). Prefer set **before `run`**. Details: [capacity-and-limits.md](capacity-and-limits.md).

## Topics and addresses

- You cannot **`send_to` / `send_all` / `subscribe` / `unsubscribe`** to your **own** source topic; those calls fail with an error message.
- **`send_to` / `send_all`** need known addresses for the destination topic. The client caches addresses from the store (see **Internal channel** below).
- If no addresses exist for a topic, send fails with “not found addr for topic …”.
- You cannot **`subscribe` / `unsubscribe`** the reserved internal topic **`__#internal_channel`** via the public API; the library subscribes automatically on **`run`**.

### Internal channel (`__#internal_channel`)

Every running client is subscribed to the reserved topic **`__#internal_channel`**. The broker uses it for **control events only** (not delivered to your receive callback):

| Event | When | Effect on other clients |
|-------|------|-------------------------|
| `client_connected` | after **`run`** | refresh address cache for the peer’s **source topic** and for the internal channel |
| `client_disconnected` | on client teardown | same refresh from the store |
| `subscribed` | after **`subscribe`** while running | refresh cache for the **subscribed topic** |
| `unsubscribed` | after **`unsubscribe`** while running | refresh cache for that topic (no subscribers left ⇒ **`send_to`** fails after refresh) |

Payloads are JSON, for example: `{"event":"subscribed","client":"peer_name","topic":"foo"}`.

**Typical mesh (happy path):** a producer does **not** need **`refresh_address_topic`** before every **`send_to`** when peers **`run`** or **`subscribe`** while the producer is already running — peers pull each other’s routes via these events.

**Nuances — when `refresh_address_topic` may still be needed:**

1. **Subscribe before `run`** — `subscribe` queues locally and registers in the store, but no `subscribed` event is emitted until the client is running. Other peers learn the route on the first **`send_to`** if their cache is empty (lookup from the store), or after **`refresh_address_topic`**.
2. **Race** — a peer just subscribed; the internal event has not arrived yet. The first **`send_to`** may fail; retry shortly or call **`refresh_address_topic`**.
3. **Stale cache** — a peer re-registered on a **new port** without a clean disconnect/disconnect event sequence. Call **`refresh_address_topic(topic)`** to force a reload from the store.
4. **Producer was not running** when the peer registered — no internal events were processed; refresh or send (empty cache loads from the store on first lookup).

**`refresh_address_topic`** remains the explicit way to force a cache reload; it **removes** a cached topic when the store has no addresses.

## Offline / persistence flags

C functions **`lnr_send_to`** and **`lnr_send_all`** take **`at_least_once_delivery`**. When `TRUE`, the stack may persist messages for offline delivery depending on topic and connection state. When `FALSE`, behavior is best-effort. Rust **`Client`** and **`Liner`** expose the same flag on **`send_to`** / **`send_all`**. If peers use **different SQLite files** (no shared store), pass **`false`** for cross-peer sends—see [using-sqlite.md](using-sqlite.md) (*Isolated files and `at_least_once_delivery`*). For persistence rules, reconnect timing, and per-message **`number_mess`** deduplication, see [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md).

## Clearing state

- **`clear_stored_messages`** and **`clear_addresses_of_topic`** are only allowed when the client is **not** running (`run` not called, after **`stop`**, or client torn down). If called while running, they return failure (`LNR_ERR_CLEAR_WHILE_RUNNING`) and log. For exactly which Redis keys / SQLite rows are affected, see [operations-redis-sqlite.md](operations-redis-sqlite.md).

## Callbacks (receive path)

The receive callback receives **pointers into transient buffers** valid only for the duration of the callback. **Copy** data if you need them after returning.

## Status / background-error callback

Additive API: **`lnr_set_status_cb`** (C), **`Client::set_status_cb`**, **`Liner::set_status_callback`**, Python **`set_status_callback`**. Pass a null/empty callback to clear. Safe before or after **`run`**.

Signature (C): `void (*)(int kind, const char* topic, const char* peer, const char* message, lnr_uData)`. Pointers are valid **only during the call**.

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

**Related filter (peer kinds only):** `LNR_PEER_*` events are delivered only for topics this client has previously **sent to**, **subscribed to**, or **refreshed** via `refresh_address_topic`. The internal channel still fans out control events to all peers for cache refresh; the filter applies only to the user status callback. Local sender/listener error kinds are not filtered that way.

**Threading:** status callbacks may run on **listener** or **sender** background threads (same caution as the receive callback — do not re-enter the same client without care).

Synchronous API failures still return **`false` / `NULL`** and may log to **stderr** (or the log hook); they are not redirected exclusively into the status callback.

## Checklist for integrators

1. Verify **store connectivity** before relying on `run` (create client already opens the store once).
2. After **`run`**, expect **stderr** / log hook for non-fatal store issues during steady operation.
3. Treat **`LNR_ERR_STARTUP`** on `run` as a recoverable setup failure (see [store-startup-failure-semantics.md](store-startup-failure-semantics.md)).
4. For SQLite on a shared file, expect **`SQLITE_BUSY`** under contention; tune workload or timeout at the SQLite/OS level if needed ([backends.md](backends.md)).
