# Using the API (lifecycle, threading, pitfalls)

## Lifecycle (typical order)

1. **Create** a client with store parameters and local identity (`unique_name`, initial `topic`, `localhost` bind address, Redis URL or SQLite path).
2. Optionally call **`subscribe` / `unsubscribe`** before `run` (subscriptions are queued and applied when the listener starts).
3. Call **`run`** (C: `lnr_run`) to start the internal listener and sender loops. Until then, **`send_to` / `send_all`** return failure (“client not is running”).
4. Send and receive on the **same thread or different threads** only according to your binding’s thread-safety rules (see below).
5. **Destroy** the client (C: `lnr_delete_client`) when finished so connections and threads are torn down cleanly.

## Threading

- The Rust **`Client`** is guarded by an internal **`Mutex`**. Concurrent calls from multiple threads are serialized; avoid deadlocking by not calling back into the same client from inside a callback if that callback is invoked with the lock held (depends on your integration).
- After **`run`**, background **listener** and **sender** tasks own their own store handles (`open_store_mutex`) and event loops; they do not replace the client’s main `db` instance.
- **C / Python / other FFI**: assume **single-threaded use of a given `lnr_hClient`** unless you add your own synchronization. The Rust side will serialize if you call through from multiple threads, but your language bindings may not be safe across threads without care.

## TCP `localhost` / bind address

`localhost` must be a string **`ToSocketAddrs`** can resolve (for example `127.0.0.1:2255` or `0.0.0.0:2255`). If resolution or **bind** fails, **`run`** returns **`false`** (and logs).

## Topics and addresses

- You cannot **`send_to` / `send_all` / `subscribe` / `unsubscribe`** to your **own** source topic; those calls fail with an error message.
- **`send_to` / `send_all`** need known addresses for the destination topic. The client caches addresses from the store; use **`refresh_address_topic`** when a new peer registers under the same topic name.
- If no addresses exist for a topic, send fails with “not found addr for topic …”.

## Offline / persistence flags

C functions **`lnr_send_to`** and **`lnr_send_all`** take **`at_least_once_delivery`**. When `TRUE`, the stack may persist messages for offline delivery depending on topic and connection state. When `FALSE`, behavior is best-effort. Rust `Client` exposes the same flag on `send_to` / `send_all`; the **`Liner`** wrapper hard-codes **`true`** for sends. For persistence rules, reconnect timing, and per-message **`number_mess`** deduplication, see [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md).

## Clearing state

- **`clear_stored_messages`** and **`clear_addresses_of_topic`** are only allowed when the client is **not** running (`run` not called or client torn down). If called while running, they return failure and log. For exactly which Redis keys / SQLite rows are affected, see [operations-redis-sqlite.md](operations-redis-sqlite.md).

## Callbacks (receive path)

The receive callback receives **pointers into transient buffers** valid only for the duration of the callback. **Copy** data if you need it after returning.

## Checklist for integrators

1. Verify **store connectivity** before relying on `run` (create client already opens the store once).
2. After **`run`**, expect **stderr** for non-fatal store issues during steady operation.
3. Plan for **rare panic** on listener/sender store startup if the store fails between client creation and internal `open_store_mutex` (see [store-startup-failure-semantics.md](store-startup-failure-semantics.md)).
4. For SQLite on a shared file, expect **`SQLITE_BUSY`** under contention; tune workload or timeout at the SQLite/OS level if needed ([backends.md](backends.md)).
