# Errors and logging

This document explains how liner reports failures: stderr / log hook text, per-client sync error codes, C and Rust return values, and how those relate to the optional status callback.

For lifecycle and when to call each API, see [using-the-api.md](using-the-api.md). For listener startup failures inside `run`, see [store-startup-failure-semantics.md](store-startup-failure-semantics.md).

---

## Where human-readable messages go

Failures in the Rust core are often logged with the `print_error!` macro. Each line looks like:

```text
Error <file>:<line>: <message>
```

**Default sink:** standard error (`eprintln!`).

**Optional sink:** a **process-global** log hook.

| Binding | Install / clear |
|---------|-----------------|
| C | `lnr_set_log_cb(lnr_log_cb cb, lnr_uData)` — pass `cb == NULL` to restore stderr |
| Rust | `liner_broker::set_log_cb` / `Liner::set_log_callback` |
| Python | `set_log_callback` |

Notes:

- The hook is **one per process**, not per client. Daemons typically install it once at startup.
- Calling `lnr_set_log_cb` again **replaces** the previous callback and userdata.
- `lnr_set_log_cb` always returns **`TRUE`** for a valid call (install or clear).
- There is **no** public API that returns the error message string to the caller. Use stderr or the log hook for text; use **`lnr_last_error_code`** for a stable machine-readable code.

---

## Sync error codes (`LNR_OK` / `LNR_ERR_*`)

Each client keeps a **last error code** updated by synchronous API calls. Successful calls set **`LNR_OK`**.

| Code | Name | Typical case |
|------|------|----------------|
| 0 | `LNR_OK` | Success, or cleared after a successful call. Also set by an idempotent **`run`** while already running. |
| 1 | `LNR_ERR_NOT_RUNNING` | `send_to` / `send_all` before `run`, or after `stop` while not running. |
| 2 | `LNR_ERR_ALREADY_RUNNING` | `set_advertise_addr` while the client is running. |
| 3 | `LNR_ERR_SELF_TOPIC` | Send or subscribe/unsubscribe targeting your own source topic. |
| 4 | `LNR_ERR_INTERNAL_TOPIC` | Public subscribe/unsubscribe of `__#internal_channel`. |
| 5 | `LNR_ERR_NO_ADDR` | Destination topic has no addresses in cache/store. |
| 6 | `LNR_ERR_BIND` | Bind string could not be resolved, or TCP `bind` failed. |
| 7 | `LNR_ERR_STORE` | Redis / SQLite / PostgreSQL operation failed. |
| 8 | `LNR_ERR_INVALID_ARG` | Invalid advertise address (or related argument validation). |
| 9 | `LNR_ERR_CLEAR_WHILE_RUNNING` | `clear_stored_messages` / `clear_addresses_of_topic` while running. |
| 10 | `LNR_ERR_STARTUP` | Listener startup failed after TCP bind and catalog registration (mio poll/register/waker, or `get_topic_key`). |

**Accessors**

- C: `int lnr_last_error_code(lnr_hClient client)` — returns `LNR_OK` for a null handle
- Rust: `Client::last_error() -> ErrorCode`
- Python: `last_error_code() -> int`

---

## C API outcomes (`include/liner.h`)

| Situation | Typical outcome |
|-----------|------------------|
| Invalid client handle (`NULL`) on any function that takes `lnr_hClient` | `FALSE` / `0` / `NULL` as appropriate; may log `client was not created` |
| `lnr_new_client_redis` / `lnr_new_client` | `NULL` on failure: null/invalid UTF-8 pointers, empty `unique_name`, `topic`, `localhost`, or store string, or **store could not be opened** (Redis unreachable, etc.) |
| `lnr_new_client_sqlite` | `NULL` for the same pointer/empty-string rules, **SQLite open failure**, **invalid non-empty `receivers_json`**, or **`seed_receivers`** / DB errors. **`NULL` or empty `receivers_json`**, or JSON **`[]`**, is **not** an error (no seeding). |
| `lnr_new_client_postgres` | `NULL` on failure (requires build with **`postgres`** feature): null/invalid pointers, empty strings, or **PostgreSQL connection / schema errors** |
| `lnr_run` | `TRUE` if started, or already running (`LNR_OK`). `FALSE` + `LNR_ERR_BIND` / `LNR_ERR_STORE` on bind or registration failure. `FALSE` + **`LNR_ERR_STARTUP`** if listener setup fails after bind (no panic). |
| `lnr_stop` | `TRUE` (idempotent). Clears `published_addr`, keeps `bound_listen_addr`. Sets `LNR_OK`. |
| `lnr_set_advertise_addr` | `TRUE` before `run` (including clear with `NULL`/`""`). `FALSE` + `LNR_ERR_ALREADY_RUNNING` while running; `FALSE` + `LNR_ERR_INVALID_ARG` for a bad address. |
| `lnr_last_error_code` | Last sync code for that client; `LNR_OK` for null handle. |
| `lnr_set_log_cb` | Always `TRUE`; installs or clears (`cb == NULL`) the process-global error log sink. |
| `lnr_list_addresses` | `TRUE` and zero or more `lnr_addr_cb` invocations (empty topic ⇒ no callbacks). `FALSE` + `LNR_ERR_STORE` on DB error. |
| `lnr_pending_count` | Non-negative depth of this sender’s offline blobs; `0` if none; `-1` on error (then check `lnr_last_error_code`). |
| `lnr_set_max_message_size` / `lnr_set_compress_threshold` | Process-global. `FALSE` if `bytes == 0`. Prefer set before `run`. |
| `lnr_set_status_cb` | `TRUE` if the client handle is valid; `FALSE` on null/unknown handle. Registers or clears (`cb == NULL`) the status callback. |
| `lnr_send_to`, `lnr_send_all`, subscribe, refresh, clear, … | `FALSE` on logical or I/O errors; inspect **`lnr_last_error_code`** and stderr/log hook. |

### Sync return vs status callback

These channels answer different questions:

| Concern | Where it surfaces |
|---------|-------------------|
| Create / `run` / `send_*` / subscribe validation / clear / advertise | Immediate **`NULL` / `FALSE`** + **`lnr_last_error_code`**, often plus stderr / log hook |
| Peer connect / disconnect / subscribe / unsubscribe (related topics only) | Status callback `LNR_PEER_*` |
| TCP connect fail / stream close / write flush fail (**sender**) | Status callback `LNR_SENDER_ROUTE_LOST` / `LNR_SENDER_SEND_ERROR`, plus stderr / log hook |
| Background store errors on reconnect/persist (**sender**) | Status callback `LNR_SENDER_STORE_ERROR`, plus stderr / log hook |
| Background store errors on ack/lookup (**listener**) | Status callback `LNR_LISTENER_STORE_ERROR`, plus stderr / log hook |

The status callback does **not** replace sync return codes. See [using-the-api.md](using-the-api.md) (*Status / background-error callback*) for kinds and the related-topic filter.

---

## Rust `Client` (`liner_broker::client::Client`)

| API | Success | Failure |
|-----|---------|---------|
| `Client::new_redis` / `Client::new` | `Some(Client)` | `None` if the store cannot be opened — **silent** (no `print_error!` from this path); check `None` |
| `Client::new_sqlite` | `Some(Client)` | `None` if the store cannot be opened (silent), **`receivers_json` cannot be parsed** as a JSON array of seed entries (logs), **`seed_receivers`** fails (logs), or invalid UTF-8 would only arise from Rust `&str` callers |
| `Client::new_postgres` | `Some(Client)` | `None` if PostgreSQL cannot be opened (**`postgres`** feature required at compile time) |
| `run` | `true` if the event loop starts, or already running (`ErrorCode::Ok`) | `false` + `Bind` / `Store` / **`Startup`** / … |
| `stop` | `true` (idempotent) | N/A for a live client |
| `set_advertise_addr` | `true` when not running | `false` + `AlreadyRunning` / `InvalidArg` |
| `set_status_cb` | always succeeds for a live client (registers or clears) | N/A (invalid handle only via C `lnr_set_status_cb`) |
| `list_addresses` | `Some(rows)` including empty | `None` + `Store` |
| `pending_count` | `Some(n)` (`0` if none) | `None` + `Store` |
| `send_to` / `send_all` | `true` if the send path reports success | `false` + last error (`NotRunning`, `SelfTopic`, `NoAddr`, `Store`, …) |
| `subscribe` / `unsubscribe` | `true` | `false` + last error |
| `refresh_address_topic` | `true` if addresses were found | `false` + `NoAddr` / `Store` |
| `clear_stored_messages` / `clear_addresses_of_topic` | `true` only when **not** running | `false` + `ClearWhileRunning` or `Store` |

Internal store errors are wrapped as `DbError` (string message from Redis or SQLite / `rusqlite`). They propagate as `false` / failed operations where the client checks `Result`; they do **not** automatically panic in the client layer.

---

## Rust `Liner` wrapper (`liner_broker::Liner`)

`Liner::new` / `Liner::new_sqlite` / `Liner::new_postgres` use the C constructors. If the returned handle is null, the wrapper **`panic!`**s (`error create client`). They also use `CString::new(...).unwrap()` — strings with an **embedded NUL** byte will panic.

Prefer **`Client::new_*` directly** if you need non-panicking construction and typed `ErrorCode` after sync calls.

---

## Mutex poison

A few paths use `Mutex::lock().unwrap()` on the client’s internal mutex. If another thread panics while holding that lock, subsequent operations can **panic** with a poison error. This is unrelated to Redis/SQLite being “busy”; it indicates an earlier panic in your process.

---

## Summary for production

1. Treat **`NULL` / `None` / `FALSE`** as normal failure modes. Read **`lnr_last_error_code`** for the machine code and **stderr** (or the log hook) for the detail line. Optionally register **`lnr_set_status_cb`** for peer and background operational events.
2. **`lnr_run`** returning `FALSE` with **`LNR_ERR_STARTUP`** means listener setup failed after bind — the process stays alive; fix configuration / store / resources and retry.
3. For maximum control over construction errors, use **`Client::new_*` in Rust** instead of `Liner::new` / `Liner::new_sqlite`.
