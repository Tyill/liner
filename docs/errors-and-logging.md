# Errors and logging

## Where messages go

Failures in the Rust core are often logged with the `print_error!` macro: a line is written to **standard error** in the form:

`Error <file>:<line>: <message>`

Sync API calls also set a per-client **last error code** (`lnr_last_error_code` / `Client::last_error`). There is **no** public error-message string API — use stderr for detail text.

## Sync error codes (`LNR_OK` / `LNR_ERR_*`)

| Code | Name | Typical case |
|------|------|----------------|
| 0 | `LNR_OK` | success / cleared (including idempotent `run` while already running) |
| 1 | `LNR_ERR_NOT_RUNNING` | send before `run` / after `stop` |
| 2 | `LNR_ERR_ALREADY_RUNNING` | `set_advertise_addr` while running |
| 3 | `LNR_ERR_SELF_TOPIC` | send/sub own topic |
| 4 | `LNR_ERR_INTERNAL_TOPIC` | sub/unsub `__#internal_channel` |
| 5 | `LNR_ERR_NO_ADDR` | topic has no addresses |
| 6 | `LNR_ERR_BIND` | resolve/bind failure |
| 7 | `LNR_ERR_STORE` | Redis/SQLite/Postgres op failed |
| 8 | `LNR_ERR_INVALID_ARG` | bad advertise address |
| 9 | `LNR_ERR_CLEAR_WHILE_RUNNING` | `clear_*` while running |

## C API (`include/liner.h`)

| Situation | Typical outcome |
|-----------|------------------|
| Invalid client handle (`NULL`) on any function that takes `lnr_hClient` | `FALSE` / `0`; may log `client was not created` |
| `lnr_new_client_redis` / `lnr_new_client` | `NULL` on failure: null/invalid UTF-8 pointers, empty `unique_name`, `topic`, `localhost`, or store string, or **store could not be opened** (Redis unreachable, etc.) |
| `lnr_new_client_sqlite` | `NULL` for the same pointer/empty-string rules, **SQLite open failure**, **invalid non-empty `receivers_json`**, or **`seed_receivers`** / DB errors. **`NULL` or empty `receivers_json`**, or JSON **`[]`**, is **not** an error (no seeding). |
| `lnr_new_client_postgres` | `NULL` on failure (requires build with **`postgres`** feature): null/invalid pointers, empty strings, or **PostgreSQL connection / schema errors** |
| `lnr_run` | `TRUE` if started or **already running** (`LNR_OK`); `FALSE` if registration or bind failed (`LNR_ERR_BIND` / `LNR_ERR_STORE`); **may panic** if listener/sender store startup fails internally (see [store-startup-failure-semantics.md](store-startup-failure-semantics.md)) |
| `lnr_stop` | `TRUE` (idempotent); clears `published_addr`, keeps `bound_listen_addr` |
| `lnr_set_advertise_addr` | `TRUE` before `run`; `FALSE` + `LNR_ERR_ALREADY_RUNNING` while running |
| `lnr_last_error_code` | `LNR_OK` / `LNR_ERR_*` for last sync call; `LNR_OK` for null handle |
| `lnr_set_status_cb` | `TRUE` if the client handle is valid; `FALSE` on null/unknown handle. Registers or clears (`cb == NULL`) the status callback |
| `lnr_send_to`, `lnr_send_all`, … | `FALSE` on logical or I/O errors; check **`lnr_last_error_code`**; see [using-the-api.md](using-the-api.md) |

### Sync return vs status callback

| Concern | Where it surfaces |
|---------|-------------------|
| Create / `run` / `send_*` / subscribe validation | Immediate **`NULL` / `false`** + **`lnr_last_error_code`** (+ often stderr) |
| Peer connect/disconnect/sub/unsub (related topics only) | Status callback `LNR_PEER_*` |
| TCP connect fail / stream close / write flush fail (**sender**) | Status callback `LNR_SENDER_ROUTE_LOST` / `LNR_SENDER_SEND_ERROR` (+ stderr) |
| Background store errors on reconnect/persist (**sender**) | Status callback `LNR_SENDER_STORE_ERROR` (+ stderr) |
| Background store errors on ack/lookup (**listener**) | Status callback `LNR_LISTENER_STORE_ERROR` (+ stderr) |

See [using-the-api.md](using-the-api.md) (*Status / background-error callback*) for kinds and the related-topic filter.

## Rust `Client` (`liner_broker::client::Client`)

| API | Success | Failure |
|-----|---------|---------|
| `Client::new_redis` / `Client::new` | `Some(Client)` | `None` if the store cannot be opened — **silent** (no `print_error!` from this path); check `None` |
| `Client::new_sqlite` | `Some(Client)` | `None` if the store cannot be opened (silent), **`receivers_json` cannot be parsed** as a JSON array of seed entries (logs), **`seed_receivers`** fails (logs), or invalid UTF-8 would only arise from Rust `&str` callers |
| `Client::new_postgres` | `Some(Client)` | `None` if PostgreSQL cannot be opened (**`postgres`** feature required at compile time) |
| `run` | `true` if the event loop can start, or **already running** (`ErrorCode::Ok`) | `false` + `ErrorCode::Bind` / `Store` / … |
| `stop` | `true` (idempotent) | N/A for a live client |
| `set_advertise_addr` | `true` when not running | `false` + `AlreadyRunning` / `InvalidArg` |
| `set_status_cb` | always succeeds for a live client (registers or clears) | N/A (invalid handle only via C `lnr_set_status_cb`) |
| `send_to` / `send_all` | `true` if the send path reports success | `false` + last error (`NotRunning`, `SelfTopic`, `NoAddr`, `Store`, …) |
| `subscribe` / `unsubscribe` | `true` | `false` + last error |
| `refresh_address_topic` | `true` if addresses were found | `false` + `NoAddr` / `Store` |
| `clear_stored_messages` / `clear_addresses_of_topic` | `true` only when **not** running | `false` + `ClearWhileRunning` or `Store` |

Internal store errors are wrapped as `DbError` (string message from Redis or SQLite / `rusqlite`). They propagate as `false` / failed operations where the client checks `Result`; they do **not** automatically panic in the client layer.

## Rust `Liner` wrapper (`liner_broker::Liner`)

`Liner::new` / `Liner::new_sqlite` / `Liner::new_postgres` use the C constructors. If the returned handle is null, the wrapper **`panic!`**s (`error create client`). They also use `CString::new(...).unwrap()` — strings with an **embedded NUL** byte will panic. Prefer `Client` directly if you need non-panicking construction.

## Mutex poison

A few paths use `Mutex::lock().unwrap()` on the client’s internal mutex. If another thread panics while holding that lock, subsequent operations can **panic** with a poison error. This is unrelated to Redis/SQLite being “busy”; it indicates an earlier panic in your process.

## Summary for production

1. Treat **`NULL` / `None` / `FALSE`** as normal failure modes; read **`lnr_last_error_code`** and **stderr** for context. Optionally register **`lnr_set_status_cb`** for peer and background operational events.
2. Do not assume `lnr_run` returning `TRUE` guarantees the process will never abort later — listener/sender threads can still panic on unexpected store failure at their startup (documented separately).
3. For maximum control over construction errors, use **`Client::new_*` in Rust** instead of `Liner::new` / `Liner::new_sqlite`.
