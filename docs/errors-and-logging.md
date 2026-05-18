# Errors and logging

## Where messages go

Failures in the Rust core are often logged with the `print_error!` macro: a line is written to **standard error** in the form:

`Error <file>:<line>: <message>`

There is no separate error code enum exposed to C beyond **success vs failure** on each call. For details, rely on stderr (or wrap the library and capture stderr in tests).

## C API (`include/liner.h`)

| Situation | Typical outcome |
|-----------|------------------|
| Invalid client handle (`NULL`) on any function that takes `lnr_hClient` | `FALSE` / `0`; may log `client was not created` |
| `lnr_new_client_redis` / `lnr_new_client` | `NULL` on failure: null/invalid UTF-8 pointers, empty `unique_name`, `topic`, `localhost`, or store string, or **store could not be opened** (Redis unreachable, etc.) |
| `lnr_new_client_sqlite` | `NULL` for the same pointer/empty-string rules, **SQLite open failure**, **invalid non-empty `receivers_json`**, or **`seed_receivers`** / DB errors. **`NULL` or empty `receivers_json`**, or JSON **`[]`**, is **not** an error (no seeding). |
| `lnr_new_client_postgres` | `NULL` on failure (requires build with **`postgres`** feature): null/invalid pointers, empty strings, or **PostgreSQL connection / schema errors** |
| `lnr_run` | `TRUE` if the client was already marked running; `FALSE` if registration or bind failed (see below); **may panic** if listener/sender store startup fails internally (see [store-startup-failure-semantics.md](store-startup-failure-semantics.md)) |
| `lnr_send_to`, `lnr_send_all`, … | `FALSE` on logical or I/O errors; see individual operations in [using-the-api.md](using-the-api.md) |

Creation helpers validate pointers and C strings; invalid input returns `NULL` without necessarily printing every case.

## Rust `Client` (`liner_broker::client::Client`)

| API | Success | Failure |
|-----|---------|---------|
| `Client::new_redis` / `Client::new` | `Some(Client)` | `None` if the store cannot be opened — **silent** (no `print_error!` from this path); check `None` |
| `Client::new_sqlite` | `Some(Client)` | `None` if the store cannot be opened (silent), **`receivers_json` cannot be parsed** as a JSON array of seed entries (logs), **`seed_receivers`** fails (logs), or invalid UTF-8 would only arise from Rust `&str` callers |
| `Client::new_postgres` | `Some(Client)` | `None` if PostgreSQL cannot be opened (**`postgres`** feature required at compile time) |
| `run` | `true` if the event loop can start | `false` if `regist_topic` fails, `localhost` does not resolve, or TCP bind fails; logs reason. Returns `true` if the client **was already running** (idempotent success) |
| `send_to` / `send_all` | `true` if the send path reports success | `false` if not running, self-topic, unknown topic addresses, or sender failure |
| `subscribe` / `unsubscribe` | `true` | `false` on store errors or invalid topic |
| `refresh_address_topic` | `true` if addresses were found | `false` if none or store error |
| `clear_stored_messages` / `clear_addresses_of_topic` | `true` only when **not** running | `false` if already running or store error |

Internal store errors are wrapped as `DbError` (string message from Redis or SQLite / `rusqlite`). They propagate as `false` / failed operations where the client checks `Result`; they do **not** automatically panic in the client layer.

## Rust `Liner` wrapper (`liner_broker::Liner`)

`Liner::new` / `Liner::new_sqlite` / `Liner::new_postgres` use the C constructors. If the returned handle is null, the wrapper **`panic!`**s (`error create client`). They also use `CString::new(...).unwrap()` — strings with an **embedded NUL** byte will panic. Prefer `Client` directly if you need non-panicking construction.

## Mutex poison

A few paths use `Mutex::lock().unwrap()` on the client’s internal mutex. If another thread panics while holding that lock, subsequent operations can **panic** with a poison error. This is unrelated to Redis/SQLite being “busy”; it indicates an earlier panic in your process.

## Summary for production

1. Treat **`NULL` / `None` / `FALSE`** as normal failure modes; read **stderr** for context.
2. Do not assume `lnr_run` returning `TRUE` guarantees the process will never abort later — listener/sender threads can still panic on unexpected store failure at their startup (documented separately).
3. For maximum control over construction errors, use **`Client::new_*` in Rust** instead of `Liner::new` / `Liner::new_sqlite`.
