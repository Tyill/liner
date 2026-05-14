# Troubleshooting index

Use this page as a **map**: each row points to an existing doc. It does not repeat full explanations.

| Symptom or question | Where to read |
|---------------------|----------------|
| `lnr_new_client_*` returns **NULL** / Rust `Client::new_*` is **None** | [errors-and-logging.md](errors-and-logging.md), [backends.md](backends.md) (connectivity, paths, permissions) |
| **`lnr_run`** / **`run`** returns **false** | [using-the-api.md](using-the-api.md), [errors-and-logging.md](errors-and-logging.md) (bind address, `regist_topic`, stderr) |
| **Panic** right after **`run`** (store / `get_topic_key`) | [store-startup-failure-semantics.md](store-startup-failure-semantics.md) |
| **Send** fails or “not found addr for topic” | [behavior-topics-delivery-and-errors.md](behavior-topics-delivery-and-errors.md), [routing-and-store-layout.md](routing-and-store-layout.md), [using-the-api.md](using-the-api.md) (`refresh_address_topic`) |
| Messages **missing** after reconnect, or **duplicates** | [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) (`at_least_once_delivery`, `number_mess`) |
| **Redis** keys / what **`clear_*`** touches | [operations-redis-sqlite.md](operations-redis-sqlite.md), [routing-and-store-layout.md](routing-and-store-layout.md) |
| **SQLite** WAL, backup, lock / `BUSY` | [backends.md](backends.md), [operations-redis-sqlite.md](operations-redis-sqlite.md) |
| **Large messages**, memory, compression thresholds | [capacity-and-limits.md](capacity-and-limits.md) |
| **TLS**, trust boundaries, who can read/write the store | [security-defaults.md](security-defaults.md) |
| **Link / DLL** errors, header paths, Windows vs Linux | [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md), [bindings.md](bindings.md) |
| Python **`loadLib`**, ctypes, C++ **Makefile** | [bindings.md](bindings.md) |
| **`liner_debug`**, `cargo test`, integration tests | [debug-and-tests.md](debug-and-tests.md), [README](../README.md) |
| stderr **`Error file:line:`** lines | [errors-and-logging.md](errors-and-logging.md) |

If nothing matches, search the repo for the exact log line or symbol name, then open the linked doc from the table above.
