# Troubleshooting index

Use this page as a **map**: each row points to an existing doc. It does not repeat full explanations.

| Symptom or question | Where to read |
|---------------------|----------------|
| `lnr_new_client_*` returns **NULL** / Rust `Client::new_*` is **None** | [errors-and-logging.md](errors-and-logging.md), [backends.md](backends.md) (connectivity, paths, permissions) |
| **`lnr_run`** / **`run`** returns **false** | Check **`lnr_last_error_code`**: bind → [using-the-api.md](using-the-api.md); store regist → [errors-and-logging.md](errors-and-logging.md); listener after bind (`LNR_ERR_STARTUP`) → [store-startup-failure-semantics.md](store-startup-failure-semantics.md) |
| Expected a **panic** on listener startup after `run` | That path returns **`false` + `LNR_ERR_STARTUP`** now (no panic). See [store-startup-failure-semantics.md](store-startup-failure-semantics.md) |
| How to read the last sync failure | [errors-and-logging.md](errors-and-logging.md) (`lnr_last_error_code` / `Client::last_error`; detail text on stderr or log hook) |
| Redirect **`Error file:line:`** away from stderr | [errors-and-logging.md](errors-and-logging.md) (`lnr_set_log_cb`) |
| Peers cannot connect after bind on `0.0.0.0` / port `0` | [using-the-api.md](using-the-api.md) (*TCP bind vs advertise*), getters `bound_listen_addr` / `published_addr` |
| **`set_advertise_addr`** fails while running | Expected: **`LNR_ERR_ALREADY_RUNNING`**. Stop first or set before `run`. |
| **Send** fails or “not found addr for topic” | [behavior-topics-delivery-and-errors.md](behavior-topics-delivery-and-errors.md), [routing-and-store-layout.md](routing-and-store-layout.md), [using-the-api.md](using-the-api.md) (`refresh_address_topic`, `list_addresses`) |
| Inspect catalog / offline queue depth from the API | [using-the-api.md](using-the-api.md) (*Introspection*: `list_addresses`, `pending_count`) |
| Messages **missing** after reconnect, or **duplicates** | [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) (`at_least_once_delivery`, `number_mess`) |
| **`clear_*`** fails while client is running | Call **`stop`** first, or clear before `run`. Code **`LNR_ERR_CLEAR_WHILE_RUNNING`**. |
| **Redis** keys / what **`clear_*`** touches | [operations-redis-sqlite.md](operations-redis-sqlite.md), [routing-and-store-layout.md](routing-and-store-layout.md) |
| **SQLite** WAL, backup, lock / `BUSY` | [backends.md](backends.md), [operations-redis-sqlite.md](operations-redis-sqlite.md) |
| **Large messages**, memory, compression thresholds | [capacity-and-limits.md](capacity-and-limits.md) (`lnr_set_max_message_size`, `lnr_set_compress_threshold`) |
| **TLS**, trust boundaries, who can read/write the store | [security-defaults.md](security-defaults.md) |
| **Link / DLL** errors, header paths, Windows vs Linux | [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md), [bindings.md](bindings.md) |
| Python **`loadLib`**, ctypes, C++ **Makefile** | [bindings.md](bindings.md) |
| **`liner_debug`**, `cargo test`, integration tests | [debug-and-tests.md](debug-and-tests.md), [README](../README.md) |
| stderr **`Error file:line:`** lines | [errors-and-logging.md](errors-and-logging.md) |

If nothing matches, search the repo for the exact log line or symbol name, then open the linked doc from the table above.
