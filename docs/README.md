# liner documentation (library usage)

Supplements the [crate docs on docs.rs](https://docs.rs/liner_broker/) and the project [README](../README.md).

**Debug & tests:** [debug-and-tests.md](debug-and-tests.md) (`liner_debug`, pointers to the README for integration commands).

**Quick index:** [troubleshooting.md](troubleshooting.md) (symptom → which doc to open).

| Document | Contents |
|----------|----------|
| [troubleshooting.md](troubleshooting.md) | Symptom → link to the right doc |
| [bindings.md](bindings.md) | Python `ctypes` and C++ sample: build, link, lifecycle; SQLite: see [using-sqlite.md](using-sqlite.md) |
| [behavior-topics-delivery-and-errors.md](behavior-topics-delivery-and-errors.md) | **Product behavior:** topics, routing, delivery, errors (no wire format) |
| [store-startup-failure-semantics.md](store-startup-failure-semantics.md) | Client vs listener/sender store failures and fail-fast startup |
| [errors-and-logging.md](errors-and-logging.md) | How errors surface (C `BOOL`, Rust `bool` / `Option`, stderr logging) |
| [backends.md](backends.md) | Redis vs SQLite vs PostgreSQL: URLs, files, locking, `unique_name` |
| [using-sqlite.md](using-sqlite.md) | **SQLite how-to:** `new_sqlite`, `receivers_json`, C API, reference test walkthrough |
| [using-postgres.md](using-postgres.md) | **PostgreSQL how-to:** `--features postgres`, shared URL, C/Rust/Python, tests |
| [using-the-api.md](using-the-api.md) | Lifecycle, threading, common pitfalls |
| [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) | Disconnects, store-backed queues, reconnect interval, `number_mess`, deduplication |
| [routing-and-store-layout.md](routing-and-store-layout.md) | Topic → address routing, Redis keys, SQLite tables, operator troubleshooting |
| [operations-redis-sqlite.md](operations-redis-sqlite.md) | `lnr_*` prefix, `clear_*` scope, Redis ≥ 6.2, SQLite WAL and backup |
| [capacity-and-limits.md](capacity-and-limits.md) | Mempool / bytestream limits, zstd threshold, sizing checklist |
| [security-defaults.md](security-defaults.md) | No TLS, trust model for TCP, Redis, and SQLite |
| [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md) | C symbol/header stability expectations, `cargo` artifacts, Linux vs Windows linking |
| [debug-and-tests.md](debug-and-tests.md) | `liner_debug` feature, `cargo test`, link to README for integration / Python tests |

All of these are written for integrators (Rust, C, Python bindings over the C API).
