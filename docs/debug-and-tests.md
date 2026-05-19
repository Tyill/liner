# Debug and tests

## `liner_debug` Cargo feature

The crate defines an empty feature **`liner_debug`** in `Cargo.toml`. When enabled, the **`print_debug!`** macro emits **`println!`-style traces**; when disabled (default), **`print_debug!` is a no-op** (see `src/lib.rs`).

Build with debug output:

```bash
cargo build --release --features liner_debug
```

Today, **`print_debug!`** is used only in a few places (for example TCP connect failures in the sender and some listener paths). It does **not** replace structured logging; **`print_error!`** still goes to **stderr** regardless of this flag (see [errors-and-logging.md](errors-and-logging.md)).

## Unit tests

From the repository root:

```bash
cargo test
```

## Integration tests and Python harness

**Authoritative copy-paste commands** for Redis, SQLite, and PostgreSQL harnesses are in the **Tests** part of the [project README](../README.md).

### Redis integration tests (`test/redis/`)

Python integration scripts using a **shared Redis** URL (default `redis://localhost/`). From the repository root:

```bash
cargo build --release
python3 test/redis/run_integration.py
```

`--list`, `--only`, and `--continue-on-fail` are supported. Some tests auto-start Redis via Docker; see README for `LINER_TEST_REDIS_*` variables. Integration scripts live under **`test/redis/`** (not the repo-root `test/` folder). How-to: [using-redis.md](using-redis.md).

### SQLite integration tests (`test/sqlite/`)

A parallel set of scenarios using a **shared SQLite file** (no Redis). From the repository root:

```bash
python3 test/sqlite/run_integration.py
```

`--list`, `--only`, and `--continue-on-fail` match `test/redis/run_integration.py`. You need a built **`target/release/libliner_broker.so`** and **`python/liner.py`** with **`Client.new_sqlite`**.

Tests seed the “listener offline” catalog via **`receivers_json`** at client construction; do not run ad-hoc `sqlite3` on the same file while liner clients on that path are still alive (risk of process crash).

### PostgreSQL integration tests (`test/postgres/`)

Same scenarios as `test/sqlite/`, using a **shared PostgreSQL database** (no `receivers_json`; catalog lives in the DB). From the repository root:

```bash
export LINER_TEST_POSTGRES_URL='postgresql://user:pass@127.0.0.1/liner_test'
cargo build --release --features postgres
pip install psycopg2-binary   # for catalog / queue inspection in tests
python3 test/postgres/run_integration.py
```

`--list`, `--only`, and `--continue-on-fail` match `test/sqlite/run_integration.py`. Requires **`Client.new_postgres`** in `python/liner.py` and a library built with **`--features postgres`**. How-to: [using-postgres.md](using-postgres.md).

## Contributing

Changes are welcome via the usual **fork → branch → pull request** flow. Match existing **Rust style** and **MIT** licensing; keep C headers (`include/liner.h`) synchronized with any new or changed FFI. For behavior changes, add or extend **tests** where practical.
