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

**Authoritative copy-paste commands**, Redis environment variables, Docker auto-start for Python tests, and `test/run_integration.py` options are maintained in the **Tests** part of the [project README](../README.md) (search for “Run Rust unit tests” / `LINER_TEST_REDIS` / `run_integration.py`).

## Contributing

Changes are welcome via the usual **fork → branch → pull request** flow. Match existing **Rust style** and **MIT** licensing; keep C headers (`include/liner.h`) synchronized with any new or changed FFI. For behavior changes, add or extend **tests** where practical.
