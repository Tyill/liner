# C API compatibility and building

## Stability of symbols and layout

The C surface is defined by **`include/liner.h`** and **`#[no_mangle] pub extern "C"`** entry points in the Rust crate (see `src/lib.rs`). **`lnr_hClient`** is an **opaque pointer** to the Rust `Client`; its internal layout is **not** part of the C contract, so changes inside `Client` do not by themselves break the pointer ABI.

The project **does not currently publish a separate ABI stability policy** (for example “patch-only symbol compatibility”). **Semantic versioning of the Rust crate** (`Cargo.toml` / crates.io) tracks the **library as a whole**, not a formally tested C ABI matrix. In practice:

- **Additive** changes (new functions) are backward compatible for callers that only use older symbols.
- **Renames, signature changes, or removals** of C functions, or **behavioral changes** documented in release notes, require **rebuilding and retesting** all native bindings.
- Changes to **`liner.h`** (types, callbacks, constants) should be treated as **potentially breaking** for C/C++ consumers until you verify otherwise.

**Recommendation:** pin the **exact crate / git tag** you ship, vendor **`liner.h`** next to your binding, and run your integration tests when upgrading.

## Canonical header

Ship and compile against **`include/liner.h`** (the C++ examples include it via `cpp/liner_broker.h`). Keep the header **in lockstep** with the `liner_broker` artifact you link.

## Building the shared library (Linux and Windows)

The root [README](../README.md) describes **cross-platform** use (Linux, Windows) and the standard Rust build:

```bash
cargo build --release
```

Artifacts land under **`target/release/`**. The crate is built as a **`cdylib`** (`Cargo.toml`), so you get a **native shared library** whose base name follows Cargo’s rules (for example **`libliner_broker.so`** on typical Linux GNU targets, **`liner_broker.dll`** on Windows MSVC—confirm the exact filename in your `target/release` after the first build). Link that library from C/C++ the same way you would any other Rust `cdylib` produced on your target triple.

The sample **`cpp/Makefile`** assumes a Unix-like linker line (`-L ../target/release -lliner_broker`). On **Windows**, point your toolchain at the **`.lib` import library / `.dll` pair** (or your environment’s equivalent) produced for your MSVC or GNU target; the flags differ from `g++` on Linux—follow MSVC or MinGW documentation for linking Rust DLLs.

## Runtime dependencies

- **Redis backend:** a reachable **Redis** server compatible with the versions described in [operations-redis-sqlite.md](operations-redis-sqlite.md).
- **SQLite backend:** no server; the bundled SQLite inside the Rust binary is used.
- **PostgreSQL backend:** optional; build with **`cargo build --features postgres`**. Requires a reachable **PostgreSQL** server and **`lnr_new_client_postgres`** in the linked artifact. See [using-postgres.md](using-postgres.md).
- **Platform:** the Rust standard library and **libc** (on Unix) apply as for any other `cdylib`; Windows builds use the usual MSVC or GNU runtime for your Rust toolchain.

## Related

- [errors-and-logging.md](errors-and-logging.md) — C return values and `NULL` handles.  
- [using-the-api.md](using-the-api.md) — lifecycle and threading cautions for FFI.  
- [bindings.md](bindings.md) — Python and C++ example wrappers over this API.
