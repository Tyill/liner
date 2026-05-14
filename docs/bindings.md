# Python and C++ bindings (over the C API)

The **contract** for native code is **`include/liner.h`** plus the `cdylib` produced by **`cargo build --release`**. The shipped **Python** and **C++** layers are **examples**: they thin-wrap a subset of the C API. Treat them as a starting point, not a second source of truth for symbols.

## Build the shared library first

See [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md). After a release build, the library lives under **`target/release/`** (name depends on the OS, e.g. `libliner_broker.so` on Linux GNU targets).

## C++

- **Header:** compile against **`include/liner.h`**. The sample class includes it via **`cpp/liner_broker.h`** (`#include "../include/liner.h"`).
- **Link line (Unix sample):** the **`cpp/Makefile`** uses  
  `-L ../target/release -lliner_broker`  
  and builds each `*.cpp` next to the Makefile. Run **`make`** from **`cpp/`** after `cargo build --release`.
- **Windows:** use your toolchain’s rules for linking the **`liner_broker`** DLL / import library from `target/release` (flags differ from `g++` on Linux); see the compatibility doc.
- **Lifecycle:** `LinerBroker` calls **`lnr_new_client_redis`** in the constructor and **`lnr_delete_client`** in the destructor. Do not destroy the object while **`lnr_run`** is still logically active on another thread unless you have coordinated shutdown (the library owns background threads after a successful `run`).
- **Strings:** `std::string` passed as `.c_str()` must not contain embedded **NUL** bytes; topics and addresses are C strings.
- **SQLite:** the sample C++ class only uses **`lnr_new_client_redis`**. For SQLite, call **`lnr_new_client_sqlite`** the same way (four C strings) or extend the wrapper.

## Python

- **Load the library once:** `liner.loadLib(path)` must run before creating **`liner.Client`**. `path` is the full path to the shared library (`.so` / `.dylib` / `.dll`), not the Rust crate name.
- **Shipped `python/liner.py`:** constructs the client with **`lnr_new_client_redis`** only. A failed constructor raises **`error init client, check redisPath`** (also used for other creation failures, e.g. invalid parameters). To use **SQLite**, add a parallel constructor that calls **`lnr_new_client_sqlite`** via `ctypes` (same pattern as Redis).
- **Shutdown:** **`Client.close()`** calls **`lnr_delete_client`**. Prefer **`with Client(...) as c:`** so `close` runs on exit. If the process exits without `close`, you rely on process teardown (risky for clean thread shutdown).
- **Callbacks:** `run` installs a **`CFUNCTYPE`** callback stored on **`self.recvCBack_`** so it is not garbage-collected while Rust may call it. Keep the callback **short**; heavy work can delay I/O inside the library.
- **Threading:** the library runs listener/sender work on its own threads; receive callbacks may be invoked from those paths. Avoid calling back into the same **`Client`** from the callback in a way that could **deadlock** with your own locks. Prefer queuing work to another thread if needed.
- **Data:** `send_to` / `send_all` use **`bytearray`** in the sample; other buffer types may need copying into a form ctypes can pin for the duration of the call.

## Keeping bindings in sync

When upgrading **`liner_broker`**, rebuild the `cdylib`, refresh **`include/liner.h`** in your tree, and re-run your tests. See [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md) for ABI expectations.

## Related

- [using-the-api.md](using-the-api.md) — `run` order, `at_least_once_delivery`, threading notes.  
- [errors-and-logging.md](errors-and-logging.md) — `NULL` / `FALSE` and stderr.  
- [troubleshooting.md](troubleshooting.md) — quick symptom index.
