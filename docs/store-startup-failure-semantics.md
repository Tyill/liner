# Store startup and failure semantics

## Client vs internal tasks

The **client** path is written to handle store errors without panicking: operations that touch the backing store can fail (for example Redis timeouts, SQLite `SQLITE_BUSY` after the configured busy wait, or I/O errors). Those failures surface as errors or a controlled shutdown from the client API rather than aborting the whole process.

The **listener** and **sender** tasks open their own store handle (same `unique_name` and `StoreBackend` as the client) when `run` starts them. On that startup path, opening the store or resolving the topic key uses fail-fast checks: if the store cannot be opened or an early query fails, the process may **panic** with an explicit message. That is intentional.

## Why listener startup is stricter

After the client has initialized successfully, it is **uncommon** for the listener’s store open to fail while the client’s store still works. Both sides use the same logical resource (Redis URL or SQLite path). A mismatch usually means something abnormal: a race (permissions, disk space), another process holding an exclusive SQLite lock, or infrastructure flapping between the two opens.

In those cases, a half-started broker (network up, store down only on the listener side) is harder to reason about than failing immediately. The library therefore assumes: **if you call `run`, the store must be reachable for listener/sender startup**, not only for the client object you constructed first.

## Operational takeaway

- Expect **graceful** handling of store errors on **ongoing client** operations.
- Expect **fail-fast** (possible panic) if the store is unavailable when **listener or sender** spins up inside `run`, even though the client was created successfully.

If you need the process to stay alive when the second open fails, that would require a deliberate API change (propagating `Result` from internal startup instead of `expect`), which is not the current design.
