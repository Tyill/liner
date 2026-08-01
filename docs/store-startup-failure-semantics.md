# Store startup and failure semantics

## Client vs internal tasks

The **client** path handles store errors without panicking: operations that touch the backing store can fail (Redis timeouts, SQLite `SQLITE_BUSY`, I/O). Those surface as **`false` / `NULL`** plus **`lnr_last_error_code`** (`LNR_ERR_STORE`, …) and stderr / the optional log hook.

**Listener** and **sender** share the client’s store handle (`Arc<Mutex<dyn Store>>`) — they do **not** open a second independent store connection at `run` time.

## Listener startup inside `run`

After TCP **bind** and catalog **regist_topic**, `run` constructs the **listener** (mio poll / register / waker, and `get_topic_key` for the source topic). If that startup fails, **`run` returns `false`** and sets **`LNR_ERR_STARTUP`**. The process does **not** panic on this path. Partial state is cleaned up (unregister topic when needed; listener/sender not left running).

TCP bind / resolve failures still use **`LNR_ERR_BIND`**. Store errors during regist use **`LNR_ERR_STORE`**.

## Operational takeaway

- Ongoing client ops: graceful failure codes + stderr/log hook.
- Listener startup failure after a successful client create: **`run` → false + `LNR_ERR_STARTUP`**, not process abort.

Sender construction does not use the old fail-fast `expect` path on the store.
