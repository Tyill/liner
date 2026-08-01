# Store startup and failure semantics

This document clarifies what fails gracefully, what `run` returns, and how listener/sender relate to the store. It replaces older wording that incorrectly suggested a second `open_store` and process panic on listener startup.

For the full error-code table, see [errors-and-logging.md](errors-and-logging.md). For the recommended call order, see [using-the-api.md](using-the-api.md).

---

## Client path vs background tasks

### Client API

The **client** path is written to handle store errors without panicking. Operations that touch Redis, SQLite, or PostgreSQL can fail (timeouts, `SQLITE_BUSY`, I/O, connection loss). Those failures surface as:

- **`false` / `NULL`** on the API call,
- a per-client **`lnr_last_error_code`** such as **`LNR_ERR_STORE`**,
- and a detail line on **stderr** or the optional **log hook**.

### Listener and sender

After **`run`**, the **listener** and **sender** share the **same** store handle as the client: `Arc<Mutex<dyn Store>>`.

They do **not** open a second independent store connection at `run` time. Older documentation that described a separate `open_store` for listener/sender is **out of date**.

---

## What `run` does (relevant to failures)

Rough order inside a successful first `run`:

1. Resolve and **TCP bind** the constructor bind string (`localhost`).
2. Compute the **published** catalog address (advertise, or bound address).
3. **`regist_topic`** into the store so peers can find this client.
4. Construct the **listener** (mio poll, register the TCP listener, create a waker, resolve `get_topic_key` for the source topic).
5. Construct the **sender**, subscribe to the internal channel, emit `client_connected`.

Each step can fail differently:

| Failure | Return | Last error | Process panic? |
|---------|--------|------------|----------------|
| Invalid bind string / TCP bind failure | `false` | `LNR_ERR_BIND` | No |
| Catalog `regist_topic` failure | `false` | `LNR_ERR_STORE` | No |
| Listener construction failure after bind + regist | `false` | **`LNR_ERR_STARTUP`** | **No** |
| Internal-channel subscribe failure after listener/sender were created | `false` | `LNR_ERR_STORE` | No |
| Already running (second `run`) | `true` | `LNR_OK` | No |

### `LNR_ERR_STARTUP` in detail

Listener startup covers local I/O and early store lookups needed to enter the event loop, for example:

- creating the mio `Poll` instance,
- registering the TCP listener,
- creating the mio `Waker`,
- calling `get_topic_key` for the source topic.

If any of those fail, **`run` returns `false`** and sets **`LNR_ERR_STARTUP`**. Partial state is cleaned up: the topic registration is **unregistered**, `published_addr` is cleared, and listener/sender are not left running. The process does **not** abort.

This is intentionally distinct from **`LNR_ERR_BIND`**: bind means “could not listen on the TCP address”; startup means “TCP listen succeeded, but the listener event loop could not be brought up.”

---

## Operational takeaway

- Steady-state client operations: expect graceful **`false` + last_error`** and log lines, not panics, for store problems.
- After a successful **create**, a failed **`run`** with **`LNR_ERR_STARTUP`** is a **recoverable setup failure**. Fix resources / store consistency and call `run` again (or recreate the client if you prefer).
- Sender construction does not use the old fail-fast `expect` path on the store for this startup sequence.

---

## Related

- [using-the-api.md](using-the-api.md) — lifecycle, advertise, stop/restart
- [errors-and-logging.md](errors-and-logging.md) — full `LNR_ERR_*` table
- [backends.md](backends.md) — Redis / SQLite / PostgreSQL backend notes
