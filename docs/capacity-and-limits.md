# Capacity, limits, and tuning constants

All numeric defaults live in **`src/settings.rs`** unless stated otherwise. This is a **sizing guide** for integrators, not a guarantee of every edge case in the code.

Two limits are also **runtime-tunable** through the public API (crate **1.4.0**): maximum framed message size, and the zstd compression threshold. Prefer setting them **before** `run`.

Related:

- [using-the-api.md](using-the-api.md) — when to call the setters
- [errors-and-logging.md](errors-and-logging.md) — invalid `0` values return `FALSE`
- [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) — offline queues and timeouts

---

## Hard cap: single framed message on the wire

### Default and runtime API

| Item | Value / API |
|------|-------------|
| Compile-time default constant | `BYTESTREAM_MAX_MESSAGE_SIZE` = **1 GiB** (`1024 * 1024 * 1024`) |
| Runtime value | `settings::max_message_size()` |
| C | `lnr_get_max_message_size` / `lnr_set_max_message_size` |
| Rust | `settings::max_message_size` / `set_max_message_size`, also on `Liner` |
| Python | `get_max_message_size` / `set_max_message_size` |

`lnr_set_max_message_size(0)` (and the Rust/Python equivalents) return **`FALSE` / `false`**. Non-zero values are accepted.

Prefer setting the limit **before** `run`. Changing it later is allowed, but only **new** framed reads/writes observe the new value; in-flight frames are unaffected.

**Send path:** `send_to` / `send_all` reject early with **`LNR_ERR_INVALID_ARG`** if the payload is **empty**, or if the **uncompressed** framed message body (message header + payload length + payload) would exceed the runtime max. Compression can only shrink the body, so the size check is conservative before routing or enqueue.

### How enforcement works

- Each TCP frame starts with a **4-byte big-endian `u32` length**, then the payload (`bytestream::read_stream`).
- If the declared length is **0** or **greater than the runtime max**, the reader logs an error and treats the stream as **shut down** (`is_shutdown` path): no message is returned to the application.
- Payload bytes are allocated in the connection’s **mempool** (`mempool.alloc(msg_len)`). A malicious or buggy peer advertising a length close to the cap can force a **very large RAM reservation** on the receiver.

**Practical ceiling:** treat **~1 GiB per framed read** as the default limit. The `u32` header could express up to 4 GiB in principle, but the runtime max is the enforced bound.

Smaller internal I/O buffers (`BYTESTREAM_READ_BUFFER_SIZE` / `BYTESTREAM_WRITE_BUFFER_SIZE`, default **8 KiB**) only control **chunking** of reads/writes. They do **not** change the maximum message size.

**DoS / untrusted peers:** the length header is untrusted. Lower `lnr_set_max_message_size` before `run` if 1 GiB is unacceptable for your deployment.

---

## Optional payload compression (zstd)

### Default and runtime API

| Item | Value / API |
|------|-------------|
| Compile-time default constant | `MIN_SIZE_DATA_FOR_COMPRESS_BYTE` = **1 MiB** (`1024 * 1024`) |
| Runtime value | `settings::compress_threshold()` |
| C | `lnr_get_compress_threshold` / `lnr_set_compress_threshold` |
| Rust / Python | matching getters/setters on settings / `Liner` / module helpers |

`0` is rejected (`FALSE` / `false`). Prefer set **before** `run`; later changes apply to new `Message::new` calls only.

### Behavior

- In **`Message::new`**, if **`data.len() > compress_threshold()`** (strictly greater), the implementation tries **`zstd::stream::encode_all`**.
- On success, the wire message stores **compressed bytes** and sets a **`COMPRESS`** flag so **`get_data`** decompresses on the listener.
- Payloads **≤ threshold** are sent **uncompressed** regardless of compressibility.

**`DATA_COMPRESS_LEVEL`** (default **`0`**) is passed to zstd. Level **`0`** means “use zstd’s default” (documented in-code as currently similar to level **3**).

Compression is **best-effort**: if encoding fails, the code logs and may still mark the message compressed with an **empty** compressed payload. Treat compression as an optimization, not a substitute for application-level integrity checks.

---

## Mempool (per-connection RAM)

The mempool is a bump-style arena backed by **`Vec<Vec<u8>>`** in chunks of **`MEMPOOL_CHUNK_SIZE_BYTE`** (default **256 KiB**). Allocations grow the backing store by whole chunks as needed.

**Listener:** there is **one mempool per accept slot** (`mempools[ix]`), tied to the peer’s sticky `SocketAddr → ix` (see [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md#listener-accept-slots-index-affinity)). Do not reassign that slot’s mempool to another address.

| Constant | Default | Role |
|----------|---------|------|
| **`MEMPOOL_CHUNK_SIZE_BYTE`** | 256 KiB | Growth / alignment unit for backing storage. |
| **`MEMPOOL_MIN_PERCENT_FOR_COMPRESS`** | `0.2` (20%) | When satisfying an allocation from **fragmented free space**, the allocator may **decline** coalescing if **remaining free** after the alloc would fall **below 20%** of the current pool size—then it **adds new chunks** instead. This trades memory for less aggressive merging under pressure (name is historical, not “turn on zstd”). |
| **`MEMPOOL_MIN_PERCENT_FOR_RESIZE`** | `0.25` (25%) | Together with size, enables **tail shrink** of the backing buffer when a large aligned free block sits at the end. |
| **`MEMPOOL_OVER_SIZE_MB`** | **16** | Pool backing must exceed **16 MiB** before tail-shrink logic runs (lower than the old **64** default so RSS drops sooner after bursts). |
| **`MEMPOOL_FREE_COUNT_FOR_RESIZE`** | **4096** | After this many **`free`** calls, the pool runs a **`check_free_mem(0)`** pass to **defragment / coalesce** free blocks (more frequent than the old default so RSS can drop after bursts). |

**What happens with huge messages**

- Building or receiving a **large message** grows the mempool by enough **256 KiB** chunks to hold it (plus headers). There is **no explicit cap** in the mempool itself beyond **OS memory** and the **bytestream max** on read.
- Very large single messages ⇒ **large RSS** for that connection’s mempool and, for **at-least-once**, potentially large **Redis/SQLite** queue entries as well.

---

## TCP buffer sizes (Rust side)

| Constant | Default | Where |
|----------|---------|--------|
| **`READ_BUFFER_CAPASITY`** | 64 KiB | `BufReader` around TCP read in listener. |
| **`WRITE_BUFFER_CAPASITY`** | 64 KiB | `BufWriter` around TCP write in sender. |

These affect syscall batching, not the logical max message size.

---

## Short sizing checklist

1. **Per application payload:** size ≤ compression threshold (1 MiB default) → no zstd attempt; **above threshold** → zstd may run (CPU cost; smaller wire if data is compressible).
2. **Per framed TCP message:** declared length must be **≤ runtime `max_message_size`** (1 GiB default) or the connection is aborted for that read path.
3. **RAM:** plan for **peak concurrent messages × mempool footprint** per active connection (listener and sender each use mempools for their worklists). Add headroom for **fragmentation** (the allocator may keep extra chunks when the 20% rule blocks merging).
4. **Disk / Redis memory:** **at-least-once** offline queues store **encoded** message blobs; size ≈ wire size (compressed if compression was used). Use **`lnr_pending_count`** / **`lnr_pending_by_peer`** for offline depth; **`lnr_send_queue_depth`** / **`lnr_send_queue_depth_by_peer`** for live in-memory depth.
5. **DoS / untrusted peers:** lower **`lnr_set_max_message_size`** before `run` if the default 1 GiB cap is too high. Cap in-memory send queues with **`lnr_set_max_send_queue`** when producers can outrun drains.

### Send queue and timeouts

- **`max_send_queue`** (default **`0` = unlimited**): max in-memory messages **per peer slot**. Full queue → `LNR_ERR_BUSY` / `LNR_SENDER_BUSY`. Inspect live depth with **`lnr_send_queue_depth`**.
- Stream-check / would-block timeouts default to 10 s; tunable via **`lnr_set_stream_check_timeout_ms`** / **`lnr_set_would_block_timeout_ms`** (`0` rejected). Prefer before `run`.

---

## Related

- [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) — persistence and `number_mess`.
- [routing-and-store-layout.md](routing-and-store-layout.md) — where queued payloads live in Redis/SQLite.
- [operations-redis-sqlite.md](operations-redis-sqlite.md) — backing-store operations.
