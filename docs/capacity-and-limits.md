# Capacity, limits, and tuning constants

All numeric constants live in **`src/settings.rs`** unless stated otherwise. This is a **sizing guide** for integrators, not a guarantee of every edge case in the code.

---

## Hard cap: single framed message on the wire

**`BYTESTREAM_MAX_MESSAGE_SIZE`** (default **1 GiB**, `1024 * 1024 * 1024`)

- Each TCP frame starts with a **4-byte big-endian `u32` length**, then payload (`bytestream::read_stream`).
- If the declared length is **0** or **greater than `BYTESTREAM_MAX_MESSAGE_SIZE`**, the reader logs an error and treats the stream as **shut down** (`is_shutdown` path): no message is returned.
- Payload is allocated in the connection’s **mempool** (`mempool.alloc(msg_len)`). A malicious or buggy peer advertising a length close to the cap can force a **very large RAM reservation** on the receiver.

**Practical limit:** treat **~1 GiB per framed read** as the library’s configured ceiling; the **`u32` header** allows up to 4 GiB in principle, but this constant is the enforced bound.

Smaller internal I/O buffers (**`BYTESTREAM_READ_BUFFER_SIZE`** / **`BYTESTREAM_WRITE_BUFFER_SIZE`**, default **8 KiB**) only control **chunking** of reads/writes, not the maximum message size.

---

## Optional payload compression (zstd)

**`MIN_SIZE_DATA_FOR_COMPRESS_BYTE`** (default **1 MiB**, `1024 * 1024`)

- In **`Message::new`**, if **`data.len() > MIN_SIZE_DATA_FOR_COMPRESS_BYTE`** (strictly **greater than** 1 MiB), the implementation tries **`zstd::stream::encode_all`**.
- On success, the wire message stores **compressed bytes** and sets a **`COMPRESS`** flag so **`get_data`** decompresses on the listener.
- Payloads **≤ 1 MiB** are sent **uncompressed** regardless of compressibility.

**`DATA_COMPRESS_LEVEL`** (default **`0`**) — passed to zstd; level **`0`** means “use zstd’s default” (documented in-code as currently similar to level **3**).

Compression is **best-effort**: if encoding fails, the code logs and may still mark the message compressed with an **empty** compressed payload—callers should treat compression as an optimization, not a substitute for application-level integrity checks.

---

## Mempool (per-connection RAM)

The mempool is a bump-style arena backed by **`Vec<Vec<u8>>`** in chunks of **`MEMPOOL_CHUNK_SIZE_BYTE`** (default **256 KiB**). Allocations grow the backing store by whole chunks as needed.

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

1. **Per message:** application payload ≤ **1 MiB** → no zstd attempt; **> 1 MiB** → zstd may run (CPU cost, smaller wire if data is compressible).
2. **Per framed TCP message:** declared length must be **≤ `BYTESTREAM_MAX_MESSAGE_SIZE`** (1 GiB default) or the connection is aborted for that read path.
3. **RAM:** plan for **peak concurrent messages × mempool footprint** per active connection (listener + sender each use mempools for their worklists). Add headroom for **fragmentation** (allocator may keep extra chunks when the 20% rule blocks merging).
4. **Disk / Redis memory:** **at-least-once** offline queues store **encoded** message blobs; size ≈ wire size (compressed if compression was used).
5. **DoS / untrusted peers:** the length header is untrusted; cap expectations at the network boundary or use a smaller maximum at the application layer if 1 GiB is unacceptable.

---

## Related

- [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) — persistence and `number_mess`.  
- [routing-and-store-layout.md](routing-and-store-layout.md) — where queued payloads live in Redis/SQLite.  
- [operations-redis-sqlite.md](operations-redis-sqlite.md) — backing-store operations.

Constants for timeouts (`CHECK_AVAILABLE_STREAM_TIMEOUT_MS`, etc.) are documented in [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md).
