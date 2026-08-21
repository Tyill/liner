# Security defaults (expectations)

liner is designed as **messaging infrastructure inside a trusted zone**. It does **not** ship transport encryption, mutual authentication, or authorization between clients. This page states those defaults explicitly so integrators can place the right controls around the library.

Also see [capacity-and-limits.md](capacity-and-limits.md) for framed-message size caps (relevant when peers are untrusted on the network), and [operations-redis-sqlite.md](operations-redis-sqlite.md) for store isolation recommendations.

---

## Peer-to-peer TCP

All application bytes move over **plain TCP** between a sender and a peer’s listener.

Anyone who can **observe**, **inject**, or **redirect** traffic on that path can read or tamper with messages unless you add your own protection, for example:

- TLS termination via a sidecar / stunnel / mesh proxy, or
- running only on a network you fully trust (same host, locked-down VLAN, private overlay).

There is **no** TLS, mTLS, or application-level crypto in the stock library.

**Bind vs advertise:** the constructor `localhost` string is the **listen** address. Use **`set_advertise_addr`** when peers must dial a different reachable address (NAT, `0.0.0.0` bind, VPN IP). Publishing a wrong advertise address is an availability issue; publishing an overly broad bind (for example `0.0.0.0` on an exposed interface) expands who can attempt a TCP connect. See [using-the-api.md](using-the-api.md).

---

## Redis store

Redis is a **separate trust surface** from TCP peers.

- The server accepts whatever your URL and **`ACL` / `AUTH`** allow.
- Any principal that can reach the server and issue commands in the same logical database can **read, write, or delete** `lnr_*` keys.
- That is enough to **spoof routing**, **drop offline queues**, or **inject catalog entries**.

**Recommendations**

- Dedicated Redis instance or dedicated logical DB index for liner.
- Strong credentials and network restrictions (firewall, private network, TLS to Redis if your ops stack supports it).
- Prefer `SCAN lnr_*` over broad `KEYS` / `FLUSHDB` habits on shared instances.

Details: [operations-redis-sqlite.md](operations-redis-sqlite.md), [using-redis.md](using-redis.md).

---

## SQLite store

SQLite is a **normal filesystem file**.

- Confidentiality and integrity depend on **OS permissions** and backup policy.
- Any user or process with **read/write** access to the path can alter or copy broker state (catalog, queues, ack cursors).

**Recommendations**

- Restrict file mode and ownership to the liner service account.
- Treat `-wal` / `-shm` siblings as part of the same secret state when backing up.
- Do not place the DB on a world-readable share.

Details: [using-sqlite.md](using-sqlite.md), [operations-redis-sqlite.md](operations-redis-sqlite.md).

---

## PostgreSQL store (optional `postgres` feature)

The stock connector uses **libpq with `NoTls`**.

- Credentials in the URL and **database-level roles** define who can read or change liner tables.
- On an untrusted network, terminate TLS at the server or a proxy; the library does not enable TLS by default.

**Recommendations**

- Dedicated database per deployment.
- Least-privilege roles (no superuser for the app).
- Same operational caution as Redis for shared multi-tenant clusters.

Details: [using-postgres.md](using-postgres.md), [security note in backends](backends.md).

---

## Framed message size

The on-wire length header is **untrusted**. The runtime max framed size defaults to **1 GiB**. Lower it with **`lnr_set_max_message_size`** before `run` if peers or the network path are not fully trusted. See [capacity-and-limits.md](capacity-and-limits.md).

---

## Summary

Treat liner as **trusted-zone infrastructure**, not as a hardened Internet-facing service out of the box. Protect the **TCP path**, the **store**, and the **message-size ceiling** according to your threat model.
