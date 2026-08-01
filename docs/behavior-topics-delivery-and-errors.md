# Behavior: topics, delivery, and errors

This page describes **what the library does** from an application perspective: naming, routing, delivery semantics, and failure modes. It does **not** specify TCP byte layout.

Related documents:

- [routing-and-store-layout.md](routing-and-store-layout.md) — Redis keys and SQL tables (operators)
- [errors-and-logging.md](errors-and-logging.md) — stderr, last-error codes, status callback
- [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) — offline queues, reconnect timing, `number_mess`
- [using-the-api.md](using-the-api.md) — lifecycle, advertise, stop, introspection
- [store-startup-failure-semantics.md](store-startup-failure-semantics.md) — what `run` returns on listener startup failure

---

## Topics

### Source topic (the client’s own topic)

At construction you pass a **topic string** and a **TCP bind address** (`localhost`). When **`run`** succeeds, the client **registers** in the shared store: other processes learn *“this topic is served at this TCP address by this `unique_name`.”*

The address written to the catalog is the **published** address:

- by default, the bound listen address after TCP bind; or
- the optional **advertise** address if you called `set_advertise_addr` before `run` (see [using-the-api.md](using-the-api.md)).

That topic is the client’s **source topic**. The API **forbids** sending to it or subscribing to it as if it were a remote peer — those calls fail with **`LNR_ERR_SELF_TOPIC`** (and a log line).

### Destination topics and addresses

**`send_to(topic, …)`** and **`send_all(topic, …)`** use `topic` as a **logical name**. The client looks up **TCP addresses** currently registered for that topic in the store.

- If **no addresses** are registered, the send fails with **`LNR_ERR_NO_ADDR`** (“not found addr for topic …”).
- If **several addresses** exist (multiple clients on the same topic name), **`send_to`** picks **one address per call** using **round-robin** over the cached list. **`send_all`** sends the same payload **once per registered address** (broadcast to every replica listed for that topic).

The client **caches** addresses after the first successful lookup. While peers are running, the **internal channel** (`__#internal_channel`) refreshes that cache on connect, disconnect, subscribe, and unsubscribe (see [using-the-api.md](using-the-api.md)).

Call **`refresh_address_topic(topic)`** when you need to force a reload — for example after a port change without a clean disconnect, a subscribe-before-`run` race, or when the sender was offline while peers registered.

For operator / ops inspection without sending, use **`list_addresses(topic)`** (store directory as `(addr, unique_name)` rows).

### Internal channel (peer discovery)

On **`run`**, each client auto-subscribes to **`__#internal_channel`**. The library exchanges JSON control events between peers and updates address caches **without** invoking the application receive callback. In the common case, senders do not need manual **`refresh_address_topic`** after peers **`run`** or **`subscribe`** at runtime. Edge cases are documented in [using-the-api.md](using-the-api.md).

You cannot subscribe or unsubscribe `__#internal_channel` through the public API (`LNR_ERR_INTERNAL_TOPIC`).

### Subscriptions (receive side)

**`subscribe(topic)`** registers interest in receiving **application payloads** whose **wire metadata** refers to that topic’s internal **topic key**. Only topics you have subscribed to are passed to the **receive callback**; others are dropped (with a debug message when `liner_debug` is enabled). **`unsubscribe`** removes that mapping.

You typically **`subscribe`** to topics you want to listen to **in addition** to your source topic (for example broadcast channels). Subscriptions can be registered **before** or **after** **`run`**; if before, they are applied when the listener starts.

---

## Delivery

### Transport

After routing resolves a **TCP address**, the built-in **sender** thread opens a **plain TCP** connection to the peer’s **listener** and streams framed messages. There is **no TLS** in the library itself (see [security-defaults.md](security-defaults.md)).

### At-least-once vs best-effort

The send APIs expose **`at_least_once_delivery`** (C: `BOOL`).

- When **enabled**, the stack may **persist** messages that could not be delivered yet and **retry** after reconnects, using per-channel **message numbers** and stored **ack** positions so the receiver does not apply duplicates.
- When **disabled**, sends are **best-effort**: failures or offline peers may **drop** data without writing it to the store.

Exact rules, timers, and deduplication are in [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md).

To inspect how many offline blobs this sender currently has in the store, use **`pending_count`** / `lnr_pending_count` (depth can lag until in-memory queues flush).

### Ordering

The library aims for **FIFO per logical sender–receiver channel** (`connection_key`), subject to failures and retries. It does **not** guarantee a global total order across all topics or all clients.

### Callback contract

The receive callback receives **pointers valid only for the duration of the call**; copy bytes if you need them later (see [using-the-api.md](using-the-api.md)).

---

## Errors (behavioral summary)

- **Creation:** If the store cannot be opened or parameters are invalid, Rust returns **`None`**; C returns **`NULL`**. Plain open failure is otherwise silent on stderr for some Rust constructors (see [errors-and-logging.md](errors-and-logging.md)).
- **`run`:** Returns **`false`** with a last-error code when:
  - bind/resolve fails → **`LNR_ERR_BIND`**
  - catalog registration fails → **`LNR_ERR_STORE`**
  - listener startup fails after bind → **`LNR_ERR_STARTUP`** (no process panic)
  - Returns **`true`** if the client was **already running** (idempotent, **`LNR_OK`**).
  Details: [store-startup-failure-semantics.md](store-startup-failure-semantics.md).
- **`stop`:** Unregisters, joins threads, clears `published_addr`, keeps `bound_listen_addr`. Idempotent. After stop, `clear_*` and a fresh `run` are allowed.
- **Sends / subscribe / refresh / clear / advertise:** Return **`false`** on validation or store errors; set **`lnr_last_error_code`**; details usually go to **stderr** or the process-global **log hook**.
- **Status callback** (`lnr_set_status_cb`): asynchronous peer up/down (related topics only) and background route/store/send failures. Does **not** replace sync return codes.
- **Clearing stored data:** **`clear_stored_messages`** and **`clear_addresses_of_topic`** only succeed when the client is **not** running (`LNR_ERR_CLEAR_WHILE_RUNNING` otherwise).

For the full matrix (Rust `Client`, `Liner`, C, mutex poison), use [errors-and-logging.md](errors-and-logging.md).

---

## Quick mental model

1. **Store** = shared phone book: topic → TCP addresses + metadata for offline queues.
2. **Bind** = where this process listens; **published / advertise** = what peers dial.
3. **Source topic** = your published name in that phone book.
4. **`send_to` / `send_all`** = look up peers by topic string, then TCP + optional persistence.
5. **`subscribe`** = which incoming logical topics your callback accepts.
6. **Errors** = sync **boolean / null** + **last_error code** + **stderr/log hook**; async peer/route issues optionally via **status callback**.
