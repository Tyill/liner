# Behavior: topics, delivery, and errors

This page describes **what the library does** from an application perspective: naming, routing, delivery semantics, and failure modes. It does **not** specify TCP byte layout.

For **Redis/SQLite key names and tables** (operators), see [routing-and-store-layout.md](routing-and-store-layout.md). For **stderr, return codes, and panics**, see [errors-and-logging.md](errors-and-logging.md). For **offline queues, reconnect timing, and `number_mess`**, see [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md).

---

## Topics

### Source topic (the client’s own topic)

At construction you pass a **topic string** and a **TCP bind address** (`localhost`). When **`run`** succeeds, the client **registers** that pair in the shared store: other processes learn *“this topic is served at this TCP address by this `unique_name`.”*

That topic is the client’s **source topic**. The API **forbids** sending to it or subscribing to it as if it were a remote peer—those calls fail with an error message.

### Destination topics and addresses

**`send_to(topic, …)`** and **`send_all(topic, …)`** use `topic` as a **logical name**. The client looks up **TCP addresses** currently registered for that topic in the store.

- If **no addresses** are registered, the send fails (“not found addr for topic …”).
- If **several addresses** exist (multiple clients on the same topic name), **`send_to`** picks **one address per call** using **round-robin** over the cached list. **`send_all`** sends the same payload **once per registered address** (broadcast to every replica listed for that topic).

The client **caches** addresses after the first successful lookup. While peers are running, the **internal channel** (`__#internal_channel`) refreshes that cache on connect, disconnect, subscribe, and unsubscribe (see [using-the-api.md](using-the-api.md)). Call **`refresh_address_topic(topic)`** when you need to force a reload—for example after a port change without a clean disconnect, a subscribe-before-`run` race, or when the sender was offline while peers registered.

### Internal channel (peer discovery)

On **`run`**, each client auto-subscribes to **`__#internal_channel`**. The library exchanges JSON control events between peers and updates address caches **without** invoking the application receive callback. In the common case, senders do not need manual **`refresh_address_topic`** after peers **`run`** or **`subscribe`** at runtime. See [using-the-api.md](using-the-api.md) for edge cases (pre-`run` subscribe, races, stale cache).

### Subscriptions (receive side)

**`subscribe(topic)`** registers interest in receiving **application payloads** whose **wire metadata** refers to that topic’s internal **topic key**. Only topics you have subscribed to are passed to the **receive callback**; others are dropped (with a debug message). **`unsubscribe`** removes that mapping.

You typically **`subscribe`** to topics you want to listen to **in addition** to your source topic (for example broadcast channels). Subscriptions can be registered **before** or **after** **`run`**; if before, they are applied when the listener starts.

---

## Delivery

### Transport

After routing resolves a **TCP address**, the built-in **sender** thread opens a **plain TCP** connection to the peer’s **listener** and streams framed messages. There is **no TLS** in the library itself (see [security-defaults.md](security-defaults.md)).

### At-least-once vs best-effort

The send APIs expose **`at_least_once_delivery`** (C: `BOOL`). When **enabled**, the stack may **persist** messages that could not be delivered yet and **retry** after reconnects, using per-channel **message numbers** and stored **ack** positions so the receiver does not apply duplicates. When **disabled**, sends are **best-effort**: failures or offline peers may **drop** data without writing it to the store.

Exact rules, timers, and deduplication are in [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md).

### Ordering

The library aims for **FIFO per logical sender–receiver channel** (`connection_key`), subject to failures and retries. It does **not** guarantee a global total order across all topics or all clients.

### Callback contract

The receive callback receives **pointers valid only for the duration of the call**; copy bytes if you need them later (see [using-the-api.md](using-the-api.md)).

---

## Errors (behavioral summary)

- **Creation**: If the store cannot be opened or parameters are invalid, Rust returns **`None`**; C returns **`NULL`**. Creation is otherwise silent on stderr for a plain open failure (see [errors-and-logging.md](errors-and-logging.md)).
- **`run`**: Returns **`false`** if topic registration fails, the bind address is invalid, or TCP bind fails. Returns **`true`** if the client was **already running** (idempotent). Internal listener/sender startup may still **panic** on rare store failures—see [store-startup-failure-semantics.md](store-startup-failure-semantics.md).
- **Sends / subscribe / refresh**: Return **`false`** on validation errors, missing addresses, or store errors; details are usually printed to **stderr** via `print_error!`.
- **Status callback** (`lnr_set_status_cb`): asynchronous peer up/down (related topics only) and background route/store/send failures — see [using-the-api.md](using-the-api.md) and [errors-and-logging.md](errors-and-logging.md). Does not replace sync return codes.
- **Clearing stored data**: **`clear_stored_messages`** and **`clear_addresses_of_topic`** only succeed when the client is **not** running.

For a full matrix (Rust `Client`, `Liner`, C, mutex poison), use [errors-and-logging.md](errors-and-logging.md).

---

## Quick mental model

1. **Store** = shared phone book: topic → TCP addresses + metadata for offline queues.  
2. **Source topic** = your published name and bind address.  
3. **`send_to` / `send_all`** = look up peers by topic string, then TCP + optional persistence.  
4. **`subscribe`** = which incoming logical topics your callback accepts.  
5. **Errors** = sync **boolean / null** failure plus **stderr**; async peer/route issues optionally via **status callback**.
