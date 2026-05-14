# Offline delivery, reconnects, and message numbers

This describes **runtime behavior** in the sender and listener threads. Constants live in `src/settings.rs` (search there if values change).

## Scope: “at least once” vs best-effort

Persistence to the store (Redis or SQLite) for disconnected peers applies only to messages sent with **`at_least_once_delivery == true`**.

If **`at_least_once_delivery` is false**, the stack does not treat those messages as durable: on TCP failure or while the peer is down, they may be dropped and are **not** written to the store by the sender’s save path.

## What happens when a peer (listener) is down or TCP fails

**Sender side** (background thread in `sender.rs`):

1. **TCP connect fails** (`TcpStream::connect` in `append_streams`): queued messages for that logical connection are passed to `save_messages_from_sender` in the store **only** for messages that satisfy `at_least_once_delivery` **and** have a **`number_mess` strictly greater** than the last **listener-acknowledged** number read from the store (`get_last_mess_number_for_sender`). Other messages are freed in memory.

2. **TCP breaks during a write** (`write_stream`): messages that were not successfully written, but are still “ahead” of the listener’s persisted acknowledgement and marked at-least-once, are put back on the in-memory queue (or handled on stream teardown—same persistence rules on close).

3. **Stream is torn down** (`check_streams_close`): any remaining queued messages for that connection go through the same **`save_mess_to_db`** filter before being appended to the store’s per-connection queue.

So for **at-least-once** traffic, data that never received listener acknowledgement can survive **process-level disconnects** in the **backing store**, not only in RAM.

**On reconnect**, a successful `TcpStream::connect` triggers **`load_messages_for_sender`**: the sender loads the persisted queue from the store and **merges** it with anything still in memory for that connection, then continues sending.

## How often the sender retries TCP

Every **`CHECK_AVAILABLE_STREAM_TIMEOUT_MS`** (currently **10 000 ms**), the sender thread decides it should try **`append_streams`** again (unless it was triggered earlier by a **new address** flag). While an address remains unreachable, it stays on the internal retry list; each cycle attempts **`TcpStream::connect`** again.

So “try again about every 10 seconds” is accurate for **reconnection attempts**, modulo scheduling and the short wait/sleep logic in the same loop.

## How the listener avoids duplicate delivery (idempotency)

Each logical TCP stream from a sender is associated with a **`connection_key`**. Within that channel, every payload message carries a monotonically increasing **`number_mess`** assigned by the sender when the message is created.

On the **listener**, when reading from the wire (`read_stream`):

- The listener keeps **`last_mess_num`** for that stream (initialized from the store via **`get_last_mess_number_for_listener`** when the connection key is known, so restarts reconnect with the right baseline).
- An incoming message is **accepted** only if **`mess.number_mess > last_mess_num`**. Otherwise it is **discarded** (freed without invoking the receive callback).

Therefore **retries or duplicates with the same or an older number cannot be delivered to the application twice**. New work only appears when the sender uses a **strictly larger** `number_mess`.

## How sender and listener stay in sync with the store

- The **listener** periodically persists the highest **`last_mess_num`** it has accepted to the store (**`set_last_mess_number_from_listener`**), gated by **`UPDATE_LAST_MESS_NUMBER_TIMEOUT_MS`** (currently **1000 ms**) in the receive path—so acknowledgements are flushed to the DB about once per second under normal timing, not on every single message.

- The **sender** periodically calls **`get_last_mess_number_for_sender`** (same key) to align in-memory queues with what the listener has acknowledged, and to **drop** from RAM messages that are now fully acked (see `update_last_mess_number`).

Together, **`number_mess`** plus the stored **last acknowledged number per `connection_key`** define what may be resent after a failure and what the listener must ignore as already processed.

## Practical summary

| Topic | Behavior |
|-------|-----------|
| Listener offline / TCP down (at-least-once) | Unacknowledged messages can be **saved in Redis/SQLite**; sender **retries TCP ~every 10 s**; on success, **loads queued messages** from the store and sends them. |
| Best-effort sends | **No** guarantee of persistence across disconnects. |
| Duplicate wire deliveries | **Suppressed** on the listener when `number_mess` is not greater than the last accepted value for that connection. |
| Ack timing | Listener flushes acks to the store on a **~1 s** cadence (`UPDATE_LAST_MESS_NUMBER_TIMEOUT_MS`). |

## Related reading

- [using-the-api.md](using-the-api.md) — `at_least_once_delivery` on C/Rust APIs.
- [backends.md](backends.md) — where queues and counters live (Redis keys / SQLite tables).
