# liner

**liner** is a lightweight, serverless, peer-to-peer message broker written in Rust. It provides a decentralized messaging mesh backed by Redis, SQLite, or PostgreSQL, with pure TCP between peers.

- Decentralized architecture — peer-to-peer mesh, no central broker process.
- Flexible storage — Redis, SQLite, or PostgreSQL ([backends](#supported-backends)).
- At-least-once delivery — store-backed persistence and offline queues (per backend).
- Cross-language — Rust, Python, and C++ (C API).
- High message bandwidth — raw TCP; see [benchmark](#benchmark).
- Messaging patterns — one-to-one, one-to-many, many-to-many, topic subscription.
- Unlimited payload size — not fixed by the wire format.
- Cross-platform — Linux and Windows.

---

### Supported backends

| Backend | Mode | Best for |
| :--- | :--- | :--- |
| SQLite | Embedded single-file | Local dev, edge/IoT, zero extra services |
| Redis | In-memory key-value | Low latency, ephemeral catalog |
| PostgreSQL | Shared relational DB | Production persistence, ops familiarity |

- Redis — shared URL, default build. See [using-redis.md](docs/using-redis.md).
- SQLite — per-process files; `receivers_json` seeds peers on isolated DBs. See [using-sqlite.md](docs/using-sqlite.md).
- PostgreSQL — one shared database URL (Cargo feature `postgres`). See [using-postgres.md](docs/using-postgres.md).

---

### Quick example (Redis)

Rust:

```rust
use liner_broker::Liner;

fn main() {
    let mut client1 = Liner::new("client1", "topic_client1", "localhost:2255", "redis://localhost/");
    let mut client2 = Liner::new("client2", "topic_client2", "localhost:2256", "redis://localhost/");

    client1.run(Box::new(|_to, from, _data| println!("receive_from {}", from)));
    client2.run(Box::new(|_to, _from, _data| {}));

    let payload = [0u8; 100];
    for _ in 0..10 {
        client1.send_to("topic_client2", &payload, true);
    }
}
```

Python:

```python
import liner

liner.loadLib("./target/release/libliner_broker.so")

def on_msg(_to: str, from_: str, data: bytes):
    print(f"receive_from {from_}, data: {data}")

client1 = liner.Client("client1", "topic_client", "localhost:2255", "redis://localhost/")
client2 = liner.Client("client2", "topic_client", "localhost:2256", "redis://localhost/")

client1.run(on_msg)
client2.run(on_msg)

client1.send_to("topic_client", b"hello", True)
```

SQLite / PostgreSQL constructors: `Liner::new_sqlite`, `Client.new_sqlite`, `Liner::new_postgres`, `Client.new_postgres` — see docs below.

---

### Build

Install [Rust and Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html), then:

```bash
cargo build --release
```

PostgreSQL backend:

```bash
cargo build --release --features postgres
```

---

### Architecture

![Library architecture](docs/img/arch.png)

---

### Examples of use

One to one: [Python](python/one_to_one.py) / [CPP](cpp/one_to_one.cpp) / [Rust](rust/one_to_one.rs)

<p float="left">
  <img src="docs/img/one_to_one.gif" width="500" height="150" alt="One-to-one messaging demo">
</p>

One to one for many: [Python](python/one_to_one_for_many.py) / [CPP](cpp/one_to_one_for_many.cpp) / [Rust](rust/one_to_one_for_many.rs)

<p float="left">
  <img src="docs/img/one_to_one_for_many.gif" width="500" height="200" alt="One-to-one for many demo">
</p>

One to many: [Python](python/one_to_many.py) / [CPP](cpp/one_to_many.cpp) / [Rust](rust/one_to_many.rs)

<p float="left">
  <img src="docs/img/one_to_many.gif" width="500" height="200" alt="One-to-many messaging demo">
</p>

Many to many: [Python](python/many_to_many.py) / [CPP](cpp/many_to_many.cpp) / [Rust](rust/many_to_many.rs)

<p float="left">
  <img src="docs/img/many_to_many.gif" width="500" height="200" alt="Many-to-many messaging demo">
</p>

Producer-consumer: [Python](python/producer_consumer.py) / [CPP](cpp/producer_consumer.cpp) / [Rust](rust/producer_consumer.rs)

<p float="left">
  <img src="docs/img/producer_consumer.gif" width="500" height="200" alt="Producer-consumer demo">
</p>

---

### Benchmark

Three binaries run the same workload: **two clients, `send_to` loop** (10k messages × 100 cycles). Only the store differs ([`benchmark/`](benchmark/)).

| Binary | Store | Notes |
| :--- | :--- | :--- |
| `bench_pair_sendto_redis` | Redis | `redis://localhost/`, default build |
| `bench_pair_sendto_sqlite` | SQLite | Two temp `.sqlite` files; fixed ports; each side seeds the peer via `receivers_json` |
| `bench_pair_sendto_postgres` | PostgreSQL | One shared URL; `cargo build --release --features postgres` |

```bash
cargo build --release --bin bench_pair_sendto_redis
./target/release/bench_pair_sendto_redis

cargo build --release --bin bench_pair_sendto_sqlite
./target/release/bench_pair_sendto_sqlite

export LINER_BENCH_POSTGRES_URL='postgresql://user:pass@127.0.0.1/liner_test'
cargo build --release --features postgres --bin bench_pair_sendto_postgres
./target/release/bench_pair_sendto_postgres
```

Sample output (liner + Redis, 10k messages per cycle):

```text
$ ./bench_pair_sendto_redis
send_to 8 ms
receive_from 8 ms
send_to 5 ms
receive_from 5 ms
send_to 7 ms
receive_from 3 ms
send_to 11 ms
receive_from 3 ms
send_to 6 ms
receive_from 3 ms
```

About **10 ms** on average for 10k messages.

Comparison with [ZeroMQ](benchmark/compare_with_zeromq/) on the same machine (`make` && `./compare_with_zmq`):

```text
$ make
g++ -Wall -O2 -std=c++17 -g -Wno-write-strings -o compare_with_zmq compare_with_zmq.cpp -lzmq
$ ./compare_with_zmq
Connecting to tcp://127.0.0.1:34079
send_to 20.198 ms
send_to 16.504 ms
send_to 11.5 ms
send_to 13.153 ms
send_to 10.964 ms
send_to 10.788 ms
send_to 10.785 ms
send_to 11.119 ms
send_to 11.348 ms
send_to 10.826 ms
```

For ZeroMQ it is similar (on the order of ~10 ms per 10k messages).

---

### Tests

Rust unit tests:

```bash
cargo test
# PostgreSQL store tests (optional):
LINER_TEST_POSTGRES_URL='postgresql://user:pass@127.0.0.1/liner_test' \
  cargo test --features postgres
```

Python integration suites (build release library first):

```bash
cargo build --release
python3 test/redis/run_integration.py --list
python3 test/sqlite/run_integration.py
```

PostgreSQL (requires `postgres` feature and a running database):

```bash
export LINER_TEST_POSTGRES_URL='postgresql://user:pass@127.0.0.1/liner_test'
cargo build --release --features postgres
python3 test/postgres/run_integration.py
```

Redis tests under **`test/redis/`** auto-start Redis via Docker when needed. Optional env:

```bash
LINER_TEST_REDIS_PORT=16379 LINER_TEST_REDIS_CONTAINER=liner-test-redis \
  python3 test/redis/run_integration.py --only offline,burst
```

---

### Docs

- [Using Redis](docs/using-redis.md) — `new_redis`, shared URL, `test/redis/`
- [Using SQLite](docs/using-sqlite.md) — `new_sqlite`, `receivers_json`, isolated DB limits
- [Using PostgreSQL](docs/using-postgres.md) — `--features postgres`, shared database
- [Crate API on docs.rs](https://docs.rs/liner_broker/1.3.1/liner_broker/)
- [Developer notes](docs/README.md) — errors, backends, C API, lifecycle
- [C API compatibility and build](docs/c-api-compatibility-and-build.md)

---

### License

Licensed under the [MIT License](LICENSE).
