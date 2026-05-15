//! Same workload as `bench_pair_sendto_redis`, but catalog in **SQLite** (no Redis).
//!
//! **Two isolated temp SQLite files** — one per `Liner`, with fixed bind addresses so each side
//! seeds **only the peer** in `receivers_json` (`topic`, `addr`, `client_name`). Empty DBs ⇒ first wire
//! `connection_key` is **1** and seeded **`topic_key`** is **1**; seeding fills `conn_sender` for the
//! receive callback `from` (see `docs/using-sqlite.md`).
//!
//! **Alternative:** one shared temp `.sqlite` and **empty** `receivers_json` for both clients (same
//! idea as one Redis URL); not exercised in this binary.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;
use std::time::SystemTime;
use std::{thread, time};

use liner_broker::Liner;

/// Fixed ports so `receivers_json` can be built before `run` (no dynamic `bound_listen_addr`).
const ADDR1: &str = "127.0.0.1:22771";
const ADDR2: &str = "127.0.0.1:22782";

fn db_path(suffix: &str) -> std::path::PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "liner_bench_pair_sendto_sqlite_{}_{}_{}.sqlite",
        std::process::id(),
        nonce,
        suffix
    ))
}

static RECEIVE_COUNT: AtomicI32 = AtomicI32::new(0);
static LAST_SEND_MS: Mutex<u64> = Mutex::new(0);

const MESS_SEND_COUNT: usize = 10000;
const MESS_SEND_COUNT_I32: i32 = MESS_SEND_COUNT as i32;
const MESS_SIZE: usize = 1024;
const SEND_CYCLE_COUNT: usize = 100;

fn remove_sqlite_sidecars(p: &std::path::Path) {
    for path in [
        p,
        std::path::Path::new(&format!("{}-wal", p.display())),
        std::path::Path::new(&format!("{}-shm", p.display())),
    ] {
        if path.exists() && std::fs::remove_file(path).is_err() {
            eprintln!(
                "bench_pair_sendto_sqlite: could not remove {:?} (in use?); new run uses a fresh path",
                path
            );
        }
    }
}

fn main() {
    let path1 = db_path("a");
    let path2 = db_path("b");
    for p in [&path1, &path2] {
        remove_sqlite_sidecars(p);
    }
    let s1 = path1.to_str().expect("UTF-8 path");
    let s2 = path2.to_str().expect("UTF-8 path");

    let catalog1 = serde_json::json!([{
        "topic": "topic_client2",
        "addr": ADDR2,
        "client_name": "client2"
    }])
    .to_string();

    let catalog2 = serde_json::json!([{
        "topic": "topic_client1",
        "addr": ADDR1,
        "client_name": "client1"
    }])
    .to_string();

    let mut client1 = Liner::new_sqlite(
        "client1",
        "topic_client1",
        ADDR1,
        s1,
        &catalog1,
    );
    let mut client2 = Liner::new_sqlite(
        "client2",
        "topic_client2",
        ADDR2,
        s2,
        &catalog2,
    );

    client1.clear_stored_messages();
    client2.clear_stored_messages();

    client1.clear_addresses_of_topic();
    client2.clear_addresses_of_topic();

    assert!(client1.run(Box::new(|_to: &str, from: &str, _data: &[u8]| {
        println!("receive_from {}", from);
    })));
    assert!(client2.run(Box::new(|_to: &str, _from: &str, _data: &[u8]| {
        let n = RECEIVE_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        if n == MESS_SEND_COUNT_I32 {
            RECEIVE_COUNT.store(0, Ordering::Relaxed);
            let send = *LAST_SEND_MS.lock().unwrap();
            println!("receive_from {} ms", current_time_ms() - send);
        }
    })));

    let array = [0u8; MESS_SIZE];
    for _ in 0..SEND_CYCLE_COUNT {
        let send_begin = current_time_ms();
        for _ in 0..MESS_SEND_COUNT {
            let _ = client1.send_to("topic_client2", array.as_slice());
        }
        let send = current_time_ms();
        println!("send_to {} ms", send - send_begin);
        *LAST_SEND_MS.lock().unwrap() = send;

        thread::sleep(time::Duration::from_millis(1000));
    }

    drop(client1);
    drop(client2);
    for p in [&path1, &path2] {
        remove_sqlite_sidecars(p);
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
