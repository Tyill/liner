//! Same workload as `bench_pair_sendto_sqlite` / `bench_pair_sendto_redis`, catalog in **PostgreSQL**.
//!
//! **One shared database URL** for both `Liner` clients (like Redis). Fixed bind addresses;
//! each `run()` registers its row in `topic_addr` (no `receivers_json` on `new_postgres`).
//!
//! ```text
//! export LINER_BENCH_POSTGRES_URL='postgresql://liner:liner@127.0.0.1:15432/liner_test'
//! cargo build --release --features postgres --bin bench_pair_sendto_postgres
//! ./target/release/bench_pair_sendto_postgres
//! ```
//!
//! Create the database once if needed (Docker example):
//! `docker exec liner-test-postgres psql -U liner -d postgres -c 'CREATE DATABASE liner_bench;'`

#![cfg(feature = "postgres")]

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;
use std::time::SystemTime;
use std::{thread, time};

use liner_broker::Liner;

/// Fixed ports (same as sqlite/redis pair benches).
const ADDR1: &str = "127.0.0.1:22771";
const ADDR2: &str = "127.0.0.1:22782";

static RECEIVE_COUNT: AtomicI32 = AtomicI32::new(0);
static LAST_SEND_MS: Mutex<u64> = Mutex::new(0);

const MESS_SEND_COUNT: usize = 10000;
const MESS_SEND_COUNT_I32: i32 = MESS_SEND_COUNT as i32;
const MESS_SIZE: usize = 1024;
const SEND_CYCLE_COUNT: usize = 100;

const TRUNCATE_SQL: &str = r"
TRUNCATE TABLE conn_messages, conn_mess_number, conn_sender, topic_key,
    conn_key_map, sender_listener, topic_addr;
UPDATE seq SET v = 0 WHERE id = 1;
";

fn postgres_url() -> String {
    for key in ["LINER_BENCH_POSTGRES_URL", "LINER_TEST_POSTGRES_URL"] {
        if let Ok(url) = std::env::var(key) {
            let url = url.trim().to_string();
            if !url.is_empty() {
                return url;
            }
        }
    }
    eprintln!(
        "Set LINER_BENCH_POSTGRES_URL or LINER_TEST_POSTGRES_URL\n\
         e.g. postgresql://liner:liner@127.0.0.1:15432/liner_test\n\
         (create DB: docker exec <pg> psql -U liner -d postgres -c 'CREATE DATABASE liner_bench;')"
    );
    std::process::exit(2);
}

fn reset_liner_tables(url: &str) {
    use postgres::{Client, NoTls};
    let mut client = Client::connect(url, NoTls).unwrap_or_else(|e| {
        eprintln!("postgres connect failed ({url}): {e}");
        eprintln!("hint: create the database (CREATE DATABASE …) or fix the URL path");
        std::process::exit(2);
    });
    if let Err(e) = client.batch_execute(TRUNCATE_SQL) {
        let missing = e
            .as_db_error()
            .is_some_and(|db| db.code().code() == "42P01");
        if missing {
            return;
        }
        panic!("bench reset tables: {e}");
    }
}

fn main() {
    let url = postgres_url();
    reset_liner_tables(&url);

    let mut client1 = Liner::new_postgres("client1", "topic_client1", ADDR1, &url);
    let mut client2 = Liner::new_postgres("client2", "topic_client2", ADDR2, &url);

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

    assert!(client1.refresh_address_topic("topic_client2"));

    let array = [0u8; MESS_SIZE];
    for _ in 0..SEND_CYCLE_COUNT {
        let send_begin = current_time_ms();
        for _ in 0..MESS_SEND_COUNT {
            let _ = client1.send_to("topic_client2", array.as_slice(), false);
        }
        let send = current_time_ms();
        println!("send_to {} ms", send - send_begin);
        *LAST_SEND_MS.lock().unwrap() = send;

        thread::sleep(time::Duration::from_millis(1000));
    }

    drop(client1);
    drop(client2);
    reset_liner_tables(&url);
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
