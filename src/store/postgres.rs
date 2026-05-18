//! PostgreSQL-backed [`Store`](super::store::Store) (parity with [`super::sqlite::Sqlite`]).
//!
//! Enable with Cargo feature **`postgres`** (`--features postgres`).

use crate::{message::Message, mempool::Mempool, print_error};

use super::store::{DbError, DbResult, ReceiverSeedEntry, Store};
use postgres::{Client, Error, NoTls};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[cfg(test)]
use std::sync::Mutex as TestDbMutex;

const LOCK_TIMEOUT: &str = "5s";

/// First `connection_key` on an **empty** database (isolated-DB pair model); not passed in `receivers_json`.
pub(crate) const FIRST_ISOLATED_CONNECTION_KEY: i32 = 1;

/// Wire `topic_key` for every topic row seeded from `receivers_json`; not in JSON.
pub(crate) const FIRST_ISOLATED_TOPIC_KEY: i32 = 1;

fn map_pg<T>(r: std::result::Result<T, Error>) -> DbResult<T> {
    r.map_err(|e| DbError::new(e.to_string()))
}

fn sender_key(unique_name: &str, source_topic: &str) -> String {
    format!("{}:{}", unique_name, source_topic)
}

fn connection_composite(unique_name: &str, source_topic: &str, listener_name: &str) -> String {
    format!("{}:{}:{}", unique_name, source_topic, listener_name)
}

fn encode_and_free_messages(mempool: &Arc<Mutex<Mempool>>, mess: Vec<Message>) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(mess.len());
    for m in mess {
        let mut buf: Vec<u8> = Vec::new();
        m.to_stream(mempool, &mut buf);
        m.free(mempool);
        out.push(buf);
    }
    out
}

const SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS seq (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    v INTEGER NOT NULL
);
INSERT INTO seq (id, v) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS topic_addr (
    topic TEXT NOT NULL,
    addr TEXT NOT NULL,
    client_name TEXT NOT NULL,
    PRIMARY KEY (topic, addr)
);

CREATE TABLE IF NOT EXISTS sender_listener (
    sender_key TEXT NOT NULL,
    addr TEXT NOT NULL,
    listener_topic TEXT NOT NULL,
    PRIMARY KEY (sender_key, addr)
);

CREATE TABLE IF NOT EXISTS conn_key_map (
    composite TEXT PRIMARY KEY,
    connection_key INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS topic_key (
    topic TEXT PRIMARY KEY,
    k INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS conn_sender (
    connection_key INTEGER PRIMARY KEY,
    sender_topic TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS conn_mess_number (
    connection_key INTEGER PRIMARY KEY,
    v BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS conn_messages (
    id BIGSERIAL PRIMARY KEY,
    connection_key INTEGER NOT NULL,
    payload BYTEA NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_conn_messages_ck
    ON conn_messages(connection_key, id);
";

pub struct Postgres {
    unique_name: String,
    source_topic: String,
    source_localhost: String,
    client: Client,
    topic_addr_cache: HashMap<String, Vec<String>>,
    topic_key_cache: HashMap<String, i32>,
    unique_name_cache: HashMap<String, String>,
    last_mess_number: HashMap<i32, u64>,
}

#[cfg(test)]
fn ensure_schema_inner(client: &mut Client) -> DbResult<()> {
    map_pg(client.batch_execute(SCHEMA))
}

impl Postgres {
    pub fn new(unique_name: &str, url: &str) -> DbResult<Self> {
        let mut client = map_pg(Client::connect(url, NoTls))?;
        #[cfg(test)]
        ensure_schema_inner(&mut client)?;
        #[cfg(not(test))]
        map_pg(client.batch_execute(SCHEMA))?;
        // SET does not accept bind parameters ($1) in PostgreSQL.
        map_pg(client.batch_execute(&format!("SET lock_timeout = '{LOCK_TIMEOUT}'")))?;

        Ok(Postgres {
            unique_name: unique_name.to_string(),
            source_topic: String::new(),
            source_localhost: String::new(),
            client,
            topic_addr_cache: HashMap::new(),
            topic_key_cache: HashMap::new(),
            unique_name_cache: HashMap::new(),
            last_mess_number: HashMap::new(),
        })
    }

    fn next_connection_id(&mut self) -> DbResult<i32> {
        let mut tx = map_pg(self.client.transaction())?;
        map_pg(tx.execute("UPDATE seq SET v = v + 1 WHERE id = 1", &[]))?;
        let row = map_pg(tx.query_one("SELECT v FROM seq WHERE id = 1", &[]))?;
        let v: i32 = map_pg(row.try_get(0))?;
        map_pg(tx.commit())?;
        Ok(v)
    }

    fn init_addresses_of_topic(&mut self, topic: &str) -> DbResult<()> {
        let rows = map_pg(self.client.query(
            "SELECT addr, client_name FROM topic_addr WHERE topic = $1 ORDER BY addr ASC",
            &[&topic],
        ))?;
        let mut addrs: Vec<String> = Vec::new();
        for row in rows {
            let addr: String = map_pg(row.try_get(0))?;
            let name: String = map_pg(row.try_get(1))?;
            self.unique_name_cache.insert(addr.clone(), name);
            addrs.push(addr);
        }
        self.topic_addr_cache.insert(topic.to_string(), addrs);
        Ok(())
    }

    fn init_last_mess_number_from_sender(&mut self, connection_key: i32) -> DbResult<()> {
        map_pg(self.client.execute(
            "INSERT INTO conn_mess_number (connection_key, v) VALUES ($1, 0)
             ON CONFLICT (connection_key) DO NOTHING",
            &[&connection_key],
        ))?;
        Ok(())
    }

    fn sync_seq_after_seed(&mut self) -> DbResult<()> {
        let max_ck: i32 = map_pg(
            map_pg(self.client.query_one(
                "SELECT COALESCE(MAX(connection_key), 0) FROM conn_key_map",
                &[],
            ))?
            .try_get(0),
        )?;
        let max_sender_ck: i32 = map_pg(
            map_pg(self.client.query_one(
                "SELECT COALESCE(MAX(connection_key), 0) FROM conn_sender",
                &[],
            ))?
            .try_get(0),
        )?;
        let cur: i32 = map_pg(
            map_pg(self.client.query_one("SELECT v FROM seq WHERE id = 1", &[]))?.try_get(0),
        )?;
        let m = cur.max(max_ck).max(max_sender_ck);
        if m > cur {
            map_pg(
                self.client
                    .execute("UPDATE seq SET v = $1 WHERE id = 1", &[&m]),
            )?;
        }
        Ok(())
    }

    fn seed_receivers_pg(&mut self, entries: &[ReceiverSeedEntry]) -> DbResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut tx = map_pg(self.client.transaction())?;
        for e in entries {
            map_pg(tx.execute(
                "INSERT INTO topic_addr (topic, addr, client_name) VALUES ($1, $2, $3)
                 ON CONFLICT (topic, addr) DO UPDATE SET client_name = EXCLUDED.client_name",
                &[&e.topic, &e.addr, &e.client_name],
            ))?;
            let topic_key = FIRST_ISOLATED_TOPIC_KEY;
            map_pg(tx.execute(
                "INSERT INTO topic_key (topic, k) VALUES ($1, $2)
                 ON CONFLICT (topic) DO UPDATE SET k = EXCLUDED.k",
                &[&e.topic, &topic_key],
            ))?;
            if e.topic != self.source_topic {
                let composite =
                    connection_composite(&self.unique_name, &self.source_topic, &e.client_name);
                let conn_key = FIRST_ISOLATED_CONNECTION_KEY;
                map_pg(tx.execute(
                    "INSERT INTO conn_key_map (composite, connection_key) VALUES ($1, $2)
                     ON CONFLICT (composite) DO UPDATE SET connection_key = EXCLUDED.connection_key",
                    &[&composite, &conn_key],
                ))?;
                map_pg(tx.execute(
                    "INSERT INTO conn_sender (connection_key, sender_topic) VALUES ($1, $2)
                     ON CONFLICT (connection_key) DO UPDATE SET sender_topic = EXCLUDED.sender_topic",
                    &[&conn_key, &e.topic],
                ))?;
            }
        }
        let source_topic_key = FIRST_ISOLATED_TOPIC_KEY;
        map_pg(tx.execute(
            "INSERT INTO topic_key (topic, k) VALUES ($1, $2) ON CONFLICT (topic) DO NOTHING",
            &[&self.source_topic, &source_topic_key],
        ))?;
        map_pg(tx.commit())?;

        let mut topic_key_last: HashMap<String, i32> = HashMap::new();
        for e in entries {
            topic_key_last.insert(e.topic.clone(), FIRST_ISOLATED_TOPIC_KEY);
        }
        topic_key_last.insert(self.source_topic.clone(), FIRST_ISOLATED_TOPIC_KEY);
        let topics: HashSet<String> = topic_key_last.keys().cloned().collect();
        for t in topics {
            self.init_addresses_of_topic(&t)?;
            if let Some(&k) = topic_key_last.get(&t) {
                self.topic_key_cache.insert(t, k);
            }
        }
        self.sync_seq_after_seed()?;
        Ok(())
    }
}

impl Store for Postgres {
    fn set_source_topic(&mut self, topic: &str) {
        self.source_topic = topic.to_string();
    }

    fn set_source_localhost(&mut self, localhost: &str) {
        self.source_localhost = localhost.to_string();
    }

    fn regist_topic(&mut self, topic: &str) -> DbResult<()> {
        let localhost = self.source_localhost.clone();
        let unique = self.unique_name.clone();
        map_pg(self.client.execute(
            "INSERT INTO topic_addr (topic, addr, client_name) VALUES ($1, $2, $3)
             ON CONFLICT (topic, addr) DO UPDATE SET client_name = EXCLUDED.client_name",
            &[&topic, &localhost, &unique],
        ))?;
        self.init_addresses_of_topic(topic)?;
        Ok(())
    }

    fn unregist_topic(&mut self, topic: &str) -> DbResult<()> {
        let localhost = self.source_localhost.clone();
        map_pg(self.client.execute(
            "DELETE FROM topic_addr WHERE topic = $1 AND addr = $2",
            &[&topic, &localhost],
        ))?;
        self.init_addresses_of_topic(topic)?;
        Ok(())
    }

    fn clear_addresses_of_topic(&mut self) -> DbResult<()> {
        let topic = self.source_topic.clone();
        map_pg(
            self.client
                .execute("DELETE FROM topic_addr WHERE topic = $1", &[&topic]),
        )?;
        Ok(())
    }

    fn clear_stored_messages(&mut self) -> DbResult<()> {
        let sk = sender_key(&self.unique_name, &self.source_topic);
        let rows = map_pg(self.client.query(
            "SELECT addr, listener_topic FROM sender_listener WHERE sender_key = $1",
            &[&sk],
        ))?;
        for row in rows {
            let addr: String = map_pg(row.try_get(0))?;
            let listener_topic: String = map_pg(row.try_get(1))?;
            if let Ok(listener_name) = self.get_listener_unique_name(&listener_topic, &addr) {
                if let Ok(connection_key) = self.get_connection_key_for_sender(&listener_name) {
                    map_pg(self.client.execute(
                        "DELETE FROM conn_messages WHERE connection_key = $1",
                        &[&connection_key],
                    ))?;
                    map_pg(self.client.execute(
                        "DELETE FROM conn_mess_number WHERE connection_key = $1",
                        &[&connection_key],
                    ))?;
                }
            }
        }
        map_pg(
            self.client
                .execute("DELETE FROM sender_listener WHERE sender_key = $1", &[&sk]),
        )?;
        Ok(())
    }

    fn save_listener_for_sender(&mut self, listener_addr: &str, listener_topic: &str) -> DbResult<()> {
        let sk = sender_key(&self.unique_name, &self.source_topic);
        map_pg(self.client.execute(
            "INSERT INTO sender_listener (sender_key, addr, listener_topic) VALUES ($1, $2, $3)
             ON CONFLICT (sender_key, addr) DO UPDATE SET listener_topic = EXCLUDED.listener_topic",
            &[&sk, &listener_addr, &listener_topic],
        ))?;
        Ok(())
    }

    fn get_listeners_of_sender(&mut self) -> DbResult<Vec<(String, String)>> {
        let sk = sender_key(&self.unique_name, &self.source_topic);
        let rows = map_pg(self.client.query(
            "SELECT addr, listener_topic FROM sender_listener WHERE sender_key = $1",
            &[&sk],
        ))?;
        let mut out = Vec::new();
        for row in rows {
            out.push((map_pg(row.try_get(0))?, map_pg(row.try_get(1))?));
        }
        Ok(out)
    }

    fn get_addresses_of_topic(&mut self, without_cache: bool, topic: &str) -> DbResult<Vec<String>> {
        if !self.topic_addr_cache.contains_key(topic) || without_cache {
            self.init_addresses_of_topic(topic)?;
        }
        Ok(self.topic_addr_cache[topic].clone())
    }

    fn get_listener_unique_name(&mut self, topic: &str, address: &str) -> DbResult<String> {
        if !self.topic_addr_cache.contains_key(topic) {
            self.init_addresses_of_topic(topic)?;
        }
        self.unique_name_cache
            .get(address)
            .cloned()
            .ok_or_else(|| DbError::new("!unique_name_cache.contains_key"))
    }

    fn get_connection_key_for_sender(&mut self, listener_name: &str) -> DbResult<i32> {
        let composite = connection_composite(&self.unique_name, &self.source_topic, listener_name);
        if let Some(row) = map_pg(self.client.query_opt(
            "SELECT connection_key FROM conn_key_map WHERE composite = $1",
            &[&composite],
        ))? {
            return map_pg(row.try_get(0));
        }
        let id = self.next_connection_id()?;
        map_pg(self.client.execute(
            "INSERT INTO conn_key_map (composite, connection_key) VALUES ($1, $2)",
            &[&composite, &id],
        ))?;
        Ok(id)
    }

    fn get_topic_key(&mut self, topic: &str) -> DbResult<i32> {
        if let Some(k) = self.topic_key_cache.get(topic) {
            return Ok(*k);
        }
        if let Some(row) =
            map_pg(self.client.query_opt("SELECT k FROM topic_key WHERE topic = $1", &[&topic]))?
        {
            let k: i32 = map_pg(row.try_get(0))?;
            self.topic_key_cache.insert(topic.to_owned(), k);
            return Ok(k);
        }
        let id = self.next_connection_id()?;
        map_pg(self.client.execute(
            "INSERT INTO topic_key (topic, k) VALUES ($1, $2)",
            &[&topic, &id],
        ))?;
        self.topic_key_cache.insert(topic.to_owned(), id);
        Ok(id)
    }

    fn set_sender_topic_by_connection_key_from_sender(
        &mut self,
        connection_key: i32,
    ) -> DbResult<()> {
        let source_topic = self.source_topic.clone();
        map_pg(self.client.execute(
            "INSERT INTO conn_sender (connection_key, sender_topic) VALUES ($1, $2)
             ON CONFLICT (connection_key) DO UPDATE SET sender_topic = EXCLUDED.sender_topic",
            &[&connection_key, &source_topic],
        ))?;
        Ok(())
    }

    fn get_sender_topic_by_connection_key(&mut self, connection_key: i32) -> DbResult<String> {
        let row = map_pg(self.client.query_one(
            "SELECT sender_topic FROM conn_sender WHERE connection_key = $1",
            &[&connection_key],
        ))?;
        map_pg(row.try_get(0))
    }

    fn set_last_mess_number_from_listener(&mut self, connection_key: i32, val: u64) -> DbResult<()> {
        let v = i64::try_from(val).map_err(|_| DbError::new("mess_number too large for i64"))?;
        map_pg(self.client.execute(
            "INSERT INTO conn_mess_number (connection_key, v) VALUES ($1, $2)
             ON CONFLICT (connection_key) DO UPDATE SET v = EXCLUDED.v",
            &[&connection_key, &v],
        ))?;
        self.last_mess_number.insert(connection_key, val);
        Ok(())
    }

    fn get_last_mess_number_for_listener(&mut self, connection_key: i32) -> DbResult<u64> {
        if !self.last_mess_number.contains_key(&connection_key) {
            let value = match map_pg(self.client.query_opt(
                "SELECT v FROM conn_mess_number WHERE connection_key = $1",
                &[&connection_key],
            ))? {
                Some(row) => {
                    let v: i64 = map_pg(row.try_get(0))?;
                    u64::try_from(v).map_err(|_| DbError::new("invalid mess_number"))?
                }
                None => {
                    self.init_last_mess_number_from_sender(connection_key)?;
                    0
                }
            };
            self.last_mess_number.insert(connection_key, value);
        }
        Ok(self.last_mess_number[&connection_key])
    }

    fn get_last_mess_number_for_sender(&mut self, connection_key: i32) -> DbResult<u64> {
        match map_pg(self.client.query_opt(
            "SELECT v FROM conn_mess_number WHERE connection_key = $1",
            &[&connection_key],
        ))? {
            Some(row) => {
                let v: i64 = map_pg(row.try_get(0))?;
                Ok(u64::try_from(v).map_err(|_| DbError::new("invalid mess_number"))?)
            }
            None => {
                self.init_last_mess_number_from_sender(connection_key)?;
                Ok(0)
            }
        }
    }

    fn save_messages_from_sender(
        &mut self,
        mempool: &Arc<Mutex<Mempool>>,
        connection_key: i32,
        mess: Vec<Message>,
    ) -> DbResult<()> {
        let encoded = encode_and_free_messages(mempool, mess);
        let mut tx = map_pg(self.client.transaction())?;
        for buf in encoded {
            map_pg(tx.execute(
                "INSERT INTO conn_messages (connection_key, payload) VALUES ($1, $2)",
                &[&connection_key, &buf],
            ))?;
        }
        map_pg(tx.commit())?;
        Ok(())
    }

    fn load_messages_for_sender(
        &mut self,
        mempool: &Arc<Mutex<Mempool>>,
        connection_key: i32,
    ) -> DbResult<Vec<Message>> {
        let rows = map_pg(self.client.query(
            "SELECT id, payload FROM conn_messages WHERE connection_key = $1 ORDER BY id ASC",
            &[&connection_key],
        ))?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let pairs: Vec<(i64, Vec<u8>)> = rows
            .into_iter()
            .map(|row| Ok((map_pg(row.try_get(0))?, map_pg(row.try_get(1))?)))
            .collect::<DbResult<_>>()?;
        let ids: Vec<i64> = pairs.iter().map(|(id, _)| *id).collect();
        let mut tx = map_pg(self.client.transaction())?;
        for id in &ids {
            map_pg(tx.execute("DELETE FROM conn_messages WHERE id = $1", &[&id]))?;
        }
        map_pg(tx.commit())?;

        let mut out = Vec::new();
        for (_, b) in pairs {
            let mut is_shutdown = false;
            if let Some(mess) = Message::from_stream(mempool, &mut &b[..], &mut is_shutdown) {
                out.push(mess);
            } else {
                print_error!("!Message::from_stream");
            }
        }
        Ok(out)
    }

    fn load_last_message_for_sender(
        &mut self,
        mempool: &Arc<Mutex<Mempool>>,
        connection_key: i32,
    ) -> DbResult<Option<Message>> {
        let row = map_pg(self.client.query_opt(
            "SELECT payload FROM conn_messages WHERE connection_key = $1 ORDER BY id DESC LIMIT 1",
            &[&connection_key],
        ))?;
        let mut out = None;
        if let Some(row) = row {
            let b: Vec<u8> = map_pg(row.try_get(0))?;
            let mut is_shutdown = false;
            if let Some(mess) = Message::from_stream(mempool, &mut &b[..], &mut is_shutdown) {
                out = Some(mess);
            } else {
                print_error!("!Message::from_stream");
            }
        }
        Ok(out)
    }

    fn seed_receivers(&mut self, entries: &[ReceiverSeedEntry]) -> DbResult<()> {
        self.seed_receivers_pg(entries)
    }
}

#[cfg(test)]
static TEST_DB_LOCK: TestDbMutex<()> = TestDbMutex::new(());

/// Serializes access to the shared integration-test database (parallel `cargo test` safe).
#[cfg(test)]
pub fn test_db_lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_DB_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Clears liner tables. Caller must hold [`test_db_lock`] (see store postgres tests).
#[cfg(test)]
pub(crate) fn test_reset_tables_inner(url: &str) {
    const TRUNCATE_SQL: &str = r"
TRUNCATE TABLE conn_messages, conn_mess_number, conn_sender, topic_key,
        conn_key_map, sender_listener, topic_addr;
UPDATE seq SET v = 0 WHERE id = 1;
";
    let mut client = Client::connect(url, NoTls).expect("postgres connect for test reset");
    if let Err(e) = client.batch_execute(TRUNCATE_SQL) {
        let missing = e
            .as_db_error()
            .is_some_and(|db| db.code().code() == "42P01");
        if missing {
            ensure_schema_inner(&mut client).expect("postgres test schema");
            client
                .batch_execute(TRUNCATE_SQL)
                .expect("postgres test_reset_tables");
        } else {
            panic!("postgres test_reset_tables: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{ReceiverSeedEntry, Store};

    macro_rules! require_pg_url {
        () => {{
            match std::env::var("LINER_TEST_POSTGRES_URL") {
                Ok(u) => u,
                Err(_) => {
                    eprintln!(
                        "skip {}: LINER_TEST_POSTGRES_URL unset",
                        std::module_path!()
                    );
                    return;
                }
            }
        }};
    }

    fn fresh_db(unique_name: &str, url: &str) -> Postgres {
        test_reset_tables_inner(url);
        Postgres::new(unique_name, url).expect("Postgres::new")
    }

    #[test]
    fn postgres_seed_receivers_empty_noop() {
        let url = require_pg_url!();
        let _lock = test_db_lock();
        let mut db = fresh_db("u", &url);
        db.seed_receivers(&[]).unwrap();
    }

    #[test]
    fn postgres_seed_receivers_populates_catalog() {
        let url = require_pg_url!();
        let _lock = test_db_lock();
        let mut db = fresh_db("listener", &url);
        db.set_source_topic("me");
        db.set_source_localhost("127.0.0.1:9");
        db.seed_receivers(&[ReceiverSeedEntry {
            topic: "peer_t".into(),
            addr: "127.0.0.1:4000".into(),
            client_name: "peer_name".into(),
        }])
        .unwrap();
        let addrs = db.get_addresses_of_topic(true, "peer_t").unwrap();
        assert_eq!(addrs, vec!["127.0.0.1:4000"]);
        assert_eq!(
            db.get_listener_unique_name("peer_t", "127.0.0.1:4000")
                .unwrap(),
            "peer_name"
        );
        assert_eq!(
            db.get_topic_key("peer_t").unwrap(),
            FIRST_ISOLATED_TOPIC_KEY
        );
        assert_eq!(
            db.get_sender_topic_by_connection_key(FIRST_ISOLATED_CONNECTION_KEY)
                .unwrap(),
            "peer_t"
        );
    }

    #[test]
    fn postgres_seed_receivers_peer_row_only_seeds_conn_sender_and_source_topic_key() {
        let url = require_pg_url!();
        let _lock = test_db_lock();
        let mut db = fresh_db("me", &url);
        db.set_source_topic("me");
        db.set_source_localhost("127.0.0.1:1");
        db.seed_receivers(&[ReceiverSeedEntry {
            topic: "peer_t".into(),
            addr: "127.0.0.1:2".into(),
            client_name: "p".into(),
        }])
        .unwrap();
        assert_eq!(
            db.get_sender_topic_by_connection_key(FIRST_ISOLATED_CONNECTION_KEY)
                .unwrap(),
            "peer_t"
        );
        assert_eq!(db.get_topic_key("me").unwrap(), FIRST_ISOLATED_TOPIC_KEY);
    }

    #[test]
    fn postgres_two_handles_same_url_both_resolve_seeded_conn_key() {
        let url = require_pg_url!();
        let _lock = test_db_lock();
        test_reset_tables_inner(&url);
        let entries = [ReceiverSeedEntry {
            topic: "topic_client2".into(),
            addr: "127.0.0.1:22782".into(),
            client_name: "client2".into(),
        }];
        let mut a = Postgres::new("client1", &url).unwrap();
        a.set_source_topic("topic_client1");
        a.set_source_localhost("127.0.0.1:22771");
        a.seed_receivers(&entries).unwrap();

        let mut b = Postgres::new("client1", &url).unwrap();
        b.set_source_topic("topic_client1");
        b.set_source_localhost("127.0.0.1:22771");

        assert_eq!(
            Store::get_connection_key_for_sender(&mut a, "client2").unwrap(),
            FIRST_ISOLATED_CONNECTION_KEY
        );
        assert_eq!(
            Store::get_connection_key_for_sender(&mut b, "client2").unwrap(),
            FIRST_ISOLATED_CONNECTION_KEY
        );
    }

    #[test]
    fn postgres_message_queue_drain_and_peek() {
        let url = require_pg_url!();
        let _lock = test_db_lock();
        let mut db = fresh_db("u", &url);
        db.set_source_topic("st");
        let ck = 42i32;
        let pool = Arc::new(Mutex::new(Mempool::new()));
        let m1 = Message::new(&pool, ck, 10, 1, b"a", true);
        let m2 = Message::new(&pool, ck, 10, 2, b"b", true);
        db.save_messages_from_sender(&pool, ck, vec![m1, m2]).unwrap();

        let peek = db
            .load_last_message_for_sender(&pool, ck)
            .unwrap()
            .unwrap();
        assert_eq!(peek.number_mess, 2);

        let loaded = db.load_messages_for_sender(&pool, ck).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].number_mess, 1);
        assert_eq!(loaded[1].number_mess, 2);

        let empty = db.load_messages_for_sender(&pool, ck).unwrap();
        assert!(empty.is_empty());
        assert!(db
            .load_last_message_for_sender(&pool, ck)
            .unwrap()
            .is_none());
    }

    #[test]
    fn postgres_open_store_box_dyn_store() {
        let url = require_pg_url!();
        let _lock = test_db_lock();
        test_reset_tables_inner(&url);
        let mut db = crate::store::open_store(
            "u",
            crate::store::StoreBackend::Postgres { url: url.clone() },
        )
        .unwrap();
        db.set_source_topic("t");
        db.set_source_localhost("127.0.0.1:1");
        db.regist_topic("t").unwrap();
        let addrs = db.get_addresses_of_topic(true, "t").unwrap();
        assert_eq!(addrs.len(), 1);
    }
}
