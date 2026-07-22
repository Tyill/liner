//! SQLite-backed [`Store`](super::store::Store) implementation (parity with Redis `Redis`).

use crate::{message::Message, mempool::Mempool, print_error};

use super::store::{DbError, DbResult, ReceiverSeedEntry, Store};
use rusqlite::{params, Connection, OptionalExtension};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

const BUSY_MS: i32 = 5000;

/// First `connection_key` on an **empty** SQLite file (isolated-DB pair model); not passed in `receivers_json`.
pub(crate) const FIRST_ISOLATED_CONNECTION_KEY: i32 = 1;

/// Wire `topic_key` (`topic_key` table column `k`) for every topic row seeded from `receivers_json`; not in JSON.
pub(crate) const FIRST_ISOLATED_TOPIC_KEY: i32 = 1;

fn map_sql<T>(r: rusqlite::Result<T>) -> DbResult<T> {
    r.map_err(|e| DbError::new(e.to_string()))
}

fn sender_key(unique_name: &str, source_topic: &str) -> String {
    format!("{}:{}", unique_name, source_topic)
}

fn connection_composite(unique_name: &str, source_topic: &str, listener_name: &str) -> String {
    format!("{}:{}:{}", unique_name, source_topic, listener_name)
}

fn cache_name_key(topic: &str, address: &str) -> String {
    format!("{topic}\x1f{address}")
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

pub struct Sqlite {
    unique_name: String,
    source_topic: String,
    source_localhost: String,
    conn: Connection,
    topic_addr_cache: HashMap<String, Vec<String>>,
    topic_key_cache: HashMap<String, i32>,
    unique_name_cache: HashMap<String, String>,
    last_mess_number: HashMap<i32, u64>,
}

impl Sqlite {
    pub fn new(unique_name: &str, path: &str) -> DbResult<Self> {
        let conn = map_sql(Connection::open(path))?;
        map_sql(conn.execute("PRAGMA foreign_keys = ON", []))?;
        map_sql(conn.pragma_update(None, "journal_mode", "WAL"))?;
        map_sql(conn.pragma_update(None, "busy_timeout", BUSY_MS))?;

        conn.execute_batch(
            r"
            CREATE TABLE IF NOT EXISTS seq (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                v INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO seq (id, v) VALUES (1, 0);

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
                client_name TEXT NOT NULL DEFAULT '',
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
                v INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS conn_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_key INTEGER NOT NULL,
                payload BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_conn_messages_ck
                ON conn_messages(connection_key, id);
            ",
        )
        .map_err(|e| DbError::new(e.to_string()))?;

        let _ = conn.execute(
            "ALTER TABLE sender_listener ADD COLUMN client_name TEXT NOT NULL DEFAULT ''",
            [],
        );

        Ok(Sqlite {
            unique_name: unique_name.to_string(),
            source_topic: String::new(),
            source_localhost: String::new(),
            conn,
            topic_addr_cache: HashMap::new(),
            topic_key_cache: HashMap::new(),
            unique_name_cache: HashMap::new(),
            last_mess_number: HashMap::new(),
        })
    }

    fn next_connection_id(&mut self) -> DbResult<i32> {
        let tx = map_sql(self.conn.transaction())?;
        map_sql(tx.execute("UPDATE seq SET v = v + 1 WHERE id = 1", []))?;
        let v: i32 = map_sql(tx.query_row("SELECT v FROM seq WHERE id = 1", [], |r| r.get(0)))?;
        map_sql(tx.commit())?;
        Ok(v)
    }

    fn init_addresses_of_topic(&mut self, topic: &str) -> DbResult<()> {
        let mut stmt =
            map_sql(self.conn.prepare(
                "SELECT addr, client_name FROM topic_addr WHERE topic = ?1 ORDER BY addr ASC",
            ))?;
        let rows = map_sql(stmt.query_map(params![topic], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        }))?;
        let mut addrs: Vec<String> = Vec::new();
        for row in rows {
            let (addr, name): (String, String) = map_sql(row)?;
            self.unique_name_cache.insert(cache_name_key(topic, &addr), name);
            addrs.push(addr);
        }
        self.topic_addr_cache.insert(topic.to_string(), addrs);
        Ok(())
    }

    fn init_last_mess_number_from_sender(&mut self, connection_key: i32) -> DbResult<()> {
        map_sql(self.conn.execute(
            "INSERT OR IGNORE INTO conn_mess_number (connection_key, v) VALUES (?1, 0)",
            params![connection_key],
        ))?;
        Ok(())
    }

    /// Keep monotonic `seq` so the next [`next_connection_id`](Self::next_connection_id) does not
    /// collide with manually inserted `connection_key` values (`conn_key_map`, `conn_sender`).
    ///
    /// **Do not** use `MAX(topic_key.k)` here: seeded wire topic keys are often **1** while
    /// `connection_key` for the first channel must also be **1**; bumping `seq` from topic keys would
    /// make the first `next_connection_id` return **2** and break the isolated pair contract.
    fn sync_seq_after_seed(&mut self) -> DbResult<()> {
        let max_ck: i32 = map_sql(
            self.conn.query_row(
                "SELECT COALESCE(MAX(connection_key), 0) FROM conn_key_map",
                [],
                |r| r.get(0),
            ),
        )?;
        let max_sender_ck: i32 = map_sql(
            self.conn.query_row(
                "SELECT COALESCE(MAX(connection_key), 0) FROM conn_sender",
                [],
                |r| r.get(0),
            ),
        )?;
        let cur: i32 = map_sql(
            self.conn
                .query_row("SELECT v FROM seq WHERE id = 1", [], |r| r.get(0)),
        )?;
        let m = cur.max(max_ck).max(max_sender_ck);
        if m > cur {
            map_sql(
                self.conn
                    .execute("UPDATE seq SET v = ?1 WHERE id = 1", params![m]),
            )?;
        }
        Ok(())
    }

    fn seed_receivers_sqlite(&mut self, entries: &[ReceiverSeedEntry]) -> DbResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let tx = map_sql(self.conn.transaction())?;
        for e in entries {
            map_sql(tx.execute(
                "INSERT OR REPLACE INTO topic_addr (topic, addr, client_name) VALUES (?1, ?2, ?3)",
                params![&e.topic, &e.addr, &e.client_name],
            ))?;
            map_sql(tx.execute(
                "INSERT OR REPLACE INTO topic_key (topic, k) VALUES (?1, ?2)",
                params![&e.topic, FIRST_ISOLATED_TOPIC_KEY],
            ))?;
            // Isolated empty DBs: first wire `connection_key` is 1; `conn_sender` / `conn_key_map` only
            // for peers (`topic` ≠ `source_topic`). Own `topic_key` is inserted after the loop.
            if e.topic != self.source_topic {
                let composite =
                    connection_composite(&self.unique_name, &self.source_topic, &e.client_name);
                map_sql(tx.execute(
                    "INSERT OR REPLACE INTO conn_key_map (composite, connection_key) VALUES (?1, ?2)",
                    params![composite, FIRST_ISOLATED_CONNECTION_KEY],
                ))?;
                map_sql(tx.execute(
                    "INSERT OR REPLACE INTO conn_sender (connection_key, sender_topic) VALUES (?1, ?2)",
                    params![FIRST_ISOLATED_CONNECTION_KEY, &e.topic],
                ))?;
            }
        }
        map_sql(tx.execute(
            "INSERT OR IGNORE INTO topic_key (topic, k) VALUES (?1, ?2)",
            params![&self.source_topic, FIRST_ISOLATED_TOPIC_KEY],
        ))?;
        map_sql(tx.commit())?;

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

impl Store for Sqlite {
    fn set_source_topic(&mut self, topic: &str) {
        self.source_topic = topic.to_string();
    }

    fn set_source_localhost(&mut self, localhost: &str) {
        self.source_localhost = localhost.to_string();
    }

    fn regist_topic(&mut self, topic: &str) -> DbResult<()> {
        let localhost = self.source_localhost.clone();
        let unique = self.unique_name.clone();
        map_sql(self.conn.execute(
            "INSERT OR REPLACE INTO topic_addr (topic, addr, client_name) VALUES (?1, ?2, ?3)",
            params![topic, localhost, unique],
        ))?;
        self.init_addresses_of_topic(topic)?;
        Ok(())
    }

    fn unregist_topic(&mut self, topic: &str) -> DbResult<()> {
        let localhost = self.source_localhost.clone();
        map_sql(self.conn.execute(
            "DELETE FROM topic_addr WHERE topic = ?1 AND addr = ?2",
            params![topic, localhost],
        ))?;
        self.init_addresses_of_topic(topic)?;
        Ok(())
    }

    fn clear_addresses_of_topic(&mut self) -> DbResult<()> {
        let topic = self.source_topic.clone();
        map_sql(
            self.conn
                .execute("DELETE FROM topic_addr WHERE topic = ?1", params![topic]),
        )?;
        Ok(())
    }

    fn clear_stored_messages(&mut self) -> DbResult<()> {
        let sk = sender_key(&self.unique_name, &self.source_topic);
        let listeners: Vec<(String, String)> = {
            let mut stmt = map_sql(self.conn.prepare(
                "SELECT addr, listener_topic FROM sender_listener WHERE sender_key = ?1",
            ))?;
            let rows = map_sql(stmt.query_map(params![sk.as_str()], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            }))?;
            let mut v = Vec::new();
            for row in rows {
                v.push(map_sql(row)?);
            }
            v
        };
        for (addr, listener_topic) in listeners {
            if let Ok(listener_name) = self.get_listener_unique_name(&listener_topic, &addr) {
                if let Ok(connection_key) = self.get_connection_key_for_sender(&listener_name) {
                    map_sql(self.conn.execute(
                        "DELETE FROM conn_messages WHERE connection_key = ?1",
                        params![connection_key],
                    ))?;
                    map_sql(self.conn.execute(
                        "DELETE FROM conn_mess_number WHERE connection_key = ?1",
                        params![connection_key],
                    ))?;
                }
            }
        }
        map_sql(
            self.conn
                .execute("DELETE FROM sender_listener WHERE sender_key = ?1", params![sk]),
        )?;
        Ok(())
    }

    fn save_listener_for_sender(
        &mut self,
        listener_addr: &str,
        listener_topic: &str,
        listener_name: &str,
    ) -> DbResult<()> {
        let sk = sender_key(&self.unique_name, &self.source_topic);
        map_sql(self.conn.execute(
            "INSERT OR REPLACE INTO sender_listener (sender_key, addr, listener_topic, client_name) VALUES (?1, ?2, ?3, ?4)",
            params![sk, listener_addr, listener_topic, listener_name],
        ))?;
        Ok(())
    }

    fn remove_sender_listeners_on_topic(&mut self, listener_topic: &str) -> DbResult<()> {
        let sk = sender_key(&self.unique_name, &self.source_topic);
        map_sql(self.conn.execute(
            "DELETE FROM sender_listener WHERE sender_key = ?1 AND listener_topic = ?2",
            params![sk, listener_topic],
        ))?;
        Ok(())
    }

    fn get_listeners_of_sender(&mut self) -> DbResult<Vec<(String, String)>> {
        let sk = sender_key(&self.unique_name, &self.source_topic);
        let mut stmt = map_sql(self.conn.prepare(
            "SELECT addr, listener_topic FROM sender_listener WHERE sender_key = ?1",
        ))?;
        let rows = map_sql(stmt.query_map(params![sk], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        }))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(map_sql(row)?);
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
        let ck = cache_name_key(topic, address);
        if let Some(name) = self.unique_name_cache.get(&ck) {
            return Ok(name.clone());
        }
        let sk = sender_key(&self.unique_name, &self.source_topic);
        if let Ok(name) = map_sql(
            self.conn.query_row(
                "SELECT client_name FROM sender_listener WHERE sender_key = ?1 AND addr = ?2 AND listener_topic = ?3",
                params![sk, address, topic],
                |r| r.get::<_, String>(0),
            ),
        ) {
            if !name.is_empty() {
                self.unique_name_cache.insert(ck, name.clone());
                return Ok(name);
            }
        }
        Err(DbError::new("!unique_name_cache.contains_key"))
    }

    fn get_connection_key_for_sender(&mut self, listener_name: &str) -> DbResult<i32> {
        let composite = connection_composite(&self.unique_name, &self.source_topic, listener_name);
        let existing: Option<i32> = map_sql(
            self.conn
                .query_row(
                    "SELECT connection_key FROM conn_key_map WHERE composite = ?1",
                    params![composite],
                    |r| r.get(0),
                )
                .optional(),
        )?;
        if let Some(k) = existing {
            return Ok(k);
        }
        let id = self.next_connection_id()?;
        map_sql(self.conn.execute(
            "INSERT INTO conn_key_map (composite, connection_key) VALUES (?1, ?2)",
            params![composite, id],
        ))?;
        Ok(id)
    }

    fn get_topic_key(&mut self, topic: &str) -> DbResult<i32> {
        if let Some(k) = self.topic_key_cache.get(topic) {
            return Ok(*k);
        }
        let existing: Option<i32> = map_sql(
            self.conn
                .query_row("SELECT k FROM topic_key WHERE topic = ?1", params![topic], |r| {
                    r.get(0)
                })
                .optional(),
        )?;
        if let Some(k) = existing {
            self.topic_key_cache.insert(topic.to_owned(), k);
            return Ok(k);
        }
        let id = self.next_connection_id()?;
        map_sql(self.conn.execute(
            "INSERT INTO topic_key (topic, k) VALUES (?1, ?2)",
            params![topic, id],
        ))?;
        self.topic_key_cache.insert(topic.to_owned(), id);
        Ok(id)
    }

    fn set_sender_topic_by_connection_key_from_sender(
        &mut self,
        connection_key: i32,
    ) -> DbResult<()> {
        let source_topic = self.source_topic.clone();
        map_sql(self.conn.execute(
            "INSERT OR REPLACE INTO conn_sender (connection_key, sender_topic) VALUES (?1, ?2)",
            params![connection_key, source_topic],
        ))?;
        Ok(())
    }

    fn get_sender_topic_by_connection_key(&mut self, connection_key: i32) -> DbResult<String> {
        map_sql(self.conn.query_row(
            "SELECT sender_topic FROM conn_sender WHERE connection_key = ?1",
            params![connection_key],
            |r| r.get(0),
        ))
    }

    fn set_last_mess_number_from_listener(&mut self, connection_key: i32, val: u64) -> DbResult<()> {
        let v = i64::try_from(val).map_err(|_| DbError::new("mess_number too large for i64"))?;
        map_sql(self.conn.execute(
            "INSERT OR REPLACE INTO conn_mess_number (connection_key, v) VALUES (?1, ?2)",
            params![connection_key, v],
        ))?;
        self.last_mess_number.insert(connection_key, val);
        Ok(())
    }

    fn get_last_mess_number_for_listener(&mut self, connection_key: i32) -> DbResult<u64> {
        if !self.last_mess_number.contains_key(&connection_key) {
            let res: Option<i64> = map_sql(
                self.conn
                    .query_row(
                        "SELECT v FROM conn_mess_number WHERE connection_key = ?1",
                        params![connection_key],
                        |r| r.get(0),
                    )
                    .optional(),
            )?;
            let value = match res {
                Some(v) => {
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
        let res: Option<i64> = map_sql(
            self.conn
                .query_row(
                    "SELECT v FROM conn_mess_number WHERE connection_key = ?1",
                    params![connection_key],
                    |r| r.get(0),
                )
                .optional(),
        )?;
        match res {
            Some(v) => Ok(u64::try_from(v).map_err(|_| DbError::new("invalid mess_number"))?),
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
        let tx = map_sql(self.conn.transaction())?;
        for buf in encoded {
            map_sql(tx.execute(
                "INSERT INTO conn_messages (connection_key, payload) VALUES (?1, ?2)",
                params![connection_key, buf],
            ))?;
        }
        map_sql(tx.commit())?;
        Ok(())
    }

    fn load_messages_for_sender(
        &mut self,
        mempool: &Arc<Mutex<Mempool>>,
        connection_key: i32,
    ) -> DbResult<Vec<Message>> {
        let pairs: Vec<(i64, Vec<u8>)> = {
            let mut stmt = map_sql(self.conn.prepare(
                "SELECT id, payload FROM conn_messages WHERE connection_key = ?1 ORDER BY id ASC",
            ))?;
            let rows = map_sql(stmt.query_map(params![connection_key], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
            }))?;
            let mut p = Vec::new();
            for row in rows {
                p.push(map_sql(row)?);
            }
            p
        };
        if pairs.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<i64> = pairs.iter().map(|(id, _)| *id).collect();
        let tx = map_sql(self.conn.transaction())?;
        for id in &ids {
            map_sql(tx.execute("DELETE FROM conn_messages WHERE id = ?1", params![id]))?;
        }
        map_sql(tx.commit())?;

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
        let row: Option<Vec<u8>> = map_sql(
            self.conn
                .query_row(
                    "SELECT payload FROM conn_messages WHERE connection_key = ?1 ORDER BY id DESC LIMIT 1",
                    params![connection_key],
                    |r| r.get(0),
                )
                .optional(),
        )?;
        let mut out = None;
        if let Some(b) = row {
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
        self.seed_receivers_sqlite(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{ReceiverSeedEntry, Store};

    #[test]
    fn sqlite_seed_receivers_empty_noop() {
        let mut db = Sqlite::new("u", ":memory:").unwrap();
        db.seed_receivers(&[]).unwrap();
    }

    #[test]
    fn sqlite_seed_receivers_populates_catalog() {
        let mut db = Sqlite::new("listener", ":memory:").unwrap();
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
    fn sqlite_seed_receivers_peer_row_only_seeds_conn_sender_and_source_topic_key() {
        let mut db = Sqlite::new("me", ":memory:").unwrap();
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
    fn sqlite_seed_receivers_upsert_same_keys() {
        let mut db = Sqlite::new("u", ":memory:").unwrap();
        db.set_source_topic("s");
        db.set_source_localhost("127.0.0.1:1");
        db.seed_receivers(&[ReceiverSeedEntry {
            topic: "t".into(),
            addr: "127.0.0.1:2".into(),
            client_name: "n1".into(),
        }])
        .unwrap();
        db.seed_receivers(&[ReceiverSeedEntry {
            topic: "t".into(),
            addr: "127.0.0.1:2".into(),
            client_name: "n2".into(),
        }])
        .unwrap();
        assert_eq!(db.get_listener_unique_name("t", "127.0.0.1:2").unwrap(), "n2");
        assert_eq!(db.get_topic_key("t").unwrap(), FIRST_ISOLATED_TOPIC_KEY);
        let addrs = db.get_addresses_of_topic(true, "t").unwrap();
        assert_eq!(addrs.len(), 1);
    }

    #[test]
    fn sqlite_two_open_handles_same_file_both_resolve_seeded_conn_key() {
        let path = std::env::temp_dir().join(format!(
            "liner_sqlite_two_conn_{}.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
        let ps = path.to_str().unwrap();
        let entries = [ReceiverSeedEntry {
            topic: "topic_client2".into(),
            addr: "127.0.0.1:22782".into(),
            client_name: "client2".into(),
        }];
        let mut a = Sqlite::new("client1", ps).unwrap();
        a.set_source_topic("topic_client1");
        a.set_source_localhost("127.0.0.1:22771");
        a.seed_receivers(&entries).unwrap();

        let mut b = Sqlite::new("client1", ps).unwrap();
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
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    #[test]
    fn sqlite_connection_key_stays_one_after_seed_clear_and_regist_like_bench() {
        const A1: &str = "127.0.0.1:22771";
        const A2: &str = "127.0.0.1:22782";
        let mut db = Sqlite::new("client1", ":memory:").unwrap();
        db.set_source_topic("topic_client1");
        db.set_source_localhost(A1);
        db.seed_receivers(&[ReceiverSeedEntry {
            topic: "topic_client2".into(),
            addr: A2.into(),
            client_name: "client2".into(),
        }])
        .unwrap();
        db.clear_stored_messages().unwrap();
        db.clear_addresses_of_topic().unwrap();
        db.regist_topic("topic_client1").unwrap();
        assert_eq!(
            db.get_connection_key_for_sender("client2").unwrap(),
            FIRST_ISOLATED_CONNECTION_KEY
        );
    }

    #[test]
    fn sqlite_seed_updates_seq_for_new_topic_keys() {
        let mut db = Sqlite::new("u", ":memory:").unwrap();
        db.set_source_topic("x");
        db.set_source_localhost("127.0.0.1:1");
        db.seed_receivers(&[ReceiverSeedEntry {
            topic: "seeded".into(),
            addr: "127.0.0.1:2".into(),
            client_name: "n".into(),
        }])
        .unwrap();
        let nk = db.get_topic_key("other").unwrap();
        assert!(nk > FIRST_ISOLATED_TOPIC_KEY, "expected new key above seeded, got {}", nk);
    }

    #[test]
    fn sqlite_topic_roundtrip_and_order() {
        let mut db = Sqlite::new("u1", ":memory:").unwrap();
        db.set_source_topic("t1");
        db.set_source_localhost("127.0.0.1:1");
        db.regist_topic("t1").unwrap();
        db.set_source_localhost("127.0.0.1:2");
        db.regist_topic("t1").unwrap();
        let addrs = db.get_addresses_of_topic(true, "t1").unwrap();
        assert_eq!(addrs, vec!["127.0.0.1:1", "127.0.0.1:2"]);
    }

    #[test]
    fn sqlite_seq_connection_and_topic_keys_distinct() {
        let mut db = Sqlite::new("u", ":memory:").unwrap();
        db.set_source_topic("st");
        db.set_source_localhost("l");
        let k1 = db.get_connection_key_for_sender("L1").unwrap();
        let k2 = db.get_topic_key("topicA").unwrap();
        assert_ne!(k1, k2);
        let k1b = db.get_connection_key_for_sender("L1").unwrap();
        assert_eq!(k1, k1b);
    }

    #[test]
    fn sqlite_message_queue_drain_and_peek() {
        let mut db = Sqlite::new("u", ":memory:").unwrap();
        db.set_source_topic("st");
        let ck = 42i32;
        let pool = Arc::new(Mutex::new(Mempool::new()));
        let m1 = Message::new(&pool, ck, 10, 1, b"a", true).unwrap();
        let m2 = Message::new(&pool, ck, 10, 2, b"b", true).unwrap();
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
}
