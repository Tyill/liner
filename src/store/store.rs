//! Abstraction over persistent storage so backends (Redis, SQLite, …) can be swapped.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::message::Message;
use crate::mempool::Mempool;

#[derive(Debug, Clone)]
pub struct DbError(String);

pub type DbResult<T> = Result<T, DbError>;

/// One catalog row for [`Store::seed_receivers`] (JSON shape matches the C `receivers_json` array).
///
/// **`topic`:** the peer’s **registered topic name** (their `source_topic` / argument to `send_to` toward
/// them). It is also the string delivered as **`from`** when that peer is the sender. You normally list
/// **only peers**; SQLite seeding still assigns your own wire **`topic_key`** for `source_topic` (see
/// `seed_receivers_sqlite`). Rows equal to your own `source_topic` are optional legacy and do not seed
/// `conn_sender`.
///
/// **Wire `topic_key`:** not part of JSON — SQLite seeding always writes **`topic_key.k = 1`** for every
/// catalog topic (isolated empty-DB pair model; see `docs/using-sqlite.md`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ReceiverSeedEntry {
    pub topic: String,
    pub addr: String,
    pub client_name: String,
}

impl DbError {
    pub fn new(msg: impl Into<String>) -> Self {
        DbError(msg.into())
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DbError {}

/// Operations the broker needs from a key–value / queue style store.
///
/// A Redis implementation exists today; a SQLite (or other) backend can implement the same contract.
pub trait Store: Send {
    fn set_source_topic(&mut self, topic: &str);
    fn set_source_localhost(&mut self, localhost: &str);

    fn regist_topic(&mut self, topic: &str) -> DbResult<()>;
    fn unregist_topic(&mut self, topic: &str) -> DbResult<()>;
    fn clear_addresses_of_topic(&mut self) -> DbResult<()>;
    fn clear_stored_messages(&mut self) -> DbResult<()>;

    fn save_listener_for_sender(&mut self, listener_addr: &str, listener_topic: &str) -> DbResult<()>;
    fn get_listeners_of_sender(&mut self) -> DbResult<Vec<(String, String)>>;
    fn get_addresses_of_topic(&mut self, without_cache: bool, topic: &str) -> DbResult<Vec<String>>;
    fn get_listener_unique_name(&mut self, topic: &str, address: &str) -> DbResult<String>;

    fn get_connection_key_for_sender(&mut self, listener_name: &str) -> DbResult<i32>;
    fn get_topic_key(&mut self, topic: &str) -> DbResult<i32>;

    fn set_sender_topic_by_connection_key_from_sender(&mut self, connection_key: i32) -> DbResult<()>;
    fn get_sender_topic_by_connection_key(&mut self, connection_key: i32) -> DbResult<String>;

    fn set_last_mess_number_from_listener(&mut self, connection_key: i32, val: u64) -> DbResult<()>;
    fn get_last_mess_number_for_listener(&mut self, connection_key: i32) -> DbResult<u64>;
    fn get_last_mess_number_for_sender(&mut self, connection_key: i32) -> DbResult<u64>;

    fn save_messages_from_sender(
        &mut self,
        mempool: &Arc<Mutex<Mempool>>,
        connection_key: i32,
        mess: Vec<Message>,
    ) -> DbResult<()>;

    fn load_messages_for_sender(
        &mut self,
        mempool: &Arc<Mutex<Mempool>>,
        connection_key: i32,
    ) -> DbResult<Vec<Message>>;

    fn load_last_message_for_sender(
        &mut self,
        mempool: &Arc<Mutex<Mempool>>,
        connection_key: i32,
    ) -> DbResult<Option<Message>>;

    /// Upsert remote listener catalog (SQLite). Empty slice is always `Ok(())`.
    /// **SQLite:** also seeds `topic_key` (wire key **1** per catalog topic), `conn_sender` for the first channel, and `topic_addr`; see `docs/using-sqlite.md`.
    /// **Redis:** no-op — deployments use a shared catalog, not `receivers_json` seeding.
    fn seed_receivers(&mut self, entries: &[ReceiverSeedEntry]) -> DbResult<()>;
}
