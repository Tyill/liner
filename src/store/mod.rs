//! Persistent storage: shared [`Store`](store::Store) trait and backend modules (Redis, SQLite).
//!
//! Use [`StoreBackend`] with [`open_store`] (single owner) or [`open_store_mutex`] (shared
//! `Arc<Mutex<…>>` for listener/sender threads). No URL prefix sniffing.

pub mod redis;
pub mod sqlite;
pub mod store;

use std::sync::{Arc, Mutex};

use redis::Redis;
use sqlite::Sqlite;
use store::{DbError, DbResult};

#[derive(Debug, Clone)]
pub enum StoreBackend {
    Redis { url: String },
    Sqlite { path: String },
}

pub fn open_store(unique_name: &str, backend: StoreBackend) -> DbResult<Box<dyn store::Store>> {
    match backend {
        StoreBackend::Redis { url } => {
            let c = Redis::new(unique_name, &url).map_err(|e| DbError::new(e.to_string()))?;
            Ok(Box::new(c))
        }
        StoreBackend::Sqlite { path } => {
            let s = Sqlite::new(unique_name, &path)?;
            Ok(Box::new(s))
        }
    }
}

/// Same backends as [`open_store`], wrapped for listener/sender threads.
pub fn open_store_mutex(
    unique_name: &str,
    backend: StoreBackend,
) -> DbResult<Arc<Mutex<dyn store::Store>>> {
    match backend {
        StoreBackend::Redis { url } => {
            let c = Redis::new(unique_name, &url).map_err(|e| DbError::new(e.to_string()))?;
            Ok(Arc::new(Mutex::new(c)))
        }
        StoreBackend::Sqlite { path } => {
            let s = Sqlite::new(unique_name, &path)?;
            Ok(Arc::new(Mutex::new(s)))
        }
    }
}

pub use store::{ReceiverSeedEntry, Store};
