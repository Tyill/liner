//! Persistent storage: shared [`Store`](store::Store) trait and backend modules (Redis, SQLite, …).
//!
//! Use [`StoreBackend`] with [`open_store`] (single owner) or [`open_store_mutex`] (shared
//! `Arc<Mutex<…>>` for listener/sender threads). No URL prefix sniffing.
//!
//! PostgreSQL: enable Cargo feature **`postgres`** and use [`StoreBackend::Postgres`].

pub mod redis;
pub mod sqlite;
pub mod store;

#[cfg(feature = "postgres")]
pub mod postgres;

use std::sync::{Arc, Mutex};

use redis::Redis;
use sqlite::Sqlite;
use store::{DbError, DbResult};

#[cfg(feature = "postgres")]
use postgres::Postgres;

#[derive(Debug, Clone)]
pub enum StoreBackend {
    Redis { url: String },
    Sqlite { path: String },
    #[cfg(feature = "postgres")]
    Postgres { url: String },
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
        #[cfg(feature = "postgres")]
        StoreBackend::Postgres { url } => {
            let p = Postgres::new(unique_name, &url)?;
            Ok(Box::new(p))
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
        #[cfg(feature = "postgres")]
        StoreBackend::Postgres { url } => {
            let p = Postgres::new(unique_name, &url)?;
            Ok(Arc::new(Mutex::new(p)))
        }
    }
}

pub use store::{ReceiverSeedEntry, Store};
