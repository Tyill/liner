//! Persistent storage: shared [`Store`](store::Store) trait and backend modules (Redis, SQLite, …).
//!
//! Use [`StoreBackend`] with [`open_store`] (single owner) or [`open_store_mutex`] (shared
//! `Arc<Mutex<…>>` for listener/sender threads). No URL prefix sniffing.
//!
//! Redis and SQLite are on by default (`--no-default-features --features sqlite` / `redis` /
//! `postgres` to pick backends). PostgreSQL: Cargo feature **`postgres`**.

#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "sqlite")]
pub mod sqlite;
pub mod store;

#[cfg(feature = "postgres")]
pub mod postgres;

use std::sync::{Arc, Mutex};

#[cfg(feature = "redis")]
use redis::Redis;
#[cfg(feature = "sqlite")]
use sqlite::Sqlite;
#[cfg(feature = "redis")]
use store::{DbError, DbResult};
#[cfg(not(feature = "redis"))]
use store::DbResult;

#[cfg(feature = "postgres")]
use postgres::Postgres;

#[cfg(not(any(feature = "redis", feature = "sqlite", feature = "postgres")))]
compile_error!(
    "enable at least one store backend: default is redis+sqlite, or \
     --no-default-features --features sqlite|redis|postgres"
);

#[derive(Debug, Clone)]
pub enum StoreBackend {
    #[cfg(feature = "redis")]
    Redis { url: String },
    #[cfg(feature = "sqlite")]
    Sqlite { path: String },
    #[cfg(feature = "postgres")]
    Postgres { url: String },
}

pub fn open_store(unique_name: &str, backend: StoreBackend) -> DbResult<Box<dyn store::Store>> {
    match backend {
        #[cfg(feature = "redis")]
        StoreBackend::Redis { url } => {
            let c = Redis::new(unique_name, &url).map_err(|e| DbError::new(e.to_string()))?;
            Ok(Box::new(c))
        }
        #[cfg(feature = "sqlite")]
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

/// Same backends as [`open_store`], shared across Client / Listener / Sender threads.
pub fn open_store_mutex(
    unique_name: &str,
    backend: StoreBackend,
) -> DbResult<Arc<Mutex<dyn store::Store>>> {
    match backend {
        #[cfg(feature = "redis")]
        StoreBackend::Redis { url } => {
            let c = Redis::new(unique_name, &url).map_err(|e| DbError::new(e.to_string()))?;
            Ok(Arc::new(Mutex::new(c)))
        }
        #[cfg(feature = "sqlite")]
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
