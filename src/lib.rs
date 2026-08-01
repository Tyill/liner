//! # liner_broker
//!
//! `liner_broker` is a simple and fast redis based message serverless broker.  
//! Data transfer via TCP.
//! 
//! # Examples
//!
//! ```no_run
//! use liner_broker::Liner;
//! 
//! fn  main() {
//! 
//!     let mut client1 = Liner::new("client1", "topic_client1", "localhost:2255", "redis://localhost/");
//!     let mut client2 = Liner::new("client2", "topic_client2", "localhost:2256", "redis://localhost/");
//!    
//!     client1.run(Box::new(|_to: &str, _from: &str, _data: &[u8]|{
//!         println!("receive_from {}", _from);
//!     }));
//!     client2.run(Box::new(|_to: &str, _from: &str, _data: &[u8]|{
//!         println!("receive_from {}", _from);
//!     }));
//!  
//!     let array = [0; 100];
//!     for _ in 0..10{
//!         client1.send_to("topic_client2", array.as_slice(), true);
//!         println!("send_to client2");       
//!     }
//! }
//! 
//! ```

mod store;
pub use store::{open_store, open_store_mutex, ReceiverSeedEntry, Store, StoreBackend};
pub use store::redis;

mod status;
pub use status::{
    StatusCbackIntern, StatusEmitter, StatusMsg, LNR_LISTENER_STORE_ERROR, LNR_PEER_CONNECTED,
    LNR_PEER_DISCONNECTED, LNR_PEER_SUBSCRIBED, LNR_PEER_UNSUBSCRIBED, LNR_SENDER_BUSY,
    LNR_SENDER_ROUTE_LOST, LNR_SENDER_SEND_ERROR, LNR_SENDER_STORE_ERROR,
};

mod error;
pub use error::ErrorCode;

/// Crate version string (same as `lnr_version` / `CARGO_PKG_VERSION`).
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

mod log;
pub use log::set_log_cb;

mod client;
pub use client::Client;
mod message;
mod mempool;
mod bytestream;
mod listener;
mod sender;
mod settings;
mod common;

use std::collections::HashSet;
use std::ffi::CStr;
use std::ffi::CString;
use std::sync::{Mutex, OnceLock};

type UCback = Box<dyn FnMut(&str, &str, &[u8])>;
type StatusUCback = Box<dyn FnMut(i32, &str, &str, &str)>;

fn live_clients() -> &'static Mutex<HashSet<usize>> {
    static LIVE_CLIENTS: OnceLock<Mutex<HashSet<usize>>> = OnceLock::new();
    LIVE_CLIENTS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn register_live_client(ptr: *mut Client) {
    if ptr.is_null() {
        return;
    }
    if let Ok(mut live) = live_clients().lock() {
        live.insert(ptr as usize);
    }
}

fn take_live_client(ptr: *mut Client) -> bool {
    if ptr.is_null() {
        return false;
    }
    live_clients()
        .lock()
        .map(|mut live| live.remove(&(ptr as usize)))
        .unwrap_or(false)
}

fn cstring_or_empty(s: &str) -> CString {
    CString::new(s).unwrap_or_else(|_| CString::new("").unwrap_or_default())
}

extern "C" fn cb_(to: *const i8, from: *const i8,  data: *const u8, dsize: usize, udata: *mut libc::c_void){
    unsafe {    
        if let Some(liner) = udata.cast::<Liner>().as_mut(){
            if let Some(ucback) = liner.ucback.as_mut(){
                let Ok(to) = CStr::from_ptr(to).to_str() else { return; };
                let Ok(from) = CStr::from_ptr(from).to_str() else { return; };
                (ucback)(to, from, std::slice::from_raw_parts(data, dsize));
            }
        }
    }
}

extern "C" fn status_cb_(
    kind: i32,
    topic: *const i8,
    peer: *const i8,
    message: *const i8,
    udata: *mut libc::c_void,
) {
    unsafe {
        if let Some(liner) = udata.cast::<Liner>().as_mut() {
            if let Some(cb) = liner.status_ucback.as_mut() {
                let topic = if topic.is_null() {
                    ""
                } else {
                    CStr::from_ptr(topic).to_str().unwrap_or("")
                };
                let peer = if peer.is_null() {
                    ""
                } else {
                    CStr::from_ptr(peer).to_str().unwrap_or("")
                };
                let message = if message.is_null() {
                    ""
                } else {
                    CStr::from_ptr(message).to_str().unwrap_or("")
                };
                (cb)(kind, topic, peer, message);
            }
        }
    }
}

pub struct Liner{
    hclient: *mut Client,
    ucback: Option<UCback>,
    status_ucback: Option<StatusUCback>,
}

impl Liner {
    /// Creates a client backed by **Redis** (`redis_path` is a Redis URL, e.g. `redis://127.0.0.1/`).
    pub fn new(unique_name: &str, topic: &str, localhost: &str, redis_path: &str) -> Liner {
        unsafe {
            let unique = cstring_or_empty(unique_name);
            let dbpath = cstring_or_empty(redis_path);
            let localhost = cstring_or_empty(localhost);
            let topic_client = cstring_or_empty(topic);
            let hclient = lnr_new_client_redis(
                unique.as_ptr(),
                topic_client.as_ptr(),
                localhost.as_ptr(),
                dbpath.as_ptr(),
            );
            Self::from_raw_handle(hclient)
        }
    }

    /// Creates a client backed by **SQLite** (`sqlite_path` is the database file path).
    /// Use empty `receivers_json` (`""` / `[]`) when sharing one DB file so the catalog and
    /// `conn_sender` come from the store; with isolated empty files, pass JSON per `docs/using-sqlite.md`.
    pub fn new_sqlite(
        unique_name: &str,
        topic: &str,
        localhost: &str,
        sqlite_path: &str,
        receivers_json: &str,
    ) -> Liner {
        unsafe {
            let unique = cstring_or_empty(unique_name);
            let path = cstring_or_empty(sqlite_path);
            let localhost = cstring_or_empty(localhost);
            let topic_c = cstring_or_empty(topic);
            let recv = cstring_or_empty(receivers_json);
            let hclient = lnr_new_client_sqlite(
                unique.as_ptr(),
                topic_c.as_ptr(),
                localhost.as_ptr(),
                path.as_ptr(),
                recv.as_ptr(),
            );
            Self::from_raw_handle(hclient)
        }
    }

    /// Creates a client backed by **PostgreSQL** (requires library built with feature **`postgres`**).
    #[cfg(feature = "postgres")]
    pub fn new_postgres(
        unique_name: &str,
        topic: &str,
        localhost: &str,
        postgres_url: &str,
    ) -> Liner {
        unsafe {
            let unique = cstring_or_empty(unique_name);
            let url = cstring_or_empty(postgres_url);
            let localhost = cstring_or_empty(localhost);
            let topic_c = cstring_or_empty(topic);
            let hclient = lnr_new_client_postgres(
                unique.as_ptr(),
                topic_c.as_ptr(),
                localhost.as_ptr(),
                url.as_ptr(),
            );
            Self::from_raw_handle(hclient)
        }
    }

    fn from_raw_handle(hclient: *mut Client) -> Self {
        if hclient.is_null() {
            panic!("error create client");
        }
        Self {
            hclient,
            ucback: None,
            status_ucback: None,
        }
    }

    /// Register a status / background-error callback. Pass `None` to clear.
    /// Peer events are filtered to topics this client has sent to, subscribed to, or refreshed.
    pub fn set_status_callback(&mut self, cb: Option<Box<dyn FnMut(i32, &str, &str, &str)>>) {
        unsafe {
            self.status_ucback = cb;
            if self.status_ucback.is_some() {
                let ud = self as *const Self as *mut libc::c_void;
                lnr_set_status_cb(self.hclient, Some(status_cb_), ud);
            } else {
                lnr_set_status_cb(self.hclient, None, std::ptr::null_mut());
            }
        }
    }

    pub fn last_error_code(&self) -> i32 {
        unsafe { lnr_last_error_code(self.hclient) }
    }

    pub fn last_error_message(&self) -> String {
        unsafe {
            let p = lnr_last_error_message(self.hclient);
            if p.is_null() {
                return String::new();
            }
            CStr::from_ptr(p).to_string_lossy().into_owned()
        }
    }

    pub fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    pub fn set_log_callback(cb: Option<extern "C" fn(*const i8, *mut libc::c_void)>, udata: *mut libc::c_void) {
        unsafe {
            lnr_set_log_cb(cb, udata);
        }
    }

    pub fn set_max_message_size(bytes: usize) -> bool {
        unsafe { lnr_set_max_message_size(bytes) }
    }

    pub fn max_message_size() -> usize {
        unsafe { lnr_get_max_message_size() }
    }

    pub fn set_compress_threshold(bytes: usize) -> bool {
        unsafe { lnr_set_compress_threshold(bytes) }
    }

    pub fn compress_threshold() -> usize {
        unsafe { lnr_get_compress_threshold() }
    }

    pub fn set_max_send_queue(n: usize) -> bool {
        unsafe { lnr_set_max_send_queue(n) }
    }

    pub fn max_send_queue() -> usize {
        unsafe { lnr_get_max_send_queue() }
    }

    pub fn set_stream_check_timeout_ms(ms: u64) -> bool {
        unsafe { lnr_set_stream_check_timeout_ms(ms) }
    }

    pub fn stream_check_timeout_ms() -> u64 {
        unsafe { lnr_get_stream_check_timeout_ms() }
    }

    pub fn set_would_block_timeout_ms(ms: u64) -> bool {
        unsafe { lnr_set_would_block_timeout_ms(ms) }
    }

    pub fn would_block_timeout_ms() -> u64 {
        unsafe { lnr_get_would_block_timeout_ms() }
    }

    pub fn list_addresses(&mut self, topic: &str) -> Option<Vec<(String, String)>> {
        unsafe { (*self.hclient).list_addresses(topic) }
    }

    pub fn pending_count(&mut self) -> Option<u64> {
        unsafe { (*self.hclient).pending_count() }
    }

    pub fn pending_by_peer(&mut self) -> Option<Vec<(String, String, String, u64)>> {
        unsafe { (*self.hclient).pending_by_peer() }
    }

    pub fn send_queue_depth(&self) -> u64 {
        unsafe { (*self.hclient).send_queue_depth() }
    }

    pub fn send_queue_depth_by_peer(&self) -> Vec<(String, u64)> {
        unsafe { (*self.hclient).send_queue_depth_by_peer() }
    }

    pub fn list_subscriptions(&self) -> Vec<String> {
        unsafe { (*self.hclient).list_subscriptions() }
    }

    pub fn list_related_topics(&self) -> Vec<String> {
        unsafe { (*self.hclient).list_related_topics() }
    }

    /// Address published to the store instead of the bind string. `None` clears.
    pub fn set_advertise_addr(&mut self, addr: Option<&str>) -> bool {
        unsafe {
            match addr {
                None => lnr_set_advertise_addr(self.hclient, std::ptr::null()),
                Some(s) => {
                    let c = cstring_or_empty(s);
                    lnr_set_advertise_addr(self.hclient, c.as_ptr())
                }
            }
        }
    }

    pub fn stop(&mut self) -> bool {
        unsafe { lnr_stop(self.hclient) }
    }

    pub fn is_running(&self) -> bool {
        unsafe { lnr_is_running(self.hclient) }
    }

    pub fn run(&mut self, ucback: UCback)->bool{        
        unsafe{
            self.ucback = Some(ucback);
            let ud = self as *const Self as *mut libc::c_void;
            lnr_run(self.hclient, cb_, ud)
        }
    }
    /// Send to a single peer subscribed on `topic`. `at_least_once_delivery` matches C `lnr_send_to`
    /// (persist / retry semantics; use `false` when peers use different SQLite files — see `docs/using-sqlite.md`).
    pub fn send_to(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        unsafe {
            let topic = cstring_or_empty(topic);
            lnr_send_to(
                self.hclient,
                topic.as_ptr(),
                data.as_ptr(),
                data.len(),
                at_least_once_delivery,
            )
        }
    }
    /// Broadcast to all peers on `topic`. Same `at_least_once_delivery` semantics as [`Liner::send_to`].
    pub fn send_all(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        unsafe {
            let topic = cstring_or_empty(topic);
            lnr_send_all(
                self.hclient,
                topic.as_ptr(),
                data.as_ptr(),
                data.len(),
                at_least_once_delivery,
            )
        }
    }
    pub fn subscribe(&mut self, topic: &str)->bool{
        unsafe{
            let topic = cstring_or_empty(topic);
            lnr_subscribe(self.hclient, topic.as_ptr())
        }
    }
    pub fn unsubscribe(&mut self, topic: &str)->bool{
        unsafe{
            let topic = cstring_or_empty(topic);
            lnr_unsubscribe(self.hclient, topic.as_ptr())
        }
    }
    pub fn refresh_address_topic(&mut self, topic: &str)->bool{
        unsafe{
            let topic = cstring_or_empty(topic);
            lnr_refresh_address_topic(self.hclient, topic.as_ptr())
        }
    }
    pub fn clear_stored_messages(&mut self)->bool{
        unsafe{
            lnr_clear_stored_messages(self.hclient)
        }
    }
    pub fn clear_addresses_of_topic(&mut self)->bool{
        unsafe{
            lnr_clear_addresses_of_topic(self.hclient)
        }
    }

    /// After a successful [`Liner::run`], the resolved bind address (e.g. when `localhost` used port `0`).
    /// Kept after [`Liner::stop`].
    pub fn bound_listen_addr(&self) -> Option<String> {
        unsafe { (*self.hclient).bound_listen_addr().map(|s| s.to_string()) }
    }

    /// Catalog address while registered; `None` after stop.
    pub fn published_addr(&self) -> Option<String> {
        unsafe { (*self.hclient).published_addr().map(|s| s.to_string()) }
    }

    pub fn unique_name(&self) -> String {
        unsafe { (*self.hclient).unique_name().to_string() }
    }

    pub fn topic(&self) -> String {
        unsafe { (*self.hclient).topic().to_string() }
    }

    pub fn bind_addr(&self) -> String {
        unsafe { (*self.hclient).bind_addr().to_string() }
    }

    pub fn advertise_addr(&self) -> Option<String> {
        unsafe { (*self.hclient).advertise_addr().map(|s| s.to_string()) }
    }
}

impl Drop for Liner {
    fn drop(&mut self) {
        unsafe {
            lnr_delete_client(self.hclient);
        }
    }
}

unsafe fn new_client_inner(
    unique_name: *const i8,
    topic: *const i8,
    localhost: *const i8,
    store_path: *const i8,
    receivers_json: *const i8,
    sqlite: bool,
) -> *mut Client {
    if unique_name.is_null() || topic.is_null() || localhost.is_null() || store_path.is_null() {
        print_error!("null pointer argument");
        return std::ptr::null_mut();
    }
    let Ok(unique_name) = CStr::from_ptr(unique_name).to_str() else { return std::ptr::null_mut(); };
    let Ok(topic) = CStr::from_ptr(topic).to_str() else { return std::ptr::null_mut(); };
    let Ok(localhost) = CStr::from_ptr(localhost).to_str() else { return std::ptr::null_mut(); };
    let Ok(store_path) = CStr::from_ptr(store_path).to_str() else { return std::ptr::null_mut(); };

    if unique_name.is_empty() {
        print_error!("unique_name empty");
        return std::ptr::null_mut();
    }
    if topic.is_empty() {
        print_error!("topic empty");
        return std::ptr::null_mut();
    }
    if localhost.is_empty() {
        print_error!("localhost empty");
        return std::ptr::null_mut();
    }
    if store_path.is_empty() {
        print_error!("store_path empty");
        return std::ptr::null_mut();
    }
    let receivers_ref: &str = if sqlite {
        if receivers_json.is_null() {
            ""
        } else {
            match CStr::from_ptr(receivers_json).to_str() {
                Ok(s) => s,
                Err(_) => {
                    print_error!("receivers_json invalid UTF-8");
                    return std::ptr::null_mut();
                }
            }
        }
    } else {
        ""
    };

    let client_opt = if sqlite {
        Client::new_sqlite(unique_name, topic, localhost, store_path, receivers_ref)
    } else {
        Client::new_redis(unique_name, topic, localhost, store_path)
    };
    if let Some(c) = client_opt {
        let ptr = Box::into_raw(Box::new(c));
        register_live_client(ptr);
        ptr
    } else {
        std::ptr::null_mut()
    }
}

/// Create new client (Redis URL).
///
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_new_client_redis(
    unique_name: *const i8,
    topic: *const i8,
    localhost: *const i8,
    redis_url: *const i8,
) -> *mut Client {
    new_client_inner(unique_name, topic, localhost, redis_url, std::ptr::null(), false)
}

/// Create new client (SQLite database file path).
///
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_new_client_sqlite(
    unique_name: *const i8,
    topic: *const i8,
    localhost: *const i8,
    sqlite_path: *const i8,
    receivers_json: *const i8,
) -> *mut Client {
    new_client_inner(unique_name, topic, localhost, sqlite_path, receivers_json, true)
}

/// Build marker so Python can detect a postgres-enabled `cdylib` (kept by `lnr_new_client`).
#[cfg(feature = "postgres")]
#[no_mangle]
pub extern "C" fn lnr_postgres_enabled() -> u8 {
    1
}

/// Create new client backed by PostgreSQL (requires build with feature **`postgres`**).
///
/// # Safety
#[cfg(feature = "postgres")]
#[no_mangle]
pub unsafe extern "C" fn lnr_new_client_postgres(
    unique_name: *const i8,
    topic: *const i8,
    localhost: *const i8,
    postgres_url: *const i8,
) -> *mut Client {
    if unique_name.is_null() || topic.is_null() || localhost.is_null() || postgres_url.is_null() {
        print_error!("null pointer argument");
        return std::ptr::null_mut();
    }
    let Ok(unique_name) = CStr::from_ptr(unique_name).to_str() else { return std::ptr::null_mut(); };
    let Ok(topic) = CStr::from_ptr(topic).to_str() else { return std::ptr::null_mut(); };
    let Ok(localhost) = CStr::from_ptr(localhost).to_str() else { return std::ptr::null_mut(); };
    let Ok(postgres_url) = CStr::from_ptr(postgres_url).to_str() else { return std::ptr::null_mut(); };

    if unique_name.is_empty() {
        print_error!("unique_name empty");
        return std::ptr::null_mut();
    }
    if topic.is_empty() {
        print_error!("topic empty");
        return std::ptr::null_mut();
    }
    if localhost.is_empty() {
        print_error!("localhost empty");
        return std::ptr::null_mut();
    }
    if postgres_url.is_empty() {
        print_error!("postgres_url empty");
        return std::ptr::null_mut();
    }

    if let Some(c) = Client::new_postgres(unique_name, topic, localhost, postgres_url) {
        let ptr = Box::into_raw(Box::new(c));
        register_live_client(ptr);
        ptr
    } else {
        std::ptr::null_mut()
    }
}

/// Deprecated: use `lnr_new_client_redis`. Same behavior as `lnr_new_client_redis`.
///
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_new_client(
    unique_name: *const i8,
    topic: *const i8,
    localhost: *const i8,
    redis_path: *const i8,
) -> *mut Client {
    // Keep additive C symbols in the cdylib export table (linker GC otherwise drops them).
    std::hint::black_box(lnr_new_client_redis);
    std::hint::black_box(lnr_new_client_sqlite);
    std::hint::black_box(lnr_set_status_cb);
    std::hint::black_box(lnr_set_log_cb);
    std::hint::black_box(lnr_list_addresses);
    std::hint::black_box(lnr_pending_count);
    std::hint::black_box(lnr_pending_by_peer);
    std::hint::black_box(lnr_send_queue_depth);
    std::hint::black_box(lnr_send_queue_depth_by_peer);
    std::hint::black_box(lnr_list_subscriptions);
    std::hint::black_box(lnr_list_related_topics);
    std::hint::black_box(lnr_set_max_message_size);
    std::hint::black_box(lnr_get_max_message_size);
    std::hint::black_box(lnr_set_compress_threshold);
    std::hint::black_box(lnr_get_compress_threshold);
    std::hint::black_box(lnr_set_max_send_queue);
    std::hint::black_box(lnr_get_max_send_queue);
    std::hint::black_box(lnr_set_stream_check_timeout_ms);
    std::hint::black_box(lnr_get_stream_check_timeout_ms);
    std::hint::black_box(lnr_set_would_block_timeout_ms);
    std::hint::black_box(lnr_get_would_block_timeout_ms);
    std::hint::black_box(lnr_last_error_code);
    std::hint::black_box(lnr_last_error_message);
    std::hint::black_box(lnr_version);
    std::hint::black_box(lnr_set_advertise_addr);
    std::hint::black_box(lnr_stop);
    std::hint::black_box(lnr_is_running);
    std::hint::black_box(lnr_unique_name);
    std::hint::black_box(lnr_topic);
    std::hint::black_box(lnr_bind_addr);
    std::hint::black_box(lnr_advertise_addr);
    std::hint::black_box(lnr_bound_listen_addr);
    std::hint::black_box(lnr_published_addr);
    #[cfg(feature = "postgres")]
    {
        std::hint::black_box(lnr_new_client_postgres);
        std::hint::black_box(lnr_postgres_enabled);
    }
    lnr_new_client_redis(unique_name, topic, localhost, redis_path)
}

pub struct UData(*mut libc::c_void);
type UCbackIntern = extern "C" fn(to: *const i8, from: *const i8, data: *const u8, dsize: usize, udata: *mut libc::c_void);

unsafe impl Send for UData {}

impl UData {
    pub fn null() -> Self {
        UData(std::ptr::null_mut())
    }
}

/// Set or clear the status / background-error callback.
///
/// Peer events (`LNR_PEER_*`) are delivered only for topics this client has previously
/// sent to, subscribed to, or refreshed via `lnr_refresh_address_topic`.
/// Local `LNR_SENDER_ROUTE_LOST` / `LNR_SENDER_SEND_ERROR` / `LNR_SENDER_STORE_ERROR` and
/// `LNR_LISTENER_STORE_ERROR` are not filtered that way.
///
/// Pass `cb == NULL` to clear. Safe before or after `lnr_run`.
///
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_set_status_cb(
    client: *mut Client,
    cb: Option<StatusCbackIntern>,
    udata: *mut libc::c_void,
) -> bool {
    if !has_client(client) {
        return false;
    }
    (*client).set_status_cb(cb, UData(udata));
    true
}

pub type LogCbackC = Option<extern "C" fn(message: *const i8, udata: *mut libc::c_void)>;
pub type AddrCbackC =
    Option<extern "C" fn(addr: *const i8, unique_name: *const i8, udata: *mut libc::c_void)>;

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_set_log_cb(cb: LogCbackC, udata: *mut libc::c_void) -> bool {
    set_log_cb(cb, UData(udata));
    true
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_list_addresses(
    client: *mut Client,
    topic: *const i8,
    cb: AddrCbackC,
    udata: *mut libc::c_void,
) -> bool {
    if !has_client(client) {
        return false;
    }
    if topic.is_null() {
        print_error!("null pointer argument");
        return false;
    }
    let Ok(topic) = CStr::from_ptr(topic).to_str() else {
        return false;
    };
    let Some(rows) = (*client).list_addresses(topic) else {
        return false;
    };
    if let Some(cb) = cb {
        for (addr, name) in rows {
            let Ok(a) = CString::new(addr) else { continue };
            let Ok(n) = CString::new(name) else { continue };
            cb(a.as_ptr(), n.as_ptr(), udata);
        }
    }
    true
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_pending_count(client: *mut Client) -> i64 {
    if !has_client(client) {
        return -1;
    }
    match (*client).pending_count() {
        Some(n) => i64::try_from(n).unwrap_or(i64::MAX),
        None => -1,
    }
}

pub type PendingCbackC = Option<
    extern "C" fn(
        addr: *const i8,
        topic: *const i8,
        unique_name: *const i8,
        count: i64,
        udata: *mut libc::c_void,
    ),
>;
pub type TopicCbackC = Option<extern "C" fn(topic: *const i8, udata: *mut libc::c_void)>;

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_pending_by_peer(
    client: *mut Client,
    cb: PendingCbackC,
    udata: *mut libc::c_void,
) -> bool {
    if !has_client(client) {
        return false;
    }
    let Some(rows) = (*client).pending_by_peer() else {
        return false;
    };
    if let Some(cb) = cb {
        for (addr, topic, name, count) in rows {
            let Ok(a) = CString::new(addr) else { continue };
            let Ok(t) = CString::new(topic) else { continue };
            let Ok(n) = CString::new(name) else { continue };
            cb(
                a.as_ptr(),
                t.as_ptr(),
                n.as_ptr(),
                i64::try_from(count).unwrap_or(i64::MAX),
                udata,
            );
        }
    }
    true
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_send_queue_depth(client: *mut Client) -> i64 {
    if !has_client(client) {
        return 0;
    }
    i64::try_from((*client).send_queue_depth()).unwrap_or(i64::MAX)
}

pub type QueueCbackC =
    Option<extern "C" fn(addr: *const i8, count: i64, udata: *mut libc::c_void)>;

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_send_queue_depth_by_peer(
    client: *mut Client,
    cb: QueueCbackC,
    udata: *mut libc::c_void,
) -> bool {
    if !has_client(client) {
        return false;
    }
    let rows = (*client).send_queue_depth_by_peer();
    if let Some(cb) = cb {
        for (addr, count) in rows {
            let Ok(a) = CString::new(addr) else { continue };
            cb(
                a.as_ptr(),
                i64::try_from(count).unwrap_or(i64::MAX),
                udata,
            );
        }
    }
    true
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_list_subscriptions(
    client: *mut Client,
    cb: TopicCbackC,
    udata: *mut libc::c_void,
) -> bool {
    if !has_client(client) {
        return false;
    }
    let topics = (*client).list_subscriptions();
    if let Some(cb) = cb {
        for topic in topics {
            let Ok(t) = CString::new(topic) else { continue };
            cb(t.as_ptr(), udata);
        }
    }
    true
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_list_related_topics(
    client: *mut Client,
    cb: TopicCbackC,
    udata: *mut libc::c_void,
) -> bool {
    if !has_client(client) {
        return false;
    }
    let topics = (*client).list_related_topics();
    if let Some(cb) = cb {
        for topic in topics {
            let Ok(t) = CString::new(topic) else { continue };
            cb(t.as_ptr(), udata);
        }
    }
    true
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_set_max_message_size(bytes: usize) -> bool {
    settings::set_max_message_size(bytes)
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_get_max_message_size() -> usize {
    settings::max_message_size()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_set_compress_threshold(bytes: usize) -> bool {
    settings::set_compress_threshold(bytes)
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_get_compress_threshold() -> usize {
    settings::compress_threshold()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_set_max_send_queue(n: usize) -> bool {
    settings::set_max_send_queue(n)
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_get_max_send_queue() -> usize {
    settings::max_send_queue()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_set_stream_check_timeout_ms(ms: u64) -> bool {
    settings::set_stream_check_timeout_ms(ms)
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_get_stream_check_timeout_ms() -> u64 {
    settings::stream_check_timeout_ms()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_set_would_block_timeout_ms(ms: u64) -> bool {
    settings::set_would_block_timeout_ms(ms)
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_get_would_block_timeout_ms() -> u64 {
    settings::would_block_timeout_ms()
}

/// Last sync-API error code (`LNR_OK` / `LNR_ERR_*`). Returns `LNR_OK` for a null handle.
///
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_last_error_code(client: *mut Client) -> i32 {
    if !has_client(client) {
        return ErrorCode::Ok.as_i32();
    }
    (*client).last_error().as_i32()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_last_error_message(client: *mut Client) -> *const i8 {
    if !has_client(client) {
        return std::ptr::null();
    }
    (*client).last_error_message_c_str()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_version() -> *const i8 {
    static VER: OnceLock<CString> = OnceLock::new();
    VER.get_or_init(|| CString::new(env!("CARGO_PKG_VERSION")).unwrap_or_default())
        .as_ptr()
}

/// Set advertise address before `lnr_run`. `addr == NULL` or empty clears.
///
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_set_advertise_addr(client: *mut Client, addr: *const i8) -> bool {
    if !has_client(client) {
        return false;
    }
    if addr.is_null() {
        return (*client).set_advertise_addr(None);
    }
    let Ok(addr) = CStr::from_ptr(addr).to_str() else {
        return false;
    };
    (*client).set_advertise_addr(Some(addr))
}

/// Stop the client (unregister + join threads). Idempotent.
///
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_stop(client: *mut Client) -> bool {
    if !has_client(client) {
        return false;
    }
    (*client).stop()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_is_running(client: *mut Client) -> bool {
    if !has_client(client) {
        return false;
    }
    (*client).is_running()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_unique_name(client: *mut Client) -> *const i8 {
    if !has_client(client) {
        return std::ptr::null();
    }
    (*client).unique_name_c_str()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_topic(client: *mut Client) -> *const i8 {
    if !has_client(client) {
        return std::ptr::null();
    }
    (*client).topic_c_str()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_bind_addr(client: *mut Client) -> *const i8 {
    if !has_client(client) {
        return std::ptr::null();
    }
    (*client).bind_addr_c_str()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_advertise_addr(client: *mut Client) -> *const i8 {
    if !has_client(client) {
        return std::ptr::null();
    }
    (*client).advertise_addr_c_str()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_bound_listen_addr(client: *mut Client) -> *const i8 {
    if !has_client(client) {
        return std::ptr::null();
    }
    (*client).bound_listen_addr_c_str()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_published_addr(client: *mut Client) -> *const i8 {
    if !has_client(client) {
        return std::ptr::null();
    }
    (*client).published_addr_c_str()
}

/// Launching a client to send messages and listen for incoming messages. 
/// 
/// Possible errors when launching a client:
/// - no connection to redis
/// - the address for the client is busy
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_run(client: *mut Client, receive_cb: UCbackIntern, udata: *mut libc::c_void)->bool{
    if !has_client(client){
        return false;
    }
    let udata: UData = UData(udata);
    (*client).run(receive_cb, udata)
}

/// Send message to other client.
/// Call only when the client is already running. 
/// 
/// Possible errors:
/// - no connection to redis
/// - no other client with this topic
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_send_to(client: *mut Client,
                          topic: *const i8,
                          data: *const u8, data_size: usize,
                          at_least_once_delivery: bool)->bool{
    if !has_client(client){
        return false;
    }
    if topic.is_null() || (data_size > 0 && data.is_null()) {
        print_error!("null pointer argument");
        return false;
    }
    let Ok(topic) = CStr::from_ptr(topic).to_str() else { return false; };
    if topic.is_empty(){
        print_error!("topic name empty");
        return false;
    }
    // Empty payload is rejected inside Client (`LNR_ERR_INVALID_ARG`); allow null `data` when size is 0.
    let data = if data_size == 0 {
        &[][..]
    } else {
        std::slice::from_raw_parts(data, data_size)
    };
    (*client).send_to(topic, data, at_least_once_delivery)
}

/// Send message to other clients. 
/// Call only when the client is already running.
/// 
/// Possible errors:
/// - no connection to redis
/// - no other client with this topic
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_send_all(client: *mut Client,
                          topic: *const i8,
                          data: *const u8, data_size: usize,
                          at_least_once_delivery: bool)->bool{
    if !has_client(client){
        return false;
    }
    if topic.is_null() || (data_size > 0 && data.is_null()) {
        print_error!("null pointer argument");
        return false;
    }
    let Ok(topic) = CStr::from_ptr(topic).to_str() else { return false; };
    if topic.is_empty(){
        print_error!("topic.is_empty()");
        return false;
    }
    let data = if data_size == 0 {
        &[][..]
    } else {
        std::slice::from_raw_parts(data, data_size)
    };
    (*client).send_all(topic, data, at_least_once_delivery)
}

/// Subscribe to the topic and receive messages from other clients.
/// 
/// Possible errors:
/// - no connection to redis
/// - no other client with this topic
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_subscribe(client: *mut Client,
                          topic: *const i8)->bool{
    if !has_client(client){
        return false;
    }
    if topic.is_null() {
        print_error!("null pointer argument");
        return false;
    }
    let Ok(topic) = CStr::from_ptr(topic).to_str() else { return false; };
    if topic.is_empty(){
        print_error!("topic.is_empty()");
        return false;
    }
    (*client).subscribe(topic)
}

/// Unsubscribe from the topic and do not receive messages from other clients.
/// 
/// Possible errors:
/// - no connection to redis
/// - no other client with this topic
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_unsubscribe(client: *mut Client,
                          topic: *const i8)->bool{
    if !has_client(client){
        return false;
    }
    if topic.is_null() {
        print_error!("null pointer argument");
        return false;
    }
    let Ok(topic) = CStr::from_ptr(topic).to_str() else { return false; };
    if topic.is_empty(){
        print_error!("topic.is_empty()");
        return false;
    }
    (*client).unsubscribe(topic)
}

/// Refresh address of topic (actual for new clients)
/// 
/// Possible errors:
/// - no connection to redis
/// - no other client with this topic
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_refresh_address_topic(client: *mut Client,
                                                   topic: *const i8)->bool{
    if !has_client(client){
        return false;
    }
    if topic.is_null() {
        print_error!("null pointer argument");
        return false;
    }
    let Ok(topic) = CStr::from_ptr(topic).to_str() else { return false; };
    if topic.is_empty(){
        print_error!("topic.is_empty()");
        return false;
    }
    (*client).refresh_address_topic(topic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn fns_return_false_on_null_client_without_derefing_args() {
        unsafe {
            assert!(!lnr_send_to(ptr::null_mut(), ptr::null(), ptr::null(), 0, true));
            assert!(!lnr_send_all(ptr::null_mut(), ptr::null(), ptr::null(), 0, true));
            assert!(!lnr_subscribe(ptr::null_mut(), ptr::null()));
            assert!(!lnr_unsubscribe(ptr::null_mut(), ptr::null()));
            assert!(!lnr_refresh_address_topic(ptr::null_mut(), ptr::null()));
            assert!(!lnr_clear_stored_messages(ptr::null_mut()));
            assert!(!lnr_clear_addresses_of_topic(ptr::null_mut()));
            assert!(!lnr_delete_client(ptr::null_mut()));
            assert!(!lnr_set_status_cb(ptr::null_mut(), None, ptr::null_mut()));
            assert_eq!(lnr_last_error_code(ptr::null_mut()), 0);
            assert!(!lnr_set_advertise_addr(ptr::null_mut(), ptr::null()));
            assert!(!lnr_stop(ptr::null_mut()));
            assert!(!lnr_is_running(ptr::null_mut()));
            assert!(lnr_unique_name(ptr::null_mut()).is_null());
            assert!(lnr_topic(ptr::null_mut()).is_null());
            assert!(lnr_bind_addr(ptr::null_mut()).is_null());
            assert!(lnr_advertise_addr(ptr::null_mut()).is_null());
            assert!(lnr_bound_listen_addr(ptr::null_mut()).is_null());
            assert!(lnr_published_addr(ptr::null_mut()).is_null());
            assert!(!lnr_list_addresses(ptr::null_mut(), ptr::null(), None, ptr::null_mut()));
            assert_eq!(lnr_pending_count(ptr::null_mut()), -1);
            assert!(!lnr_pending_by_peer(ptr::null_mut(), None, ptr::null_mut()));
            assert_eq!(lnr_send_queue_depth(ptr::null_mut()), 0);
            assert!(!lnr_send_queue_depth_by_peer(ptr::null_mut(), None, ptr::null_mut()));
            assert!(!lnr_list_subscriptions(ptr::null_mut(), None, ptr::null_mut()));
            assert!(!lnr_list_related_topics(ptr::null_mut(), None, ptr::null_mut()));
            assert!(lnr_last_error_message(ptr::null_mut()).is_null());
            assert!(!lnr_version().is_null());
        }
    }

    #[test]
    fn send_to_rejects_zero_data_size_without_ub() {
        unsafe {
            // With null client, we must not dereference pointers at all.
            assert!(!lnr_send_to(ptr::null_mut(), ptr::null(), ptr::null(), 0, true));
        }
    }

    extern "C" fn log_hook_sets_flag(msg: *const i8, udata: *mut libc::c_void) {
        if udata.is_null() || msg.is_null() {
            return;
        }
        let s = unsafe { std::ffi::CStr::from_ptr(msg) };
        if s.to_bytes().windows(b"Error".len()).any(|w| w == b"Error") {
            unsafe {
                (*(udata as *const AtomicBool)).store(true, Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn log_hook_receives_print_error_from_null_client() {
        let _lock = crate::log::test_log_hook_lock();
        let seen = AtomicBool::new(false);
        unsafe {
            assert!(lnr_set_log_cb(
                Some(log_hook_sets_flag),
                &seen as *const AtomicBool as *mut libc::c_void,
            ));
            assert!(!lnr_delete_client(ptr::null_mut()));
            assert!(lnr_set_log_cb(None, ptr::null_mut()));
        }
        assert!(seen.load(Ordering::SeqCst));
    }

    #[test]
    fn runtime_limits_reject_zero_and_roundtrip() {
        let _lock = settings::test_limits_lock();
        let prev_max = unsafe { lnr_get_max_message_size() };
        let prev_thr = unsafe { lnr_get_compress_threshold() };
        let prev_q = unsafe { lnr_get_max_send_queue() };
        let prev_sc = unsafe { lnr_get_stream_check_timeout_ms() };
        let prev_wb = unsafe { lnr_get_would_block_timeout_ms() };
        unsafe {
            assert!(!lnr_set_max_message_size(0));
            assert!(!lnr_set_compress_threshold(0));
            assert!(!lnr_set_stream_check_timeout_ms(0));
            assert!(!lnr_set_would_block_timeout_ms(0));
            assert!(lnr_set_max_message_size(12345));
            assert_eq!(lnr_get_max_message_size(), 12345);
            assert!(lnr_set_compress_threshold(6789));
            assert_eq!(lnr_get_compress_threshold(), 6789);
            assert!(lnr_set_max_send_queue(42));
            assert_eq!(lnr_get_max_send_queue(), 42);
            assert!(lnr_set_max_send_queue(0)); // unlimited allowed
            assert_eq!(lnr_get_max_send_queue(), 0);
            assert!(lnr_set_stream_check_timeout_ms(1234));
            assert_eq!(lnr_get_stream_check_timeout_ms(), 1234);
            assert!(lnr_set_would_block_timeout_ms(5678));
            assert_eq!(lnr_get_would_block_timeout_ms(), 5678);
            assert!(lnr_set_max_message_size(prev_max));
            assert!(lnr_set_compress_threshold(prev_thr));
            assert!(lnr_set_max_send_queue(prev_q));
            assert!(lnr_set_stream_check_timeout_ms(prev_sc));
            assert!(lnr_set_would_block_timeout_ms(prev_wb));
        }
    }

    #[test]
    fn version_matches_cargo_pkg() {
        unsafe {
            let p = lnr_version();
            assert!(!p.is_null());
            let s = std::ffi::CStr::from_ptr(p).to_str().unwrap();
            assert_eq!(s, env!("CARGO_PKG_VERSION"));
        }
    }
}


/// Clearing messages that were not previously sent for some reason.
/// Call only when the client is not running yet.
/// 
/// Possible errors:
/// - no connection to redis
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_clear_stored_messages(client: *mut Client)->bool{
    if !has_client(client){
        return false;
    }
    (*client).clear_stored_messages()
}

/// Cleaning client addresses.
/// Call only when the client is not running yet.
/// 
/// Possible errors:
/// - no connection to redis
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn lnr_clear_addresses_of_topic(client: *mut Client)->bool{
    if !has_client(client){
        return false;
    }
    (*client).clear_addresses_of_topic()
}

/// Deleting a client.
///
/// # Safety
/// `client` must be a valid pointer returned from `lnr_new_client_*`, or null.
#[no_mangle]
pub unsafe extern "C" fn lnr_delete_client(client: *mut Client)->bool{
    if client.is_null() {
        print_error!("client was not created");
        return false;
    }
    if !take_live_client(client) {
        print_error!("client already deleted or unknown");
        return false;
    }
    drop(Box::from_raw(client));
    true
}

fn has_client(client: *mut Client)->bool{
    if client.is_null() {
        print_error!("client was not created");
        return false;
    }
    live_clients()
        .lock()
        .map(|live| live.contains(&(client as usize)))
        .unwrap_or(false)
}

#[macro_export]
macro_rules! print_error {
    ($arg:expr) => {{
        let line = format!("Error {}:{}: {}", file!(), line!(), $arg);
        $crate::log::emit_error_line(&line);
    }}
}

// The debug version
#[cfg(feature = "liner_debug")]
#[macro_export]
macro_rules! print_debug {
    ($( $args:expr ),*) => { println!("Debug", $( $args ),* ) }
}

// Non-debug version
#[cfg(not(feature = "liner_debug"))]
#[macro_export]
macro_rules! print_debug {
    ($( $args:expr ),*) => {}
}