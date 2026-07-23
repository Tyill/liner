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
    LNR_PEER_DISCONNECTED, LNR_PEER_SUBSCRIBED, LNR_PEER_UNSUBSCRIBED, LNR_SENDER_ROUTE_LOST,
    LNR_SENDER_SEND_ERROR, LNR_SENDER_STORE_ERROR,
};

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
    pub fn bound_listen_addr(&self) -> Option<String> {
        unsafe { (*self.hclient).bound_listen_addr().map(|s| s.to_string()) }
    }

    pub fn unique_name(&self) -> String {
        unsafe { (*self.hclient).unique_name().to_string() }
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
    if data_size == 0{
        print_error!("data_size empty");
        return false;
    }
    let data = std::slice::from_raw_parts(data, data_size);
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
    if data_size == 0{
        print_error!("data_size == 0");
        return false;
    }
    let data = std::slice::from_raw_parts(data, data_size);
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
        }
    }

    #[test]
    fn send_to_rejects_zero_data_size_without_ub() {
        unsafe {
            // With null client, we must not dereference pointers at all.
            assert!(!lnr_send_to(ptr::null_mut(), ptr::null(), ptr::null(), 0, true));
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
    ($arg:expr) => { eprintln!("Error {}:{}: {}", file!(), line!(), $arg) }
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