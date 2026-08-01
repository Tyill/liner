use crate::store::Store;
use crate::{UCbackIntern, UData};
use crate::error::ErrorCode;
use crate::listener::Listener;
use crate::message;
use crate::sender::{EnqueueResult, Sender};
use crate::print_error;
use crate::settings::INTERNAL_CHANNEL_TOPIC;
use crate::status::{
    StatusCbackIntern, StatusEmitter, StatusMsg, LNR_PEER_CONNECTED, LNR_PEER_DISCONNECTED,
    LNR_PEER_SUBSCRIBED, LNR_PEER_UNSUBSCRIBED, LNR_SENDER_BUSY,
};

use std::net::{SocketAddr, ToSocketAddrs};
use std::ffi::{CStr, CString};
use mio::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};

pub struct Client{
    unique_name: String,
    /// NUL-terminated copy for C `lnr_unique_name`.
    c_unique_name: CString,
    source_topic: String,
    /// TCP bind string from the constructor (not overwritten by advertise / ephemeral port).
    localhost: String,
    /// Optional address published to the store catalog (see [`Client::set_advertise_addr`]).
    advertise: Option<String>,
    db: Arc<Mutex<dyn Store>>,
    listener: Option<Listener>,
    sender: Option<Sender>,
    last_send_index: HashMap<String, usize>,
    is_run: bool,
    mtx: Mutex<()>,
    address_topic: HashMap<String, Vec<String>>,
    /// Topics this client has sent to, subscribed to, or explicitly refreshed (status filter).
    related_topics: HashSet<String>,
    subscriptions: HashMap<i32, String>,
    /// Actual `SocketAddr` after `run` binds `localhost` (e.g. when port is `0`). Kept after `stop`.
    bound_listen_addr: Option<String>,
    c_bound_listen_addr: Option<CString>,
    /// Address written to the store while registered; cleared on `stop`.
    published_addr: Option<String>,
    c_published_addr: Option<CString>,
    last_error: ErrorCode,
    last_error_msg: String,
    c_last_error_msg: Option<CString>,
    user_receive_cb: Option<UCbackIntern>,
    user_receive_udata: UData,
    status_emitter: StatusEmitter,
}

fn cstring_lossy(s: &str) -> CString {
    CString::new(s).unwrap_or_else(|_| CString::new("").unwrap_or_default())
}

fn empty_cstr() -> &'static CStr {
    // SAFETY: single trailing NUL, no interior NUL.
    unsafe { CStr::from_bytes_with_nul_unchecked(b"\0") }
}

fn client_fields(
    unique_name: String,
    source_topic: String,
    localhost: String,
    db: Arc<Mutex<dyn Store>>,
) -> Client {
    let c_unique_name = cstring_lossy(&unique_name);
    Client {
        unique_name,
        c_unique_name,
        source_topic,
        localhost,
        advertise: None,
        db,
        listener: None,
        sender: None,
        last_send_index: HashMap::new(),
        is_run: false,
        mtx: Mutex::new(()),
        address_topic: HashMap::new(),
        related_topics: HashSet::new(),
        subscriptions: HashMap::new(),
        bound_listen_addr: None,
        c_bound_listen_addr: None,
        published_addr: None,
        c_published_addr: None,
        last_error: ErrorCode::Ok,
        last_error_msg: String::new(),
        c_last_error_msg: None,
        user_receive_cb: None,
        user_receive_udata: UData::null(),
        status_emitter: StatusEmitter::new(),
    }
}

fn set_ok(
    last_error: &mut ErrorCode,
    last_error_msg: &mut String,
    c_last_error_msg: &mut Option<CString>,
) {
    *last_error = ErrorCode::Ok;
    last_error_msg.clear();
    // Drop previous detail only when present — no empty CString alloc on warm OK path.
    if c_last_error_msg.is_some() {
        *c_last_error_msg = None;
    }
}

fn set_fail(
    last_error: &mut ErrorCode,
    last_error_msg: &mut String,
    c_last_error_msg: &mut Option<CString>,
    code: ErrorCode,
    msg: &str,
) -> bool {
    *last_error = code;
    *last_error_msg = msg.to_string();
    *c_last_error_msg = Some(cstring_lossy(msg));
    print_error!(msg);
    false
}

macro_rules! client_ok {
    ($self:expr) => {
        set_ok(
            &mut $self.last_error,
            &mut $self.last_error_msg,
            &mut $self.c_last_error_msg,
        )
    };
}

macro_rules! client_fail {
    ($self:expr, $code:expr, $msg:expr $(,)?) => {
        set_fail(
            &mut $self.last_error,
            &mut $self.last_error_msg,
            &mut $self.c_last_error_msg,
            $code,
            $msg,
        )
    };
}

impl Client {
    pub fn new_redis(unique_name: &str, topic: &str, localhost: &str, redis_url: &str) -> Option<Client> {
        let store_backend = crate::store::StoreBackend::Redis {
            url: redis_url.to_string(),
        };
        let db = crate::store::open_store_mutex(unique_name, store_backend).ok()?;
        {
            let mut db = db.lock().ok()?;
            db.set_source_topic(topic);
            db.set_source_localhost(localhost);
        }
        Some(client_fields(
            unique_name.to_string(),
            topic.to_string(),
            localhost.to_string(),
            db,
        ))
    }

    pub fn new_sqlite(
        unique_name: &str,
        topic: &str,
        localhost: &str,
        sqlite_path: &str,
        receivers_json: &str,
    ) -> Option<Client> {
        let store_backend = crate::store::StoreBackend::Sqlite {
            path: sqlite_path.to_string(),
        };
        let db = crate::store::open_store_mutex(unique_name, store_backend).ok()?;
        {
            let mut db = db.lock().ok()?;
            db.set_source_topic(topic);
            db.set_source_localhost(localhost);
            let trimmed = receivers_json.trim();
            if !trimmed.is_empty() {
                match serde_json::from_str::<Vec<crate::store::ReceiverSeedEntry>>(trimmed) {
                    Ok(entries) => {
                        if let Err(err) = db.seed_receivers(&entries) {
                            print_error!(&format!("seed_receivers: {}", err));
                            return None;
                        }
                    }
                    Err(err) => {
                        print_error!(&format!("receivers_json parse error: {}", err));
                        return None;
                    }
                }
            }
        }
        Some(client_fields(
            unique_name.to_string(),
            topic.to_string(),
            localhost.to_string(),
            db,
        ))
    }

    /// PostgreSQL-backed client (requires Cargo feature **`postgres`**).
    ///
    /// `postgres_url` is a libpq connection string (e.g. `postgresql://user:pass@127.0.0.1/liner`).
    /// Peers share one database; the catalog comes from the store (like Redis), not JSON seeding.
    #[cfg(feature = "postgres")]
    pub fn new_postgres(
        unique_name: &str,
        topic: &str,
        localhost: &str,
        postgres_url: &str,
    ) -> Option<Client> {
        let store_backend = crate::store::StoreBackend::Postgres {
            url: postgres_url.to_string(),
        };
        let db = crate::store::open_store_mutex(unique_name, store_backend).ok()?;
        {
            let mut db = db.lock().ok()?;
            db.set_source_topic(topic);
            db.set_source_localhost(localhost);
        }
        Some(client_fields(
            unique_name.to_string(),
            topic.to_string(),
            localhost.to_string(),
            db,
        ))
    }

    pub fn last_error(&self) -> ErrorCode {
        self.last_error
    }

    /// Bare detail string for the last sync failure (empty when OK).
    pub fn last_error_message(&self) -> &str {
        &self.last_error_msg
    }

    pub fn last_error_message_c_str(&self) -> *const i8 {
        self.c_last_error_msg
            .as_ref()
            .map(|c| c.as_ptr())
            .unwrap_or_else(|| empty_cstr().as_ptr())
    }

    pub fn unique_name(&self) -> &str {
        &self.unique_name
    }

    /// C-compatible pointer owned by this client (stable until the client is dropped).
    pub fn unique_name_c_str(&self) -> *const i8 {
        self.c_unique_name.as_ptr()
    }

    /// After [`Client::run`], the resolved bind address if `localhost` used port `0`. Kept after [`Client::stop`].
    pub fn bound_listen_addr(&self) -> Option<&str> {
        self.bound_listen_addr.as_deref()
    }

    pub fn bound_listen_addr_c_str(&self) -> *const i8 {
        self.c_bound_listen_addr
            .as_ref()
            .map(|c| c.as_ptr())
            .unwrap_or(std::ptr::null())
    }

    /// Catalog address while registered; `None` after [`Client::stop`] or before the first successful `run`.
    pub fn published_addr(&self) -> Option<&str> {
        self.published_addr.as_deref()
    }

    pub fn published_addr_c_str(&self) -> *const i8 {
        self.c_published_addr
            .as_ref()
            .map(|c| c.as_ptr())
            .unwrap_or(std::ptr::null())
    }

    pub fn is_running(&self) -> bool {
        self.is_run
    }

    /// Address published to the store instead of the bind string. Call before [`Client::run`].
    /// `None` or empty clears advertise (publish bind / bound address again).
    pub fn set_advertise_addr(&mut self, addr: Option<&str>) -> bool {
        let _lock = self.mtx.lock();
        if self.is_run {
            return client_fail!(self, 
                ErrorCode::AlreadyRunning,
                "you can't set_advertise_addr because client already is running",
            );
        }
        match addr {
            None => {
                self.advertise = None;
                client_ok!(self);
                true
            }
            Some(s) if s.is_empty() => {
                self.advertise = None;
                client_ok!(self);
                true
            }
            Some(s) => {
                if str_to_socket_addr(s).is_none() {
                    return client_fail!(self, 
                        ErrorCode::InvalidArg,
                        &format!("invalid advertise address: {}", s),
                    );
                }
                self.advertise = Some(s.to_string());
                client_ok!(self);
                true
            }
        }
    }

    /// Set or clear the status / background-error callback. Pass `None` to clear.
    pub fn set_status_cb(&mut self, cb: Option<StatusCbackIntern>, udata: UData) {
        let _lock = self.mtx.lock();
        self.status_emitter
            .set_callback(cb, udata);
    }

    /// Backward compatible: same as [`Client::new_redis`].
    pub fn new(unique_name: &str, topic: &str, localhost: &str, redis_path: &str) -> Option<Client> {
        Self::new_redis(unique_name, topic, localhost, redis_path)
    }
    pub fn run(&mut self, receive_cb: UCbackIntern, udata: UData) -> bool {
        let client_ptr = std::ptr::from_mut(self);
        let _lock = self.mtx.lock();
        if self.is_run {
            client_ok!(self);
            return true;
        }
        let sa = match str_to_socket_addr(&self.localhost) {
            Some(sa) => sa,
            None => {
                return client_fail!(self, 
                    ErrorCode::Bind,
                    &format!("invalid bind address: {}", self.localhost),
                );
            }
        };
        let tcp_listener = match TcpListener::bind(sa) {
            Ok(l) => l,
            Err(err) => {
                return client_fail!(self, ErrorCode::Bind, &format!("{}", err));
            }
        };
        let bound = tcp_listener.local_addr().ok().map(|a| a.to_string());
        self.bound_listen_addr = bound.clone();
        self.c_bound_listen_addr = bound.as_ref().map(|s| cstring_lossy(s));

        let published = match compute_published_addr(
            self.advertise.as_deref(),
            self.bound_listen_addr.as_deref(),
            &self.localhost,
        ) {
            Ok(p) => p,
            Err(msg) => {
                return client_fail!(self, ErrorCode::InvalidArg, &msg);
            }
        };
        self.published_addr = Some(published.clone());
        self.c_published_addr = Some(cstring_lossy(&published));

        // Register only after bind so port-0 peers never see `host:0`.
        {
            let mut db = self.db.lock().unwrap();
            db.set_source_localhost(&published);
            if let Err(err) = db.regist_topic(&self.source_topic) {
                self.published_addr = None;
                self.c_published_addr = None;
                return client_fail!(self, ErrorCode::Store, &format!("{}", err));
            }
        }
        self.user_receive_cb = Some(receive_cb);
        self.user_receive_udata = udata;
        let listener = match Listener::new(
            tcp_listener,
            self.db.clone(),
            &self.source_topic,
            &self.subscriptions,
            client_receive_wrapper,
            UData(client_ptr as *mut libc::c_void),
            self.status_emitter.clone(),
        ) {
            Ok(l) => l,
            Err(err) => {
                self.published_addr = None;
                self.c_published_addr = None;
                let _ = self.db.lock().unwrap().unregist_topic(&self.source_topic);
                return client_fail!(self, ErrorCode::Startup, &err);
            }
        };
        self.listener = Some(listener);
        self.sender = Some(Sender::new(
            self.db.clone(),
            &self.source_topic,
            self.status_emitter.clone(),
        ));
        if let Some(sender) = self.sender.as_mut() {
            sender.load_prev_connects(&mut *self.db.lock().unwrap());
        }
        self.is_run = true;
        if !subscribe_inner(
            INTERNAL_CHANNEL_TOPIC,
            &self.source_topic,
            &mut *self.db.lock().unwrap(),
            self.is_run,
            &mut self.listener,
            &mut self.subscriptions,
        ) {
            self.is_run = false;
            self.published_addr = None;
            self.c_published_addr = None;
            drop(self.listener.take());
            drop(self.sender.take());
            return client_fail!(self, 
                ErrorCode::Store,
                "failed to subscribe to internal channel",
            );
        }
        emit_internal_event(
            self.is_run,
            &self.unique_name,
            &self.source_topic,
            self.published_addr.as_deref(),
            &mut self.address_topic,
            &self.db,
            self.sender.as_mut().unwrap(),
            "client_connected",
            None,
        );

        client_ok!(self);
        true
    }

    /// Stop listener/sender threads and unregister from the store. Idempotent.
    /// Keeps [`Client::bound_listen_addr`]; clears [`Client::published_addr`].
    pub fn stop(&mut self) -> bool {
        let (listener, sender) = {
            let _lock = self.mtx.lock();
            if !self.is_run {
                client_ok!(self);
                return true;
            }
            // Drop extra catalog registrations (subscribe topics). Do not emit
            // "unsubscribed" here — crash/teardown must keep sender_listener so
            // at-least-once offline delivery still works; only explicit
            // `unsubscribe` clears those routes via the internal event.
            let extra: Vec<String> = self
                .subscriptions
                .values()
                .filter(|t| t.as_str() != INTERNAL_CHANNEL_TOPIC)
                .cloned()
                .collect();
            for topic in extra {
                let _ = unsubscribe_inner(
                    &topic,
                    &self.source_topic,
                    &mut *self.db.lock().unwrap(),
                    self.is_run,
                    &mut self.listener,
                    &mut self.subscriptions,
                );
            }
            // Unregister before announcing disconnect so peers refresh a catalog without us.
            if let Err(err) = self.db.lock().unwrap().unregist_topic(&self.source_topic) {
                print_error!(&format!("{}", err));
            }
            let _ = unsubscribe_inner(
                INTERNAL_CHANNEL_TOPIC,
                &self.source_topic,
                &mut *self.db.lock().unwrap(),
                self.is_run,
                &mut self.listener,
                &mut self.subscriptions,
            );
            if let Some(sender) = self.sender.as_mut() {
                emit_internal_event(
                    self.is_run,
                    &self.unique_name,
                    &self.source_topic,
                    self.published_addr.as_deref(),
                    &mut self.address_topic,
                    &self.db,
                    sender,
                    "client_disconnected",
                    None,
                );
            }
            self.is_run = false;
            self.published_addr = None;
            self.c_published_addr = None;
            client_ok!(self);
            (self.listener.take(), self.sender.take())
        };
        // Join threads outside Client.mtx — receive path also takes that lock.
        drop(listener);
        drop(sender);
        true
    }

    pub fn send_to(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        // Hold mtx for route + ensure + enqueue so concurrent FFI calls stay serialized
        // (see docs/using-the-api.md). Store is still only locked briefly in ensure_send_route.
        let _lock = self.mtx.lock().unwrap();
        if !self.is_run {
            return client_fail!(self, 
                ErrorCode::NotRunning,
                "you can't send_to because client not is running",
            );
        }
        if topic == self.source_topic {
            return client_fail!(self, ErrorCode::SelfTopic, "you can't send on your own topic");
        }
        if data.is_empty() {
            return client_fail!(self, ErrorCode::InvalidArg,
                "payload empty",
            );
        }
        if message::payload_exceeds_max_message_size(data.len()) {
            return client_fail!(self, ErrorCode::InvalidArg,
                &format!(
                    "payload too large for max_message_size (payload {}, framed body {}, max {})",
                    data.len(),
                    message::framed_body_size_raw(data.len()),
                    crate::settings::max_message_size()
                ),
            );
        }
        apply_failed_routes(&mut self.address_topic, self.sender.as_mut());
        // Resolve routes first so round-robin can borrow the address without cloning.
        if self
            .address_topic
            .get(topic)
            .map(|a| a.is_empty())
            .unwrap_or(true)
        {
            match resolve_send_addresses(
                topic,
                at_least_once_delivery,
                &mut self.address_topic,
                &mut *self.db.lock().unwrap(),
            ) {
                ResolveAddrs::Ok(_) => {}
                ResolveAddrs::NoAddr => {
                    self.address_topic.remove(topic);
                    return client_fail!(self, 
                        ErrorCode::NoAddr,
                        &format!("not found addr for topic {}", topic),
                    );
                }
                ResolveAddrs::Store(err) => {
                    self.address_topic.remove(topic);
                    return client_fail!(self, ErrorCode::Store, &err);
                }
            }
        }
        let addr_len = self.address_topic.get(topic).map(|a| a.len()).unwrap_or(0);
        if addr_len == 0 {
            return client_fail!(self, 
                ErrorCode::NoAddr,
                &format!("not found addr for topic {}", topic),
            );
        }
        mark_related_topic(&mut self.related_topics, topic);
        let index = if let Some(slot) = self.last_send_index.get_mut(topic) {
            let i = *slot % addr_len;
            *slot = (i + 1) % addr_len;
            i
        } else {
            self.last_send_index.insert(topic.to_owned(), if addr_len > 1 { 1 } else { 0 });
            0
        };
        let addr = self.address_topic.get(topic).unwrap()[index].as_str();
        let sender = self.sender.as_mut().unwrap();
        if sender.needs_store_for_send(addr, topic) {
            let mut db = self.db.lock().unwrap();
            if !sender.ensure_send_route(&mut *db, addr, topic) {
                return client_fail!(self, ErrorCode::Store, "ensure_send_route failed");
            }
        }
        match sender.send_to(addr, topic, data, at_least_once_delivery) {
            EnqueueResult::Ok => {
                client_ok!(self);
                true
            }
            EnqueueResult::Busy => {
                self.status_emitter.emit_msg(
                    LNR_SENDER_BUSY,
                    topic,
                    addr,
                    StatusMsg::SendQueueFull,
                    &[],
                );
                client_fail!(self, ErrorCode::Busy, "send queue full")
            }
            EnqueueResult::Fail => client_fail!(self, ErrorCode::Store, "send_to failed"),
        }
    }

    pub fn send_all(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        let _lock = self.mtx.lock().unwrap();
        if !self.is_run {
            return client_fail!(self, 
                ErrorCode::NotRunning,
                "you can't send_all because client not is running",
            );
        }
        if topic == self.source_topic {
            return client_fail!(self, ErrorCode::SelfTopic, "you can't send on your own topic");
        }
        if data.is_empty() {
            return client_fail!(self, ErrorCode::InvalidArg,
                "payload empty",
            );
        }
        if message::payload_exceeds_max_message_size(data.len()) {
            return client_fail!(self, ErrorCode::InvalidArg,
                &format!(
                    "payload too large for max_message_size (payload {}, framed body {}, max {})",
                    data.len(),
                    message::framed_body_size_raw(data.len()),
                    crate::settings::max_message_size()
                ),
            );
        }
        apply_failed_routes(&mut self.address_topic, self.sender.as_mut());
        // Populate cache without retaining a borrow across the sender loop.
        if !self
            .address_topic
            .get(topic)
            .is_some_and(|a| !a.is_empty())
        {
            match resolve_send_addresses(
                topic,
                at_least_once_delivery,
                &mut self.address_topic,
                &mut *self.db.lock().unwrap(),
            ) {
                ResolveAddrs::Ok(_) => {}
                ResolveAddrs::NoAddr => {
                    self.address_topic.remove(topic);
                    return client_fail!(self, ErrorCode::NoAddr,
                        &format!("not found addr for topic {}", topic),
                    );
                }
                ResolveAddrs::Store(err) => {
                    self.address_topic.remove(topic);
                    return client_fail!(self, ErrorCode::Store, &err);
                }
            }
        }
        let addr_len = self.address_topic.get(topic).map(|a| a.len()).unwrap_or(0);
        if addr_len == 0 {
            return client_fail!(self, ErrorCode::NoAddr,
                &format!("not found addr for topic {}", topic),
            );
        }
        mark_related_topic(&mut self.related_topics, topic);
        // Disjoint field borrows: addresses from cache, mutable sender — no addr Vec clone.
        let addrs = self.address_topic.get(topic).unwrap().as_slice();
        let sender = self.sender.as_mut().unwrap();
        let mut warm_ok = vec![true; addr_len];
        if addrs
            .iter()
            .any(|addr| sender.needs_store_for_send(addr, topic))
        {
            let mut db = self.db.lock().unwrap();
            for (i, addr) in addrs.iter().enumerate() {
                if sender.needs_store_for_send(addr, topic)
                    && !sender.ensure_send_route(&mut *db, addr, topic)
                {
                    warm_ok[i] = false;
                }
            }
        }
        let mut ok = true;
        let mut saw_busy = false;
        for (i, addr) in addrs.iter().enumerate() {
            if !warm_ok[i] {
                ok = false;
                continue;
            }
            match sender.send_to(addr, topic, data, at_least_once_delivery) {
                EnqueueResult::Ok => {}
                EnqueueResult::Busy => {
                    ok = false;
                    saw_busy = true;
                    self.status_emitter.emit_msg(
                        LNR_SENDER_BUSY,
                        topic,
                        addr,
                        StatusMsg::SendQueueFull,
                        &[],
                    );
                }
                EnqueueResult::Fail => {
                    ok = false;
                }
            }
        }
        if ok {
            client_ok!(self);
            true
        } else if saw_busy {
            client_fail!(self, ErrorCode::Busy, "send queue full")
        } else {
            client_fail!(self, ErrorCode::Store, "send_all failed for one or more peers")
        }
    }

    pub fn subscribe(&mut self, topic: &str) -> bool {
        let _lock = self.mtx.lock();
        if topic == self.source_topic {
            return client_fail!(self, 
                ErrorCode::SelfTopic,
                "you can't subscribe on your own topic",
            );
        }
        if topic == INTERNAL_CHANNEL_TOPIC {
            return client_fail!(self, 
                ErrorCode::InternalTopic,
                "you can't subscribe on internal channel topic",
            );
        }
        if !subscribe_inner(
            topic,
            &self.source_topic,
            &mut *self.db.lock().unwrap(),
            self.is_run,
            &mut self.listener,
            &mut self.subscriptions,
        ) {
            return client_fail!(self, ErrorCode::Store, "subscribe failed");
        }
        mark_related_topic(&mut self.related_topics, topic);
        if self.is_run {
            emit_internal_event(
                self.is_run,
                &self.unique_name,
                &self.source_topic,
                self.published_addr.as_deref(),
                &mut self.address_topic,
                &self.db,
                self.sender.as_mut().unwrap(),
                "subscribed",
                Some(topic),
            );
        }
        client_ok!(self);
        true
    }

    pub fn unsubscribe(&mut self, topic: &str) -> bool {
        let _lock = self.mtx.lock();
        if topic == self.source_topic {
            return client_fail!(self, 
                ErrorCode::SelfTopic,
                "you can't unsubscribe on your own topic",
            );
        }
        if topic == INTERNAL_CHANNEL_TOPIC {
            return client_fail!(self, 
                ErrorCode::InternalTopic,
                "you can't unsubscribe on internal channel topic",
            );
        }
        if !unsubscribe_inner(
            topic,
            &self.source_topic,
            &mut *self.db.lock().unwrap(),
            self.is_run,
            &mut self.listener,
            &mut self.subscriptions,
        ) {
            return client_fail!(self, ErrorCode::Store, "unsubscribe failed");
        }
        if self.is_run {
            emit_internal_event(
                self.is_run,
                &self.unique_name,
                &self.source_topic,
                self.published_addr.as_deref(),
                &mut self.address_topic,
                &self.db,
                self.sender.as_mut().unwrap(),
                "unsubscribed",
                Some(topic),
            );
        }
        client_ok!(self);
        true
    }

    pub fn refresh_address_topic(&mut self, topic: &str) -> bool {
        let _lock = self.mtx.lock();
        match refresh_address_topic_cache(
            &mut self.address_topic,
            &mut *self.db.lock().unwrap(),
            topic,
        ) {
            RefreshResult::Ok => {
                mark_related_topic(&mut self.related_topics, topic);
                client_ok!(self);
                true
            }
            RefreshResult::NoAddr => client_fail!(self, 
                ErrorCode::NoAddr,
                &format!("not found addr for topic {}", topic),
            ),
            RefreshResult::Store(err) => client_fail!(self, ErrorCode::Store, &err),
        }
    }

    pub fn clear_stored_messages(&mut self) -> bool {
        let _lock = self.mtx.lock();
        if self.is_run {
            return client_fail!(self, 
                ErrorCode::ClearWhileRunning,
                "you can't clear_stored_messages because client already is running",
            );
        }
        if let Err(err) = self.db.lock().unwrap().clear_stored_messages() {
            return client_fail!(self, ErrorCode::Store, &format!("{}", err));
        }
        client_ok!(self);
        true
    }
    pub fn clear_addresses_of_topic(&mut self) -> bool {
        let _lock = self.mtx.lock();
        if self.is_run {
            return client_fail!(self, 
                ErrorCode::ClearWhileRunning,
                "you can't clear_addresses_of_topic because client already is running",
            );
        }
        if let Err(err) = self.db.lock().unwrap().clear_addresses_of_topic() {
            return client_fail!(self, ErrorCode::Store, &format!("{}", err));
        }
        client_ok!(self);
        true
    }

    /// Topic directory from the store: `(addr, unique_name)` pairs (empty vec is success).
    pub fn list_addresses(&mut self, topic: &str) -> Option<Vec<(String, String)>> {
        let _lock = self.mtx.lock();
        match self.db.lock().unwrap().get_topic_directory(topic) {
            Ok(rows) => {
                let addrs: Vec<String> = rows.iter().map(|(a, _)| a.clone()).collect();
                if addrs.is_empty() {
                    self.address_topic.remove(topic);
                } else {
                    self.address_topic.insert(topic.to_string(), addrs);
                }
                client_ok!(self);
                Some(rows)
            }
            Err(err) => {
                client_fail!(self, ErrorCode::Store, &format!("{}", err));
                None
            }
        }
    }

    /// Sum of offline queued message blobs for this sender identity.
    pub fn pending_count(&mut self) -> Option<u64> {
        let _lock = self.mtx.lock();
        let mut db = self.db.lock().unwrap();
        let listeners = match db.get_listeners_of_sender() {
            Ok(l) => l,
            Err(err) => {
                client_fail!(self, ErrorCode::Store, &format!("{}", err));
                return None;
            }
        };
        let mut total: u64 = 0;
        for (addr, listener_topic) in listeners {
            let name = match db.get_listener_unique_name(&listener_topic, &addr) {
                Ok(n) => n,
                Err(_) => continue,
            };
            let Some(ck) = (match db.find_connection_key_for_sender(&name) {
                Ok(v) => v,
                Err(err) => {
                    client_fail!(self, ErrorCode::Store, &format!("{}", err));
                    return None;
                }
            }) else {
                continue;
            };
            match db.count_pending_messages(ck) {
                Ok(n) => total = total.saturating_add(n as u64),
                Err(err) => {
                    client_fail!(self, ErrorCode::Store, &format!("{}", err));
                    return None;
                }
            }
        }
        client_ok!(self);
        Some(total)
    }

    /// App-facing subscriptions (excludes `__#internal_channel`).
    pub fn list_subscriptions(&self) -> Vec<String> {
        let _lock = self.mtx.lock();
        self.subscriptions
            .values()
            .filter(|t| t.as_str() != INTERNAL_CHANNEL_TOPIC)
            .cloned()
            .collect()
    }

    /// Topics this client has sent to, subscribed to, or refreshed (status peer filter).
    pub fn list_related_topics(&self) -> Vec<String> {
        let _lock = self.mtx.lock();
        self.related_topics.iter().cloned().collect()
    }

    /// Per-peer offline queue depths for this sender (`sender_listener` routes).
    pub fn pending_by_peer(&mut self) -> Option<Vec<(String, String, String, u64)>> {
        let _lock = self.mtx.lock();
        let mut db = self.db.lock().unwrap();
        let listeners = match db.get_listeners_of_sender() {
            Ok(l) => l,
            Err(err) => {
                client_fail!(self, ErrorCode::Store, &format!("{}", err));
                return None;
            }
        };
        let mut rows = Vec::new();
        for (addr, listener_topic) in listeners {
            let name = match db.get_listener_unique_name(&listener_topic, &addr) {
                Ok(n) => n,
                Err(_) => continue,
            };
            let Some(ck) = (match db.find_connection_key_for_sender(&name) {
                Ok(v) => v,
                Err(err) => {
                    client_fail!(self, ErrorCode::Store, &format!("{}", err));
                    return None;
                }
            }) else {
                continue;
            };
            match db.count_pending_messages(ck) {
                Ok(n) => rows.push((addr, listener_topic, name, n as u64)),
                Err(err) => {
                    client_fail!(self, ErrorCode::Store, &format!("{}", err));
                    return None;
                }
            }
        }
        client_ok!(self);
        Some(rows)
    }
}

fn subscribe_inner(
    topic: &str,
    source_topic: &str,
    db: &mut dyn Store,
    is_run: bool,
    listener: &mut Option<Listener>,
    subscriptions: &mut HashMap<i32, String>,
) -> bool {
    if topic == source_topic {
        print_error!("you can't subscribe on your own topic");
        return false;
    }
    if let Err(err) = db.regist_topic(topic) {
        print_error!(&format!("{}", err));
        return false;
    }
    match db.get_topic_key(topic) {
        Ok(topic_key) => {
            if is_run {
                listener.as_mut().unwrap().subscribe(topic, topic_key);
            }
            subscriptions.insert(topic_key, topic.to_owned());
        }
        Err(err) => {
            print_error!(&format!("{}", err));
            return false;
        }
    }
    true
}

fn unsubscribe_inner(
    topic: &str,
    source_topic: &str,
    db: &mut dyn Store,
    is_run: bool,
    listener: &mut Option<Listener>,
    subscriptions: &mut HashMap<i32, String>,
) -> bool {
    if topic == source_topic {
        print_error!("you can't unsubscribe on your own topic");
        return false;
    }
    let topic_key = match db.get_topic_key(topic) {
        Ok(topic_key) => topic_key,
        Err(err) => {
            print_error!(&format!("{}", err));
            return false;
        }
    };
    if let Err(err) = db.unregist_topic(topic) {
        print_error!(&format!("{}", err));
        return false;
    }
    if is_run {
        listener.as_mut().unwrap().unsubscribe(topic_key);
    }
    subscriptions.remove(&topic_key);
    true
}

fn emit_internal_event(
    is_run: bool,
    unique_name: &str,
    source_topic: &str,
    bound_listen_addr: Option<&str>,
    address_topic: &mut HashMap<String, Vec<String>>,
    db: &Arc<Mutex<dyn Store>>,
    sender: &mut Sender,
    event: &str,
    subscription_topic: Option<&str>,
) {
    if !is_run {
        return;
    }
    let topic_field = subscription_topic.unwrap_or(source_topic);
    let mut value = serde_json::json!({
        "event": event,
        "client": unique_name,
        "topic": topic_field,
    });
    if event == "client_connected" {
        let addr = bound_listen_addr.unwrap_or("");
        value["addr"] = serde_json::Value::String(addr.to_string());
    }
    let Ok(bytes) = serde_json::to_vec(&value) else {
        return;
    };
    let address = {
        let mut db = db.lock().unwrap();
        // Always reload peers from the store — a fresh process may emit
        // subscribe/unsubscribe before its in-memory route cache is warm.
        if let Ok(Some(addr)) = get_address_topic(INTERNAL_CHANNEL_TOPIC, &mut *db, true) {
            address_topic.insert(INTERNAL_CHANNEL_TOPIC.to_string(), addr);
        }
        match resolve_send_addresses(
            INTERNAL_CHANNEL_TOPIC,
            false,
            address_topic,
            &mut *db,
        ) {
            ResolveAddrs::Ok(address) => address.to_vec(),
            _ => {
                address_topic.remove(INTERNAL_CHANNEL_TOPIC);
                return;
            }
        }
    };
    for addr in address {
        {
            let mut db = db.lock().unwrap();
            if let Ok(name) = db.get_listener_unique_name(INTERNAL_CHANNEL_TOPIC, &addr) {
                if name == unique_name {
                    continue;
                }
            }
            if !sender.ensure_send_route(&mut *db, &addr, INTERNAL_CHANNEL_TOPIC) {
                continue;
            }
        }
        // Durable for subscribe/unsubscribe: a fresh process may emit before TCP is up,
        // and peers must clear sender_listener from "unsubscribed". Connect/disconnect
        // stay best-effort to avoid filling the offline queue on teardown races.
        let durable = matches!(event, "subscribed" | "unsubscribed");
        let _ = sender.send_to(&addr, INTERNAL_CHANNEL_TOPIC, &bytes, durable);
    }
}

extern "C" fn client_receive_wrapper(
    to: *const i8,
    from: *const i8,
    data: *const u8,
    dsize: usize,
    udata: *mut libc::c_void,
) {
    let client = udata as *mut Client;
    if client.is_null() {
        return;
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        if let Ok(to_str) = CStr::from_ptr(to).to_str() {
            if to_str == INTERNAL_CHANNEL_TOPIC {
                let slice = std::slice::from_raw_parts(data, dsize);
                if let Ok(_lock) = (*client).mtx.lock() {
                    apply_internal_channel_event(&mut *client, slice);
                }
                return;
            }
        }
        if let Some(user_cb) = (*client).user_receive_cb {
            user_cb(to, from, data, dsize, (*client).user_receive_udata.0);
        }
    }));
    if result.is_err() {
        print_error!("client_receive_wrapper panicked");
    }
}

fn apply_internal_channel_event(client: &mut Client, data: &[u8]) {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(data) else {
        return;
    };
    let Some(event) = value.get("event").and_then(|v| v.as_str()) else {
        return;
    };
    let topic = value
        .get("topic")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let peer = value
        .get("client")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let related = is_related_topic(topic, &client.related_topics);
    if !topic.is_empty() && topic != INTERNAL_CHANNEL_TOPIC {
        let _ = refresh_address_topic_cache(
            &mut client.address_topic,
            &mut *client.db.lock().unwrap(),
            topic,
        );
        if event == "unsubscribed" {
            let _ = client
                .db
                .lock()
                .unwrap()
                .remove_sender_listeners_on_topic(topic);
        }
    }
    if matches!(event, "client_connected" | "client_disconnected") {
        let _ = refresh_address_topic_cache(
            &mut client.address_topic,
            &mut *client.db.lock().unwrap(),
            INTERNAL_CHANNEL_TOPIC,
        );
    }
    if related {
        let kind = match event {
            "client_connected" => Some(LNR_PEER_CONNECTED),
            "client_disconnected" => Some(LNR_PEER_DISCONNECTED),
            "subscribed" => Some(LNR_PEER_SUBSCRIBED),
            "unsubscribed" => Some(LNR_PEER_UNSUBSCRIBED),
            _ => None,
        };
        if let Some(kind) = kind {
            client
                .status_emitter
                .emit(kind, topic, peer, event);
        }
    }
}

fn is_related_topic(topic: &str, related_topics: &HashSet<String>) -> bool {
    !topic.is_empty()
        && topic != INTERNAL_CHANNEL_TOPIC
        && related_topics.contains(topic)
}

/// Mark topic as related for status filtering. No alloc when already present.
#[inline]
fn mark_related_topic(related_topics: &mut HashSet<String>, topic: &str) {
    if !related_topics.contains(topic) {
        related_topics.insert(topic.to_owned());
    }
}

fn refresh_address_topic_cache(
    address_topic: &mut HashMap<String, Vec<String>>,
    db: &mut dyn Store,
    topic: &str,
) -> RefreshResult {
    match get_address_topic(topic, db, true) {
        Ok(Some(addr)) => {
            address_topic.insert(topic.to_string(), addr);
            RefreshResult::Ok
        }
        Ok(None) => {
            address_topic.remove(topic);
            RefreshResult::NoAddr
        }
        Err(err) => {
            address_topic.remove(topic);
            RefreshResult::Store(err)
        }
    }
}

enum RefreshResult {
    Ok,
    NoAddr,
    Store(String),
}

fn apply_failed_routes(
    address_topic: &mut HashMap<String, Vec<String>>,
    sender: Option<&mut Sender>,
) {
    let Some(sender) = sender else {
        return;
    };
    let failed = sender.drain_failed_addrs();
    if failed.is_empty() {
        return;
    }
    let mut topics_to_refresh = Vec::new();
    for (topic, addrs) in address_topic.iter_mut() {
        let before = addrs.len();
        addrs.retain(|a| !failed.contains(a));
        if addrs.len() != before {
            topics_to_refresh.push(topic.clone());
        }
    }
    for topic in topics_to_refresh {
        if address_topic.get(&topic).is_some_and(|a| a.is_empty()) {
            address_topic.remove(&topic);
        }
    }
}

fn get_address_topic(
    topic: &str,
    db: &mut dyn Store,
    without_cache: bool,
) -> Result<Option<Vec<String>>, String> {
    match db.get_addresses_of_topic(without_cache, topic) {
        Ok(addresses) => {
            if addresses.is_empty() {
                Ok(None)
            } else {
                Ok(Some(addresses))
            }
        }
        Err(err) => Err(format!("{}", err)),
    }
}

enum ResolveAddrs<'a> {
    Ok(&'a [String]),
    NoAddr,
    Store(String),
}

/// Prefer the in-memory route cache (kept fresh by the internal channel / `refresh_address_topic`).
/// On miss, resolve from the store catalog and populate the cache. With `at_least_once_delivery`,
/// fall back to `sender_listener` so offline queueing still works after the listener drops from
/// the catalog on `Drop` (distinct from `unsubscribe`, which clears routes via refresh/internal events).
fn resolve_send_addresses<'a>(
    topic: &str,
    at_least_once_delivery: bool,
    address_topic: &'a mut HashMap<String, Vec<String>>,
    db: &mut dyn Store,
) -> ResolveAddrs<'a> {
    if address_topic.get(topic).is_some_and(|a| !a.is_empty()) {
        return ResolveAddrs::Ok(address_topic[topic].as_slice());
    }
    address_topic.remove(topic);
    let addrs = match get_address_topic(topic, db, true) {
        Ok(Some(addr)) => addr,
        Ok(None) => {
            if at_least_once_delivery {
                let listeners = match db.get_listeners_of_sender() {
                    Ok(l) => l,
                    Err(err) => return ResolveAddrs::Store(format!("{}", err)),
                };
                let mut from_listeners = Vec::new();
                for (addr, listener_topic) in listeners {
                    if listener_topic == topic {
                        from_listeners.push(addr);
                    }
                }
                if from_listeners.is_empty() {
                    return ResolveAddrs::NoAddr;
                }
                from_listeners
            } else {
                return ResolveAddrs::NoAddr;
            }
        }
        Err(err) => return ResolveAddrs::Store(err),
    };
    let slot = address_topic.entry(topic.to_string()).or_insert(addrs);
    ResolveAddrs::Ok(slot.as_slice())
}

/// Bind stays `bind`; catalog gets advertise (with port-0 rewritten from `bound`) or bound/bind.
fn compute_published_addr(
    advertise: Option<&str>,
    bound: Option<&str>,
    bind: &str,
) -> Result<String, String> {
    if let Some(adv) = advertise {
        let mut adv_sa = str_to_socket_addr(adv)
            .ok_or_else(|| format!("invalid advertise address: {}", adv))?;
        if adv_sa.port() == 0 {
            let bound = bound.ok_or_else(|| {
                "advertise port is 0 but bind did not produce a local address".to_string()
            })?;
            let bound_sa = str_to_socket_addr(bound)
                .ok_or_else(|| format!("invalid bound address: {}", bound))?;
            adv_sa.set_port(bound_sa.port());
        }
        Ok(adv_sa.to_string())
    } else {
        Ok(bound.unwrap_or(bind).to_string())
    }
}

fn str_to_socket_addr(localhost: &str) -> Option<SocketAddr> {
    match localhost.to_socket_addrs() {
        Ok(mut sa_) => sa_.next(),
        Err(err) => {
            print_error!(&format!("{}", err));
            None
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UData;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    /// Serializes tests that start listener/sender threads against a shared Redis/Postgres.
    static CLIENT_RUN_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn client_run_test_lock() -> std::sync::MutexGuard<'static, ()> {
        CLIENT_RUN_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    extern "C" fn recv_ping_flag(
        _to: *const i8,
        _from: *const i8,
        data: *const u8,
        dsize: usize,
        udata: *mut libc::c_void,
    ) {
        unsafe {
            if udata.is_null() {
                return;
            }
            let flag = &*(udata as *const AtomicBool);
            let slice = std::slice::from_raw_parts(data, dsize);
            if slice == b"ping" {
                flag.store(true, Ordering::SeqCst);
            }
        }
    }

    extern "C" fn recv_noop(
        _to: *const i8,
        _from: *const i8,
        _data: *const u8,
        _dsize: usize,
        _udata: *mut libc::c_void,
    ) {
    }

    #[test]
    fn str_to_socket_addr_rejects_invalid() {
        assert!(str_to_socket_addr("not-a-socket-addr").is_none());
    }

    #[test]
    fn str_to_socket_addr_accepts_localhost_port() {
        assert!(str_to_socket_addr("127.0.0.1:0").is_some());
        assert!(str_to_socket_addr("localhost:0").is_some());
    }

    #[test]
    fn compute_published_rewrites_advertise_port_zero() {
        let published = compute_published_addr(
            Some("127.0.0.1:0"),
            Some("127.0.0.1:34567"),
            "0.0.0.0:0",
        )
        .unwrap();
        assert_eq!(published, "127.0.0.1:34567");
    }

    #[test]
    fn run_maps_listener_startup_err_to_startup_code() {
        let _run_lock = client_run_test_lock();
        let mut c = Client::new_sqlite("u_startup", "t_startup", "127.0.0.1:0", ":memory:", "")
            .expect("sqlite client");
        let _force = crate::listener::test_force_listener_new_error();
        let ok = c.run(recv_noop, UData::null());
        assert!(!ok);
        assert_eq!(c.last_error(), ErrorCode::Startup);
        assert!(!c.is_running());
    }

    #[test]
    fn send_to_not_running_sets_last_error() {
        let mut c = Client::new_sqlite("u_err", "t", "127.0.0.1:0", ":memory:", "")
            .expect("sqlite client");
        assert!(!c.send_to("other", b"x", false));
        assert_eq!(c.last_error(), ErrorCode::NotRunning);
        assert!(!c.last_error_message().is_empty());
        assert!(c.last_error_message().contains("not is running") || c.last_error_message().contains("not running"));
    }

    #[test]
    fn send_rejects_empty_or_oversized_payload() {
        let mut c = Client::new_sqlite("u_max", "t_max", "127.0.0.1:0", ":memory:", "")
            .expect("sqlite client");
        assert!(c.run(recv_noop, UData::null()));
        assert!(!c.send_to("other", b"", false));
        assert_eq!(c.last_error(), ErrorCode::InvalidArg);
        assert!(!c.send_all("other", b"", false));
        assert_eq!(c.last_error(), ErrorCode::InvalidArg);
        let prev = crate::settings::max_message_size();
        assert!(crate::settings::set_max_message_size(64));
        let too_big = vec![0u8; 128];
        assert!(!c.send_to("other", &too_big, false));
        assert_eq!(c.last_error(), ErrorCode::InvalidArg);
        assert!(!c.send_all("other", &too_big, false));
        assert_eq!(c.last_error(), ErrorCode::InvalidArg);
        assert!(crate::settings::set_max_message_size(prev));
        assert!(c.stop());
    }

    #[test]
    fn advertise_while_running_sets_already_running() {
        let _run_lock = client_run_test_lock();
        let mut c = Client::new_sqlite("u_adv", "t_adv", "127.0.0.1:0", ":memory:", "")
            .expect("sqlite client");
        assert!(c.run(recv_noop, UData::null()));
        assert!(!c.set_advertise_addr(Some("127.0.0.1:1")));
        assert_eq!(c.last_error(), ErrorCode::AlreadyRunning);
        assert!(c.stop());
    }

    #[test]
    fn run_idempotent_clears_last_error_to_ok() {
        let _run_lock = client_run_test_lock();
        let mut c = Client::new_sqlite("u_idemp", "t_idemp", "127.0.0.1:0", ":memory:", "")
            .expect("sqlite client");
        assert!(c.run(recv_noop, UData::null()));
        let _ = c.send_to("missing", b"x", false);
        assert_ne!(c.last_error(), ErrorCode::Ok);
        assert!(c.run(recv_noop, UData::null()));
        assert_eq!(c.last_error(), ErrorCode::Ok);
        assert!(c.stop());
    }

    #[test]
    fn stop_clears_published_keeps_bound_allows_clear_and_rerun() {
        let _run_lock = client_run_test_lock();
        let path = format!(
            "{}/liner_stop_test_{}.sqlite",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let _ = std::fs::remove_file(&path);
        let mut c = Client::new_sqlite("u_stop", "t_stop", "127.0.0.1:0", &path, "")
            .expect("sqlite client");
        assert!(c.set_advertise_addr(Some("127.0.0.1:0")));
        assert!(c.run(recv_noop, UData::null()));
        assert!(c.is_running());
        let bound = c.bound_listen_addr().map(|s| s.to_string());
        assert!(bound.is_some());
        assert!(c.published_addr().is_some());
        assert_eq!(c.published_addr(), bound.as_deref());
        assert!(c.stop());
        assert!(!c.is_running());
        assert_eq!(c.bound_listen_addr(), bound.as_deref());
        assert!(c.published_addr().is_none());
        assert!(c.clear_stored_messages());
        assert_eq!(c.last_error(), ErrorCode::Ok);
        assert!(c.run(recv_noop, UData::null()));
        assert!(c.is_running());
        assert!(c.published_addr().is_some());
        assert!(c.stop());
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
    }

    #[test]
    fn advertise_host_with_ephemeral_port_published() {
        let _run_lock = client_run_test_lock();
        let path = format!(
            "{}/liner_adv_test_{}.sqlite",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let _ = std::fs::remove_file(&path);
        let mut c = Client::new_sqlite("u_adv2", "t_adv2", "0.0.0.0:0", &path, "")
            .expect("sqlite client");
        assert!(c.set_advertise_addr(Some("127.0.0.1:0")));
        assert!(c.run(recv_noop, UData::null()));
        let published = c.published_addr().unwrap().to_string();
        assert!(published.starts_with("127.0.0.1:"), "{published}");
        assert!(!published.ends_with(":0"));
        let bound = c.bound_listen_addr().unwrap();
        let bound_port = bound.rsplit(':').next().unwrap();
        assert!(published.ends_with(&format!(":{bound_port}")));
        assert!(c.stop());
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
    }

    #[test]
    fn new_sqlite_rejects_invalid_receivers_json() {
        assert!(Client::new_sqlite("u", "t", "127.0.0.1:0", ":memory:", "not-json").is_none());
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn shared_postgres_two_clients_send_to() {
        let _run_lock = client_run_test_lock();
        let Some(url) = std::env::var("LINER_TEST_POSTGRES_URL").ok() else {
            eprintln!("skip shared_postgres_two_clients_send_to: LINER_TEST_POSTGRES_URL unset");
            return;
        };
        let _pg_lock = crate::store::postgres::test_db_lock();
        crate::store::postgres::test_reset_tables_inner(&url);

        let topic_a = format!("topic_pg_a_{}", std::process::id());
        let topic_b = format!("topic_pg_b_{}", std::process::id());
        let flag = Box::new(AtomicBool::new(false));
        let raw_flag = Box::into_raw(flag);

        let mut client_a = Client::new_postgres(
            &format!("pg_a_{}", std::process::id()),
            &topic_a,
            "127.0.0.1:0",
            &url,
        )
        .expect("client_a");
        assert!(client_a.run(
            recv_ping_flag,
            UData(raw_flag as *mut libc::c_void),
        ));
        assert!(client_a.bound_listen_addr().is_some());

        let mut client_b = Client::new_postgres(
            &format!("pg_b_{}", std::process::id()),
            &topic_b,
            "127.0.0.1:0",
            &url,
        )
        .expect("client_b");
        assert!(client_b.run(recv_noop, UData::null()));
        assert!(client_b.refresh_address_topic(&topic_a));

        let mut sent = false;
        for _ in 0..400 {
            if client_b.send_to(&topic_a, b"ping", false) {
                sent = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(sent, "send_to should succeed once routes connect");

        for _ in 0..500 {
            if unsafe { (*raw_flag).load(Ordering::SeqCst) } {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            unsafe { (*raw_flag).load(Ordering::SeqCst) },
            "peer A should receive ping"
        );

        drop(client_b);
        drop(client_a);
        unsafe {
            drop(Box::from_raw(raw_flag));
        }
        crate::store::postgres::test_reset_tables_inner(&url);
    }

    #[test]
    fn isolated_sqlite_two_clients_via_receivers_json_catalog_file() {
        let _run_lock = client_run_test_lock();
        let dir = std::env::temp_dir().join(format!(
            "liner_iso_{}_{}",
            std::process::id(),
            std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_a = dir.join("a.sqlite");
        let db_b = dir.join("b.sqlite");
        let catalog_path = dir.join("catalog.json");

        let topic_a = "topic_iso_a";
        let flag = Box::new(AtomicBool::new(false));
        let raw_flag = Box::into_raw(flag);

        let mut client_a = Client::new_sqlite(
            "unique_a_iso",
            topic_a,
            "127.0.0.1:0",
            db_a.to_str().unwrap(),
            "",
        )
        .expect("client_a");

        assert!(client_a.run(
            recv_ping_flag,
            UData(raw_flag as *mut libc::c_void),
        ));

        let listen = client_a
            .bound_listen_addr()
            .expect("bound after run")
            .to_string();

        let catalog = serde_json::json!([{
            "topic": topic_a,
            "addr": listen,
            "client_name": client_a.unique_name(),
        }]);
        std::fs::write(&catalog_path, serde_json::to_string(&catalog).unwrap()).unwrap();

        let catalog = std::fs::read_to_string(&catalog_path).unwrap();
        let mut client_b = Client::new_sqlite(
            "unique_b_iso",
            "topic_iso_b",
            "127.0.0.1:0",
            db_b.to_str().unwrap(),
            &catalog,
        )
        .expect("client_b");
        assert!(client_b.run(recv_noop, UData::null()));

        let mut sent = false;
        for _ in 0..400 {
            // Isolated DB paths: listener acks live in A's file; B's sender must not use at_least_once.
            if client_b.send_to(topic_a, b"ping", false) {
                sent = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(sent, "send_to should succeed once routes connect");

        for _ in 0..500 {
            if unsafe { (*raw_flag).load(Ordering::SeqCst) } {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            unsafe { (*raw_flag).load(Ordering::SeqCst) },
            "peer A should receive"
        );

        drop(client_b);
        drop(client_a);
        unsafe {
            drop(Box::from_raw(raw_flag));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn liner_test_redis_url() -> Option<String> {
        let url = std::env::var("LINER_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
        let pid = std::process::id();
        let topic = format!("__redis_probe_{pid}");
        let mut client = Client::new_redis(
            &format!("__redis_probe_{pid}"),
            &topic,
            "127.0.0.1:0",
            &url,
        )?;
        if !client.run(recv_noop, UData::null()) {
            return None;
        }
        drop(client);
        Some(url)
    }

    #[test]
    fn shared_sqlite_send_to_fails_after_runtime_unsubscribe() {
        let _run_lock = client_run_test_lock();
        let dir = std::env::temp_dir().join(format!(
            "liner_unsub_{}_{}",
            std::process::id(),
            std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("shared.sqlite");
        let db = db_path.to_str().unwrap();
        let sub_topic = format!("topic_sub_rt_{}", std::process::id());

        let mut listener = Client::new_sqlite(
            &format!("listener_{}", std::process::id()),
            &format!("topic_l_{}", std::process::id()),
            "127.0.0.1:0",
            db,
            "",
        )
        .expect("listener");
        assert!(listener.run(recv_noop, UData::null()));
        assert!(listener.subscribe(&sub_topic));

        let mut sender = Client::new_sqlite(
            &format!("sender_{}", std::process::id()),
            &format!("topic_s_{}", std::process::id()),
            "127.0.0.1:0",
            db,
            "",
        )
        .expect("sender");
        assert!(sender.run(recv_noop, UData::null()));
        assert!(sender.refresh_address_topic(&sub_topic));
        assert!(sender.send_to(&sub_topic, b"one", true));

        assert!(listener.unsubscribe(&sub_topic));
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !sender.refresh_address_topic(&sub_topic),
            "store should have no subscribers after unsubscribe"
        );
        assert!(
            !sender.send_to(&sub_topic, b"two", true),
            "send_to should fail when topic has no subscribers"
        );

        drop(listener);
        drop(sender);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn apply_internal_channel_event_ignores_invalid_json() {
        let _run_lock = client_run_test_lock();
        let pid = std::process::id();
        let topic = format!("int_invalid_{pid}");
        let mut client = Client::new_sqlite(
            &format!("int_invalid_{pid}"),
            &topic,
            "127.0.0.1:0",
            ":memory:",
            "",
        )
        .expect("client");
        assert!(client.run(recv_noop, UData::null()));
        apply_internal_channel_event(&mut client, b"not-json");
        apply_internal_channel_event(&mut client, br#"{"topic":"x"}"#);
        drop(client);
    }

    extern "C" fn recv_track_user_cb(
        _to: *const i8,
        _from: *const i8,
        _data: *const u8,
        _dsize: usize,
        udata: *mut libc::c_void,
    ) {
        unsafe {
            if !udata.is_null() {
                (*(udata as *const AtomicBool)).store(true, Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn client_receive_wrapper_routes_internal_channel_without_user_cb() {
        let _run_lock = client_run_test_lock();
        let Some(url) = liner_test_redis_url() else {
            eprintln!("skip client_receive_wrapper_routes_internal_channel_without_user_cb: redis unavailable");
            return;
        };
        let pid = std::process::id();
        let topic = format!("int_wrap_{pid}");
        let mut client = Client::new_redis(
            &format!("int_wrap_{pid}"),
            &topic,
            "127.0.0.1:0",
            &url,
        )
        .expect("client");
        let called = Box::new(AtomicBool::new(false));
        let raw_called = Box::into_raw(called);
        assert!(client.run(
            recv_track_user_cb,
            UData(raw_called as *mut libc::c_void),
        ));

        let client_ptr = &mut client as *mut Client as *mut libc::c_void;
        let internal_to = std::ffi::CString::new(INTERNAL_CHANNEL_TOPIC).unwrap();
        let app_to = std::ffi::CString::new(topic.as_str()).unwrap();
        let from = std::ffi::CString::new("peer").unwrap();
        let internal_data =
            br#"{"event":"subscribed","client":"peer","topic":"some_topic"}"#;

        unsafe {
            (*raw_called).store(false, Ordering::SeqCst);
        }
        client_receive_wrapper(
            internal_to.as_ptr(),
            from.as_ptr(),
            internal_data.as_ptr(),
            internal_data.len(),
            client_ptr,
        );
        assert!(
            !unsafe { (*raw_called).load(Ordering::SeqCst) },
            "internal channel must not invoke user callback"
        );

        unsafe {
            (*raw_called).store(false, Ordering::SeqCst);
        }
        let app_data = b"hi";
        client_receive_wrapper(
            app_to.as_ptr(),
            from.as_ptr(),
            app_data.as_ptr(),
            app_data.len(),
            client_ptr,
        );
        assert!(
            unsafe { (*raw_called).load(Ordering::SeqCst) },
            "regular messages must still invoke user callback"
        );

        drop(client);
        unsafe {
            drop(Box::from_raw(raw_called));
        }
    }

    #[test]
    fn internal_client_connected_not_delivered_to_self() {
        let _run_lock = client_run_test_lock();
        let Some(url) = liner_test_redis_url() else {
            eprintln!("skip internal_client_connected_not_delivered_to_self: redis unavailable");
            return;
        };
        let pid = std::process::id();
        let topic = format!("int_solo_{pid}");
        let called = Box::new(AtomicBool::new(false));
        let raw_called = Box::into_raw(called);

        let mut client = Client::new_redis(
            &format!("int_solo_{pid}"),
            &topic,
            "127.0.0.1:0",
            &url,
        )
        .expect("client");
        assert!(client.run(
            recv_track_user_cb,
            UData(raw_called as *mut libc::c_void),
        ));

        for _ in 0..50 {
            if unsafe { (*raw_called).load(Ordering::SeqCst) } {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !unsafe { (*raw_called).load(Ordering::SeqCst) },
            "client_connected on run must not reach own user callback"
        );

        drop(client);
        unsafe {
            drop(Box::from_raw(raw_called));
        }
    }

    #[test]
    fn redis_internal_channel_peer_address_without_manual_refresh() {
        let _run_lock = client_run_test_lock();
        let Some(url) = liner_test_redis_url() else {
            eprintln!("skip redis_internal_channel_peer_address_without_manual_refresh: redis unavailable");
            return;
        };
        let pid = std::process::id();
        let topic_a = format!("int_peer_a_{pid}");
        let topic_b = format!("int_peer_b_{pid}");
        let flag = Box::new(AtomicBool::new(false));
        let raw_flag = Box::into_raw(flag);

        let mut client_a = Client::new_redis(
            &format!("int_peer_a_{pid}"),
            &topic_a,
            "127.0.0.1:0",
            &url,
        )
        .expect("client_a");
        assert!(client_a.run(recv_noop, UData::null()));

        let mut client_b = Client::new_redis(
            &format!("int_peer_b_{pid}"),
            &topic_b,
            "127.0.0.1:0",
            &url,
        )
        .expect("client_b");
        assert!(client_b.run(
            recv_ping_flag,
            UData(raw_flag as *mut libc::c_void),
        ));

        let mut sent = false;
        for _ in 0..400 {
            if client_a.send_to(&topic_b, b"ping", false) {
                sent = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(
            sent,
            "client_a should reach client_b via address cache updated from internal channel"
        );

        for _ in 0..500 {
            if unsafe { (*raw_flag).load(Ordering::SeqCst) } {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            unsafe { (*raw_flag).load(Ordering::SeqCst) },
            "client_b should receive ping"
        );

        drop(client_b);
        drop(client_a);
        unsafe {
            drop(Box::from_raw(raw_flag));
        }
    }

    struct StatusCapture {
        kinds: Mutex<Vec<(i32, String, String)>>,
    }

    extern "C" fn status_capture_cb(
        kind: i32,
        topic: *const i8,
        peer: *const i8,
        _message: *const i8,
        udata: *mut libc::c_void,
    ) {
        if udata.is_null() {
            return;
        }
        let topic = if topic.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(topic).to_string_lossy().into_owned() }
        };
        let peer = if peer.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(peer).to_string_lossy().into_owned() }
        };
        let cap = unsafe { &*(udata as *const StatusCapture) };
        if let Ok(mut g) = cap.kinds.lock() {
            g.push((kind, topic, peer));
        }
    }

    #[test]
    fn status_cb_skips_unrelated_peer_events() {
        let _run_lock = client_run_test_lock();
        let pid = std::process::id();
        let topic = format!("st_unrel_{pid}");
        let mut client = Client::new_sqlite(
            &format!("st_unrel_{pid}"),
            &topic,
            "127.0.0.1:0",
            ":memory:",
            "",
        )
        .expect("client");
        let capture = Box::new(StatusCapture {
            kinds: Mutex::new(Vec::new()),
        });
        let raw = Box::into_raw(capture);
        client.set_status_cb(
            Some(status_capture_cb),
            UData(raw as *mut libc::c_void),
        );
        assert!(client.run(recv_noop, UData::null()));

        let payload = br#"{"event":"client_disconnected","client":"other","topic":"not_related_topic"}"#;
        apply_internal_channel_event(&mut client, payload);
        assert!(
            unsafe { (*raw).kinds.lock().unwrap().is_empty() },
            "unrelated peer events must not reach status callback"
        );

        drop(client);
        unsafe {
            drop(Box::from_raw(raw));
        }
    }

    #[test]
    fn status_cb_emits_related_peer_disconnect() {
        let _run_lock = client_run_test_lock();
        let pid = std::process::id();
        let topic = format!("st_rel_{pid}");
        let peer_topic = format!("st_peer_{pid}");
        let mut client = Client::new_sqlite(
            &format!("st_rel_{pid}"),
            &topic,
            "127.0.0.1:0",
            ":memory:",
            "",
        )
        .expect("client");
        let capture = Box::new(StatusCapture {
            kinds: Mutex::new(Vec::new()),
        });
        let raw = Box::into_raw(capture);
        client.set_status_cb(
            Some(status_capture_cb),
            UData(raw as *mut libc::c_void),
        );
        assert!(client.run(recv_noop, UData::null()));
        client.related_topics.insert(peer_topic.clone());

        let payload = format!(
            r#"{{"event":"client_disconnected","client":"peer_x","topic":"{peer_topic}"}}"#
        );
        apply_internal_channel_event(&mut client, payload.as_bytes());
        let events = unsafe { (*raw).kinds.lock().unwrap().clone() };
        assert!(
            events.iter().any(|(k, t, p)| {
                *k == crate::LNR_PEER_DISCONNECTED && t == &peer_topic && p == "peer_x"
            }),
            "expected PEER_DISCONNECTED for related topic, got {:?}",
            events
        );

        drop(client);
        unsafe {
            drop(Box::from_raw(raw));
        }
    }

    #[test]
    fn status_cb_route_lost_on_unreachable_peer() {
        let _run_lock = client_run_test_lock();
        let dir = std::env::temp_dir().join(format!(
            "liner_route_lost_{}_{}",
            std::process::id(),
            std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("shared.sqlite");
        let db = db_path.to_str().unwrap();
        let pid = std::process::id();
        let listener_topic = format!("rl_l_{pid}");
        let sender_topic = format!("rl_s_{pid}");

        let mut listener = Client::new_sqlite(
            &format!("rl_listener_{pid}"),
            &listener_topic,
            "127.0.0.1:0",
            db,
            "",
        )
        .expect("listener");
        assert!(listener.run(recv_noop, UData::null()));
        let peer_addr = listener.bound_listen_addr().unwrap().to_string();

        let mut sender = Client::new_sqlite(
            &format!("rl_sender_{pid}"),
            &sender_topic,
            "127.0.0.1:0",
            db,
            "",
        )
        .expect("sender");
        let capture = Box::new(StatusCapture {
            kinds: Mutex::new(Vec::new()),
        });
        let raw = Box::into_raw(capture);
        sender.set_status_cb(
            Some(status_capture_cb),
            UData(raw as *mut libc::c_void),
        );
        assert!(sender.run(recv_noop, UData::null()));

        assert!(sender.refresh_address_topic(&listener_topic));
        assert!(sender.send_to(&listener_topic, b"hi", false));
        // Let the TCP stream establish before killing the peer.
        std::thread::sleep(Duration::from_millis(50));

        drop(listener);
        // Keep a cached route so send still enqueues to the dead TCP peer (Drop clears the store
        // catalog; without this, send_to fails before any write/reconnect → no SENDER_ROUTE_LOST).
        sender
            .address_topic
            .insert(listener_topic.clone(), vec![peer_addr.clone()]);
        mark_related_topic(&mut sender.related_topics, &listener_topic);

        for _ in 0..80 {
            let _ = sender.send_to(&listener_topic, b"again", false);
            let hit = unsafe {
                (*raw).kinds.lock().unwrap().iter().any(|(k, _, _)| {
                    *k == crate::LNR_SENDER_ROUTE_LOST || *k == crate::LNR_SENDER_SEND_ERROR
                })
            };
            if hit {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let events = unsafe { (*raw).kinds.lock().unwrap().clone() };
        assert!(
            events.iter().any(|(k, _, _)| {
                *k == crate::LNR_SENDER_ROUTE_LOST || *k == crate::LNR_SENDER_SEND_ERROR
            }),
            "expected SENDER_ROUTE_LOST/SENDER_SEND_ERROR after peer drop, got {:?}; peer was {}",
            events,
            peer_addr
        );

        drop(sender);
        unsafe {
            drop(Box::from_raw(raw));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shared_sqlite_list_addresses_sees_peer() {
        let _run_lock = client_run_test_lock();
        let dir = std::env::temp_dir().join(format!(
            "liner_list_addr_{}_{}",
            std::process::id(),
            std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("shared.sqlite");
        let db = db.to_str().unwrap();
        let pid = std::process::id();
        let topic_a = format!("list_a_{pid}");
        let topic_b = format!("list_b_{pid}");
        let name_a = format!("list_peer_a_{pid}");

        let mut a = Client::new_sqlite(&name_a, &topic_a, "127.0.0.1:0", db, "").expect("a");
        assert!(a.run(recv_noop, UData::null()));

        let mut b = Client::new_sqlite(
            &format!("list_peer_b_{pid}"),
            &topic_b,
            "127.0.0.1:0",
            db,
            "",
        )
        .expect("b");
        assert!(b.run(recv_noop, UData::null()));

        let rows = b.list_addresses(&topic_a).expect("list");
        assert!(
            rows.iter().any(|(addr, name)| name == &name_a && !addr.is_empty()),
            "expected peer in directory, got {:?}",
            rows
        );
        assert_eq!(b.last_error(), ErrorCode::Ok);
        let empty = b.list_addresses("topic_does_not_exist_xyz").expect("empty ok");
        assert!(empty.is_empty());

        drop(b);
        drop(a);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shared_sqlite_pending_count_after_offline_enqueue() {
        let _run_lock = client_run_test_lock();
        let dir = std::env::temp_dir().join(format!(
            "liner_pending_{}_{}",
            std::process::id(),
            std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("shared.sqlite");
        let db = db_path.to_str().unwrap();
        let pid = std::process::id();
        let listener_topic = format!("pend_l_{pid}");
        let sender_topic = format!("pend_s_{pid}");

        let mut listener = Client::new_sqlite(
            &format!("pend_listener_{pid}"),
            &listener_topic,
            "127.0.0.1:0",
            db,
            "",
        )
        .expect("listener");
        assert!(listener.run(recv_noop, UData::null()));
        let peer_addr = listener.bound_listen_addr().unwrap().to_string();

        let mut sender = Client::new_sqlite(
            &format!("pend_sender_{pid}"),
            &sender_topic,
            "127.0.0.1:0",
            db,
            "",
        )
        .expect("sender");
        assert!(sender.run(recv_noop, UData::null()));
        assert!(sender.refresh_address_topic(&listener_topic));
        assert!(sender.send_to(&listener_topic, b"warm", true));
        std::thread::sleep(Duration::from_millis(80));

        drop(listener);
        sender
            .address_topic
            .insert(listener_topic.clone(), vec![peer_addr]);
        assert!(sender.send_to(&listener_topic, b"offline-1", true));
        assert!(sender.send_to(&listener_topic, b"offline-2", true));
        // Flush in-memory at-least-once queues into the store (Sender Drop → save_mess_to_db).
        assert!(sender.stop());

        let pending = sender.pending_count().expect("pending");
        assert!(
            pending > 0,
            "expected offline queue depth > 0 after stop flush, got {pending}"
        );
        assert_eq!(sender.last_error(), ErrorCode::Ok);

        let rows = sender.pending_by_peer().expect("by peer");
        assert!(
            rows.iter().any(|(_, _, _, c)| *c > 0),
            "expected a peer row with count > 0, got {:?}",
            rows
        );
        let sum: u64 = rows.iter().map(|(_, _, _, c)| *c).sum();
        assert_eq!(sum, pending);

        drop(sender);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn list_subscriptions_excludes_internal_and_related_tracks_send() {
        let _run_lock = client_run_test_lock();
        let dir = std::env::temp_dir().join(format!(
            "liner_list_sub_{}_{}",
            std::process::id(),
            std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("shared.sqlite");
        let db = db.to_str().unwrap();
        let pid = std::process::id();
        let topic_a = format!("ls_a_{pid}");
        let topic_b = format!("ls_b_{pid}");
        let sub = format!("ls_sub_{pid}");

        let mut a = Client::new_sqlite(
            &format!("ls_a_{pid}"),
            &topic_a,
            "127.0.0.1:0",
            db,
            "",
        )
        .expect("a");
        assert!(a.run(recv_noop, UData::null()));
        assert!(a.subscribe(&sub));
        let subs = a.list_subscriptions();
        assert!(subs.contains(&sub));
        assert!(!subs.iter().any(|t| t == INTERNAL_CHANNEL_TOPIC));

        let mut b = Client::new_sqlite(
            &format!("ls_b_{pid}"),
            &topic_b,
            "127.0.0.1:0",
            db,
            "",
        )
        .expect("b");
        assert!(b.run(recv_noop, UData::null()));
        assert!(b.refresh_address_topic(&topic_a));
        assert!(b.send_to(&topic_a, b"x", false));
        let related = b.list_related_topics();
        assert!(related.contains(&topic_a));

        drop(b);
        drop(a);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn max_send_queue_busy_is_per_peer() {
        let _run_lock = client_run_test_lock();
        let _limits = crate::settings::test_limits_lock();
        let prev = crate::settings::max_send_queue();
        struct RestoreQueue(usize);
        impl Drop for RestoreQueue {
            fn drop(&mut self) {
                let _ = crate::settings::set_max_send_queue(self.0);
            }
        }
        let _restore = RestoreQueue(prev);
        assert!(crate::settings::set_max_send_queue(1));

        let dir = std::env::temp_dir().join(format!(
            "liner_busy_{}_{}",
            std::process::id(),
            std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("shared.sqlite");
        let db = db.to_str().unwrap();
        let pid = std::process::id();
        let topic_a = format!("busy_a_{pid}");
        let topic_b = format!("busy_b_{pid}");
        let topic_s = format!("busy_s_{pid}");

        let mut a = Client::new_sqlite(&format!("busy_a_{pid}"), &topic_a, "127.0.0.1:0", db, "")
            .expect("a");
        let mut b = Client::new_sqlite(&format!("busy_b_{pid}"), &topic_b, "127.0.0.1:0", db, "")
            .expect("b");

        assert!(a.run(recv_noop, UData::null()));
        assert!(b.run(recv_noop, UData::null()));
        let addr_a = a.bound_listen_addr().unwrap().to_string();
        let addr_b = b.bound_listen_addr().unwrap().to_string();

        let mut s = Client::new_sqlite(&format!("busy_s_{pid}"), &topic_s, "127.0.0.1:0", db, "")
            .expect("s");
        assert!(s.run(recv_noop, UData::null()));
        // Register in catalog then drop listeners so enqueue stays in memory.
        assert!(s.refresh_address_topic(&topic_a));
        assert!(s.refresh_address_topic(&topic_b));
        drop(a);
        drop(b);
        s.address_topic
            .insert(topic_a.clone(), vec![addr_a]);
        s.address_topic
            .insert(topic_b.clone(), vec![addr_b]);

        assert!(s.send_to(&topic_a, b"q1", false), "first enqueue to A");
        assert!(!s.send_to(&topic_a, b"q2", false), "second to A should be busy");
        assert_eq!(s.last_error(), ErrorCode::Busy);
        assert!(
            s.send_to(&topic_b, b"qb", false),
            "peer B must accept while A is at cap"
        );

        drop(s);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
