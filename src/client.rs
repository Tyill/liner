use crate::store::Store;
use crate::{UCbackIntern, UData};
use crate::listener::Listener;
use crate::sender::Sender;
use crate::print_error;
use crate::settings::INTERNAL_CHANNEL_TOPIC;
use crate::status::{
    StatusCbackIntern, StatusEmitter, LNR_PEER_CONNECTED, LNR_PEER_DISCONNECTED,
    LNR_PEER_SUBSCRIBED, LNR_PEER_UNSUBSCRIBED,
};

use std::net::{SocketAddr, ToSocketAddrs};
use std::ffi::CStr;
use mio::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};

pub struct Client{
    unique_name: String,
    source_topic: String,
    localhost: String,
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
    /// Actual `SocketAddr` after `run` binds `localhost` (e.g. when port is `0`).
    bound_listen_addr: Option<String>,
    user_receive_cb: Option<UCbackIntern>,
    user_receive_udata: UData,
    status_emitter: StatusEmitter,
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
        Some(Self {
            unique_name: unique_name.to_string(),
            source_topic: topic.to_string(),
            localhost: localhost.to_string(),
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
            user_receive_cb: None,
            user_receive_udata: UData::null(),
            status_emitter: StatusEmitter::new(),
        })
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
        Some(Self {
            unique_name: unique_name.to_string(),
            source_topic: topic.to_string(),
            localhost: localhost.to_string(),
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
            user_receive_cb: None,
            user_receive_udata: UData::null(),
            status_emitter: StatusEmitter::new(),
        })
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
        Some(Self {
            unique_name: unique_name.to_string(),
            source_topic: topic.to_string(),
            localhost: localhost.to_string(),
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
            user_receive_cb: None,
            user_receive_udata: UData::null(),
            status_emitter: StatusEmitter::new(),
        })
    }

    pub fn unique_name(&self) -> &str {
        &self.unique_name
    }

    /// After [`Client::run`], the resolved bind address if `localhost` used port `0`.
    pub fn bound_listen_addr(&self) -> Option<&str> {
        self.bound_listen_addr.as_deref()
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
        if self.is_run{
            print_error!("client already is running");
            return true;
        }
        let sa = str_to_socket_addr(&self.localhost);
        if sa.is_none(){
            return false;
        }
        let tcp_listener = match TcpListener::bind(sa.unwrap()) {
            Ok(l) => l,
            Err(err) => {
                print_error!(&format!("{}", err));
                return false;
            }
        };
        self.bound_listen_addr = tcp_listener.local_addr().ok().map(|a| a.to_string());
        // Register only after bind so port-0 peers never see `host:0`.
        {
            let mut db = self.db.lock().unwrap();
            if let Some(bound) = self.bound_listen_addr.clone() {
                if bound != self.localhost {
                    self.localhost = bound.clone();
                }
                db.set_source_localhost(&self.localhost);
            }
            if let Err(err) = db.regist_topic(&self.source_topic){
                print_error!(&format!("{}", err));
                return false;
            }
        }
        self.user_receive_cb = Some(receive_cb);
        self.user_receive_udata = udata;
        self.listener = Some(Listener::new(
            tcp_listener,
            self.db.clone(),
            &self.source_topic,
            &self.subscriptions,
            client_receive_wrapper,
            UData(client_ptr as *mut libc::c_void),
            self.status_emitter.clone(),
        ));
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
            drop(self.listener.take());
            drop(self.sender.take());
            return false;
        }
        emit_internal_event(
            self.is_run,
            &self.unique_name,
            &self.source_topic,
            self.bound_listen_addr.as_deref(),
            &mut self.address_topic,
            &self.db,
            self.sender.as_mut().unwrap(),
            "client_connected",
            None,
        );

        true
    }

    pub fn send_to(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        // Hold mtx for route + ensure + enqueue so concurrent FFI calls stay serialized
        // (see docs/using-the-api.md). Store is still only locked briefly in ensure_send_route.
        let _lock = self.mtx.lock().unwrap();
        if !self.is_run {
            print_error!("you can't send_to because client not is running");
            return false;
        }
        if topic == self.source_topic {
            print_error!("you can't send on your own topic");
            return false;
        }
        apply_failed_routes(&mut self.address_topic, self.sender.as_mut());
        // Resolve routes first so round-robin can borrow the address without cloning.
        if self
            .address_topic
            .get(topic)
            .map(|a| a.is_empty())
            .unwrap_or(true)
        {
            if resolve_send_addresses(
                topic,
                at_least_once_delivery,
                &mut self.address_topic,
                &mut *self.db.lock().unwrap(),
            )
            .is_none()
            {
                self.address_topic.remove(topic);
                return false;
            }
        }
        let addr_len = self.address_topic.get(topic).map(|a| a.len()).unwrap_or(0);
        if addr_len == 0 {
            return false;
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
                return false;
            }
        }
        sender.send_to(addr, topic, data, at_least_once_delivery)
    }

    pub fn send_all(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        let _lock = self.mtx.lock().unwrap();
        if !self.is_run {
            print_error!("you can't send_all because client not is running");
            return false;
        }
        if topic == self.source_topic {
            print_error!("you can't send on your own topic");
            return false;
        }
        apply_failed_routes(&mut self.address_topic, self.sender.as_mut());
        let addrs = if let Some(cached) = self
            .address_topic
            .get(topic)
            .filter(|a| !a.is_empty())
        {
            cached.to_vec()
        } else {
            let Some(address) = resolve_send_addresses(
                topic,
                at_least_once_delivery,
                &mut self.address_topic,
                &mut *self.db.lock().unwrap(),
            ) else {
                self.address_topic.remove(topic);
                return false;
            };
            address.to_vec()
        };
        mark_related_topic(&mut self.related_topics, topic);
        let sender = self.sender.as_mut().unwrap();
        let mut warm_ok = vec![true; addrs.len()];
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
        for (i, addr) in addrs.iter().enumerate() {
            if !warm_ok[i] {
                ok = false;
                continue;
            }
            ok &= sender.send_to(addr, topic, data, at_least_once_delivery);
        }
        ok
    }

    pub fn subscribe(&mut self, topic: &str) -> bool {
        let _lock = self.mtx.lock();
        if topic == self.source_topic{
            print_error!("you can't subscribe on your own topic");
            return false;
        }
        if topic == INTERNAL_CHANNEL_TOPIC {
            print_error!("you can't subscribe on internal channel topic");
            return false;
        }
        if !subscribe_inner(
            topic,
            &self.source_topic,
            &mut *self.db.lock().unwrap(),
            self.is_run,
            &mut self.listener,
            &mut self.subscriptions,
        ) {
            return false;
        }
        mark_related_topic(&mut self.related_topics, topic);
        if self.is_run {
            emit_internal_event(
                self.is_run,
                &self.unique_name,
                &self.source_topic,
                self.bound_listen_addr.as_deref(),
                &mut self.address_topic,
                &self.db,
                self.sender.as_mut().unwrap(),
                "subscribed",
                Some(topic),
            );
        }
        true
    }

    pub fn unsubscribe(&mut self, topic: &str) -> bool {
        let _lock = self.mtx.lock();
        if topic == self.source_topic{
            print_error!("you can't unsubscribe on your own topic");
            return false;
        }
        if topic == INTERNAL_CHANNEL_TOPIC {
            print_error!("you can't unsubscribe on internal channel topic");
            return false;
        }
        if !unsubscribe_inner(
            topic,
            &self.source_topic,
            &mut *self.db.lock().unwrap(),
            self.is_run,
            &mut self.listener,
            &mut self.subscriptions,
        ) {
            return false;
        }
        if self.is_run {
            emit_internal_event(
                self.is_run,
                &self.unique_name,
                &self.source_topic,
                self.bound_listen_addr.as_deref(),
                &mut self.address_topic,
                &self.db,
                self.sender.as_mut().unwrap(),
                "unsubscribed",
                Some(topic),
            );
        }
        true
    }

    pub fn refresh_address_topic(&mut self, topic: &str) -> bool {
        let _lock = self.mtx.lock();
        let ok =
            refresh_address_topic_cache(&mut self.address_topic, &mut *self.db.lock().unwrap(), topic);
        if ok {
            mark_related_topic(&mut self.related_topics, topic);
        }
        ok
    }

    pub fn clear_stored_messages(&mut self) -> bool {
        let _lock = self.mtx.lock();
        if self.is_run{
            print_error!("you can't clear_stored_messages because client already is running");
            return false;
        }
        if let Err(err) = self.db.lock().unwrap().clear_stored_messages(){
            print_error!(&format!("{}", err));
            return false;
        }
        true
    }
    pub fn clear_addresses_of_topic(&mut self) -> bool {
        let _lock = self.mtx.lock();
        if self.is_run{
            print_error!("you can't clear_addresses_of_topic because client already is running");
            return false;
        }
        if let Err(err) = self.db.lock().unwrap().clear_addresses_of_topic(){
            print_error!(&format!("{}", err));
            return false;
        }
        true
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
        if let Some(addr) = get_address_topic(INTERNAL_CHANNEL_TOPIC, &mut *db, true) {
            address_topic.insert(INTERNAL_CHANNEL_TOPIC.to_string(), addr);
        }
        let Some(address) = resolve_send_addresses(
            INTERNAL_CHANNEL_TOPIC,
            false,
            address_topic,
            &mut *db,
        ) else {
            address_topic.remove(INTERNAL_CHANNEL_TOPIC);
            return;
        };
        address.to_vec()
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
        refresh_address_topic_cache(
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
        refresh_address_topic_cache(
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
) -> bool {
    if let Some(addr) = get_address_topic(topic, db, true) {
        address_topic.insert(topic.to_string(), addr);
        true
    } else {
        address_topic.remove(topic);
        false
    }
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

fn get_address_topic(topic: &str, db: &mut dyn Store, without_cache: bool) -> Option<Vec<String>> {
    match db.get_addresses_of_topic(without_cache, topic){
        Ok(addresses)=>{
            if !addresses.is_empty(){
                return Some(addresses);
            }
        },
        Err(err)=>{
            print_error!(&format!("{}", err));
        }
    }
    None
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
) -> Option<&'a [String]> {
    if address_topic.get(topic).is_some_and(|a| !a.is_empty()) {
        return Some(address_topic[topic].as_slice());
    }
    address_topic.remove(topic);
    let addrs = if let Some(addr) = get_address_topic(topic, db, true) {
        addr
    } else if at_least_once_delivery {
        let Ok(listeners) = db.get_listeners_of_sender() else {
            return None;
        };
        let addrs: Vec<String> = listeners
            .into_iter()
            .filter(|(_, listener_topic)| listener_topic == topic)
            .map(|(addr, _)| addr)
            .collect();
        if addrs.is_empty() {
            return None;
        }
        addrs
    } else {
        return None;
    };
    let slot = address_topic.entry(topic.to_string()).or_insert(addrs);
    Some(slot.as_slice())
}

fn str_to_socket_addr(localhost: &str)->Option<SocketAddr>{
    match localhost.to_socket_addrs() {
        Ok(mut sa_)=>{
            sa_.next()
        }
        Err(err)=>{
            print_error!(&format!("{}", err));
            None
        }            
    }    
}

impl Drop for Client {
    fn drop(&mut self) {
        let (listener, sender) = {
            let _lock = self.mtx.lock();
            if !self.is_run {
                return;
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
                    self.bound_listen_addr.as_deref(),
                    &mut self.address_topic,
                    &self.db,
                    sender,
                    "client_disconnected",
                    None,
                );
            }
            self.is_run = false;
            (self.listener.take(), self.sender.take())
        };
        // Join threads outside Client.mtx — receive path also takes that lock.
        drop(listener);
        drop(sender);
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
}
