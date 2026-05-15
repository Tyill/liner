use crate::store::Store;
use crate::{UCbackIntern, UData};
use crate::listener::Listener;
use crate::sender::Sender;
use crate::print_error;

use std::net::{SocketAddr, ToSocketAddrs};
use mio::net::TcpListener;
use std::sync::Mutex;
use std::collections::HashMap;

pub struct Client{
    unique_name: String,
    source_topic: String,
    localhost: String,
    store_backend: crate::store::StoreBackend,
    db: Box<dyn Store>,
    listener: Option<Listener>,
    sender: Option<Sender>,
    last_send_index: HashMap<String, usize>,
    is_run: bool,
    mtx: Mutex<()>,
    address_topic: HashMap<String, Vec<String>>,
    subscriptions: HashMap<i32, String>,
    /// Actual `SocketAddr` after `run` binds `localhost` (e.g. when port is `0`).
    bound_listen_addr: Option<String>,
}

impl Client {
    pub fn new_redis(unique_name: &str, topic: &str, localhost: &str, redis_url: &str) -> Option<Client> {
        let store_backend = crate::store::StoreBackend::Redis {
            url: redis_url.to_string(),
        };
        let mut db = crate::store::open_store(unique_name, store_backend.clone()).ok()?;
        db.set_source_topic(topic);
        db.set_source_localhost(localhost);
        Some(Self {
            unique_name: unique_name.to_string(),
            source_topic: topic.to_string(),
            localhost: localhost.to_string(),
            store_backend,
            db,
            listener: None,
            sender: None,
            last_send_index: HashMap::new(),
            is_run: false,
            mtx: Mutex::new(()),
            address_topic: HashMap::new(),
            subscriptions: HashMap::new(),
            bound_listen_addr: None,
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
        let mut db = crate::store::open_store(unique_name, store_backend.clone()).ok()?;
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
        Some(Self {
            unique_name: unique_name.to_string(),
            source_topic: topic.to_string(),
            localhost: localhost.to_string(),
            store_backend,
            db,
            listener: None,
            sender: None,
            last_send_index: HashMap::new(),
            is_run: false,
            mtx: Mutex::new(()),
            address_topic: HashMap::new(),
            subscriptions: HashMap::new(),
            bound_listen_addr: None,
        })
    }

    pub fn unique_name(&self) -> &str {
        &self.unique_name
    }

    /// After [`Client::run`], the resolved bind address if `localhost` used port `0`.
    pub fn bound_listen_addr(&self) -> Option<&str> {
        self.bound_listen_addr.as_deref()
    }

    /// Backward compatible: same as [`Client::new_redis`].
    pub fn new(unique_name: &str, topic: &str, localhost: &str, redis_path: &str) -> Option<Client> {
        Self::new_redis(unique_name, topic, localhost, redis_path)
    }
    pub fn run(&mut self, receive_cb: UCbackIntern, udata: UData) -> bool {
        let _lock = self.mtx.lock();
        if self.is_run{
            print_error!("client already is running");
            return true;
        }
        if let Err(err) = self.db.regist_topic(&self.source_topic){
            print_error!(&format!("{}", err));
            return false;
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
        self.listener = Some(Listener::new(
            tcp_listener,
            &self.unique_name,
            self.store_backend.clone(),
            &self.source_topic,
            &self.subscriptions,
            receive_cb,
            udata,
        ));
        self.sender = Some(Sender::new(
            &self.unique_name,
            self.store_backend.clone(),
            &self.source_topic,
        ));
        if let Some(sender) = self.sender.as_mut() {
            sender.load_prev_connects(self.db.as_mut());
        }
        self.is_run = true;

        true
    }

    pub fn send_to(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        let _lock = self.mtx.lock().unwrap();
        if !self.is_run{
            print_error!("you can't send_to because client not is running");
            return false;
        }
        if topic == self.source_topic{
            print_error!("you can't send on your own topic");
            return false;
        }
        if !self.address_topic.contains_key(topic){ 
            if let Some(addr) = get_address_topic(topic, self.db.as_mut()){
                self.address_topic.insert(topic.to_string(), addr);
            }
        }
        if let Some(address) = self.address_topic.get(topic){       
            let index = self.last_send_index.entry(topic.to_string()).or_insert(0);
            if *index >= address.len(){
                *index = 0;
            }
            let addr = &address[*index];
            let ok = self.sender.as_mut().unwrap().send_to(self.db.as_mut(), addr, 
                                    topic, data, at_least_once_delivery);
            *index += 1;
            ok
        }else{
            print_error!(&format!("not found addr for topic {}", topic));
            false
        }
    }

    pub fn send_all(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        let _lock = self.mtx.lock().unwrap();
        if !self.is_run{
            print_error!("you can't send_all because client not is running");
            return false;
        }
        if topic == self.source_topic{
            print_error!("you can't send on your own topic");
            return false;
        }
        if !self.address_topic.contains_key(topic){ 
            if let Some(addr) = get_address_topic(topic, self.db.as_mut()){
                self.address_topic.insert(topic.to_string(), addr);
            }
        }
        if let Some(address) = self.address_topic.get(topic){       
            let mut ok = true;
            for addr in address{
                ok &= self.sender.as_mut().unwrap().send_to(self.db.as_mut(), addr, 
                                        topic, data, at_least_once_delivery);
            }
            ok
        }else{
            print_error!(&format!("not found addr for topic {}", topic));
            false
        }
    }

    pub fn subscribe(&mut self, topic: &str) -> bool {
        let _lock = self.mtx.lock();
        if topic == self.source_topic{
            print_error!("you can't subscribe on your own topic");
            return false;
        }
        if let Err(err) = self.db.regist_topic(topic){
            print_error!(&format!("{}", err));
            return false;
        }
        match self.db.get_topic_key(topic) {
            Ok(topic_key)=>{
                if self.is_run{ 
                    self.listener.as_mut().unwrap().subscribe(topic, topic_key);
                }else{
                    self.subscriptions.insert(topic_key, topic.to_owned());
                }
            },
            Err(err)=>{
                print_error!(&format!("{}", err));
                return false;
            }
        } 
        true
    }

    pub fn unsubscribe(&mut self, topic: &str) -> bool {
        let _lock = self.mtx.lock();
        if topic == self.source_topic{
            print_error!("you can't unsubscribe on your own topic");
            return false;
        }
        if let Err(err) = self.db.unregist_topic(topic){
            print_error!(&format!("{}", err));
            return false;
        } 
        match self.db.get_topic_key(topic) {
            Ok(topic_key)=>{
                if self.is_run{                   
                    self.listener.as_mut().unwrap().unsubscribe(topic_key);
                }else{
                    self.subscriptions.remove(&topic_key);
                }
            },
            Err(err)=>{
                print_error!(&format!("{}", err));
                return false;
            }
        } 
        true
    }

    pub fn refresh_address_topic(&mut self, topic: &str) -> bool {
        let _lock = self.mtx.lock();
        
        if let Some(addr) = get_address_topic(topic, self.db.as_mut()){
            self.address_topic.insert(topic.to_string(), addr);
            true
        } else {
            false
        }
    }

    pub fn clear_stored_messages(&mut self) -> bool {
        let _lock = self.mtx.lock();
        if self.is_run{
            print_error!("you can't clear_stored_messages because client already is running");
            return false;
        }
        if let Err(err) = self.db.clear_stored_messages(){
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
        if let Err(err) = self.db.clear_addresses_of_topic(){
            print_error!(&format!("{}", err));
            return false;
        }
        true
    }
}

fn get_address_topic(topic: &str, db: &mut dyn Store) -> Option<Vec<String>> {
    match db.get_addresses_of_topic(true, topic){
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
        let _lock = self.mtx.lock();
        if !self.is_run{
            return;
        }
        drop(self.sender.take());
        drop(self.listener.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UData;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

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

    #[test]
    fn isolated_sqlite_two_clients_via_receivers_json_catalog_file() {
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
            if client_b.send_to(topic_a, b"ping", true) {
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
}
