use crate::redis;
use crate::{ReceiveCbackIntern, ErrorCbackIntern, UData};
use crate::listener::Listener;
use crate::sender::Sender;
use crate::settings;
use crate::print_error;

use std::net::{SocketAddr, ToSocketAddrs};
use mio::net::TcpListener;
use std::sync::Mutex;
use std::collections::HashMap;

pub struct Client{
    unique_name: String,
    source_topic: String,
    localhost: String,
    db: redis::Connect,
    listener: Option<Listener>,
    sender: Option<Sender>,
    last_send_index: HashMap<String, usize>,
    is_run: bool,
    mtx: Mutex<()>,
    address_topic: HashMap<String, Vec<String>>,
    subscriptions: HashMap<i32, String>,
    prev_time: [u64; 2],
}

impl Client {
    pub fn new(unique_name: &str, topic: &str, localhost: &str, redis_path: &str) -> Option<Client> {
        let mut db = redis::Connect::new(unique_name, redis_path).ok()?;
        db.set_source_topic(topic);
        db.set_source_localhost(localhost);        
        Some(
            Self{
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
                subscriptions: HashMap::new(),    
                prev_time: [0; 2]
            }
        )
    }
    pub fn run(&mut self, receive_cb: ReceiveCbackIntern, error_cb: Option<ErrorCbackIntern>, udata: UData) -> bool {
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
        let tcp_listener = TcpListener::bind(sa.unwrap());
        if let Err(err) = tcp_listener {
            print_error!(&format!("{}", err));
            return false;        
        }
        self.listener = Some(Listener::new(tcp_listener.unwrap(), &self.unique_name, &self.db.redis_path(), &self.source_topic, &self.subscriptions, receive_cb, error_cb, udata));
        self.sender = Some(Sender::new(&self.unique_name, &self.db.redis_path(), &self.source_topic, error_cb));
        self.sender.as_mut().unwrap().load_prev_connects(&mut self.db);
        self.is_run = true;

        true
    }

    pub fn send_to(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        let _lock = self.mtx.lock();
        if !self.is_run{
            print_error!("you can't send_to because client not is running");
            return false;
        }
        if topic == self.source_topic{
            print_error!("you can't send on your own topic");
            return false;
        }
        let has_addr = self.address_topic.contains_key(topic);
        if !has_addr || settings::IS_CHECK_NEW_ADDRESS_TOPIC_ENABLE{ 
            let ctime = self.sender.as_ref().unwrap().get_ctime();
            if let Some(addr) = get_address_topic(topic, &mut self.db, !has_addr, ctime, &mut self.prev_time[0]){
                self.address_topic.insert(topic.to_string(), addr);
            }
        }
        if let Some(address) = self.address_topic.get(topic){       
            let index = self.last_send_index.entry(topic.to_string()).or_insert(0);
            if *index >= address.len(){
                *index = 0;
            }
            let addr = &address[*index];
            let ok = self.sender.as_mut().unwrap().send_to(&mut self.db, addr, 
                                    topic, data, at_least_once_delivery);
            *index += 1;
            ok
        }else{
            print_error!(&format!("not found addr for topic {}", topic));
            false
        }
    }

    pub fn send_all(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        let _lock = self.mtx.lock();
        if !self.is_run{
            print_error!("you can't send_all because client not is running");
            return false;
        }
        if topic == self.source_topic{
            print_error!("you can't send on your own topic");
            return false;
        }
        let has_addr = self.address_topic.contains_key(topic);
        if !has_addr || settings::IS_CHECK_NEW_ADDRESS_TOPIC_ENABLE{ 
            let ctime = self.sender.as_ref().unwrap().get_ctime();
            if let Some(addr) = get_address_topic(topic, &mut self.db, !has_addr, ctime, &mut self.prev_time[0]){
                self.address_topic.insert(topic.to_string(), addr);
            }
        }
        if let Some(address) = self.address_topic.get(topic){       
            let mut ok = true;
            for addr in address{
                ok &= self.sender.as_mut().unwrap().send_to(&mut self.db, addr, 
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

fn get_address_topic(topic: &str, db: &mut redis::Connect, force: bool, ctime: u64, prev_time: &mut u64)->Option<Vec<String>>{
    if force || check_new_address_topic_by_time(ctime, prev_time){
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
    }
    None
}

fn check_new_address_topic_by_time(ctime: u64, prev_time: &mut u64)->bool{
    if ctime - *prev_time > settings::UPDATE_SENDER_ADDRESSES_TIMEOUT_MS{
        *prev_time = ctime;
        true
    }else{
        false
    }
}

fn str_to_socket_addr(localhost: &str)->Option<SocketAddr>{
    let mut sa: Option<SocketAddr> = None;
    match localhost.to_socket_addrs() {
        Ok(sa_)=>{
            sa = Some(sa_.last().unwrap());
        }
        Err(err)=>{
            print_error!(&format!("{}", err));
        }            
    }
    sa
}

impl Drop for Client {
    fn drop(&mut self) {
        let _lock = self.mtx.lock();
        if !self.is_run{
            return;
        }
        drop(self.sender.take().unwrap());      
        drop(self.listener.take().unwrap());      
    }
}
