use crate::redis;
use crate::UCback;
use crate::listener::Listener;
use crate::sender::Sender;
use crate::settings;
use crate::print_error;

use std::net::TcpListener;
use std::sync::Mutex;
use std::collections::HashMap;

pub struct Client{
    unique_name: String,
    topic: String,
    db: redis::Connect,
    listener: Option<Listener>,
    sender: Option<Sender>,
    last_send_index: HashMap<String, usize>,
    is_run: bool,
    mtx: Mutex<()>,
    address_topic: HashMap<String, Vec<String>>,
    prev_time: [u64; 2],
}

impl Client {
    pub fn new(unique_name: &str, topic: &str, redis_path: &str) -> Option<Client> {
        let db = redis::Connect::new(unique_name, redis_path).ok()?;
        Some(
            Self{
                unique_name: unique_name.to_string(),
                topic: topic.to_string(),
                db,
                listener: None,
                sender: None,
                last_send_index: HashMap::new(),
                is_run: false,
                mtx: Mutex::new(()),
                address_topic: HashMap::new(),
                prev_time: [0; 2]
            }
        )
    }
    pub fn run(&mut self, localhost: &str, receive_cb: UCback) -> bool {
        let _lock = self.mtx.lock();
        if self.is_run{
            return true;
        }
        let listener = TcpListener::bind(localhost);
        if let Err(err) = listener {
            print_error!(&format!("{}", err));
            return false;        
        }
        let listener = listener.unwrap();
        self.db.set_source_topic(&self.topic);
        self.db.set_source_localhost(localhost);
        if let Err(err) = self.db.regist_topic(&self.topic){
            print_error!(&format!("{}", err));
            return false;
        }        
        self.listener = Some(Listener::new(listener, &self.unique_name, &self.db.redis_path(), &self.topic, receive_cb));
        self.sender = Some(Sender::new(&self.unique_name, &self.db.redis_path(), &self.topic));
        self.sender.as_mut().unwrap().load_prev_connects(&mut self.db);
        self.is_run = true;

        true
    }

    pub fn send_to(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        let _lock = self.mtx.lock();
        if !self.is_run{
            return false;
        }
        if topic == self.topic{
            print_error!("you can't send on your own topic");
            return false;
        }
        let has_address = self.address_topic.contains_key(topic);   
        let ctime = self.sender.as_ref().unwrap().get_ctime();
        if let Some(addr) = get_address_topic(topic, &mut self.db, ctime, &mut self.prev_time[0], has_address){
            self.address_topic.insert(topic.to_string(), addr);
        }
        let address = &self.address_topic[topic];
        if address.is_empty(){
            print_error!(&format!("not found addr for topic {}", topic));
            return false;
        }  
        if !self.last_send_index.contains_key(topic){
            self.last_send_index.insert(topic.to_string(), 0);
        }
        let index = self.last_send_index.get_mut(topic).unwrap();
        if *index >= address.len(){
            *index = 0;
        }
        let addr = &address[*index];
        let ok = self.sender
        .as_mut().unwrap().send_to(&mut self.db, addr, 
                                topic, &self.topic, data, at_least_once_delivery);

        *index += 1;
        ok
    }

    pub fn send_all(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        let _lock = self.mtx.lock();
        if !self.is_run{
            return false;
        }
        if topic == self.topic{
            print_error!("you can't send on your own topic");
            return false;
        }
        let has_address = self.address_topic.contains_key(topic);   
        let ctime = self.sender.as_ref().unwrap().get_ctime();
        if let Some(addr) = get_address_topic(topic, &mut self.db, ctime, &mut self.prev_time[0], has_address){
            self.address_topic.insert(topic.to_string(), addr);
        }
        let address = &self.address_topic[topic];   
        if address.is_empty(){
            print_error!(&format!("not found addr for topic {}", topic));
            return false;
        }       
        let mut ok = false;
        for addr in address{
            ok &= self.sender.as_mut().unwrap().send_to(&mut self.db, addr, 
                                    topic, &self.topic, data, at_least_once_delivery);
        }
        ok
    }

    pub fn subscribe(&mut self, topic: &str) -> bool {
        let _lock = self.mtx.lock();
        if !self.is_run{
            return false;
        }
        if topic == self.topic{
            print_error!("you can't subscribe on your own topic");
            return false;
        }
        if let Err(err) = self.db.regist_topic(topic){
            print_error!(&format!("{}", err));
            return false;
        }        
        true
    }

    pub fn unsubscribe(&mut self, topic: &str) -> bool {
        let _lock = self.mtx.lock();
        if !self.is_run{
            return false;
        }
        if topic == self.topic{
            print_error!("you can't unsubscribe on your own topic");
            return false;
        }
        if let Err(err) = self.db.unregist_topic(topic){
            print_error!(&format!("{}", err));
            return false;
        }        
        true
    }
}

fn get_address_topic(topic: &str, db: &mut redis::Connect, ctime: u64, prev_time: &mut u64, has_address: bool)->Option<Vec<String>>{
        
    let check_new_timeout = check_new_address_topic(ctime, prev_time);
           
    if !has_address || check_new_timeout{
        match db.get_addresses_of_topic(topic){
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

fn check_new_address_topic(ctime: u64, prev_time: &mut u64)->bool{
    if ctime - *prev_time > settings::UPDATE_SENDER_ADDRESSES_TIMEOUT_MS{
        *prev_time = ctime;
        true
    }else{
        false
    }
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
