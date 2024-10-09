use crate::redis;
use crate::UCback;
use crate::listener::Listener;
use crate::sender::Sender;
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
        if !get_address_topic(topic, &mut self.db, &mut self.address_topic){
            return false           
        }
        let address = &self.address_topic[topic];   
        if address.is_empty(){
            print_error!(&format!("not found addr for topic {}", topic));
            return false;
        }  
        if !self.last_send_index.contains_key(topic){
            self.last_send_index.insert(topic.to_string(), 0);
        }
        let mut index = self.last_send_index[topic];
        if index >= address.len(){
            index = 0;
        }
        let addr = &address[index];
        let ok = self.sender
        .as_mut().unwrap().send_to(&mut self.db, addr, 
                                topic, &self.topic, data, at_least_once_delivery);

        *self.last_send_index.get_mut(topic).unwrap() = index + 1;
        ok
    }

    pub fn send_all(&mut self, topic: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        let _lock = self.mtx.lock();
        if !self.is_run{
            return false;
        }
        if !get_address_topic(topic, &mut self.db, &mut self.address_topic){
            return false           
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

fn get_address_topic(topic: &str, db: &mut redis::Connect, address_topic: &mut HashMap<String, Vec<String>>)->bool{
    if !address_topic.contains_key(topic) || address_topic[topic].is_empty(){
        let addresses = db.get_addresses_of_topic(topic);
        if let Err(err) = addresses{
            print_error!(&format!("{}", err));
            return false;           
        }
        address_topic.insert(topic.to_string(), addresses.unwrap());
    }
    true
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