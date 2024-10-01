use crate::message::MessageForReceiver;
use crate::redis;
use crate::UCback;
use crate::listener::Listener;
use crate::sender::Sender;
use crate::print_error;

use std::net::TcpListener;
use std::sync::Mutex;
use std::{thread, sync::mpsc};
use std::collections::HashMap;
use std::thread::JoinHandle;

pub struct Client{
    unique_name: String,
    topic: String,
    db: redis::Connect,
    listener: Option<Listener>,
    sender: Option<Sender>,
    last_send_index: HashMap<String, usize>,
    is_run: bool,
    mtx: Mutex<()>,
    stream_thread: Option<JoinHandle<()>>,
    address_topic: Vec<String>,
}

impl Client {
    pub fn new(unique_name: &str, redis_path: &str) -> Option<Client> {
        let db = redis::Connect::new(unique_name, redis_path).ok()?;
        Some(
            Self{
                unique_name: unique_name.to_string(),
                topic: "".to_string(),
                db,
                listener: None,
                sender: None,
                last_send_index: HashMap::new(),
                is_run: false,
                mtx: Mutex::new(()),
                stream_thread: None,
                address_topic: Vec::new(),
            }
        )
    }
    pub fn run(&mut self, topic: &str, localhost: &str, receive_cb: UCback) -> bool {
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
        self.db.set_source_topic(topic);
        self.db.set_source_localhost(localhost);
        if let Err(err) = self.db.regist_topic(topic){
            print_error!(&format!("{}", err));
            return false;
        }
        let topic_ = topic.to_string();
        let (tx_prodr, rx_prodr) = mpsc::channel::<Vec<u8>>();
        let stream_thread = thread::spawn(move||{ 
            for data in rx_prodr.iter(){
                let mess = MessageForReceiver::new(&data);
                receive_cb(topic_.as_ptr() as *const i8, 
                        mess.topic_from, 
                        mess.uuid, 
                        mess.timestamp, 
                        mess.data, mess.data_len);
            }
        });  
        self.topic = topic.to_string();      
        self.listener = Some(Listener::new(listener, tx_prodr, self.unique_name.clone(), self.db.redis_path(), topic.to_string()));
        self.sender = Some(Sender::new(self.unique_name.clone(), self.db.redis_path(), topic.to_string()));
        self.sender.as_mut().unwrap().load_prev_connects(&mut self.db);
        self.stream_thread = Some(stream_thread);
        self.is_run = true;

        true
    }

    pub fn send_to(&mut self, topic: &str, uuid: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        let _lock = self.mtx.lock();
        if !self.is_run{
            return false;
        }
        if !get_address_topic(topic, &mut self.db, &mut self.address_topic){
            return false           
        }
        if self.address_topic.is_empty(){
            print_error!(&format!("not found addr for topic {}", topic));
            return false;
        }       
        if !self.last_send_index.contains_key(topic){
            self.last_send_index.insert(topic.to_string(), 0);
        }
        let mut index = self.last_send_index[topic];
        if index >= self.address_topic.len(){
            index = 0;
        }
        let addr = &self.address_topic[index];
        let ok = self.sender
        .as_mut().unwrap().send_to(&mut self.db, addr, 
                                topic, &self.topic, uuid, data, at_least_once_delivery);

        *self.last_send_index.get_mut(topic).unwrap() = index + 1;
        ok
    }

    pub fn send_all(&mut self, topic: &str, uuid: &str, data: &[u8], at_least_once_delivery: bool) -> bool {
        let _lock = self.mtx.lock();
        if !self.is_run{
            return false;
        }
        if !get_address_topic(topic, &mut self.db, &mut self.address_topic){
            return false           
        }
        if self.address_topic.is_empty(){
            print_error!(&format!("not found addr for topic {}", topic));
            return false;
        }       
        let mut ok = false;
        for addr in &self.address_topic{
            ok &= self.sender.as_mut().unwrap().send_to(&mut self.db, addr, 
                                    topic, &self.topic, uuid, data, at_least_once_delivery);
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
        return true;
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
        return true;
    }
}

fn get_address_topic(topic: &str, db: &mut redis::Connect, address_topic: &mut Vec<String>)->bool{
    if address_topic.is_empty(){
        let addresses = db.get_addresses_of_topic(topic);
        if let Err(err) = addresses{
            print_error!(&format!("{}", err));
            return false;           
        }
        *address_topic = addresses.unwrap();
        if address_topic.is_empty(){
            print_error!(&format!("not found addr for topic {}", topic));
            return false;
        }  
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
    
        if let Err(err) = self.stream_thread.take().unwrap().join(){
            print_error!(&format!("stream_thread.join, {:?}", err));
        }  
    }
}