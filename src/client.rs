use crate::message::Message;
use crate::redis;
use crate::UCback;
use crate::listener::Listener;
use crate::sender::Sender;
use crate::print_error;

use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::{thread, sync::mpsc};
use std::collections::HashMap;

pub struct Client{
    unique_name: String,
    topic: String,
    localhost: String,
    db: Arc<Mutex<redis::Connect>>,
    listener: Option<Listener>,
    sender: Option<Sender>,
    last_send_index: HashMap<String, usize>,
    is_run: bool,
    mtx: Mutex<()>,
}

impl Client {
    pub fn new(name: &str, redis_path: &str) -> Option<Client> {
        let db = redis::Connect::new(redis_path).ok()?;
        Some(
            Self{
                unique_name: name.to_string(),
                topic: "".to_string(),
                localhost: "".to_string(),
                db: Arc::new(Mutex::new(db)),
                listener: None,
                sender: None,
                last_send_index: HashMap::new(),
                is_run: false,
                mtx: Mutex::new(()),
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
            print_error(&format!("{}", err), file!(), line!());
            return false;        
        }
        let listener = listener.unwrap();
        self.localhost = listener.local_addr().unwrap().to_string();
        if let Err(err) = self.db.lock().unwrap().regist_topic(topic, &self.localhost){
            print_error(&format!("{}", err), file!(), line!());
            return false;
        }
        let (tx_prodr, rx_prodr) = mpsc::channel::<Message>();
        thread::spawn(move||{ 
            for m in rx_prodr.iter(){
                receive_cb(m.to.as_ptr() as *const i8,
                           m.from.as_ptr() as *const i8, 
                           m.uuid.as_ptr() as *const i8, 
                           m.timestamp, 
                           m.data.as_ptr(), m.data.len());
            }
        });  
        self.topic = topic.to_string();      
        self.listener = Some(Listener::new(listener, tx_prodr, self.db.clone()));
        self.sender = Some(Sender::new(self.db.clone()));
        self.is_run = true;

        return true;
    }

    pub fn send_to(&mut self, topic: &str, uuid: &str, data: &[u8]) -> bool {
        let _lock = self.mtx.lock();
        if !self.is_run{
            return false;
        }
        let addresses = self.db.lock().unwrap().get_topic_addresses(topic);
        if let Err(err) = addresses{
            print_error(&format!("{}", err), file!(), line!());
            return false           
        }
        let addresses = addresses.unwrap();
        if addresses.len() == 0{
            print_error(&format!("not found addr for topic {}", topic), file!(), line!());
            return false;
        }       
        if !self.last_send_index.contains_key(topic){
            self.last_send_index.insert(topic.to_string(), 0);
        }
        let mut index = self.last_send_index[topic];
        if index >= addresses.len(){
            index = 0;
        }
        let addr = &addresses[index];
        let ok = self.sender.as_mut().unwrap().send_to(addr, topic, &self.topic, uuid, data);

        *self.last_send_index.get_mut(topic).unwrap() = index + 1;
        return ok;
    }
}

