use crate::message::Message;
use crate::redis;
use crate::topic::Topic;
use crate::UCback;
use crate::epoll_listener::EPollListener;

use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::{thread, sync::mpsc};
use std::collections::HashMap;

pub struct Client{
    name: String,
    db: Arc<Mutex<redis::Connect>>,
    epoll_listener: Option<EPollListener>,
    consumers: HashMap<String, Topic>,
    last_send_index: HashMap<String, usize>,
    is_run: bool,
    mtx: Mutex<()>,
}

impl Client {
    pub fn new(name: &str, redis_path: &str) -> Option<Client> {
        let db = redis::Connect::new(redis_path).ok()?;
        Some(
            Self{
                name: name.to_string(),
                db: Arc::new(Mutex::new(db)),
                epoll_listener: None,
                consumers: HashMap::new(),
                last_send_index: HashMap::new(),
                is_run: false,
                mtx: Mutex::new(()),
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
            eprintln!("Error {}:{}: {}", file!(), line!(), err);
            return false;        
        }
        if let Err(err) = self.db.lock().unwrap().regist_topic(&self.name, localhost){
            eprintln!("Error {}:{}: {}", file!(), line!(), err);
            return false;
        }
        let (tx_prodr, rx_prodr) = mpsc::channel::<Message>();
        thread::spawn(move||{ 
            for m in rx_prodr.iter(){
                receive_cb(m.to.as_ptr() as *const i8,
                           m.from.as_ptr() as *const i8, 
                           m.uuid.as_ptr() as *const i8, m.timestamp, 
                           m.data.as_ptr(), m.data.len());
            }
        });
        self.epoll_listener = Some(EPollListener::new(listener.unwrap(), tx_prodr, &self.db));
        self.is_run = true;

        return true;
    }

    pub fn send_to(&mut self, to: &str, uuid: &str, data: &[u8]) -> bool {
        let _lock = self.mtx.lock();

        let addresses = self.db.lock().unwrap().get_topic_addresses(to);
        if let Err(err) = addresses{
            eprintln!("Error {}:{}: {}", file!(), line!(), err);
            return false           
        }
        let addresses = addresses.unwrap();
        if addresses.len() == 0{
            eprintln!("Error not found addr for topic {}", to);
            return false;
        }       
        for addr in &addresses{
            if !self.consumers.contains_key(addr){
                self.consumers.insert(addr.clone(), Topic::new(addr, &self.db));
            }            
        }
        if !self.last_send_index.contains_key(to){
            self.last_send_index.insert(to.to_string(), 0);
        }
        let mut index = self.last_send_index[to];
        if index >= addresses.len(){
            index = 0;
        }
        let addr = &addresses[index];
        self.consumers[addr].send_to(to, &self.name, uuid, data);

        *self.last_send_index.get_mut(to).unwrap() = index + 1;
        return true;
    }
}

