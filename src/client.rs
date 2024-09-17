use crate::message::Message;
use crate::redis;
use crate::UCback;
use crate::epoll_listener::EPollListener;
use crate::epoll_sender::EPollSender;
use crate::print_error;

use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::{thread, sync::mpsc};
use std::collections::HashMap;

pub struct Client{
    name: String,
    localhost: String,
    db: Arc<Mutex<redis::Connect>>,
    epoll_listener: Option<EPollListener>,
    epoll_sender: Option<EPollSender>,
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
                localhost: "".to_string(),
                db: Arc::new(Mutex::new(db)),
                epoll_listener: None,
                epoll_sender: None,
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
            print_error(&format!("Error {}:{}: {}", file!(), line!(), err));
            return false;        
        }
        let listener = listener.unwrap();
        self.localhost = listener.local_addr().unwrap().to_string();
        if let Err(err) = self.db.lock().unwrap().regist_topic(&self.name, &self.localhost){
            print_error(&format!("Error {}:{}: {}", file!(), line!(), err));
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
        self.epoll_listener = Some(EPollListener::new(listener, tx_prodr, self.db.clone()));
        self.epoll_sender = Some(EPollSender::new(self.db.clone()));
        self.is_run = true;

        return true;
    }

    pub fn send_to(&mut self, to: &str, uuid: &str, data: &[u8]) -> bool {
        let _lock = self.mtx.lock();
        if !self.is_run{
            return false;
        }
        let addresses = self.db.lock().unwrap().get_topic_addresses(to);
        if let Err(err) = addresses{
            print_error(&format!("Error {}:{}: {}", file!(), line!(), err));
            return false           
        }
        let addresses = addresses.unwrap();
        if addresses.len() == 0{
            print_error(&format!("Error not found addr for topic {}", to));
            return false;
        }       
        if !self.last_send_index.contains_key(to){
            self.last_send_index.insert(to.to_string(), 0);
        }
        let mut index = self.last_send_index[to];
        if index >= addresses.len(){
            index = 0;
        }
        let addr = &addresses[index];
        let ok = self.epoll_sender.as_mut().unwrap().send_to(addr, to, &self.name, uuid, data);

        *self.last_send_index.get_mut(to).unwrap() = index + 1;
        return ok;
    }
}

