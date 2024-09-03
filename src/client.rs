use crate::redis;
use crate::topic::Topic;

use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::os::raw::c_void;
use std::thread;
use std::collections::HashMap;

pub struct Client{
    name: String,
    db: redis::Connect,
    thr: Option<thread::JoinHandle<()>>,
    topics: HashMap<String, Topic>,
}

impl Client {
    pub fn new(name: &str, localhost: &str, redis_path: &str) -> Option<Client> {
        let mut db = redis::Connect::new(redis_path).ok()?;
        match db.regist_topic(name, localhost){
            Err(err)=> {
                eprintln!("Error {}:{}: {}", file!(), line!(), err);
                return None
            }
            _ =>()
        }       
        let listener = TcpListener::bind(localhost);
        match listener {
            Ok(listener) => {
                let mut slf = Self {
                    name : name.to_string(),
                    db,
                    thr: None,
                    topics: HashMap::new()
                };
                let thr = thread::spawn(move|| {
                    for stream in listener.incoming(){
                        slf.handle_connection(stream.unwrap());
                    }                    
                });
                Some(slf)
            }
            Err(err) => {
                eprintln!("Error {}:{}: {}", file!(), line!(), err);
                None
            }
        }
    }

    pub fn send_to(&mut self, name: &str, data: &[u8]) -> bool {

        let addr = self.db.get_topic_addr(name);
        match addr{
            Err(err)=> {
                eprintln!("Error {}:{}: {}", file!(), line!(), err);
                return false
            }
            _ =>()
        }
        let addr = addr.unwrap();
        if addr.len() == 0{
            eprintln!("Error not found addr for topic {}", name);
            return false
        }
        
        return false
    }

    fn handle_connection(&mut self, mut stream: TcpStream) {
        
    }
}

