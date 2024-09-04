use crate::redis;
use crate::topic::Topic;
use crate::message::Message;

use std::net::{TcpListener, TcpStream};
use std::{thread, sync::{Arc, Mutex, mpsc::{self, Receiver}}};
use std::collections::HashMap;

pub struct Client{
    name: String,
    db: redis::Connect,
    topics_receive: Arc<Mutex<Vec<Topic>>>,
    topics_send: Arc<Mutex<HashMap<String, Topic>>>,
    rx: Receiver<Message>,
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
                let (tx, rx) = mpsc::channel();
                let topics = Arc::new(Mutex::new(Vec::new()));
                let topics_receive = topics.clone();
                thread::spawn(move|| {
                    for stream in listener.incoming(){
                        match stream {
                            Ok(stream)=>topics.lock().unwrap().push(Topic::new_for_read(stream, tx.clone())),
                            Err(err)=>eprintln!("Error {}:{}: {}", file!(), line!(), err)
                        }
                    }
                });                
                Some(Self{
                    name: name.to_string(),
                    db,
                    topics_receive,
                    topics_send: Arc::new(Mutex::new(HashMap::new())),
                    rx,
                })
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
}

