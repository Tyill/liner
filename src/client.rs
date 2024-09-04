use crate::redis;
use crate::topic::Topic;
use crate::message::Message;

use std::net::TcpListener;
use std::{thread, sync::mpsc};

pub struct Client{
    name: String,
    db: redis::Connect,
}

impl Client {
    pub fn new(name: &str, localhost: &str, redis_path: &str, cb: extern "C" fn(*const u8, usize)) -> Option<Client> {
        let mut db = redis::Connect::new(redis_path).ok()?;
        match db.regist_topic(name, localhost){
            Err(err)=> {
                eprintln!("Error {}:{}: {}", file!(), line!(), err);
                return None
            }
            _ =>()
        }       
        match TcpListener::bind(localhost) {
            Ok(listener) => {
                let (tx, rx) = mpsc::channel();
                thread::spawn(move|| {
                    let mut topics = Vec::new();
                    for stream in listener.incoming(){
                        match stream {
                            Ok(stream)=>topics.push(Topic::new_for_read(stream, tx.clone())),
                            Err(err)=>eprintln!("Error {}:{}: {}", file!(), line!(), err)
                        }
                    }
                });
                thread::spawn(move|| loop{ 
                    match rx.recv() {
                        Ok(r)=>(),//cb(&[0;1], 123),
                        Err(err)=>eprintln!("Error {}:{}: {}", file!(), line!(), err)
                    }
                });              
                Some(Self{
                    name: name.to_string(),
                    db,
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

