use crate::redis;
use crate::topic::Topic;
use crate::UCback;

use std::net::{TcpListener, TcpStream};
use std::{thread, sync::mpsc};
use std::collections::HashMap;

pub struct Client{
    name: String,
    db: redis::Connect,
    producers: HashMap<String, Topic>,
    is_on_receive: bool
}

impl Client {
    pub fn new(name: &str, redis_path: &str) -> Option<Client> {
        let db = redis::Connect::new(redis_path).ok()?;
        Some(
            Self{
                name: name.to_string(),
                db,
                producers: HashMap::new(),
                is_on_receive: false
            }
        )
    }

    pub fn run(&mut self, localhost: &str, cb: UCback) -> bool {
        if self.is_on_receive{
            return true;
        }
        let listener = TcpListener::bind(localhost);
        if let Err(err) = listener {
            eprintln!("Error {}:{}: {}", file!(), line!(), err);
            return false;        
        }
        if let Err(err) = self.db.regist_topic(&self.name, localhost){
            eprintln!("Error {}:{}: {}", file!(), line!(), err);
            return false;
        }
        let listener = listener.unwrap();
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
        self.is_on_receive = true;
        return true;
    }

    pub fn send_to(&mut self, to: &str, uuid: &str, data: &[u8]) -> bool {
        let addrs = self.db.get_topic_addr(to);
        if let Err(err) = addrs{
            eprintln!("Error {}:{}: {}", file!(), line!(), err);
            return false           
        }
        let addrs = addrs.unwrap();
        if addrs.len() == 0{
            eprintln!("Error not found addr for topic {}", to);
            return false;
        }
        for addr in addrs{
            if !self.producers.contains_key(addr){
                match TcpStream::connect(addr){
                    Ok(stream)=>{
                        self.producers.insert(to.to_string(), Topic::new_for_write(stream));
                        self.producers.get(addr).unwrap().send_to(to, uuid, data)
                        self.producers.get_mut(addr).unwrap().is_last_sender = true;
                        break;
                    },
                    Err(err)=>{
                        eprintln!("Error {}:{}: {}", file!(), line!(), err);
                    }
                }
            }else if !self.producers[addr].is_last_sender{
                
            }
        }        
        return true;
    }
}

