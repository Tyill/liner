use crate::message::Message;
use crate::redis;
use crate::topic::Topic;
use crate::UCback;

use std::net::{TcpListener, TcpStream};
use std::{thread, sync::mpsc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Client{
    name: String,
    db: redis::Connect,
    producers: HashMap<String, Topic>,
    consumers: Arc<Mutex<Vec<Topic>>>,
    is_run: bool,
}

impl Client {
    pub fn new(name: &str, redis_path: &str) -> Option<Client> {
        let db = redis::Connect::new(redis_path).ok()?;
        Some(
            Self{
                name: name.to_string(),
                db,
                producers: HashMap::new(),
                consumers: Arc::new(Mutex::new(Vec::new())),
                is_run: false
            }
        )
    }

    pub fn run(&mut self, localhost: &str, cb: UCback) -> bool {
        if self.is_run{
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
        let (tx, rx) = mpsc::channel::<Message>();
        let consumers = self.consumers.clone();
        thread::spawn(move|| {
            for stream in listener.incoming(){
                match stream {
                    Ok(stream)=>consumers.lock().unwrap().push(Topic::new(stream)),
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
        self.is_run = true;
        return true;
    }

    pub fn send_to(&mut self, to: &str, uuid: &str, data: &[u8]) -> bool {
        let addresses = self.db.get_topic_addresses(to);
        if let Err(err) = addresses{
            eprintln!("Error {}:{}: {}", file!(), line!(), err);
            return false           
        }
        let addresses = addresses.unwrap();
        if addresses.len() == 0{
            eprintln!("Error not found addr for topic {}", to);
            return false;
        }
        let mut is_send = false;
        for addr in addresses{
            if !self.producers.contains_key(addr){
                match TcpStream::connect(addr){
                    Ok(stream)=>{
                        let mut topic = Topic::new(stream);
                        topic.send_to(to, &self.name, uuid, data);
                        topic.was_send = true;
                        self.producers.insert(to.to_string(), topic);
                        is_send = true;
                        break;
                    },
                    Err(err)=>{
                        eprintln!("Error {}:{}: {} {}", file!(), line!(), err, addr);
                    }
                }
            }else if !self.producers[addr].was_send{
                self.producers.get(addr).unwrap().send_to(to, &self.name, uuid, data);
                self.producers.get_mut(addr).unwrap().was_send = true;
                is_send = true;
                break;
            }
        }
        if !is_send{
            for addr in addresses{
                if self.producers.contains_key(addr){
                    self.producers.get_mut(addr).unwrap().was_send = false;
                    if !is_send{
                        self.producers.get(addr).unwrap().send_to(to, &self.name, uuid, data);
                        self.producers.get_mut(addr).unwrap().was_send = true;
                        is_send = true;
                    }
                }
            }
        }  
        return is_send;
    }
}

