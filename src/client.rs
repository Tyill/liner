use crate::message::Message;
use crate::redis;
use crate::topic::Topic;
use crate::UCback;
use crate::epoll::EPoll;

use std::net::{TcpListener, TcpStream};
use std::{thread, sync::mpsc};
use std::collections::HashMap;
//use std::sync::{Arc, Mutex};

pub struct Client{
    name: String,
    db: redis::Connect,
    epoll: Option<EPoll>,
    producers: HashMap<String, Topic>,
    is_run: bool,
}

impl Client {
    pub fn new(name: &str, redis_path: &str) -> Option<Client> {
        let db = redis::Connect::new(redis_path).ok()?;
        Some(
            Self{
                name: name.to_string(),
                db,
                epoll: None,
                producers: HashMap::new(),
                is_run: false
            }
        )
    }
    pub fn run(&mut self, localhost: &str, receive_cb: UCback) -> bool {
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
        let (tx, rx) = mpsc::channel::<Message>();
        thread::spawn(move|| loop{ 
            match rx.recv() {
                Ok(m)=>{
                    receive_cb(m.to.as_ptr() as *const i8,
                               m.from.as_ptr() as *const i8, 
                               m.uuid.as_ptr() as *const i8, m.timestamp, 
                               m.data.as_ptr(), m.data.len());
                },
                Err(err)=>eprintln!("Error {}:{}: {}", file!(), line!(), err)
            }
        });        
        self.is_run = true;
        self.epoll = Some(EPoll::new(listener.unwrap(), tx));
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
        for addr in addresses{
            if !self.producers.contains_key(addr){
                match TcpStream::connect(addr){
                    Ok(stream)=>{
                        self.producers.insert(to.to_string(), Topic::new(stream));
                    },
                    Err(err)=>{
                        eprintln!("Error {}:{}: {} {}", file!(), line!(), err, addr);
                    }
                }
            }            
        }
        if self.producers.is_empty(){
            return false;
        }
        let mut is_send = false;
        for p in &mut self.producers{
            if !p.1.was_send{
                p.1.send_to(to, &self.name, uuid, data);
                p.1.was_send = true;
                is_send = true;
                break;
            }
        }  
        if !is_send{
            for p in &mut self.producers{
                p.1.was_send = false;
                if !is_send{
                    p.1.send_to(to, &self.name, uuid, data);
                    p.1.was_send = true;
                    is_send = true;
                }
            }
        }
        return is_send;
    }
}

