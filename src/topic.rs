use crate::message::Message;
use crate::redis;

use std::net::TcpStream;
use std::sync::{Arc, Mutex};
//use std::{sync::mpsc::Sender, thread};

pub struct Topic {
    stream: Arc<Mutex<TcpStream>>,
}

impl Topic {
    pub fn new(addr: &String, db: &Arc<Mutex<redis::Connect>>) -> Topic {
        Self {
            // match TcpStream::connect(addr){
            //     Ok(stream)=>{
            //         self.consumers.insert(to.to_string(), Topic::new(stream));
            //     },
            //     Err(err)=>{
            //         eprintln!("Error {}:{}: {} {}", file!(), line!(), err, addr);
            //     }
            // }
            was_send: false,
            stream: Arc::new(Mutex::new(stream)),
        }
    }
    pub fn send_to(&self, to: &str, from: &str, uuid: &str, data: &[u8]) {
        let stream = self.stream.clone();
        let mess = Message::new(to, from, uuid, data);
        rayon::spawn(move || {
            if !mess.to_stream(&stream){
                
            }           
        });
    }
}
