use crate::message::Message;
use crate::redis;

use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub struct Topic {
    addr: String,
    stream: Arc<Mutex<Option<TcpStream>>>,
    db: Arc<Mutex<redis::Connect>>,
}

impl Topic {
    pub fn new(addr: &String, db: &Arc<Mutex<redis::Connect>>) -> Topic {
        Self {
            addr: addr.to_string(),
            stream: Arc::new(Mutex::new(None)),
            db: db.clone(),
        }
    }
    pub fn send_to(&self, to: &str, from: &str, uuid: &str, data: &[u8]) {

        if self.stream.lock().unwrap().is_none(){
            match TcpStream::connect(&self.addr){
                Ok(stream)=>{
                    *self.stream.lock().unwrap() = Some(stream);
                },
                Err(err)=>{
                    eprintln!("Error {}:{}: {} {}", file!(), line!(), err, self.addr);
                    return;
                }
            }
        }
        let db = self.db.clone();
        let stream = self.stream.clone();
        let mess = Message::new(to, from, uuid, data);
        rayon::spawn(move || {
            let mut is_send = false;
            if let Some(stream) = stream.lock().unwrap().as_mut(){
                is_send = mess.to_stream(stream);
            }
            if !is_send{
                *stream.lock().unwrap() = None;
                db.lock().unwrap().get_topic_addresses("name");
            }        
        });
    }
}
