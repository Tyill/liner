use crate::message::Message;

use std::net::TcpStream;
use std::sync::{Arc, Mutex};
//use std::{sync::mpsc::Sender, thread};

pub struct Topic {
    pub was_send: bool,
    stream: Arc<Mutex<TcpStream>>,
}

impl Topic {
    pub fn new(stream: TcpStream) -> Topic {
        Self {
            was_send: false,
            stream: Arc::new(Mutex::new(stream)),
        }
    }
    pub fn send_to(&self, to: &str, from: &str, uuid: &str, data: &[u8]) {
        let stream = self.stream.clone();
        let mess = Message::new(to, from, uuid, data);
        rayon::spawn(move || {
            mess.to_stream(&stream);           
        });
    }
}
