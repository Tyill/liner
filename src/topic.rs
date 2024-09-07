use crate::message::Message;

use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::{sync::mpsc::Sender, thread};

pub struct Topic {
    pub is_last_sender: bool,
    stream: Arc<Mutex<TcpStream>>,
}

impl Topic {
    pub fn new_for_read(stream: TcpStream, tx: Sender<Message>) -> Topic {
        let stream = Arc::new(Mutex::new(stream));
        let stream_: Arc<Mutex<TcpStream>> = stream.clone();
        thread::spawn(move || loop {
            let mess = Message::from_stream(&stream_);
            if let Err(e) = tx.send(mess) {
                dbg!(e);
                break;
            }
        });
        Self {
            is_last_sender: false,
            stream,
        }
    }

    pub fn new_for_write(stream: TcpStream) -> Topic {
        Self {
            is_last_sender: false,
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
