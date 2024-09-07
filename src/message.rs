use crate::bytestream::{read_stream, write_string, write_number, write_bytes};

use std::net::{TcpStream};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;


pub struct Message {
    to: String,
    from: String,
    uuid: String,
    timestamp: u64,
    data: Vec<u8>,
}

impl Message {
    pub fn new(to: &str, from: &str, uuid: &str, data: &[u8]) -> Message {
        let ms: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            to: to.to_string(),
            from: from.to_string(),
            uuid: uuid.to_string(),
            timestamp: ms,
            data: data.to_owned(),
        }
    }    
    pub fn from_stream(stream: &Arc<Mutex<TcpStream>>) -> Option<Message> {
        
        let indata = read_stream(&stream);
        if indata.len() == 0{
            return None;
        }

        let offs = 0;
        let sz: usize = i32::from_be_bytes(u8_4(&indata[0..4])) as usize;               offs += 4;
        let to = String::from_utf8_lossy(&indata[offs..offs + sz]).to_string();   offs += sz;
        let sz: usize = i32::from_be_bytes(u8_4(&indata[offs..offs + 4])) as usize;     offs += 4;
        let from = String::from_utf8_lossy(&indata[offs..offs + sz]).to_string(); offs += sz;
        Self { to, from: (), uuid: (), timestamp: (), data: () }

    }

    pub fn to_stream(&self, stream: &Arc<Mutex<TcpStream>>) {
        let all_size = self.to.len() + self.from.len() + self.uuid.len() + 8 + self.data.len();
        write_number(&stream, all_size as i32) &&
        write_string(&stream, &self.to) &&
        write_string(&stream, &self.from) &&
        write_string(&stream, &self.uuid) &&
        write_number(&stream, self.timestamp) && 
        write_bytes(&stream, &self.data);        
    }
}


