use crate::bytestream::{read_stream, get_string, get_u64,
                        write_string, write_number, write_bytes};

use std::net::TcpStream;
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
        let (to, indata) = get_string(&indata);
        let (from, indata) = get_string(indata);
        let (uuid, indata) = get_string(indata);
        let (timestamp, indata) = get_u64(indata);
        
        Some(Self { to, from, uuid, timestamp, data: indata.to_owned() })
    }
    pub fn to_stream(&self, stream: &Arc<Mutex<TcpStream>>) {
        let all_size = self.to.len() + self.from.len() + self.uuid.len() + 8 + self.data.len();
        let _ = write_number(&stream, all_size as i32) &&
                write_string(&stream, &self.to) &&
                write_string(&stream, &self.from) &&
                write_string(&stream, &self.uuid) &&
                write_number(&stream, self.timestamp) && 
                write_bytes(&stream, &self.data);        
    }
}


