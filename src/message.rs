use crate::bytestream::{read_stream, get_string, get_u64, get_array,
                        write_string, write_number, write_bytes};

use std::net::TcpStream;
use std::time::SystemTime;

pub struct Message {
    pub to: String,
    pub from: String,
    pub uuid: String,
    pub timestamp: u64,
    pub number_mess: u64,
    pub data: Vec<u8>,
}

impl Message {
    pub fn new(to: &str, from: &str, uuid: &str, number_mess: u64, data: &[u8]) -> Message {
        let ms: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            to: to.to_string(),
            from: from.to_string(),
            uuid: uuid.to_string(),
            timestamp: ms,
            number_mess,
            data: data.to_owned(),
        }
    }    
    pub fn from_stream(stream: &mut TcpStream) -> Option<Message> {
        let indata = read_stream(stream);
        if indata.len() == 0{
            return None;
        }
        let (to, indata) = get_string(&indata);
        let (from, indata) = get_string(indata);
        let (uuid, indata) = get_string(indata);
        let (timestamp, indata) = get_u64(indata);
        let (number_mess, indata) = get_u64(indata);
        let (data, _indata) = get_array(indata);
        
        Some(Self { to, from, uuid, timestamp, number_mess, data: data.to_vec()})
    }
    pub fn to_stream(&self, stream: &mut TcpStream)->bool {
        let all_size = self.to.len() + self.from.len() + self.uuid.len() + std::mem::size_of::<i32>() * 3 +
                              std::mem::size_of::<u64>() * 2 + self.data.len() + std::mem::size_of::<i32>();
        let ret = write_number(stream, all_size as i32) &&
                write_string(stream, &self.to) &&
                write_string(stream, &self.from) &&
                write_string(stream, &self.uuid) &&
                write_number(stream, self.timestamp) && 
                write_number(stream, self.number_mess) && 
                write_bytes(stream, &self.data);
        return ret;
    }
}


