use crate::bytestream::{read_stream, get_string, get_u64, get_array,
                        write_string, write_number, write_bytes};
use crate::common;

use std::io::{Write, Read};

pub struct Message {
    pub topic_to: String,
    pub topic_from: String,
    pub sender_name: String,
    pub uuid: String,
    pub timestamp: u64,
    pub number_mess: u64,
    pub data: Vec<u8>,
}

impl Message {
    pub fn new(to: &str, from: &str, sender_name: &str, uuid: &str, number_mess: u64, data: &[u8]) -> Message {
        Self {
            topic_to: to.to_string(),
            topic_from: from.to_string(),
            sender_name: sender_name.to_string(),
            uuid: uuid.to_string(),
            timestamp: common::current_time_ms(),
            number_mess,
            data: data.to_owned(),
        }
    }    
    pub fn from_stream<T>(stream: &mut T) -> Option<Message>
        where T: Read
    {
        let indata = read_stream(stream);
        if indata.len() == 0{
            return None;
        }
        let (topic_to, indata) = get_string(&indata);
        let (topic_from, indata) = get_string(indata);
        let (sender_name, indata) = get_string(indata);
        let (uuid, indata) = get_string(indata);
        let (timestamp, indata) = get_u64(indata);
        let (number_mess, indata) = get_u64(indata);
        let (data, _indata) = get_array(indata);
        Some(Self { topic_to, topic_from, sender_name, uuid, timestamp, number_mess, data: data.to_vec()})
    }
    pub fn to_stream<T>(&self, stream: &mut T)->bool 
        where T: Write
    {
        let all_size = self.topic_to.len() + self.topic_from.len() + self.sender_name.len() + self.uuid.len() + std::mem::size_of::<i32>() * 4 +
                              std::mem::size_of::<u64>() * 2 + self.data.len() + std::mem::size_of::<i32>();
        let ok: bool = write_number(stream, all_size as i32) &&
                write_string(stream, &self.topic_to) &&
                write_string(stream, &self.topic_from) &&
                write_string(stream, &self.sender_name) &&
                write_string(stream, &self.uuid) &&
                write_number(stream, self.timestamp) && 
                write_number(stream, self.number_mess) && 
                write_bytes(stream, &self.data);
        return ok;
    }
}


