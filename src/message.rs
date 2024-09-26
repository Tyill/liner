use crate::bytestream::{read_stream, get_string, get_u64, get_u8, get_array,
                        write_string, write_number, write_bytes};
use crate::common;

use std::io::{Write, Read};

mod mess_flags {
    pub const _COMPRESS: u8 = 0x01;
    pub const AT_LEAST_ONCE_DELIVERY: u8 = 0x02;
}
pub struct Message<'a>{
    pub topic_to: &'a[u8],    
    pub topic_from: &'a[u8],
    pub sender_name: &'a[u8],
    pub uuid: &'a[u8],
    pub timestamp: &'a[u8],
    pub number_mess: &'a[u8],
    pub flags: &'a[u8],
    pub data: &'a[u8],

    buf: Vec<u8>,
}

impl <'b>Message<'b> {
    pub fn new(to: &'b str, from: &str, sender_name: &str, uuid: &str, number_mess: u64, data: &[u8], at_least_once_delivery: bool) -> Message {
        let mut flags = 0;
        if at_least_once_delivery{
            flags |= mess_flags::AT_LEAST_ONCE_DELIVERY;
        }
        Self {
            topic_to: to.as_bytes(),
            topic_from: from.as_bytes(),
            sender_name: sender_name.as_bytes(),
            uuid: uuid.as_bytes(),
            timestamp: common::current_time_ms().as_bytes(),
            number_mess,
            flags,
            data: data.to_owned(),
            buf: Vec::new()
        }
    }    
    pub fn from_stream<T>(stream: &mut T) -> Option<Message>
        where T: Read
    {
        let indata = read_stream(stream);
        // if indata.len() != 51 && indata.len() > 0{
        //     println!("indata.len() != 51 {}", indata.len());
        //     return None;
        // }
        if indata.is_empty(){
            return None;
        }
        let (topic_to, indata) = get_string(&indata);
        let (topic_from, indata) = get_string(indata);
        let (sender_name, indata) = get_string(indata);
        let (uuid, indata) = get_string(indata);
        let (timestamp, indata) = get_u64(indata);
        let (number_mess, indata) = get_u64(indata);
        let (flags, indata) = get_u8(indata);
        let (data, _indata) = get_array(indata);
        Some(Self { topic_to, topic_from, sender_name, uuid, timestamp, number_mess, flags, data: data.to_vec()})
    }
    pub fn to_stream<T>(&self, stream: &mut T)->bool 
        where T: Write
    {
        let all_size = self.topic_to.len() + std::mem::size_of::<i32>() +
                              self.topic_from.len() + std::mem::size_of::<i32>() +
                              self.sender_name.len() + std::mem::size_of::<i32>() +
                              self.uuid.len() + std::mem::size_of::<i32>() +
                              std::mem::size_of::<u64>() + // timestamp
                              std::mem::size_of::<u64>() + // number_mess 
                              std::mem::size_of::<u8>() +  // flags 
                              self.data.len() + std::mem::size_of::<i32>(); // data
        // if all_size != 51{
        //     println!(" all_size != 51");
        // }
        write_number(stream, all_size as i32) &&
        write_string(stream, &self.topic_to) &&
        write_string(stream, &self.topic_from) &&
        write_string(stream, &self.sender_name) &&
        write_string(stream, &self.uuid) &&
        write_number(stream, self.timestamp) && 
        write_number(stream, self.number_mess) && 
        write_number(stream, self.flags) && 
        write_bytes(stream, &self.data)
        // if !ok{
        //     println!(" !ok");
        // }
    }
    pub fn at_least_once_delivery(&self)->bool{
        self.flags & mess_flags::AT_LEAST_ONCE_DELIVERY > 0
    }
}


