use crate::bytestream::{read_stream, get_string, get_u64, get_u8, get_array,
                        write_string, write_number, write_bytes};
use crate::memarea::Memarea;
use std::cell::{RefCell, RefMut};
use crate::common;

use std::io::{Write, Read};
use std::marker;

mod mess_flags {
    pub const _COMPRESS: u8 = 0x01;
    pub const AT_LEAST_ONCE_DELIVERY: u8 = 0x02;
}



pub struct Message<'a>{
    topic_to: &'a [u8],
    topic_from: &'a[u8],
    // sender_name: &'a[u8],
    // uuid: &'a[u8],
    // timestamp: &'a[u8],
    // number_mess: &'a[u8],
    // flags: &'a[u8],
    // data: &'a[u8],
}

impl Message<'_>{
    pub fn new<'a>(memarea: &'a mut Memarea, to: &str, from: &str, sender_name: &str, uuid: &str, number_mess: u64, data: &[u8], at_least_once_delivery: bool) -> Message<'a> {
        let mut flags = 0;
        if at_least_once_delivery{
            flags |= mess_flags::AT_LEAST_ONCE_DELIVERY;
        }
        let rr = memarea.alloc_str(from);
        Message{
            topic_to: memarea.alloc_str(to),
            topic_from: rr,
            //  sender_name: memarea.borrow_mut().alloc_str(sender_name),
            //  uuid: memarea.borrow_mut().alloc_str(uuid),
            //  timestamp: memarea.borrow_mut().alloc_num(common::current_time_ms()),
            //  number_mess: memarea.borrow_mut().alloc_num(number_mess),
            //  flags: memarea.borrow_mut().alloc_num(flags),
            //  data: memarea.borrow_mut().alloc_array(data),
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
        let all_size = self.all_size();
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
        self.flags[0] & mess_flags::AT_LEAST_ONCE_DELIVERY > 0
    }

    fn all_size(&self)->usize{
        self.topic_to.len() + std::mem::size_of::<i32>() +
        self.topic_from.len() + std::mem::size_of::<i32>() +
        self.sender_name.len() + std::mem::size_of::<i32>() +
        self.uuid.len() + std::mem::size_of::<i32>() +
        std::mem::size_of::<u64>() + // timestamp
        std::mem::size_of::<u64>() + // number_mess 
        std::mem::size_of::<u8>() +  // flags 
        self.data.len() + std::mem::size_of::<i32>() // data
    }
}


