use crate::bytestream::{read_stream, get_string, get_u64, get_u8, get_array,
                        write_string, write_number, write_bytes};
use crate::common;
use crate::mempool::{Mempool, Span};
use std::io::{Write, Read};

const _COMPRESS: u8 = 0x01;
const AT_LEAST_ONCE_DELIVERY: u8 = 0x02;

pub struct Message{
    all: Span,
    topic_to: Span,
    topic_from: Span,
    sender_name: Span,
    uuid: Span,
    timestamp: Span,
    number_mess: Span,
    flags: Span,
    data: Span,
}

impl Message{
    pub fn new(mempool: &mut Mempool, to: &str, from: &str, sender_name: &str, uuid: &str, number_mess: u64, data: &[u8], at_least_once_delivery: bool) -> Message {
        let mut flags = 0;
        if at_least_once_delivery{
            flags |= AT_LEAST_ONCE_DELIVERY;
        }
        let to_len = to.len() + std::mem::size_of::<i32>();
        let from_len = from.len() + std::mem::size_of::<i32>();
        let sender_name_len = sender_name.len() + std::mem::size_of::<i32>();
        let uuid_len = uuid.len() + std::mem::size_of::<i32>();
        let data_len = data.len() + std::mem::size_of::<i32>();

        let all_size = std::mem::size_of::<i32>() +  // all_size
            to_len  +
            from_len  +
            sender_name_len +
            uuid_len + 
            std::mem::size_of::<u64>() +                    // timestamp
            std::mem::size_of::<u64>() +                    // number_mess 
            std::mem::size_of::<u8>() +                     // flags 
            data_len;                                       // data

        let all = mempool.alloc(all_size);
        let mut offs: usize = std::mem::size_of::<i32>();
    
        let mess = Self{
            all: all.clone(),
            topic_to:    Span::new_with_offs(offs, to_len, &mut offs),
            topic_from:  Span::new_with_offs(offs, from_len, &mut offs),
            sender_name: Span::new_with_offs(offs, sender_name_len, &mut offs),
            uuid:        Span::new_with_offs(offs, uuid_len, &mut offs),
            timestamp:   Span::new_with_offs(offs, std::mem::size_of::<u64>(), &mut offs),
            number_mess: Span::new_with_offs(offs, std::mem::size_of::<u64>(), &mut offs),
            flags:       Span::new_with_offs(offs, std::mem::size_of::<u8>(), &mut offs),
            data:        Span::new_with_offs(offs, data_len, &mut offs),
        };
        mempool.write_num(all.pos(), all_size as i32);
        mempool.write_str(mess.topic_to.pos(), to);
        mempool.write_str(mess.topic_from.pos(), from);
        mempool.write_str(mess.sender_name.pos(), sender_name);
        mempool.write_str(mess.uuid.pos(), uuid);
        mempool.write_num(mess.timestamp.pos(), common::current_time_ms());
        mempool.write_num(mess.number_mess.pos(), number_mess);
        mempool.write_num(mess.number_mess.pos(), flags);
        mempool.write_array(mess.data.pos(), data);
        mess
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


