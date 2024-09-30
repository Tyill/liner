use crate::bytestream;
use crate::mempool::Mempool;
use std::io::{Write, Read};

const _COMPRESS: u8 = 0x01;
const AT_LEAST_ONCE_DELIVERY: u8 = 0x02;

pub struct Message{
    pub number_mess: u64,
    flags: u8,
   // other fields:
   //  sender_topic
   //  sender_name
   //  uuid
   //  timestamp
   //  data

   mem_pos: usize,
   mem_length: usize,
}
   

impl Message{
    pub fn new(mempool: &mut Mempool, sender_topic: &str, sender_name: &str,
               uuid: &str, number_mess: u64, data: &[u8], at_least_once_delivery: bool, timestamp: u64) -> Message {
        let mut flags = 0;
        if at_least_once_delivery{
            flags |= AT_LEAST_ONCE_DELIVERY;
        }
        let all_len = std::mem::size_of::<u32>();
        let number_mess_len = std::mem::size_of::<u64>();
        let flags_len = std::mem::size_of::<u8>();
        let sender_topic_len = sender_topic.len() + std::mem::size_of::<u32>();
        let sender_name_len = sender_name.len() + std::mem::size_of::<u32>();
        let uuid_len = uuid.len() + std::mem::size_of::<u32>();
        let timestamp_len = std::mem::size_of::<u64>();
        let data_len = data.len() + std::mem::size_of::<u32>();

        let all_size = std::mem::size_of::<i32>() +                
            number_mess_len +                 
            flags_len +       
            sender_topic_len  +
            sender_name_len  +
            uuid_len + 
            timestamp_len +                    
            data_len;                         

        let all_pos = mempool.alloc(all_size);
        let number_mess_pos = all_pos + all_len; 
        let flags_pos = number_mess_pos + number_mess_len;
        let sender_topic_pos = flags_pos + flags_len;
        let sender_name_pos = sender_topic_pos + sender_topic_len;
        let uuid_pos = sender_name_pos + sender_name_len;
        let timestamp_pos = uuid_pos + uuid_len;
        let data_pos = timestamp_pos + timestamp_len;
                     
        mempool.write_num(all_pos, (all_size - all_len) as i32);
        mempool.write_num(number_mess_pos, number_mess);
        mempool.write_num(flags_pos, flags);
        mempool.write_str(sender_topic_pos, sender_topic);
        mempool.write_str(sender_name_pos, sender_name);
        mempool.write_str(uuid_pos, uuid);
        mempool.write_num(timestamp_pos, timestamp);
        mempool.write_array(data_pos, data);

        Message{number_mess, flags, mem_pos: all_pos, mem_length: all_size}
    }   

    pub fn free(&self, mempool: &mut Mempool){
        mempool.free(self.mem_pos, self.mem_length);
    }

    pub fn from_stream<T>(mempool: &mut Mempool, stream: &mut T) -> Option<Message>
        where T: Read{
        let indata = bytestream::read_stream(stream);
        if indata.is_empty(){
            return None;
        }
        let mem_length = indata.len() + std::mem::size_of::<u32>();
        let mem_pos = mempool.alloc(mem_length);

        mempool.write_array(mem_pos, &indata);
              
        let number_mess = get_number_mess(mempool, mem_pos);
        let flags = get_flags(mempool, mem_pos);
        
        Some(Message{number_mess, flags, mem_pos, mem_length})
    }
    pub fn to_stream<T>(&self, mempool: &Mempool, stream: &mut T)->bool 
        where T: Write{        
        bytestream::write_stream(stream, mempool.read_mess(self.mem_pos, self.mem_length))       
    }
    
    pub fn at_least_once_delivery(&self)->bool{
        self.flags & AT_LEAST_ONCE_DELIVERY > 0
    }
    pub fn sender_topic(&self, mempool: &Mempool)->String{
        let all_len = std::mem::size_of::<u32>();
        let number_mess_len = std::mem::size_of::<u64>(); 
        let flags_len = std::mem::size_of::<u8>(); 
        mempool.read_string(self.mem_pos + all_len + number_mess_len + flags_len)
    }
    pub fn sender_name(&self, mempool: &Mempool)->String{
        let all_len = std::mem::size_of::<u32>();
        let number_mess_len = std::mem::size_of::<u64>(); 
        let flags_len = std::mem::size_of::<u8>(); 
        let sender_topic_pos = self.mem_pos + all_len + number_mess_len + flags_len;
        let sender_topic_len = mempool.read_u32(sender_topic_pos) + std::mem::size_of::<u32>() as u32;
        mempool.read_string(sender_topic_pos + sender_topic_len as usize)
    }
}

fn get_number_mess(mempool: &Mempool, mem_pos:usize)->u64{
    let all_len = std::mem::size_of::<u32>(); 
    mempool.read_u64(mem_pos + all_len)
}
fn get_flags(mempool: &Mempool, mem_pos:usize)->u8{
    let all_len = std::mem::size_of::<u32>();
    let number_mess_len = std::mem::size_of::<u64>();        
    mempool.read_u8(mem_pos + all_len + number_mess_len)
}