use crate::bytestream;
use crate::mempool::Mempool;
use std::io::{Write, Read};

const _COMPRESS: u8 = 0x01;
const AT_LEAST_ONCE_DELIVERY: u8 = 0x02;

pub struct Message{
    pub number_mess: u64,
    flags: u8,
    // other fields:
    // sender_topic
    // sender_name
    // uuid
    // timestamp
    // data
    mess_size: usize, 
    mem_alloc_pos: usize,
    mem_alloc_length: usize,
}

pub struct MessageForReceiver{
    pub topic_from: *const i8, 
    pub uuid: *const i8, 
    pub timestamp: u64, 
    pub data: *const u8, 
    pub data_len: usize,
}

impl MessageForReceiver{
    pub fn new(raw_data: &[u8])->MessageForReceiver{
        let all_len = std::mem::size_of::<u32>();
        let number_mess_len = std::mem::size_of::<u64>();
        let flags_pos = all_len + number_mess_len; 
        let flags_len = std::mem::size_of::<u8>(); 
        let sender_topic_pos = flags_pos + flags_len;
        let sender_topic_len = bytestream::read_u32(sender_topic_pos, raw_data) + std::mem::size_of::<u32>() as u32;
        let sender_name_pos = sender_topic_pos + sender_topic_len as usize;
        let sender_name_len = bytestream::read_u32(sender_name_pos, raw_data) + std::mem::size_of::<u32>() as u32;
        let uuid_pos = sender_name_pos + sender_name_len as usize;
        let uuid_len = bytestream::read_u32(uuid_pos, raw_data) + std::mem::size_of::<u32>() as u32;
        let timestamp_pos = uuid_pos + uuid_len as usize;
        let timestamp_len = std::mem::size_of::<u64>();
        let data_pos = timestamp_pos + timestamp_len as usize;
        
        Self{
            topic_from: std::ptr::from_ref(&raw_data[sender_topic_pos..]) as *const i8,
            uuid: raw_data[uuid_pos..].as_ptr() as *const i8,
            timestamp: bytestream::read_u64(timestamp_pos, raw_data),
            data: raw_data[data_pos..].as_ptr() as *const u8,
            data_len: bytestream::read_u32(data_pos, raw_data) as usize,
        }
    }
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

        let mess_size = std::mem::size_of::<i32>() +                
            number_mess_len +                 
            flags_len +       
            sender_topic_len  +
            sender_name_len  +
            uuid_len + 
            timestamp_len +                    
            data_len;
      
        let (mem_alloc_pos, mem_alloc_length) = mempool.alloc(mess_size);
        let number_mess_pos = mem_alloc_pos + all_len; 
        let flags_pos = number_mess_pos + number_mess_len;
        let sender_topic_pos = flags_pos + flags_len;
        let sender_name_pos = sender_topic_pos + sender_topic_len;
        let uuid_pos = sender_name_pos + sender_name_len;
        let timestamp_pos = uuid_pos + uuid_len;
        let data_pos = timestamp_pos + timestamp_len;
                     
        mempool.write_num(mem_alloc_pos, (mess_size - all_len) as i32);
        mempool.write_num(number_mess_pos, number_mess);
        mempool.write_num(flags_pos, flags);
        mempool.write_str(sender_topic_pos, sender_topic);
        mempool.write_str(sender_name_pos, sender_name);
        mempool.write_str(uuid_pos, uuid);
        mempool.write_num(timestamp_pos, timestamp);
        mempool.write_array(data_pos, data);

        Message{number_mess, flags, mess_size, mem_alloc_pos, mem_alloc_length}
    }   

    pub fn free(&self, mempool: &mut Mempool){
        mempool.free(self.mem_alloc_pos, self.mem_alloc_length);
    }

    pub fn raw_data(& self, mempool: &Mempool)->Vec<u8>{
        mempool.read_data(self.mem_alloc_pos, self.mem_alloc_length).to_vec()
    }

    pub fn from_stream<T>(mempool: &mut Mempool, stream: &mut T) -> Option<Message>
        where T: Read{
        let (mem_alloc_pos, mem_alloc_length, mess_size) = bytestream::read_stream_to_mempool(stream, mempool);
        if mess_size == 0{
            return None;
        }
        let number_mess = get_number_mess(mempool, mem_alloc_pos);
        let flags = get_flags(mempool, mem_alloc_pos);
        
        Some(Message{number_mess, flags, mess_size, mem_alloc_pos, mem_alloc_length})
    }
    pub fn to_stream<T>(&self, mempool: &Mempool, stream: &mut T)->bool 
        where T: Write{        
        bytestream::write_stream(stream, mempool.read_data(self.mem_alloc_pos, self.mess_size))       
    }
    
    pub fn at_least_once_delivery(&self)->bool{
        self.flags & AT_LEAST_ONCE_DELIVERY > 0
    }
    pub fn sender_topic(&self, mempool: &Mempool, io_topic: &mut String){
        let all_len = std::mem::size_of::<u32>();
        let number_mess_len = std::mem::size_of::<u64>(); 
        let flags_pos = self.mem_alloc_pos + all_len + number_mess_len;
        let flags_len = std::mem::size_of::<u8>(); 
        let sender_topic_pos = flags_pos + flags_len;
        io_topic.clone_from(&mempool.read_string(sender_topic_pos));
    }
    pub fn sender_name(&self, mempool: &Mempool, io_name: &mut String){
        let all_len = std::mem::size_of::<u32>();
        let number_mess_len = std::mem::size_of::<u64>(); 
        let flags_pos = self.mem_alloc_pos + all_len + number_mess_len;
        let flags_len = std::mem::size_of::<u8>(); 
        let sender_topic_pos = flags_pos + flags_len;
        let sender_topic_len = mempool.read_u32(sender_topic_pos) + std::mem::size_of::<u32>() as u32;
        io_name.clone_from(&mempool.read_string(sender_topic_pos + sender_topic_len as usize));
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