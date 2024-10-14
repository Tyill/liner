use crate::{bytestream, print_error};
use crate::mempool::Mempool;
use crate::settings;
use std::io::{Write, Read};
use std::sync::{Arc, Mutex};

const COMPRESS: u8 = 0x01;
const AT_LEAST_ONCE_DELIVERY: u8 = 0x02;

pub struct Message{
    pub number_mess: u64,
    flags: u8,
    // other fields:
    //  listener_topic
    //  sender_topic
    //  sender_name
    //  data
    mess_size: usize, 
    mem_alloc_pos: usize,
    mem_alloc_length: usize,
}

impl Message{
    pub fn new(mempool: &mut Mempool, listener_topic: &str, sender_topic: &str, sender_name: &str,
               number_mess: u64, data: &[u8], at_least_once_delivery: bool) -> Message {
        let mut flags = 0;
        if at_least_once_delivery{
            flags |= AT_LEAST_ONCE_DELIVERY;
        }
        let all_len = std::mem::size_of::<u32>();
        let number_mess_len = std::mem::size_of::<u64>();
        let flags_len = std::mem::size_of::<u8>();
        let listener_topic_len = listener_topic.len() + std::mem::size_of::<u32>();
        let sender_topic_len = sender_topic.len() + std::mem::size_of::<u32>();
        let sender_name_len = sender_name.len() + std::mem::size_of::<u32>();
        let mut cdata: Option<Vec<u8>> = None;
        if data.len() > settings::MIN_SIZE_DATA_FOR_COMPRESS_BYTE{
            cdata = Some(compress(data)); 
        }
        let mut data_len = data.len() + std::mem::size_of::<u32>();
        if let Some(cdata) = &cdata{
            data_len = cdata.len() + std::mem::size_of::<u32>();
            flags |= COMPRESS;
        }
        let mess_size = std::mem::size_of::<i32>() +                
            number_mess_len +                 
            flags_len +
            listener_topic_len +  
            sender_topic_len  +
            sender_name_len  +
            data_len;
      
        let (mem_alloc_pos, mem_alloc_length) = mempool.alloc(mess_size);
        let number_mess_pos = mem_alloc_pos + all_len; 
        let flags_pos = number_mess_pos + number_mess_len;
        let listener_topic_pos = flags_pos + flags_len;
        let sender_topic_pos = listener_topic_pos + listener_topic_len;
        let sender_name_pos = sender_topic_pos + sender_topic_len;
        let data_pos = sender_name_pos + sender_name_len;
                     
        mempool.write_num(mem_alloc_pos, (mess_size - all_len) as i32);
        mempool.write_num(number_mess_pos, number_mess);
        mempool.write_num(flags_pos, flags);
        mempool.write_str(listener_topic_pos, listener_topic);
        mempool.write_str(sender_topic_pos, sender_topic);
        mempool.write_str(sender_name_pos, sender_name);
        match cdata{
            Some(cdata)=>{
                mempool.write_array(data_pos, &cdata);
            },
            None=>{
                mempool.write_array(data_pos, data);
            }
        }
        Message{number_mess, flags, mess_size, mem_alloc_pos, mem_alloc_length}
    }   

    pub fn free(&self, mempool: &mut Mempool){
        mempool.free(self.mem_alloc_pos, self.mem_alloc_length);
    }

    pub fn change_mempool(&mut self, mempool_src: &mut Mempool, mempool_dst: &mut Mempool){
        let data = mempool_src.read_data(self.mem_alloc_pos, self.mem_alloc_length);
        let (mem_alloc_pos, mem_alloc_length) = mempool_dst.alloc_with_write(data);
        mempool_src.free(self.mem_alloc_pos, self.mem_alloc_length);
        self.mem_alloc_pos = mem_alloc_pos;
        self.mem_alloc_length = mem_alloc_length;
    }

    pub fn raw_data<'a>(&self, mempool: &'a Mempool)->&'a[u8]{
        mempool.read_data(self.mem_alloc_pos, self.mem_alloc_length)
    }
    
    pub fn from_stream<T>(mempool: &Arc<Mutex<Mempool>>, stream: &mut T, is_shutdown: &mut bool) -> Option<Message>
        where T: Read{
        let (mem_alloc_pos, mem_alloc_length, 
            mess_size, is_shutdown_) = bytestream::read_stream(stream, mempool);
        if mess_size == 0{
            *is_shutdown = is_shutdown_;
            return None;
        }
        let number_mess = get_number_mess(&mempool.lock().unwrap(), mem_alloc_pos);
        let flags = get_flags(&mempool.lock().unwrap(), mem_alloc_pos);
        
        Some(Message{number_mess, flags, mess_size, mem_alloc_pos, mem_alloc_length})
    }
    pub fn to_stream<T>(&self, mempool: &Arc<Mutex<Mempool>>, stream: &mut T)->bool 
        where T: Write{        
        bytestream::write_stream(stream, self.mem_alloc_pos, self.mess_size, mempool)       
    }
    
    pub fn at_least_once_delivery(&self)->bool{
        self.flags & AT_LEAST_ONCE_DELIVERY > 0
    }
    fn is_compressed(&self)->bool{
        self.flags & COMPRESS > 0
    }
    pub fn sender_topic(&self, mempool: &Mempool, io_topic: &mut String){
        let all_len = std::mem::size_of::<u32>();
        let number_mess_len = std::mem::size_of::<u64>(); 
        let flags_pos = self.mem_alloc_pos + all_len + number_mess_len;
        let flags_len = std::mem::size_of::<u8>(); 
        let listener_topic_pos = flags_pos + flags_len;
        let listener_len = mempool.read_u32(listener_topic_pos) + std::mem::size_of::<u32>() as u32; 
        let sender_topic_pos = listener_topic_pos + listener_len as usize;
        io_topic.clone_from(&mempool.read_string(sender_topic_pos));
    }
    pub fn sender_name(&self, mempool: &Mempool, io_name: &mut String){
        let all_len = std::mem::size_of::<u32>();
        let number_mess_len = std::mem::size_of::<u64>(); 
        let flags_pos = self.mem_alloc_pos + all_len + number_mess_len;
        let flags_len = std::mem::size_of::<u8>();
        let listener_topic_pos = flags_pos + flags_len;
        let listener_len = mempool.read_u32(listener_topic_pos) + std::mem::size_of::<u32>() as u32; 
        let sender_topic_pos = listener_topic_pos + listener_len as usize;
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
fn compress(data: &[u8])->Vec<u8>{
    let mut cdata: Vec<u8> = Vec::new(); 
    match zstd::stream::encode_all(data, settings::DATA_COMPRESS_LEVEL){
        Ok(data)=>{
            cdata = data;
        },
        Err(err)=>{
            print_error!(format!("compress error, dsz {}, err {}", data.len(), err));
        }
    }
    cdata
}
fn decompress(cdata: &[u8])->Vec<u8>{
    let mut data: Vec<u8> = Vec::new(); 
    match zstd::stream::decode_all(cdata){
        Ok(data_)=>{
            data = data_;
        },
        Err(err)=>{
            print_error!(format!("decompress error, dsz {}, err {}", cdata.len(), err));
        }
    }
    data
}


pub struct MessageForReceiver{
    pub topic_to: *const i8, 
    pub topic_from: *const i8, 
    pub data: *const u8, 
    pub data_len: usize,
    pub number_mess: u64,
    _decomp_data: Option<Vec<u8>>,
    mem_alloc_pos: usize,
    mem_alloc_length: usize,
}

impl MessageForReceiver{
    pub fn new(mess: &Message, mempool: &Mempool)->MessageForReceiver{
        let raw_data =  mess.raw_data(mempool);
        
        let all_len = std::mem::size_of::<u32>();
        let number_mess_len = std::mem::size_of::<u64>();
        let flags_pos = all_len + number_mess_len; 
        let flags_len = std::mem::size_of::<u8>(); 
        let listener_topic_pos = flags_pos + flags_len;
        let listener_topic_len = bytestream::read_u32(listener_topic_pos, raw_data) + std::mem::size_of::<u32>() as u32;
        let sender_topic_pos = listener_topic_pos + listener_topic_len as usize;
        let sender_topic_len = bytestream::read_u32(sender_topic_pos, raw_data) + std::mem::size_of::<u32>() as u32;
        let sender_name_pos = sender_topic_pos + sender_topic_len as usize;
        let sender_name_len = bytestream::read_u32(sender_name_pos, raw_data) + std::mem::size_of::<u32>() as u32;
        let data_pos = sender_name_pos + sender_name_len as usize;

        let len: isize = std::mem::size_of::<u32>() as isize;
        unsafe{
            let mut data = raw_data.as_ptr().offset(data_pos as isize + len);
            let mut data_len = bytestream::read_u32(data_pos, raw_data) as usize;
            let mut _decomp_data = None;
            if mess.is_compressed(){
                let data_pos = data_pos + len as usize;
                _decomp_data = Some(decompress(&raw_data[data_pos.. data_pos + data_len]));
                if let Some(decomp_data) = &_decomp_data{
                    data = decomp_data.as_ptr();
                    data_len = decomp_data.len();
                }
            }
            Self{
                topic_to: raw_data.as_ptr().offset(listener_topic_pos as isize + len) as *const i8,
                topic_from: raw_data.as_ptr().offset(sender_topic_pos as isize + len) as *const i8,
                data,
                data_len,
                number_mess: mess.number_mess,
                _decomp_data,
                mem_alloc_pos: mess.mem_alloc_pos,
                mem_alloc_length: mess.mem_alloc_length,
            }
        }
    }
    pub fn free(&self, mempool: &mut Mempool){
        mempool.free(self.mem_alloc_pos, self.mem_alloc_length);
    }
}