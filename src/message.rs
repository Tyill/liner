use crate::{bytestream, print_error};
use crate::mempool::Mempool;
use crate::settings;
use std::io::{Write, Read};
use std::sync::{Arc, Mutex};

const COMPRESS: u8 = 0x01;
const AT_LEAST_ONCE_DELIVERY: u8 = 0x02;

pub struct Message{
    pub number_mess: u64,
    pub listener_topic_key: i32,
    flags: u8,
    mem_alloc_pos: usize,
    mem_alloc_length: usize,
}

impl Message{ 
    pub fn new(mempool: &Arc<Mutex<Mempool>>, connection_key: i32, listener_topic_key: i32,
               number_mess: u64, data: &[u8], at_least_once_delivery: bool) -> Message {
        let mut flags = 0;
        if at_least_once_delivery{
            flags |= AT_LEAST_ONCE_DELIVERY;
        }
        let number_mess_len = std::mem::size_of::<u64>();
        let connection_key_len = std::mem::size_of::<i32>();
        let listener_topic_key_len = std::mem::size_of::<i32>();
        let flags_len = std::mem::size_of::<u8>();
        let mut cdata: Option<Vec<u8>> = None;
        if data.len() > settings::MIN_SIZE_DATA_FOR_COMPRESS_BYTE{
            cdata = Some(compress(data)); 
        }
        let mut data_len = data.len() + std::mem::size_of::<u32>();
        if let Some(cdata) = &cdata{
            data_len = cdata.len() + std::mem::size_of::<u32>();
            flags |= COMPRESS;
        }
        let mess_size = number_mess_len +   
                               connection_key_len + 
                               listener_topic_key_len +              
                               flags_len +
                               data_len;
        let mut mem_alloc_pos = 0;
        let mut mem_alloc_length = 0;
        if let Ok(mut mempool) = mempool.lock(){
            (mem_alloc_pos, mem_alloc_length) = mempool.alloc(mess_size);
            let number_mess_pos = mem_alloc_pos; 
            let connection_key_pos = number_mess_pos + number_mess_len;
            let listener_topic_key_pos = connection_key_pos + connection_key_len;        
            let flags_pos = listener_topic_key_pos + listener_topic_key_len;
            let data_pos = flags_pos + flags_len;
                        
            mempool.write_num(number_mess_pos, number_mess);
            mempool.write_num(connection_key_pos, connection_key);
            mempool.write_num(listener_topic_key_pos, listener_topic_key);
            mempool.write_num(flags_pos, flags);
            match cdata{
                Some(cdata)=>{
                    mempool.write_array(data_pos, &cdata);
                },
                None=>{
                    mempool.write_array(data_pos, data);
                }
            }
        }
        Message{number_mess, listener_topic_key, flags, mem_alloc_pos, mem_alloc_length}
    }   

    pub fn free(&self, mempool: &mut Mempool){
        mempool.free(self.mem_alloc_pos, self.mem_alloc_length);
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
        if let Ok(mempool) = mempool.lock(){
            let number_mess_pos = mem_alloc_pos; 
            let number_mess_len = std::mem::size_of::<u64>();
            let number_mess = mempool.read_u64(number_mess_pos);
            
            let connection_key_pos = number_mess_pos + number_mess_len;
            let connection_key_len = std::mem::size_of::<i32>();

            let listener_topic_key_pos = connection_key_pos + connection_key_len;
            let listener_topic_key_len = std::mem::size_of::<i32>();
            let listener_topic_key = mempool.read_u32(listener_topic_key_pos) as i32;

            let flags_pos = listener_topic_key_pos + listener_topic_key_len;        
            let flags = mempool.read_u8(flags_pos);
            return Some(Message{number_mess, listener_topic_key, flags, mem_alloc_pos, mem_alloc_length});
        }
        None        
    }
    pub fn to_stream<T>(&self, mempool: &Arc<Mutex<Mempool>>, stream: &mut T)->bool 
        where T: Write{        
        bytestream::write_stream(stream, self.mem_alloc_pos, self.mem_alloc_length, mempool)       
    }
    
    pub fn at_least_once_delivery(&self)->bool{
        self.flags & AT_LEAST_ONCE_DELIVERY > 0
    }
    fn is_compressed(&self)->bool{
        self.flags & COMPRESS > 0
    }
    pub fn connection_key(&self, mempool: &Arc<Mutex<Mempool>>)->i32{
        let number_mess_len = std::mem::size_of::<u64>(); 
        let key_pos = self.mem_alloc_pos + number_mess_len;
        mempool.lock().unwrap().read_u32(key_pos) as i32
    }   
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
    pub data: *const u8, 
    pub data_len: usize,
    _decomp_data: Option<Vec<u8>>,
}

impl MessageForReceiver{
    pub fn new(mess: &Message, mempool: &Mempool)->MessageForReceiver{
        let raw_data =  mess.raw_data(mempool);
        
        let number_mess_pos = 0; 
        let number_mess_len = std::mem::size_of::<u64>();        
        let connection_key_pos = number_mess_pos + number_mess_len;
        let connection_key_len = std::mem::size_of::<u32>();
        let listener_topic_key_pos = connection_key_pos + connection_key_len;
        let listener_topic_key_len = std::mem::size_of::<u32>();
        let flags_pos = listener_topic_key_pos + listener_topic_key_len; 
        let flags_len = std::mem::size_of::<u8>(); 
        let data_pos = flags_pos + flags_len;

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
                data,
                data_len,
                _decomp_data,
            }
            
        }
    }
}