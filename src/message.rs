use crate::{bytestream, print_error};
use crate::mempool::Mempool;
use crate::settings;
use std::cell::Cell;
use std::io::{Write, Read};
use std::sync::{Arc, Mutex};

const COMPRESS: u8 = 0x01;
const AT_LEAST_ONCE_DELIVERY: u8 = 0x02;

/// Wire header: u64 number + i32 connection_key + i32 topic_key + u8 flags.
const HEADER_LEN: usize = 8 + 4 + 4 + 1;

pub struct Message{
    pub number_mess: u64,
    pub listener_topic_key: i32,
    flags: u8,
    mem_alloc_pos: usize,
    mem_alloc_length: usize,
    mempool: Arc<Mutex<Mempool>>,
    freed: Cell<bool>,
}

impl Drop for Message {
    fn drop(&mut self) {
        self.free_inner();
    }
}

impl Message{ 
    /// Encode `data` into the mempool. Returns `None` if the mempool lock fails (no number
    /// should be committed by the caller in that case).
    pub fn new(mempool: &Arc<Mutex<Mempool>>, connection_key: i32, listener_topic_key: i32,
               number_mess: u64, data: &[u8], at_least_once_delivery: bool) -> Option<Message> {
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
            if let Some(compressed) = compress(data) {
                cdata = Some(compressed);
            }
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
        let Ok(mut mp) = mempool.lock() else {
            print_error!("Message::new: mempool lock poisoned");
            return None;
        };
        let (mem_alloc_pos, mem_alloc_length) = mp.alloc(mess_size);
        let number_mess_pos = mem_alloc_pos; 
        let connection_key_pos = number_mess_pos + number_mess_len;
        let listener_topic_key_pos = connection_key_pos + connection_key_len;        
        let flags_pos = listener_topic_key_pos + listener_topic_key_len;
        let data_pos = flags_pos + flags_len;
                    
        mp.write_num(number_mess_pos, number_mess);
        mp.write_num(connection_key_pos, connection_key);
        mp.write_num(listener_topic_key_pos, listener_topic_key);
        mp.write_num(flags_pos, flags);
        match cdata{
            Some(cdata)=>{
                mp.write_array(data_pos, &cdata);
            },
            None=>{
                mp.write_array(data_pos, data);
            }
        }
        drop(mp);
        Some(Message{
            number_mess,
            listener_topic_key,
            flags,
            mem_alloc_pos,
            mem_alloc_length,
            mempool: mempool.clone(),
            freed: Cell::new(false),
        })
    }   

    pub fn free(&self, _mempool: &Arc<Mutex<Mempool>>){
        self.free_inner();
    }

    fn free_inner(&self) {
        if self.freed.replace(true) {
            return;
        }
        if self.mem_alloc_length == 0 {
            return;
        }
        if let Ok(mut mp) = self.mempool.lock() {
            mp.free(self.mem_alloc_pos, self.mem_alloc_length);
        }
    }
    
    pub fn from_stream<T>(mempool: &Arc<Mutex<Mempool>>, stream: &mut T, is_shutdown: &mut bool) -> Option<Message>
        where T: Read{
        let (mem_alloc_pos, mem_alloc_length, is_shutdown_) = bytestream::read_stream(stream, mempool);
        if mem_alloc_length == 0{
            *is_shutdown = is_shutdown_;
            return None;
        }
        if mem_alloc_length < HEADER_LEN + std::mem::size_of::<u32>() {
            print_error!(&format!(
                "message too short: {} (min {})",
                mem_alloc_length,
                HEADER_LEN + std::mem::size_of::<u32>()
            ));
            if let Ok(mut mp) = mempool.lock() {
                mp.free(mem_alloc_pos, mem_alloc_length);
            }
            *is_shutdown = true;
            return None;
        }
        if let Ok(mp) = mempool.lock(){
            let number_mess_pos = mem_alloc_pos; 
            let number_mess_len = std::mem::size_of::<u64>();
            let number_mess = mp.read_u64(number_mess_pos);
            
            let connection_key_pos = number_mess_pos + number_mess_len;
            let connection_key_len = std::mem::size_of::<i32>();

            let listener_topic_key_pos = connection_key_pos + connection_key_len;
            let listener_topic_key_len = std::mem::size_of::<i32>();
            let listener_topic_key = mp.read_u32(listener_topic_key_pos) as i32;

            let flags_pos = listener_topic_key_pos + listener_topic_key_len;        
            let flags = mp.read_u8(flags_pos);

            let data_pos = data_pos();
            let payload_len = mp.read_u32(mem_alloc_pos + data_pos) as usize;
            let need = data_pos + std::mem::size_of::<u32>() + payload_len;
            if need > mem_alloc_length {
                print_error!(&format!(
                    "message payload overruns alloc: need {}, have {}",
                    need, mem_alloc_length
                ));
                drop(mp);
                if let Ok(mut mp) = mempool.lock() {
                    mp.free(mem_alloc_pos, mem_alloc_length);
                }
                *is_shutdown = true;
                return None;
            }

            return Some(Message{
                number_mess,
                listener_topic_key,
                flags,
                mem_alloc_pos,
                mem_alloc_length,
                mempool: mempool.clone(),
                freed: Cell::new(false),
            });
        }
        // Lock failed after alloc — free to avoid leak.
        if let Ok(mut mp) = mempool.lock() {
            mp.free(mem_alloc_pos, mem_alloc_length);
        }
        None        
    }
    pub fn to_stream<T>(&self, mempool: &Arc<Mutex<Mempool>>, stream: &mut T)->bool 
        where T: Write{        
        bytestream::write_stream(stream, self.mem_alloc_pos, self.mem_alloc_length, mempool)       
    }

    pub fn get_data(&self, mempool: &Arc<Mutex<Mempool>>, out: &mut Vec<u8>)->usize{ 
        let mut data_len = 0;
        if let Ok(mp) = mempool.lock(){
            let data_pos = data_pos();
            let size_u32 = std::mem::size_of::<u32>() as usize;
            if self.mem_alloc_length < data_pos + size_u32 {
                print_error!("get_data: alloc shorter than header+len");
                return 0;
            }
            data_len = mp.read_u32(self.mem_alloc_pos + data_pos) as usize;
            if data_pos + size_u32 + data_len > self.mem_alloc_length {
                print_error!("get_data: payload overruns alloc");
                return 0;
            }
            if out.len() < data_len{
                out.resize(data_len, 0);
            }
            mp.read_data(self.mem_alloc_pos + data_pos + size_u32, &mut out[..data_len]);
        }
        if self.is_compressed(){            
            let Some(decomp_data) = decompress(&out[..data_len]) else {
                return 0;
            };
            if out.len() < decomp_data.len(){
                out.resize(decomp_data.len(), 0);
            } 
            out[..decomp_data.len()].copy_from_slice(&decomp_data);
            return decomp_data.len()
        }
        data_len
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

fn data_pos()->usize{ 
    let number_mess_pos = 0; 
    let number_mess_len = std::mem::size_of::<u64>();        
    let connection_key_pos = number_mess_pos + number_mess_len;
    let connection_key_len = std::mem::size_of::<u32>();
    let listener_topic_key_pos = connection_key_pos + connection_key_len;
    let listener_topic_key_len = std::mem::size_of::<u32>();
    let flags_pos = listener_topic_key_pos + listener_topic_key_len; 
    let flags_len = std::mem::size_of::<u8>(); 
    let data_pos = flags_pos + flags_len;
    data_pos
}

fn compress(data: &[u8])->Option<Vec<u8>>{
    match zstd::stream::encode_all(data, settings::DATA_COMPRESS_LEVEL){
        Ok(data)=>{
            Some(data)
        },
        Err(err)=>{
            print_error!(format!("compress error, dsz {}, err {}", data.len(), err));
            None
        }
    }
}
fn decompress(cdata: &[u8])->Option<Vec<u8>>{
    match zstd::stream::decode_all(cdata){
        Ok(data_)=>{
            Some(data_)
        },
        Err(err)=>{
            print_error!(format!("decompress error, dsz {}, err {}", cdata.len(), err));
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(
        connection_key: i32,
        listener_topic_key: i32,
        number_mess: u64,
        payload: &[u8],
        at_least_once: bool,
    ) -> (Message, Vec<u8>) {
        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let msg = Message::new(
            &mempool,
            connection_key,
            listener_topic_key,
            number_mess,
            payload,
            at_least_once,
        ).unwrap();

        let mut wire: Vec<u8> = Vec::new();
        assert!(msg.to_stream(&mempool, &mut wire));

        let mut shutdown = false;
        let decoded = Message::from_stream(&mempool, &mut &wire[..], &mut shutdown)
            .expect("expected message");
        assert!(!shutdown);
        (decoded, wire)
    }

    #[test]
    fn new_to_stream_from_stream_roundtrip_small_uncompressed() {
        let payload = b"hello world";
        let (msg, _wire) = roundtrip(123, 7, 42, payload, false);
        assert_eq!(msg.number_mess, 42);
        assert_eq!(msg.listener_topic_key, 7);
        assert!(!msg.at_least_once_delivery());

        let mempool = Arc::new(Mutex::new(Mempool::new()));
        // Rebuild the same wire but decode with a fresh mempool.
        let original = Message::new(&mempool, 123, 7, 42, payload, false).unwrap();
        let mut wire = Vec::new();
        assert!(original.to_stream(&mempool, &mut wire));

        let mut shutdown = false;
        let decoded =
            Message::from_stream(&mempool, &mut &wire[..], &mut shutdown).expect("decoded");
        assert_eq!(decoded.connection_key(&mempool), 123);

        let mut out = Vec::new();
        let len = decoded.get_data(&mempool, &mut out);
        assert_eq!(&out[..len], payload);
    }

    #[test]
    fn at_least_once_flag_roundtrips() {
        let payload = b"x";
        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let msg = Message::new(&mempool, 1, 1, 1, payload, true).unwrap();
        assert!(msg.at_least_once_delivery());

        let mut wire = Vec::new();
        assert!(msg.to_stream(&mempool, &mut wire));
        let mut shutdown = false;
        let decoded = Message::from_stream(&mempool, &mut &wire[..], &mut shutdown).unwrap();
        assert!(decoded.at_least_once_delivery());
    }

    #[test]
    fn connection_key_supports_negative_values() {
        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let msg = Message::new(&mempool, -123, 1, 1, b"abc", false).unwrap();
        let mut wire = Vec::new();
        assert!(msg.to_stream(&mempool, &mut wire));
        let mut shutdown = false;
        let decoded = Message::from_stream(&mempool, &mut &wire[..], &mut shutdown).unwrap();
        assert_eq!(decoded.connection_key(&mempool), -123);
    }

    #[test]
    fn get_data_resizes_output_buffer() {
        let payload = b"this is a longer payload";
        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let msg = Message::new(&mempool, 1, 1, 1, payload, false).unwrap();
        let mut out = vec![0u8; 1];
        let len = msg.get_data(&mempool, &mut out);
        assert_eq!(len, payload.len());
        assert_eq!(&out[..len], payload);
    }

    #[test]
    fn large_payload_roundtrips_with_or_without_compression() {
        // MIN_SIZE_DATA_FOR_COMPRESS_BYTE is 1MB; use slightly above.
        let payload = vec![0u8; settings::MIN_SIZE_DATA_FOR_COMPRESS_BYTE + 123];
        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let msg = Message::new(&mempool, 55, 9, 77, &payload, true).unwrap();
        let mut wire = Vec::new();
        assert!(msg.to_stream(&mempool, &mut wire));

        let mut shutdown = false;
        let decoded = Message::from_stream(&mempool, &mut &wire[..], &mut shutdown).unwrap();
        assert_eq!(decoded.number_mess, 77);
        assert_eq!(decoded.listener_topic_key, 9);
        assert_eq!(decoded.connection_key(&mempool), 55);
        assert!(decoded.at_least_once_delivery());

        let mut out = Vec::new();
        let len = decoded.get_data(&mempool, &mut out);
        assert_eq!(len, payload.len());
        assert_eq!(&out[..len], payload.as_slice());
    }

    #[test]
    fn from_stream_rejects_truncated_payload() {
        let mempool = Arc::new(Mutex::new(Mempool::new()));
        // length header claims 8 bytes but only header fragment — invalid short body.
        let mut wire: Vec<u8> = Vec::new();
        wire.extend_from_slice(&8u32.to_be_bytes());
        wire.extend_from_slice(&[0u8; 8]);
        let mut shutdown = false;
        assert!(Message::from_stream(&mempool, &mut &wire[..], &mut shutdown).is_none());
    }

    #[test]
    fn free_is_idempotent_via_drop() {
        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let msg = Message::new(&mempool, 1, 1, 1, b"abc", false).unwrap();
        msg.free(&mempool);
        // Drop must not double-free.
    }
}
