use crate::print_error;
use crate::mempool::Mempool;

use std::io::{Read, Write};

// return: mem_pos, mem_alloc_length, mess_size
pub fn read_stream_to_mempool<T>(stream: &mut T, mempool: &mut Mempool)->(usize, usize, usize) 
where 
    T: Read
{
    let mut buff = [0; 4096];
    let mut msz: usize = 0;
    let mut offs: usize = 0;
    let mut indata: Option<&mut [u8]> = None;
    let mut mem_pos = 0;
    let mut mem_alloc_length = 0; 
    let mut mem_fill_length = 0;    
    loop {
        let mut rsz: usize = msz - mem_fill_length;
        if rsz == 0{
            rsz = 4;
        }else if rsz > buff.len(){
            rsz = buff.len();
        } 
        match stream.read(&mut buff[offs..rsz]) {
            Ok(n) => {                
                if msz == 0 {
                    offs += n;                   
                    if offs == 4{
                        msz = i32::from_be_bytes(u8_4(&buff[0..4])) as usize; 
                        assert!(msz > 0);
                        let mess_len = std::mem::size_of::<u32>();
                        (mem_pos, mem_alloc_length) = mempool.alloc(msz + mess_len);
                        mempool.write_num(mem_pos, msz as i32);
                        indata = Some(mempool.read_mut_data(mem_pos + mess_len, msz));
                        offs = 0;
                    }
                    continue;
                }
                if n > 0 {
                    if let Some(indata) = indata.as_deref_mut(){
                        indata[mem_fill_length.. mem_fill_length + n].copy_from_slice(&buff[..n]);
                        mem_fill_length += n;
                        if mem_fill_length == msz as usize {                        
                            break;
                        }
                    }
                }else{ // close stream on other side
                    if mem_pos > 0{
                        mempool.free(mem_pos, mem_alloc_length);
                        mem_pos = 0;
                        mem_fill_length = 0;
                    }
                    break; 
                }
            }
            Err(e) => {                
                let e = e.kind();
                if e == std::io::ErrorKind::WouldBlock{
                    if mem_fill_length == 0{
                        break;
                    }
                }else if e != std::io::ErrorKind::Interrupted{
                    print_error!(&format!("{}", e));                    
                }
            }
        }
    }
    (mem_pos, mem_alloc_length, mem_fill_length)
}

pub fn read_u32(pos: usize, data: &[u8])->u32{
    u32::from_be_bytes(u8_4(&data[pos.. pos + std::mem::size_of::<u32>()]))
}
pub fn read_u64(pos: usize, data: &[u8])->u64{
    u64::from_be_bytes(u8_8(&data[pos.. pos + std::mem::size_of::<u64>()]))
}
fn u8_4(b: &[u8]) -> [u8; 4] {
    b.try_into().unwrap()
}
fn u8_8(b: &[u8]) -> [u8; 8] {
    b.try_into().unwrap()
}

pub fn write_stream<T>(stream: &mut T, data: &[u8])->bool
where
    T: Write,
{
    let dsz = data.len();
    let mut wsz: usize = 0;
    while wsz < dsz{
        match stream.write(&data[wsz..]) {
            Ok(n) => { 
                if n == 0{
                    break;
                }
                wsz += n;
            },
            Err(err) => {
                let e = err.kind();
                if e == std::io::ErrorKind::WouldBlock{
                    continue;
                }else if e != std::io::ErrorKind::Interrupted{
                    print_error!(&format!("{}", e));                    
                }
            },            
        }
    }
    wsz == dsz
}