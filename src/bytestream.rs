use crate::{print_error, settings};
use crate::mempool::Mempool;

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

// return: mem_pos, mem_alloc_length
pub fn read_stream<T>(stream: &mut T, mempool: &Arc<Mutex<Mempool>>)->(usize, usize, bool) 
where 
    T: Read
{
    let mut buff = [0; settings::BYTESTREAM_READ_BUFFER_SIZE];
    let mut msz: usize = 0;
    let mut offs: usize = 0;
    let mut mem_pos = 0;
    let mut mem_alloc_length = 0; 
    let mut mem_fill_length = 0;
    let mut is_shutdown = false;
    loop {
        let mut rsz: usize = msz - mem_fill_length;
        if rsz == 0{
            rsz = 4;
        }else if rsz > buff.len(){
            rsz = buff.len();
        } 
        match stream.read(&mut buff[offs..rsz]) {
            Ok(n) => {         
                if msz == 0 && n > 0{
                    offs += n;                   
                    if offs == 4{
                        msz = i32::from_be_bytes(u8_4(&buff[0..4])) as usize;
                        assert!(msz > 0);
                        if let Ok(mut mempool) = mempool.lock(){
                            (mem_pos, mem_alloc_length) = mempool.alloc(msz);
                            assert!(msz == mem_alloc_length);
                        }
                        offs = 0;
                    }
                    continue;
                }
                if n > 0 {
                    if let Ok(mut mempool) = mempool.lock(){
                        mempool.write_data(mem_pos + mem_fill_length, &buff[..n]);
                    }
                    mem_fill_length += n;
                    if mem_fill_length == msz {                        
                        break;
                    }
                }else{ // close stream on other side
                    if mem_alloc_length > 0{
                        mempool.lock().unwrap().free(mem_pos, mem_alloc_length);
                        mem_pos = 0;
                        mem_alloc_length = 0;
                    }
                    is_shutdown = true;
                    break; 
                }
            }
            Err(e) => {                
                let e = e.kind();
                if e == std::io::ErrorKind::WouldBlock{
                    if mem_fill_length == 0 && msz == 0{
                        break;
                    }
                }else if e != std::io::ErrorKind::Interrupted{
                    print_error!(&format!("{}", e));
                    if mem_alloc_length > 0{
                        mempool.lock().unwrap().free(mem_pos, mem_alloc_length);
                        mem_pos = 0;
                        mem_alloc_length = 0;
                    }
                    is_shutdown = true;
                    break;                  
                }
            }
        }
    }
    (mem_pos, mem_alloc_length, is_shutdown)
}

pub fn read_u32(pos: usize, data: &[u8])->u32{
    u32::from_be_bytes(u8_4(&data[pos.. pos + std::mem::size_of::<u32>()]))
}

fn u8_4(b: &[u8]) -> [u8; 4] {
    b.try_into().unwrap()
}


pub fn write_stream<T>(stream: &mut T, mem_alloc_pos: usize, mem_alloc_length: usize, mempool: &Arc<Mutex<Mempool>>)->bool
where
    T: Write,
{    
    const BUFF_LEN: usize = settings::BYTESTREAM_WRITE_BUFFER_SIZE;
    let mut buff = [0; BUFF_LEN];
    buff[..std::mem::size_of::<u32>()].copy_from_slice((mem_alloc_length as u32).to_be_bytes().as_ref());
    let mut wsz: usize = 0;
    let mut offs: usize = std::mem::size_of::<u32>();
    let mut is_continue = false;
    let mess_size = mem_alloc_length;
    while wsz < mess_size{
        let endlen = std::cmp::min(mess_size - wsz + offs, BUFF_LEN);
        if !is_continue{
            if let Ok(mempool) = mempool.lock(){
                mempool.read_data(mem_alloc_pos + wsz, &mut buff[offs..endlen]);
            }           
        }
        match stream.write_all(&buff[..endlen]){
            Ok(_) => {
                wsz += endlen - offs;
                offs = 0;
                is_continue = false;
            },
            Err(err) => {
                let e = err.kind();
                if e == std::io::ErrorKind::WouldBlock || e == std::io::ErrorKind::Interrupted{
                    is_continue = true;
                    continue;
                }else{
                    print_error!(&format!("{}", e));
                    break;                  
                }
            },            
        }
    }
    wsz == mess_size
}