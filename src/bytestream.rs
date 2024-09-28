use crate::print_error;

use std::io::{Read, Write};

pub fn read_stream<T>(stream: &mut T)->Vec<u8>
where 
    T: Read
{
    let mut buff = [0; 4096];
    let mut msz: i32 = 0;
    let mut offs: usize = 0;
    let mut indata: Vec<u8> = Vec::new();
    loop {
        let mut rsz: usize = msz as usize - indata.len();
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
                        msz = i32::from_be_bytes(u8_4(&buff[0..4])); 
                        assert!(msz > 0);
                        indata.reserve(msz as usize);
                        offs = 0;
                    }
                    continue;
                }
                if n > 0 {
                    indata.extend_from_slice(&buff[..n]);
                    if indata.len() == msz as usize {
                        break;
                    }
                }else{ // close stream on other side
                    indata.clear();
                    break; 
                }
            }
            Err(e) => {                
                let e = e.kind();
                if e == std::io::ErrorKind::WouldBlock{
                    if indata.is_empty(){
                        break;
                    }
                }else if e != std::io::ErrorKind::Interrupted{
                    print_error!(&format!("{}", e));                    
                }
            }
        }
    }
    indata
}

fn u8_4(b: &[u8]) -> [u8; 4] {
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