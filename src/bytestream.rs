use crate::print_error;

use std::io::{Read, Write};

pub fn get_string(indata: &[u8])->(String, &[u8])
{
    let mut offs = 0;
    let sz: usize = i32::from_be_bytes(u8_4(&indata[0..4])) as usize;               offs += 4;
    let to = String::from_utf8_lossy(&indata[offs..offs + sz]).to_string(); offs += sz;
    (to, &indata[offs..])
}

pub fn get_u64(indata: &[u8])->(u64, &[u8])
{
    let offs = std::mem::size_of::<u64>();
    let to = u64::from_be_bytes(u8_8(&indata[..offs]));
    (to, &indata[offs..])
}

pub fn get_u8(indata: &[u8])->(u8, &[u8])
{
    let offs = std::mem::size_of::<u8>();
    let to = indata[0];
    (to, &indata[offs..])
}

pub fn get_array(indata: &[u8])->(&[u8], &[u8])
{
    let mut offs = 0;
    let sz: usize = i32::from_be_bytes(u8_4(&indata[0..4])) as usize; offs += 4;
    let to = &indata[offs..offs + sz]; offs += sz;
    (to, &indata[offs..])
}


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
                        // if msz != 51{
                        //     println!(" msz != 51 {}, n {}",  msz, n);
                        // }      
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
fn u8_8(b: &[u8]) -> [u8; 8] {
    b.try_into().unwrap()
}

pub fn write_string<T>(stream: &mut T, str: &String)->bool
where
   T: Write,
{
    let str = str.as_bytes();
    let str_size = (str.len() as i32).to_be_bytes();
    write_stream(stream, &str_size) && write_stream(stream, str)
}

pub fn write_number<T, U>(stream: &mut T, v: U)->bool
where
    T: Write,
    U: ToBeBytes,
{
    write_stream(stream, v.to_be_bytes().as_ref())
}
pub trait ToBeBytes {
    type ByteArray: AsRef<[u8]>;
    fn to_be_bytes(&self) -> Self::ByteArray;
}
impl ToBeBytes for u8 {
    type ByteArray = [u8; 1];
    fn to_be_bytes(&self) -> Self::ByteArray {
        u8::to_be_bytes(*self)
    }
}
impl ToBeBytes for i32 {
    type ByteArray = [u8; 4];
    fn to_be_bytes(&self) -> Self::ByteArray {
        i32::to_be_bytes(*self)
    }
}
impl ToBeBytes for u64 {
    type ByteArray = [u8; 8];
    fn to_be_bytes(&self) -> Self::ByteArray {
        u64::to_be_bytes(*self)
    }
}

pub fn write_bytes<T>(stream: &mut T, data: &[u8])->bool
where
    T: Write,
{
    let data_size = (data.len() as i32).to_be_bytes();
    write_stream(stream, &data_size) && write_stream(stream, data)
}

fn write_stream<T>(stream: &mut T, data: &[u8])->bool
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