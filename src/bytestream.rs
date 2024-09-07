use std::io::{Read, Write};
use std::sync::{Arc, Mutex};


pub fn read_stream<T>(stream: &Arc<Mutex<T>>)->Vec<u8>
where 
    T: Read
{
    let mut buff = [0; 4096];
    let mut msz: i32 = 0;
    let mut offs: usize = 0;
    let mut indata: Vec<u8> = Vec::new();
    loop {
        match stream.lock().unwrap().read(&mut buff[offs..]) {
            Ok(n) => {
                let mut cbuff = &buff[..n];
                if msz == 0 {
                    offs += n;
                }
                if msz == 0 && offs >= 4 {
                    msz = i32::from_be_bytes(u8_4(&buff[0..4]));                        
                    assert!(msz > 0);
                    indata.clear();
                    indata.reserve(msz as usize);
                    cbuff = &buff[4..offs];
                    offs = 0;
                }                    
                if n > 0 && msz > 0 {
                    indata.extend_from_slice(cbuff);
                    if indata.len() == msz as usize {
                        break;
                    }
                }
                if n == 0 {
                    indata.clear();
                    break;
                }
            }
            Err(e) => {
                if e.kind() != std::io::ErrorKind::Interrupted{
                    eprintln!("Error {}:{}: {}", file!(), line!(), e);
                    indata.clear();
                    break;
                }
            }
        }
    }
    return indata;
}

fn u8_4(b: &[u8]) -> [u8; 4] {
    b.try_into().unwrap()
}

pub fn write_string<T>(stream: &Arc<Mutex<T>>, str: &String)->bool
where
    T: Write,
{
    let str = str.as_bytes();
    let str_size = (str.len() as i32).to_be_bytes();
    let ok = write_stream(&stream, &str_size) &
                   write_stream(&stream, &str);
    return ok;
}

pub fn write_number<T, U>(stream: &Arc<Mutex<T>>, v: U)->bool
where
    T: Write,
    U: ToBeBytes,
{
    let ok = write_stream(&stream, v.to_be_bytes().as_ref());
    return ok;
}
trait ToBeBytes {
    type ByteArray: AsRef<[u8]>;
    fn to_be_bytes(&self) -> Self::ByteArray;
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

pub fn write_bytes<T>(stream: &Arc<Mutex<T>>, data: &[u8])->bool
where
    T: Write,
{
    let data_size = (data.len() as i32).to_be_bytes();
    let ok = write_stream(&stream, &data_size) &
                   write_stream(&stream, &data);
    return ok;
}

fn write_stream<T>(stream: &Arc<Mutex<T>>, data: &[u8])->bool
where
    T: Write,
{
    let mut is = false;
    loop {
        match stream.lock().unwrap().write_all(data) {
            Err(err) => {
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                } else {
                    eprintln!("Error {}:{}: {}", file!(), line!(), err);
                    break;
                }
            },
            Ok(_) => { 
                is = true;
                break;
            },
        }
    }
    return is;
}