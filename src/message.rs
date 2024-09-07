use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

pub struct Message {
    to: String,
    from: String,
    uuid: String,
    timestamp: u64,
    data: Vec<u8>,
}

impl Message {
    pub fn new(to: &str, from: &str, uuid: &str, data: &[u8]) -> Message {
        let ms: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            to: to.to_string(),
            from: from.to_string(),
            uuid: uuid.to_string(),
            timestamp: ms,
            data: data.to_owned(),
        }
    }    
    pub fn from_stream(stream_: &Arc<Mutex<TcpStream>>) -> Message {
        let mut buff = [0; 4096];
        let mut msz: i32 = 0;
        let mut indata: Vec<u8> = Vec::new();
        loop {
            match stream_.lock().unwrap().read(&mut buff) {
                Ok(n) => {
                    let mut cbuff = &buff[..n];
                    if n > 0 && msz == 0 {
                        assert!(n > 4);
                        msz = i32::from_be_bytes(u8_arr(&buff[0..4]));
                        assert!(msz > 0);
                        indata.clear();
                        indata.reserve(msz as usize);
                        cbuff = &buff[4..n];
                    }
                    if n > 0 && msz > 0 {
                        indata.extend_from_slice(cbuff);
                        if indata.len() == msz as usize {
                            break;
                        }
                    }
                    if n == 0 {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error {}:{}: {}", file!(), line!(), e);
                    break;
                }
            }
        }
        let to_size = &indata[0..4];
    }

    pub fn to_stream(&self, stream_: &Arc<Mutex<TcpStream>>) {
        let mut buff = [0; 4096];
        let mut msz: i32 = 0;
        let mut indata: Vec<u8> = Vec::new();
        loop {
            match stream_.lock().unwrap().read(&mut buff) {
                Ok(n) => {
                    let mut cbuff = &buff[..n];
                    if n > 0 && msz == 0 {
                        assert!(n > 4);
                        msz = i32::from_be_bytes(u8_arr(&buff[0..4]));
                        assert!(msz > 0);
                        indata.clear();
                        indata.reserve(msz as usize);
                        cbuff = &buff[4..n];
                    }
                    if n > 0 && msz > 0 {
                        indata.extend_from_slice(cbuff);
                        if indata.len() == msz as usize {
                            let mess = Message::new_from(indata.clone());

                            break;
                        }
                    }
                    if n == 0 {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error {}:{}: {}", file!(), line!(), e);
                    break;
                }
            }
        }
    }
}


fn write_stream(stream: &Arc<Mutex<TcpStream>>, data: &[u8]) {
    loop {
        match stream.lock().unwrap().write_all(data) {
            Err(err) => {
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                } else {
                    eprintln!("Error {}:{}: {}", file!(), line!(), err);
                    break;
                }
            }
            Ok(_) => break,
        }
    }
}

fn u8_arr(b: &[u8]) -> [u8; 4] {
    b.try_into().unwrap()
}
