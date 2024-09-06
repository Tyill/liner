use crate::message::Message;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::{thread, sync::mpsc::Sender};
use std::sync::{Arc, Mutex};

use rayon::prelude::*;

pub struct Topic{
    pub is_last_sender: bool,
    stream: Arc<Mutex<TcpStream>>,
}

impl Topic {
    pub fn new_for_read(mut stream: TcpStream, tx: Sender<Message>) -> Topic {
        let stream = Arc::new(Mutex::new(stream));
        let stream_ = stream.clone();
        thread::spawn(move|| {
            let mut buff = [0; 4096];
            let mut msz: i32 = 0;
            let mut indata: Vec<u8> = Vec::new();
            loop {
                match stream_.lock().unwrap().read(&mut buff){
                    Ok(n)=>{
                        let mut cbuff = &buff[..n];
                        if n > 0 && msz == 0{
                            assert!(n > 4);
                            msz = i32::from_be_bytes(u8_arr(&buff[0..4]));
                            assert!(msz > 0);
                            indata.clear();
                            indata.reserve(msz as usize);
                            cbuff = &buff[4..n];
                        }
                        if n > 0 && msz > 0{
                            indata.extend_from_slice(cbuff);
                            if indata.len() == msz as usize{
                                let mess = Message::new(indata.clone());
                                match tx.send(mess){
                                    Err(e)=>{
                                        dbg!(e);
                                        break;
                                    },
                                    Ok(_) => ()
                                }
                                msz = 0;
                            }
                        }                        
                    },
                    Err(e)=>{
                        dbg!(e);
                        break;
                    }
                }
            }            
        });
        Self{
            is_last_sender: false,
            stream
        }
    }

    pub fn new_for_write(stream: TcpStream) -> Topic {
        Self{
            is_last_sender: false,
            stream: Arc::new(Mutex::new(stream))
        }
    }    
    pub fn send_to(&mut self, to: &str, uuid: &str, data: &[u8]) -> bool {
        let stream = self.stream.clone();
        let to_ = to.to_string();
        let uuid_ = uuid.to_string();
        let data_ = data.to_owned();
        rayon::spawn(move || {
            write_stream(&stream, to_.as_bytes());
            write_stream(&stream, uuid_.as_bytes());
            write_stream(&stream, &data_);
           // stream.lock().unwrap().write_all(uuid.as_bytes());        
           // stream.lock().unwrap().write_all(data);
        });
        true
    }
}

fn write_stream(stream: &Arc<Mutex<TcpStream>>, data: &[u8]){
    loop {
        match stream.lock().unwrap().write_all(data){
            Err(err)=>eprintln!("Error {}:{}: {}", file!(), line!(), err),
            Ok(_)=>break
        }
    }
}

fn u8_arr(b: &[u8]) -> [u8; 4] {
    b.try_into().unwrap()
}