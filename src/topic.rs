use crate::message::Message;

use std::io::Read;
use std::net::TcpStream;
use std::{thread, sync::mpsc::Sender};

pub struct Topic{
    pub is_last_sender: bool,
    send_stream: Option<TcpStream>,
}

impl Topic {
    pub fn new_for_read(mut stream: TcpStream, tx: Sender<Message>) -> Topic {
        thread::spawn(move|| {
            let mut buff = [0; 4096];
            let mut msz: i32 = 0;
            let mut indata: Vec<u8> = Vec::new();
            loop {
                match stream.read(&mut buff){
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
            send_stream: None
        }
    }

    pub fn new_for_write(stream: TcpStream) -> Topic {
        thread::spawn(move|| {
                  
        });
        Self{
            is_last_sender: false,
            send_stream: Some(stream)
        }
    }
    
    pub fn send_to(&mut self, to: &str, uuid: &str, data: &[u8]) -> bool {
       
        // match TcpStream::connect(&addr[0]) {
        //     Ok(mut stream) => {                                
        //         stream.write(data).unwrap();
        //         return true
        //     },
        //     Err(err) => {
        //         eprintln!("Error {}:{}: {}", file!(), line!(), err);
        //     }
        // }  
        return false
    }
}

fn u8_arr(b: &[u8]) -> [u8; 4] {
    b.try_into().unwrap()
}