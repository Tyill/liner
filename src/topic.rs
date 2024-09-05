use crate::message::Message;

use std::io::Read;
use std::net::TcpStream;
use std::{thread, sync::mpsc::Sender};

pub struct Topic{
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
                        if n >= 4 && msz == 0{
                            msz = i32::from_be_bytes(u8_arr(&buff[0..4]));
                            indata.clear();
                            indata.reserve(msz as usize);
                            indata.extend_from_slice(&buff[4..n - 4]);
                            continue;
                        }else if n > 0 && msz > 0{
                            indata.extend_from_slice(&buff[0..n]);
                        }
                        if indata.len() == msz as usize{
                            let mess = Message::new(indata.clone());
                            match tx.send(mess){
                                Err(e)=>{

                                },
                                Ok(_) => ()
                            }
                            msz == 0;
                        }
                    },
                    Err(e)=>{

                    }
                } 
                
            }            
        });
        Self{
        }
    }
    
    // pub fn send_to(data: &[u8]) -> bool {
       
    //     match TcpStream::connect(&addr[0]) {
    //         Ok(mut stream) => {                                
    //             stream.write(data).unwrap();
    //             return true
    //         },
    //         Err(err) => {
    //             eprintln!("Error {}:{}: {}", file!(), line!(), err);
    //         }
    //     }  
    //     return false
    // }

    // for stream in listener.incoming(){
    //     match stream {
    //         Ok(stream)=>topics_copy.lock().unwrap().push(Topic::new_for_read(stream)),
    //         Err(err)=>eprintln!("Error {}:{}: {}", file!(), line!(), err)
    //     }
    // }
}

fn u8_arr(b: &[u8]) -> [u8; 4] {
    b.try_into().unwrap()
}