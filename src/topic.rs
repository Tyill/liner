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
            loop {
                stream.read(&mut buff);
                tx.send(Message{});
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
