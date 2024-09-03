use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::os::raw::c_void;
use std::thread;
use crate::redis;

pub struct Topic{
    pub name: String,
    stream: TcpStream,
}

impl Topic {
    pub fn new(name: &str, stream: TcpStream) -> Topic {
        Self{
            name: name.to_string(),
            stream,
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
}
