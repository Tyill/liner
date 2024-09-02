use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::os::raw::c_void;
use std::thread;
use crate::redis;

pub struct Topic{
    pub name: String,
    pub addr: String,
    m_thread: thread::JoinHandle<()>,
}

impl Topic {
    pub fn new(name: &str, addr: &str) -> Option<Topic> {

        match redis::regist_topic(name, addr){
            Err(err)=> {
                eprintln!("Error {}:{}: {}", file!(), line!(), err);
                return None
            }
            _ =>()
        }
       
        let listener = TcpListener::bind(addr);
        match listener {
            Ok(listener) => {
                let thr = thread::spawn(move|| {
                    for stream in listener.incoming(){
                        handle_connection(stream.unwrap());
                    }                    
                });
                Some(Self {
                    name : name.to_string(),
                    addr: addr.to_string(),
                    m_thread: thr,
                })
            }
            Err(err) => {
                eprintln!("Error {}:{}: {}", file!(), line!(), err);
                return None
            }
        }
    }

    pub fn send_to(name: &str, data: &[u8]) -> bool {

        let addr = redis::get_topic_addr(name);
        match addr{
            Err(err)=> {
                eprintln!("Error {}:{}: {}", file!(), line!(), err);
                return false
            }
            _ =>()
        }
        let addr = addr.unwrap();
        if addr.len() == 0{
            eprintln!("Error not found addr for topic {}", name);
            return false
        }
        match TcpStream::connect(&addr[0]) {
            Ok(mut stream) => {                                
                stream.write(data).unwrap();
                return true
            },
            Err(err) => {
                eprintln!("Error {}:{}: {}", file!(), line!(), err);
            }
        }  
        return false
    }
}


fn handle_connection(mut stream: TcpStream) {
    // --snip--

    // let (status_line, filename) = match &request_line[..] {
    //     "GET / HTTP/1.1" => ("HTTP/1.1 200 OK", "hello.html"),
    //     "GET /sleep HTTP/1.1" => {
    //         thread::sleep(Duration::from_secs(5));
    //         ("HTTP/1.1 200 OK", "hello.html")
    //     }
    //     _ => ("HTTP/1.1 404 NOT FOUND", "404.html"),
    // };

    // --snip--
}