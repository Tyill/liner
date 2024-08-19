use std::net::{TcpListener, TcpStream};
use std::thread;

pub struct Topic{
    name: String,
    addr: String,
}

impl Topic {
    pub fn new(name: &str, addr: &str) -> Self {
        
        let listener = TcpListener::bind(addr).unwrap();

        for stream in listener.incoming() {
            let stream = stream.unwrap();
    
            thread::spawn(|| {
                handle_connection(stream);
            });
        }

        Self {
            name : name.to_string(),
            addr: addr.to_string(),
        }
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