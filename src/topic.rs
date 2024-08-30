use std::net::{TcpListener, TcpStream};
use std::thread;
use redis::Commands;

pub struct Topic{
    pub name: String,
    pub addr: String,
  //  m_thread: thread::JoinHandle<()>,
}

impl Topic {
    pub fn new(name: &str, addr: &str) -> Option<Topic> {
        let o_listener = TcpListener::bind(addr);
        match o_listener {
            Ok(listr) => {
                let thr = thread::spawn(move|| {
                    for stream in listr.incoming(){
                        handle_connection(stream.unwrap());
                    }
                });
                Some(Self {
                    name : name.to_string(),
                    addr: addr.to_string(),
                    //m_thread: ()
                })
            }
            Err(err) => None
        }

    
        // let r = fetch_an_integer();
        // match r {
        //     Ok(v)=> print!("v = {v}"),
        //     Err(err)=>  print!("err = {err}"),
        // }                
    }    
    
}

fn fetch_an_integer() -> redis::RedisResult<isize> {
    // connect to redis
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;
    // throw away the result, just make sure it does not fail
    let _: () = con.set("my_key", 42)?;
    // read back the key and return it.  Because the return value
    // from the function is a result for integer this will automatically
    // convert into one.
    con.get("my_key")
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