use crate::message::Message;
use crate::redis;

use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::thread;
use std::os::unix::io::{AsRawFd, RawFd};
use std::io;


#[allow(unused_macros)]
macro_rules! syscall {
    ($fn: ident ( $($arg: expr),* $(,)* ) ) => {{
        let res = unsafe { libc::$fn($($arg, )*) };
        if res == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(res)
        }
    }};
}

pub struct EPollSender{
    epoll_fd: i32,
    streams: HashMap<RawFd, Arc<Mutex<TcpStream>>>            
}

impl EPollSender {
    pub fn new(db: &Arc<Mutex<redis::Connect>>)->EPollSender{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        let db = db.clone();
        thread::spawn(move|| {
            let mut streams: HashMap<RawFd, Arc<Mutex<TcpStream>>> = HashMap::new();
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(128);
            loop{    
                let mut ready_count = 0;  
                match syscall!(epoll_wait(
                    epoll_fd,
                    events.as_mut_ptr() as *mut libc::epoll_event,
                    128,
                    -1,
                )){
                    Ok(res)=>ready_count = res,
                    Err(err)=>{
                        if err.kind() == std::io::ErrorKind::Interrupted{
                            continue;
                        }else{
                            eprintln!("couldn't epoll_wait: {}", err);
                            break;
                        }
                    }
                }  
                unsafe { events.set_len(ready_count as usize) };
        
                for ev in &events {  
                    if ev.events as i32 & libc::EPOLLIN > 0{
                        let stream_fd = ev.u64 as RawFd;
                        if let Some(stream) = streams.get(&stream_fd){
                            let stream = stream.clone();
                            rayon::spawn(move || {
                                let mut stream = stream.lock().unwrap();
                                continue_write_stream(epoll_fd, stream_fd).expect("couldn't event continue_write_stream");
                            });
                        }
                    }else if ev.events as i32 & (libc::EPOLLHUP | libc::EPOLLERR) > 0{
                        let stream_fd = ev.u64 as RawFd;
                        let _ = remove_write_stream(epoll_fd, stream_fd);
                        streams.remove(&stream_fd);
                    }else{
                        eprintln!("unexpected events: {}", ev.events as i32);
                    }
                }
            }
        });
        Self{
            epoll_fd,
        }
    }
    pub fn insert(&self, sddr: &str) {

        if self.stream.lock().unwrap().is_none(){
            match TcpStream::connect(&self.addr){
                Ok(stream)=>{
                    *self.stream.lock().unwrap() = Some(stream);
                },
                Err(err)=>{
                    eprintln!("Error {}:{}: {} {}", file!(), line!(), err, self.addr);
                    return;
                }
            }
        }
        let db = self.db.clone();
        let stream = self.stream.clone();
        let mess = Message::new(to, from, uuid, data);
        rayon::spawn(move || {
            let mut is_send = false;
            if let Some(stream) = stream.lock().unwrap().as_mut(){
                is_send = mess.to_stream(stream);
            }
            if !is_send{
                *stream.lock().unwrap() = None;
                db.lock().unwrap().get_topic_addresses("name");
            }        
        });
    }

    pub fn send_to(&self, to: &str, from: &str, uuid: &str, data: &[u8]) {

        if self.stream.lock().unwrap().is_none(){
            match TcpStream::connect(&self.addr){
                Ok(stream)=>{
                    *self.stream.lock().unwrap() = Some(stream);
                },
                Err(err)=>{
                    eprintln!("Error {}:{}: {} {}", file!(), line!(), err, self.addr);
                    return;
                }
            }
        }
        let db = self.db.clone();
        let stream = self.stream.clone();
        let mess = Message::new(to, from, uuid, data);
        rayon::spawn(move || {
            let mut is_send = false;
            if let Some(stream) = stream.lock().unwrap().as_mut(){
                is_send = mess.to_stream(stream);
            }
            if !is_send{
                *stream.lock().unwrap() = None;
                db.lock().unwrap().get_topic_addresses("name");
            }        
        });
    }
    
    pub fn close(&self) {
        let _ = syscall!(close(self.epoll_fd));
    }
}


fn add_write_stream(epoll_fd: i32, fd: RawFd)->io::Result<i32>{
    regist_event(epoll_fd, fd, libc::EPOLL_CTL_ADD)
}    
fn continue_write_stream(epoll_fd: i32, fd: RawFd) -> io::Result<i32> {
    regist_event(epoll_fd, fd, libc::EPOLL_CTL_MOD)
}
fn regist_event(epoll_fd: i32, fd: RawFd, ctl: i32)-> io::Result<i32> {
    let mut event = libc::epoll_event {
        events: (libc::EPOLLONESHOT | libc::EPOLLRDHUP | libc::EPOLLOUT | libc::EPOLLET) as u32,
        u64: fd as u64,
    };
    syscall!(epoll_ctl(epoll_fd, ctl, fd, &mut event))
}
fn remove_write_stream(epoll_fd: i32, fd: RawFd) -> io::Result<(i32)> {
    syscall!(epoll_ctl(epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut()))    
}















pub struct Topic {
    addr: String,
    stream: Arc<Mutex<Option<TcpStream>>>,
    db: Arc<Mutex<redis::Connect>>,
}

impl Topic {
    pub fn new(addr: &String, db: &Arc<Mutex<redis::Connect>>) -> Topic {
        Self {
            addr: addr.to_string(),
            stream: Arc::new(Mutex::new(None)),
            db: db.clone(),
        }
    }
    
}
