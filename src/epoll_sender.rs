use crate::message::Message;
use crate::redis;

use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};
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

struct WriteStream{
    addr: String,
    stream: TcpStream,  
}

pub struct EPollSender{
    epoll_fd: i32,
    addrs_for: HashSet<String>, 
    addrs_new: Arc<Mutex<Vec<String>>>,    
    messages: Arc<Mutex<HashMap<String, Vec<Message>>>>, // key - addr    
}

impl EPollSender {
    pub fn new(db: Arc<Mutex<redis::Connect>>)->EPollSender{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        let messages: Arc<Mutex<HashMap<String, Vec<Message>>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut messages_new: Arc<Mutex<HashMap<String, Vec<Message>>>> = messages.clone();
        let addrs_new: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let mut addrs_new_: Arc<Mutex<Vec<String>>> = addrs_new.clone();
        thread::spawn(move|| {
            let mut streams: HashMap<RawFd, Arc<Mutex<WriteStream>>> = HashMap::new();
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(128);
            loop{ 
                if !addrs_new_.lock().unwrap().is_empty(){
                    append_new_streams(epoll_fd, &mut addrs_new_, &mut streams, &messages_new);
                }                
                wait(epoll_fd, &mut events);
                for ev in &events {  
                    if ev.events as i32 & libc::EPOLLOUT > 0{
                        let stream_fd = ev.u64 as RawFd;
                        write_stream(stream_fd, &streams, &mut messages_new);
                    }else if ev.events as i32 & (libc::EPOLLHUP | libc::EPOLLERR) > 0{
                        let stream_fd = ev.u64 as RawFd;
                        let _ = remove_write_stream(epoll_fd, stream_fd);
                        if let Some(stream) = streams.get(&stream_fd){
                            let addr = stream.lock().unwrap().addr.clone();
                            addrs_new_.lock().unwrap().push(addr);
                        }
                        streams.remove(&stream_fd);                        
                    }else{
                        eprintln!("unexpected events: {}", ev.events as i32);
                    }
                }
            }
        });
        Self{
            epoll_fd,
            addrs_for: HashSet::new(),
            addrs_new,
            messages,
        }
    }
    
    pub fn send_to(&mut self, addr_to: &str, to: &str, from: &str, uuid: &str, data: &[u8]) {
        if !self.addrs_for.contains(addr_to){
            self.addrs_for.insert(addr_to.to_string());
            self.addrs_new.lock().unwrap().push(addr_to.to_string());
        }        
        let mess = Message::new(to, from, uuid, data);
        if let Ok(mut messages) = self.messages.lock(){
            if !messages.contains_key(addr_to){
                messages.insert(addr_to.to_string(), Vec::new());
            }
            messages.get_mut(addr_to).unwrap().push(mess);
        }        
    }
    
    pub fn close(&self) {
        let _ = syscall!(close(self.epoll_fd));
    }
}

fn wait(epoll_fd: RawFd, events: &mut Vec<libc::epoll_event>){
    match syscall!(epoll_wait(
        epoll_fd,
        events.as_mut_ptr() as *mut libc::epoll_event,
        128,
        1000,
    )){
        Ok(ready_count)=>{
            unsafe { events.set_len(ready_count as usize) };
        },
        Err(err)=>{
            unsafe { events.set_len(0); };
            if err.kind() != std::io::ErrorKind::Interrupted{
                eprintln!("couldn't epoll_wait: {}", err);
            }
        }
    }    
}

fn append_new_streams(epoll_fd: RawFd,
                      addrs: &mut Arc<Mutex<Vec<String>>>, 
                      streams: &mut HashMap<RawFd, Arc<Mutex<WriteStream>>>,
                      messages_new: &Arc<Mutex<HashMap<String, Vec<Message>>>>){
    let mut addrs_lost: Vec<String> = Vec::new();
    for addr in addrs.lock().unwrap().clone(){
        if let Ok(messages_new) = messages_new.lock(){
            if messages_new.contains_key(&addr) && messages_new[&addr].is_empty(){
                continue;
            }
        }
        for stream in streams.clone().into_values(){ 
            if stream.lock().unwrap().addr == addr{
                match TcpStream::connect(&addr){
                    Ok(stream)=>{
                        add_write_stream(epoll_fd, stream.as_raw_fd());
                        streams.insert(stream.as_raw_fd(), Arc::new(Mutex::new(WriteStream{addr: addr.clone(), stream})));
                    },
                    Err(err)=>{
                        addrs_lost.push(addr.clone());
                        eprintln!("Error {}:{}: {} {}", file!(), line!(), err, addr);
                    }
                }
            }
        }
    }
    *addrs.lock().unwrap() = addrs_lost;
}

fn write_stream(stream_fd: RawFd, streams: &HashMap<RawFd, Arc<Mutex<WriteStream>>>, messages_new: &mut Arc<Mutex<HashMap<String, Vec<Message>>>>){
    if let Some(stream) = streams.get(&stream_fd){
        let addr = stream.lock().unwrap().addr.clone();
        let messages_new = messages_new.clone();
        if !&messages_new.lock().unwrap()[&addr].is_empty(){
            let stream = stream.clone();
            rayon::spawn(move || {
                let messages = messages_new.lock().unwrap()[&addr].to_vec();
                let mut stream = stream.lock().unwrap();
                for mess in messages{
                    let _ = mess.to_stream(&mut stream.stream);
                    // if !is_send{
                    //     *stream.lock().unwrap() = None;
                    //     //db.lock().unwrap().get_topic_addresses("name");
                    // } 
                }       
            });
        }
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
