use crate::message::Message;
use crate::redis;
use crate::print_error;

use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};
use std::thread;
use std::os::unix::io::{AsRawFd, RawFd};
use std::io::{BufWriter, Write};


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
    db: Arc<Mutex<redis::Connect>>,
    addrs_for: HashSet<String>,
    addrs_new: Arc<Mutex<Vec<String>>>,
    messages: Arc<Mutex<HashMap<String, Vec<Message>>>>, // key - addr
    last_number_mess: HashMap<String, u64>,              
    streams_fd: Arc<Mutex<HashMap<String, RawFd>>>
}

impl EPollSender {
    pub fn new(localhost: String, db: Arc<Mutex<redis::Connect>>)->EPollSender{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        let messages: Arc<Mutex<HashMap<String, Vec<Message>>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut messages_new = messages.clone();
        let addrs_new: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let mut addrs_new_ = addrs_new.clone();
        let streams_fd: Arc<Mutex<HashMap<String, RawFd>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut streams_fd_ = streams_fd.clone();
        thread::spawn(move|| {
            let mut streams: HashMap<RawFd, Arc<Mutex<WriteStream>>> = HashMap::new();
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(128);
            loop{ 
                append_new_streams(epoll_fd, &mut addrs_new_, &mut streams, &mut streams_fd_, &messages_new);
                
                if !wait(epoll_fd, &mut events, 10*1000){ // 10sec
                    break;
                }
                  
                for ev in &events {  
                    if ev.events as i32 & libc::EPOLLOUT > 0{
                        let stream_fd = ev.u64 as RawFd;
                        write_stream(stream_fd, &streams, &mut messages_new, &localhost, db);
                    }else if ev.events as i32 & (libc::EPOLLHUP | libc::EPOLLERR) > 0{
                        let stream_fd = ev.u64 as RawFd;
                        remove_stream(epoll_fd, stream_fd, &mut streams, &streams_fd_, &addrs_new_);
                    }
                }               
            }
        });
        Self{
            epoll_fd,
            db,
            addrs_for: HashSet::new(),
            addrs_new,
            messages,
            last_number_mess: HashMap::new(),
            streams_fd,
        }
    }
    
    pub fn send_to(&mut self, addr_from: &str, addr_to: &str,  to: &str, from: &str, uuid: &str, data: &[u8]) {
        if !self.addrs_for.contains(addr_to){
            self.addrs_for.insert(addr_to.to_string());
            self.addrs_new.lock().unwrap().push(addr_to.to_string()); 
            if let Ok(last_mess_num) = self.db.lock().unwrap().get_last_mess_number(addr_from, addr_to){
                self.last_number_mess.insert(addr_to.to_string(), last_mess_num);
            }else{
                print_error(&format!("error get_last_mess_number from db"));
                self.last_number_mess.insert(addr_to.to_string(), 0);
            }
        }  
        let number_mess = self.last_number_mess[addr_to] + 1;
        *self.last_number_mess.get_mut(addr_to).unwrap() = number_mess;
        let mess = Message::new(to, from, uuid, number_mess, data);
        if let Ok(mut messages) = self.messages.lock(){
            if !messages.contains_key(addr_to){
                messages.insert(addr_to.to_string(), Vec::new());
            }
            messages.get_mut(addr_to).unwrap().push(mess);
            if let Some(strm_fd) = self.streams_fd.lock().unwrap().get(addr_to){
                continue_write_stream(self.epoll_fd, *strm_fd);
            }
        }   
    }
    
    pub fn close(&self) {
        let _ = syscall!(close(self.epoll_fd));
    }
}

fn wait(epoll_fd: RawFd, events: &mut Vec<libc::epoll_event>, timeout_ms: i32)->bool{
    match syscall!(epoll_wait(
        epoll_fd,
        events.as_mut_ptr() as *mut libc::epoll_event,
        128,
        timeout_ms,
    )){
        Ok(ready_count)=>{
            unsafe { events.set_len(ready_count as usize) };
            return true;
        },
        Err(err)=>{
            unsafe { events.set_len(0); };
            return err.kind() == std::io::ErrorKind::Interrupted;           
        }
    }    
}

fn append_new_streams(epoll_fd: RawFd,
                      addrs: &mut Arc<Mutex<Vec<String>>>, 
                      streams: &mut HashMap<RawFd, Arc<Mutex<WriteStream>>>,
                      streams_fd: &mut Arc<Mutex<HashMap<String, RawFd>>>,
                      messages_new: &Arc<Mutex<HashMap<String, Vec<Message>>>>){
    let mut addrs_lost: Vec<String> = Vec::new();
    for addr in addrs.lock().unwrap().clone(){
        if let Ok(messages_new) = messages_new.lock(){
            if messages_new.contains_key(&addr) && messages_new[&addr].is_empty(){
                continue;
            }
        }
        match TcpStream::connect(&addr){
            Ok(stream)=>{
                stream.set_nonblocking(true).expect("couldn't stream set_nonblocking");
                add_write_stream(epoll_fd, stream.as_raw_fd());
                streams_fd.lock().unwrap().insert(addr.clone(), stream.as_raw_fd());
                streams.insert(stream.as_raw_fd(), Arc::new(Mutex::new(WriteStream{addr: addr.clone(), stream})));
            },
            Err(err)=>{
                addrs_lost.push(addr.clone());
                print_error(&format!("Error {}:{}: {} {}", file!(), line!(), err, addr));
            }
        }
    }
    *addrs.lock().unwrap() = addrs_lost;
}

fn write_stream(stream_fd: RawFd,
                streams: &HashMap<RawFd, Arc<Mutex<WriteStream>>>, 
                messages_new: &Arc<Mutex<HashMap<String, Vec<Message>>>>,
                localhost: String){
    if let Some(stream) = streams.get(&stream_fd){
        let addr = stream.lock().unwrap().addr.clone();
        let messages_new = messages_new.clone();
        if !&messages_new.lock().unwrap()[&addr].is_empty(){
            let stream = stream.clone();
            rayon::spawn(move || {
                let mut stream = stream.lock().unwrap();
                 
                let mut buff: Vec<Message> = Vec::new(); 
                if let Some(messages) = messages_new.lock().unwrap().get_mut(&addr){
                    while !messages.is_empty() {
                        let mess = messages.pop().unwrap();
                        // if last_mess_num < mess.number_mess{
                        //     buff.push(mess);
                        // }
                    }
                }
                let mut writer = BufWriter::new(&stream.stream); 
                for mess in buff.iter().rev(){ 
                    if !mess.to_stream(&mut writer){
                        break;
                    }
                }
                writer.flush();
                if let Some(messages) = messages_new.lock().unwrap().get_mut(&addr){
                    let mut ix = 0;
                    while !buff.is_empty() {
                        let mess = buff.pop().unwrap();
                        messages.insert(ix, mess);
                        ix += 1;
                    }
                }                            
            });
        }
    }
}

fn remove_stream(epoll_fd: i32,
                 strm_fd: RawFd, 
                 streams: &mut HashMap<RawFd, Arc<Mutex<WriteStream>>>,
                 streams_fd: &Arc<Mutex<HashMap<String, RawFd>>>,
                 addrs_new: &Arc<Mutex<Vec<String>>>){
    if let Some(stream) = streams.get(&strm_fd){
        remove_write_stream(epoll_fd, strm_fd);
        let addr = stream.lock().unwrap().addr.clone();
        streams.remove(&strm_fd);
        streams_fd.lock().unwrap().remove(&addr);
        addrs_new.lock().unwrap().push(addr);
    }     
}

fn get_last_mess_number(let localhost: String, let addr_to: String, let &mut last_mess_num: u64)->bool{
    if let Ok(num) = db.lock().unwrap().get_last_mess_number(localhost, &addr_to){
        last_mess_num = num;
        return true;
    }else{
        print_error(&format!("error get_last_mess_number from db"));
        return false;
    } 
}

fn add_write_stream(epoll_fd: i32, fd: RawFd){
    regist_event(epoll_fd, fd, libc::EPOLL_CTL_ADD).expect("couldn't add_write_stream");
}
fn continue_write_stream(epoll_fd: i32, fd: RawFd){
    regist_event(epoll_fd, fd, libc::EPOLL_CTL_MOD).expect("couldn't continue_write_stream");
}
fn regist_event(epoll_fd: i32, fd: RawFd, ctl: i32)-> io::Result<i32> {
    let mut event = libc::epoll_event {
        events: (libc::EPOLLONESHOT | libc::EPOLLRDHUP | libc::EPOLLERR | libc::EPOLLOUT) as u32,
        u64: fd as u64,
    };
    syscall!(epoll_ctl(epoll_fd, ctl, fd, &mut event))
}
fn remove_write_stream(epoll_fd: i32, fd: RawFd){
    syscall!(epoll_ctl(epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut())).expect("couldn't remove_write_stream");   
}
