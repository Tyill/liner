use crate::message::Message;
use crate::redis;
use crate::print_error;
use crate::settings;

use std::net::TcpStream;
use std::os::raw::c_void;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet, LinkedList};
use std::thread;
use std::os::unix::io::{AsRawFd, RawFd};
use std::io::{self, BufWriter, Write};


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
    pub address: String,
    pub stream: TcpStream,
}

pub struct Sender{
    epoll_fd: i32,
    db: Arc<Mutex<redis::Connect>>,
    addrs_for: HashSet<String>,
    addrs_new: Arc<Mutex<Vec<String>>>,
    messages: Arc<Mutex<HashMap<String, LinkedList<Message>>>>, // key - addr
    last_mess_number: HashMap<String, u64>,              
    streams_fd: Arc<Mutex<HashMap<String, RawFd>>>,
    wakeup_fd: RawFd,
}

impl Sender {
    pub fn new(db: Arc<Mutex<redis::Connect>>)->Sender{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        let messages: Arc<Mutex<HashMap<String, LinkedList<Message>>>> = Arc::new(Mutex::new(HashMap::new()));
        let messages_new = messages.clone();
        let addrs_new: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let mut addrs_new_ = addrs_new.clone();
        let streams_fd: Arc<Mutex<HashMap<String, RawFd>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut streams_fd_ = streams_fd.clone();
        let db_ = db.clone();
        let wakeup_fd = wakeupfd_create(epoll_fd);
        thread::spawn(move|| {
            let mut streams: HashMap<RawFd, Arc<Mutex<WriteStream>>> = HashMap::new();
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(settings::EPOLL_LISTEN_EVENTS_COUNT);
            loop{ 
                append_new_streams(epoll_fd, &mut addrs_new_, &mut streams, &mut streams_fd_, &messages_new);
                if !wait(epoll_fd, &mut events){
                    break;
                }                  
                for ev in &events {  
                    let stream_fd = ev.u64 as RawFd;
                    if ev.events as i32 & libc::EPOLLIN > 0{
                        if stream_fd == wakeup_fd{
                            wakeupfd_reset(wakeup_fd);
                        }
                    }else if ev.events as i32 & libc::EPOLLOUT > 0{
                        write_stream(stream_fd, &streams, messages_new.clone(), db_.clone());
                    }else if ev.events as i32 & (libc::EPOLLHUP | libc::EPOLLERR) > 0{
                        remove_stream(epoll_fd, stream_fd, &mut streams, &streams_fd_, &addrs_new_);
                    }else{
                        print_error!(format!("unknown event {}", stream_fd as RawFd));
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
            last_mess_number: HashMap::new(),
            streams_fd,
            wakeup_fd,
        }
    }
    
    pub fn send_to(&mut self, addr_to: &str,  to: &str, from: &str, uuid: &str, data: &[u8])->bool{
        let mut is_new_addr = false;
        if !self.addrs_for.contains(addr_to){
            self.addrs_for.insert(addr_to.to_string());
            is_new_addr = true;
            let last_mess_num = self.db.lock().unwrap().get_last_mess_number_for_sender(to);
            if let Ok(last_mess_num) = last_mess_num{
                self.last_mess_number.insert(addr_to.to_string(), last_mess_num);
            }else {
                if let Err(err) = self.db.lock().unwrap().init_last_mess_number_from_sender(to){            
                    print_error!(&format!("error init_last_mess_number_from_sender from db: {}", err));
                    return false;
                }
                self.last_mess_number.insert(addr_to.to_string(), 0);
            }
        }  
        let number_mess = self.last_mess_number[addr_to] + 1;
        *self.last_mess_number.get_mut(addr_to).unwrap() = number_mess;
        let sender_name = self.db.lock().unwrap().get_unique_name();
        let mess = Message::new(to, from, &sender_name, uuid, number_mess, data);
        if let Ok(mut messages) = self.messages.lock(){
            if !messages.contains_key(addr_to){
                messages.insert(addr_to.to_string(), LinkedList::new());
            }
            messages.get_mut(addr_to).unwrap().push_back(mess);
            if let Some(strm_fd) = self.streams_fd.lock().unwrap().get(addr_to){
                continue_write_stream(self.epoll_fd, *strm_fd);
            }
        }
        if is_new_addr{
            self.addrs_new.lock().unwrap().push(addr_to.to_string());
            wakeupfd_notify(self.wakeup_fd);    
        }
        return true;
    }
    
    pub fn _close(&self) {
        let _ = syscall!(close(self.epoll_fd));
    }
}

fn wait(epoll_fd: RawFd, events: &mut Vec<libc::epoll_event>)->bool{
    match syscall!(epoll_wait(
        epoll_fd,
        events.as_mut_ptr() as *mut libc::epoll_event,
        settings::EPOLL_LISTEN_EVENTS_COUNT as i32,
        -1,
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
                      messages_new: &Arc<Mutex<HashMap<String, LinkedList<Message>>>>){
    let mut addrs_lost: Vec<String> = Vec::new();
    for addr in &addrs.lock().unwrap().clone(){
        if let Ok(messages_new) = messages_new.lock(){
            if messages_new.contains_key(addr) && messages_new[addr].is_empty(){
                continue;
            }
        }
        match TcpStream::connect(&addr){
            Ok(stream)=>{
                stream.set_nonblocking(true).expect("couldn't stream set_nonblocking");
                add_write_stream(epoll_fd, stream.as_raw_fd());
                streams_fd.lock().unwrap().insert(addr.clone(), stream.as_raw_fd());
                let wstream = WriteStream{address: addr.clone(), stream};
                streams.insert(wstream.stream.as_raw_fd(), Arc::new(Mutex::new(wstream)));
            },
            Err(err)=>{
                addrs_lost.push(addr.clone());
                print_error!(&format!("{} {}", err, addr));
            }
        }
    }
    *addrs.lock().unwrap() = addrs_lost;
}

fn write_stream(stream_fd: RawFd,
                streams: &HashMap<RawFd, Arc<Mutex<WriteStream>>>, 
                messages_new: Arc<Mutex<HashMap<String, LinkedList<Message>>>>,
                db: Arc<Mutex<redis::Connect>>){
    if let Some(stream) = streams.get(&stream_fd){
        let addr_to = stream.lock().unwrap().address.clone();
        if !&messages_new.lock().unwrap()[&addr_to].is_empty(){
            let stream = stream.clone();
            rayon::spawn(move || {
                let stream = stream.lock().unwrap();
                let topic;
                if let Some(t) = db.lock().unwrap().get_topic_by_address(&addr_to){
                    topic = t;
                }else{
                    print_error!("couldn't get_topic_by_address");
                    return;
                }                
                let mut last_send_mess_number = 0;
                let mut has_new_mess = true;
                let mut write_count = 0;
                let mut writer = BufWriter::with_capacity(settings::WRITE_BUFFER_CAPASITY, &stream.stream); 
                while has_new_mess{
                    let last_mess_number = db.lock().unwrap().get_last_mess_number_for_sender(&topic);
                    if let Err(err) = last_mess_number{
                        print_error!(&format!("error get_last_mess_number_for_sender from db: {}", err));
                        return;
                    }
                    let last_mess_number = last_mess_number.unwrap();
                    println!("sender last_mess_number {}",last_mess_number);  
                    let mut buff: LinkedList<Message> = LinkedList::new();
                    if let Some(messages) = messages_new.lock().unwrap().get_mut(&addr_to){
                        while !messages.is_empty() {
                            let mess = messages.pop_front().unwrap();
                            if last_mess_number < mess.number_mess{
                                buff.push_back(mess);
                            }
                        }
                    }
                    for mess in &buff{ 
                        if last_send_mess_number < mess.number_mess{
                            last_send_mess_number = mess.number_mess;
                            if !mess.to_stream(&mut writer){
                                break;
                            }
                            write_count += 1;
                        }                        
                    }                   
                    if let Some(messages) = messages_new.lock().unwrap().get_mut(&addr_to){
                        has_new_mess = !messages.is_empty();
                        buff.append(messages);
                        *messages = buff;
                        if has_new_mess{
                            println!("sender has_new_mess {} write_count {} last_num {}", has_new_mess, write_count, last_send_mess_number);
                        }
                    }
                }
                if let Err(err) = writer.flush(){
                    if err.kind() != std::io::ErrorKind::WouldBlock &&
                       err.kind() != std::io::ErrorKind::Interrupted{
                       print_error!(&format!("{}", err));                    
                    }
                }
                println!("sender write_count {} last_num {}", write_count, last_send_mess_number); 
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
        let addr = stream.lock().unwrap().address.clone();
        streams.remove(&strm_fd);
        streams_fd.lock().unwrap().remove(&addr);
        addrs_new.lock().unwrap().push(addr);
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
fn wakeupfd_create(epoll_fd: RawFd)->RawFd{
    let event_fd = syscall!(eventfd(0, 0)).expect("couldn't eventfd");
    let mut event = libc::epoll_event {
        events: (libc::EPOLLIN) as u32,
        u64: event_fd as u64,
    };
    syscall!(epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, event_fd, &mut event)).expect("couldn't eventfd_create");
    event_fd
}
fn wakeupfd_notify(event_fd: RawFd){
    let b: u64 = 1;
    if let Err(err) = syscall!(write(event_fd, &b as *const u64 as *const c_void, std::mem::size_of::<u64>())){
        print_error!(&format!("{}", err));
    }  
}
fn wakeupfd_reset(event_fd: i32){
    let b: u64 = 0;
    if let Err(err) = syscall!(read(event_fd, &b as *const u64 as *mut c_void, std::mem::size_of::<u64>())){
        print_error!(&format!("{}", err));
    }
}

fn remove_write_stream(epoll_fd: i32, fd: RawFd){
    syscall!(epoll_ctl(epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut())).expect("couldn't remove_write_stream");   
}
