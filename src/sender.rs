use crate::message::Message;
use crate::redis;
use crate::print_error;
use crate::settings;
use crate::common;

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
    address: String,
    topic: String,
    stream: TcpStream,
    is_active: bool,
}

#[derive(Clone)]
struct Address{
    address: String,
    topic: String,
}

pub struct Sender{
    epoll_fd: i32,
    unique_name: String,
    addrs_for: HashSet<String>,
    addrs_new: Arc<Mutex<Vec<Address>>>,
    messages: Arc<Mutex<HashMap<String, LinkedList<Message>>>>, // key - addr
    last_mess_number: HashMap<String, u64>,              
    streams_fd: Arc<Mutex<HashMap<String, RawFd>>>,
    wakeup_fd: RawFd,
    is_new_addr: Arc<Mutex<bool>>,
}

impl Sender {
    pub fn new(unique_name: String, redis_path: String, source_topic: String)->Sender{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        let messages: Arc<Mutex<HashMap<String, LinkedList<Message>>>> = Arc::new(Mutex::new(HashMap::new()));
        let messages_new = messages.clone();
        let addrs_new: Arc<Mutex<Vec<Address>>> = Arc::new(Mutex::new(Vec::new()));
        let mut addrs_new_ = addrs_new.clone();
        let streams_fd: Arc<Mutex<HashMap<String, RawFd>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut streams_fd_ = streams_fd.clone();
        let wakeup_fd = wakeupfd_create(epoll_fd);
        let is_new_addr = Arc::new(Mutex::new(false));
        let is_new_addr_ = is_new_addr.clone();
        let unique_name_ = unique_name.clone();
        thread::spawn(move|| {
            let mut streams: HashMap<RawFd, Arc<Mutex<WriteStream>>> = HashMap::new();
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(settings::EPOLL_LISTEN_EVENTS_COUNT);
            let mut prev_time = common::current_time_ms();
            if let Ok(db) = redis::Connect::new(&unique_name_, &redis_path){
                let db = Arc::new(Mutex::new(db));
                db.lock().unwrap().set_source_topic(&source_topic);
                loop{ 
                    if *is_new_addr_.lock().unwrap() || check_available_stream(&mut prev_time) {
                        *is_new_addr_.lock().unwrap() = false;
                        append_new_streams(epoll_fd, &mut addrs_new_, &mut streams, &mut streams_fd_, &db);
                    }
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
                            write_stream(epoll_fd, stream_fd, &streams, messages_new.clone(), db.clone());
                        }else if ev.events as i32 & (libc::EPOLLHUP | libc::EPOLLERR) > 0{
                            remove_stream(epoll_fd, stream_fd, &mut streams, &streams_fd_, &addrs_new_);
                        }else{
                            print_error!(format!("unknown event {}", stream_fd as RawFd));
                        }
                    }               
                }
            }else{
                print_error!(format!("couldn't edis::Connect"));
            }
        });
        Self{
            unique_name,
            epoll_fd,
            addrs_for: HashSet::new(),
            addrs_new,
            messages,
            last_mess_number: HashMap::new(),
            streams_fd,
            wakeup_fd,
            is_new_addr,
        }
    }
    
    pub fn send_to(&mut self, db: &mut redis::Connect, addr_to: &str, to: &str, from: &str, uuid: &str, data: &[u8])->bool{
        let mut is_new_addr = false;
        if !self.addrs_for.contains(addr_to){
            self.addrs_for.insert(addr_to.to_string());
            is_new_addr = true;
            let listener_name ;
            let listener_topic;
            if let Some((topic, name)) = db.get_listener_by_address(&addr_to){
                listener_topic = topic;
                listener_name = name;
            }else{
                print_error!(format!("couldn't db.get_listener_by_address {}", addr_to));
                return false;
            }
            let last_mess_num = db.get_last_mess_number_for_sender(&listener_name, &listener_topic);
            if let Ok(last_mess_num) = last_mess_num{
                self.last_mess_number.insert(addr_to.to_string(), last_mess_num);
            }else {
                if let Err(err) = db.init_last_mess_number_from_sender(&listener_name, to){            
                    print_error!(&format!("error init_last_mess_number_from_sender from db: {}", err));
                    return false;
                }
                self.last_mess_number.insert(addr_to.to_string(), 0);
            }
            self.messages.lock().unwrap().insert(addr_to.to_string(), LinkedList::new());
        }  
        let number_mess = self.last_mess_number[addr_to] + 1;
        *self.last_mess_number.get_mut(addr_to).unwrap() = number_mess;
        let mess = Message::new(to, from, &self.unique_name, uuid, number_mess, data);
        self.messages.lock().unwrap().get_mut(addr_to).unwrap().push_back(mess);
        if let Some(strm_fd) = self.streams_fd.lock().unwrap().get(addr_to){
            continue_write_stream(self.epoll_fd, *strm_fd);
        }
        if is_new_addr{
            self.addrs_new.lock().unwrap().push(Address{address: addr_to.to_string(), topic: to.to_string()});
            *self.is_new_addr.lock().unwrap() = true;
            wakeupfd_notify(self.wakeup_fd);    
        }
        return true;
    }
    
    pub fn _close(&self) {
        let _ = syscall!(close(self.epoll_fd));
    }
}

fn check_available_stream(prev_time: &mut u64)->bool{
    if common::current_time_ms() - *prev_time > settings::CHECK_AVAILABLE_STREAM_TIMEOUT_MS{
        *prev_time = common::current_time_ms();
        return true;
    }
    return false;
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
                      addrs: &mut Arc<Mutex<Vec<Address>>>, 
                      streams: &mut HashMap<RawFd, Arc<Mutex<WriteStream>>>,
                      streams_fd: &mut Arc<Mutex<HashMap<String, RawFd>>>,
                      db: &Arc<Mutex<redis::Connect>>){
    let mut addrs_lost: Vec<Address> = Vec::new();
    for addr in &addrs.lock().unwrap().clone(){
        match TcpStream::connect(&addr.address){
            Ok(stream)=>{       
                let mut to_topic= String::new();
                if let Ok(mut db) = db.lock(){
                    if let Err(err) = db.init_addresses_of_topic(&addr.topic){
                        addrs_lost.push(addr.clone());
                        print_error!(format!("couldn't db.init_addresses_of_topic {}, error {}", addr.topic, err));
                        return;
                    }
                    let listener_name;
                    let listener_topic;
                    if let Some((topic, name)) = db.get_listener_by_address(&addr.address){
                        listener_topic = topic;
                        listener_name = name;
                        to_topic = listener_topic.clone();
                        if let Ok(num) = db.get_last_mess_number_for_sender(&listener_name, &listener_topic){
                            db.set_last_send_mess_number(&listener_name, &listener_topic, num);
                        }else{
                            print_error!(format!("couldn't db.get_last_mess_number_for_sender {}", addr.address));
                            db.set_last_send_mess_number(&listener_name, &listener_topic, 0);
                        }
                    }else{
                        addrs_lost.push(addr.clone());
                        print_error!(format!("couldn't db.get_listener_by_address {}", addr.address));
                        return;
                    } 
                }  
                stream.set_nonblocking(true).expect("couldn't stream set_nonblocking");
                let stm_fd = stream.as_raw_fd();
                let wstream = WriteStream{address: addr.address.clone(), topic: to_topic, stream, is_active: false};
                streams.insert(wstream.stream.as_raw_fd(), Arc::new(Mutex::new(wstream)));
                add_write_stream(epoll_fd, stm_fd);
                streams_fd.lock().unwrap().insert(addr.address.clone(), stm_fd);
            },
            Err(err)=>{
                addrs_lost.push(addr.clone());
                print_error!(&format!("{} {}", err, addr.address));
            }
        }
    }
    *addrs.lock().unwrap() = addrs_lost;
}

fn write_stream(epoll_fd: RawFd,
                stream_fd: RawFd,
                streams: &HashMap<RawFd, Arc<Mutex<WriteStream>>>, 
                messages_new: Arc<Mutex<HashMap<String, LinkedList<Message>>>>,
                db: Arc<Mutex<redis::Connect>>){
    if let Some(stream) = streams.get(&stream_fd){
        let stream = stream.clone();
        if let Ok(mut stream) = stream.try_lock(){
            if !stream.is_active && !&messages_new.lock().unwrap()[&stream.address].is_empty(){
                stream.is_active = true;
            }else{
                return;
            }
        }else{
            continue_write_stream(epoll_fd, stream_fd);
            return;
        }
        rayon::spawn(move || {
            let mut stream = stream.lock().unwrap();
            let addr_to = stream.address.to_string();
            let listener_topic;
            let listener_name;
            if let Some((topic, name)) = db.lock().unwrap().get_listener_by_address(&addr_to){
                listener_topic = topic;
                listener_name = name;
            }else{
                print_error!(format!("couldn't db.get_listener_by_address {}", addr_to));
                return;
            }
            let last_mess_number = db.lock().unwrap().get_last_mess_number_for_sender(&listener_name, &listener_topic);
            if let Err(err) = last_mess_number{
                print_error!(&format!("error get_last_mess_number_for_sender from db: {}", err));
                return;
            }
            let last_mess_number = last_mess_number.unwrap();
            let mut last_send_mess_number = db.lock().unwrap().get_last_send_mess_number(&listener_name, &listener_topic);
            {
                let mut buff: LinkedList<Message> = LinkedList::new();
                if let Some(messages) = messages_new.lock().unwrap().get_mut(&addr_to){
                    while !messages.is_empty() {
                        let mess = messages.pop_front().unwrap();
                        if last_mess_number < mess.number_mess{
                            buff.push_back(mess);
                        }
                    }
                }
                let mut writer = BufWriter::with_capacity(settings::WRITE_BUFFER_CAPASITY, stream.stream.by_ref()); 
                loop{                    
                    for mess in &buff{ 
                        if last_send_mess_number < mess.number_mess{
                            last_send_mess_number = mess.number_mess;
                            if !mess.to_stream(&mut writer){
                                break;
                            }
                        }                        
                    }                   
                    if let Some(messages) = messages_new.lock().unwrap().get_mut(&addr_to){
                        if !messages.is_empty(){
                            while !messages.is_empty() {
                                buff.push_back(messages.pop_front().unwrap());
                            }
                        }else{                            
                            *messages = buff;
                            break;
                        }       
                    }
                }
                if let Err(err) = writer.flush(){
                    if err.kind() != std::io::ErrorKind::WouldBlock &&
                        err.kind() != std::io::ErrorKind::Interrupted{
                        print_error!(&format!("{}", err));                    
                    }
                }               
                db.lock().unwrap().set_last_send_mess_number(&listener_name, &listener_topic, last_send_mess_number);
            }
            stream.is_active = false;            
        });
    }
}

fn remove_stream(epoll_fd: i32,
                 strm_fd: RawFd, 
                 streams: &mut HashMap<RawFd, Arc<Mutex<WriteStream>>>,
                 streams_fd: &Arc<Mutex<HashMap<String, RawFd>>>,
                 addrs_new: &Arc<Mutex<Vec<Address>>>){
    if let Some(stream) = streams.get(&strm_fd){
        remove_write_stream(epoll_fd, strm_fd);
        let address = stream.lock().unwrap().address.clone();
        let topic = stream.lock().unwrap().topic.clone();
        streams.remove(&strm_fd);
        streams_fd.lock().unwrap().remove(&address);
        addrs_new.lock().unwrap().push(Address{address, topic});
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
