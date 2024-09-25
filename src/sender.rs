use crate::message::Message;
use crate::redis;
use crate::print_error;
use crate::redis::Connect;
use crate::settings;
use crate::common;

use std::net::TcpStream;
use std::ops::DerefMut;
use std::os::raw::c_void;
use std::thread::JoinHandle;
use std::time::Duration;
use std::sync::{Arc, Mutex, Condvar};
use std::collections::{HashMap, HashSet};
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
    listener_topic: String,
    listener_name: String,
    stream: TcpStream,
    last_send_mess_number: u64,
    is_active: bool,
    is_close: bool,
}

#[derive(Clone)]
struct Address{
    address: String,
    listener_topic: String,
    is_new_addr: bool,
}

type MessList = HashMap<String, Option<Vec<Message>>>;
type ArcWriteStream = Arc<Mutex<WriteStream>>;

pub struct Sender{
    epoll_fd: RawFd,
    wakeup_fd: RawFd,
    unique_name: String,
    addrs_for: HashSet<String>,
    addrs_new: Arc<Mutex<Vec<Address>>>,
    message_buffer: Arc<Mutex<MessList>>, // key - addr
    messages: Arc<Mutex<MessList>>, // key - addr
    last_mess_number: HashMap<String, u64>,              
    streams_fd: Arc<Mutex<HashMap<String, RawFd>>>,    
    is_new_addr: Arc<Mutex<bool>>,
    is_close: Arc<Mutex<bool>>,
    ctime_ms: HashMap<String, u64>, // key - addr
    delay_write_list: Arc<Mutex<HashSet<String>>>, // value - addr
    delay_write_cvar: Arc<(Mutex<bool>, Condvar)>,
    stream_thread: Option<JoinHandle<()>>,
    wdelay_thread: Option<JoinHandle<()>>,
}

impl Sender {
    pub fn new(unique_name: String, redis_path: String, source_topic: String)->Sender{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(HashMap::new()));
        let messages_ = messages.clone();
        let addrs_for: HashSet<String> = HashSet::new();
        let addrs_new: Arc<Mutex<Vec<Address>>> = Arc::new(Mutex::new(Vec::new()));
        let mut addrs_new_ = addrs_new.clone();
        let streams_fd: Arc<Mutex<HashMap<String, RawFd>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut streams_fd_ = streams_fd.clone();
        let wakeup_fd = wakeupfd_create(epoll_fd);
        let is_new_addr: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let is_new_addr_ = is_new_addr.clone();
        let unique_name_ = unique_name.clone();
      
        let stream_thread = thread::spawn(move|| {
            let mut streams: HashMap<RawFd, ArcWriteStream> = HashMap::new();
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(settings::EPOLL_LISTEN_EVENTS_COUNT);
            let mut prev_time = common::current_time_ms();
            if let Ok(db) = redis::Connect::new(&unique_name_, &redis_path){
                let db = Arc::new(Mutex::new(db));
                db.lock().unwrap().set_source_topic(&source_topic);
                loop{ // stream cycle
                    if check_available_stream(&is_new_addr_, &mut prev_time) {
                        append_streams(epoll_fd, &mut addrs_new_, &mut streams, 
                                           &mut streams_fd_, &messages_, &db);
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
                            write_stream(stream_fd, &streams, messages_.clone(), db.clone());
                        }else if ev.events as i32 & (libc::EPOLLHUP | libc::EPOLLERR) > 0{
                            remove_stream(epoll_fd, stream_fd, &mut streams, &streams_fd_,
                                          &messages_, &addrs_new_, &db);
                        }else{
                            print_error!(format!("unknown event {}", stream_fd as RawFd));
                        }
                    }               
                }
                close_streams(&messages_, &mut streams, &db);
            }else{
                print_error!(format!("couldn't redis::Connect"));
            }
        });

        let delay_write_list: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let delay_write_list_ = delay_write_list.clone();
        let delay_write_cvar: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));
        let delay_write_cvar_ = delay_write_cvar.clone();
        let messages_ = messages.clone();
        let streams_fd_ = streams_fd.clone();
        let message_buffer = Arc::new(Mutex::new(HashMap::new()));
        let message_buffer_ = message_buffer.clone();
        let is_close: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let is_close_ = is_close.clone();
        let wdelay_thread = thread::spawn(move||
            while !*is_close_.lock().unwrap(){ // write delay cycle
                if delay_write_list_.lock().unwrap().is_empty(){
                    let (lock, cvar) = &*delay_write_cvar_;
                    let mut _started = lock.lock().unwrap();
                    if !*is_close_.lock().unwrap(){
                        _started = cvar.wait(_started).unwrap();
                    }
                }
                std::thread::sleep(Duration::from_millis(settings::WRITE_MESS_TIMEOUT_MS));

                if let Ok(mut delay_write_list) = delay_write_list_.lock(){
                    for addr in &delay_write_list.clone(){
                        send_mess_to_listener(epoll_fd, addr, &message_buffer_, &messages_, &streams_fd_);
                    }
                    delay_write_list.clear();
                }
            }
        );
        Self{
            unique_name,
            epoll_fd,
            addrs_for,
            addrs_new,
            message_buffer,
            messages,
            last_mess_number: HashMap::new(),
            streams_fd,
            wakeup_fd,
            is_new_addr,
            is_close,
            ctime_ms: HashMap::new(),
            delay_write_list,
            delay_write_cvar,
            stream_thread: Some(stream_thread),
            wdelay_thread: Some(wdelay_thread),
        }
    }
    
    pub fn send_to(&mut self, db: &mut redis::Connect, addr_to: &str, to: &str, from: &str, 
                   uuid: &str, data: &[u8], at_least_once_delivery: bool)->bool{
        let mut is_new_addr = false;
        if !self.addrs_for.contains(addr_to){
            if !self.append_new_state(db, addr_to, to){
                return false;
            }
            self.addrs_for.insert(addr_to.to_string());
            is_new_addr = true;
        }
        let number_mess = self.last_mess_number[addr_to] + 1;
        *self.last_mess_number.get_mut(addr_to).unwrap() = number_mess;
       
        let mess = Message::new(to, from, &self.unique_name, uuid, number_mess, data, at_least_once_delivery);
        self.send_mess_to_buff(mess, addr_to);
               
        if is_new_addr{
            self.addrs_new.lock().unwrap().push(Address{address: addr_to.to_string(), 
                                                        listener_topic: to.to_string(),
                                                        is_new_addr: true});
            *self.is_new_addr.lock().unwrap() = true;
            wakeupfd_notify(self.wakeup_fd);    
        }
        true
    }
    
    fn send_mess_to_buff(&mut self, mess: Message, addr_to: &str){
        let ct = mess.timestamp;
        if let Ok(mut message_buffer_lock) = self.message_buffer.lock(){
            if let Some(mbuff) = message_buffer_lock.get_mut(addr_to).unwrap(){
                mbuff.push(mess);
            }else{
                *message_buffer_lock.get_mut(addr_to).unwrap() = Some(vec![mess]);
            }
        }        
        if ct - self.ctime_ms[addr_to] >= settings::WRITE_MESS_DELAY_MS {
            *self.ctime_ms.get_mut(addr_to).unwrap() = ct;
            send_mess_to_listener(self.epoll_fd, addr_to, &self.message_buffer, &self.messages, &self.streams_fd);
        }else{
            let is_first = self.delay_write_list.lock().unwrap().is_empty();
            self.delay_write_list.lock().unwrap().insert(addr_to.to_string());
            if is_first{
                self.wdelay_thread_notify();
            }
        }
    }
    fn wdelay_thread_notify(&self){
        let (lock, cvar) = &*self.delay_write_cvar;
        *lock.lock().unwrap() = true;
        cvar.notify_one();
    }
   
    fn append_new_state(&mut self, db: &mut redis::Connect, addr_to: &str, listener_topic: &str)->bool{
        let listener_name;
        if let Ok(name) = db.get_listener_unique_name(listener_topic, addr_to){
            listener_name = name;
        }else{
            print_error!(format!("couldn't db.get_listener_unique_name {}", addr_to));
            return false;
        }        
        let last_mess_num = db.get_last_mess_number_for_sender(&listener_name, listener_topic);
        if let Ok(last_mess_num) = last_mess_num{
            self.last_mess_number.insert(addr_to.to_string(), last_mess_num);
        }else {
            if let Err(err) = db.init_last_mess_number_from_sender(&listener_name, listener_topic){            
                print_error!(&format!("init_last_mess_number_from_sender from db: {}", err));
                return false;
            }
            self.last_mess_number.insert(addr_to.to_string(), 0);
        }            
        if let Ok(last_mess) = db.load_last_message_for_sender(&listener_name, listener_topic){
            if let Some(mess) = last_mess{
                if mess.number_mess > self.last_mess_number[addr_to]{
                    *self.last_mess_number.get_mut(addr_to).unwrap() = mess.number_mess;
                }
            }
        }else {
            print_error!("db.load_last_message_for_sender");
        }
        if let Err(err) = db.save_listener_for_sender(addr_to, listener_topic){
            print_error!(&format!("db.save_listener_for_sender {}", err));
        }

        self.messages.lock().unwrap().insert(addr_to.to_string(), Some(Vec::new()));
        self.message_buffer.lock().unwrap().insert(addr_to.to_string(), Some(Vec::new()));
        self.ctime_ms.insert(addr_to.to_string(), common::current_time_ms());
        true
    }

    pub fn load_prev_connects(&mut self, db: &mut Connect){
       
        let mut prev_conns: Vec<Address> = Vec::new();
        match db.get_listeners_of_sender() {
            Ok(addr_topic) => {
                for t in addr_topic{
                    prev_conns.push(Address{address: t.0, listener_topic: t.1, is_new_addr: true });
                }
            },
            Err(err)=>{
                print_error!(&format!("db.get_listeners_of_sender {}", err));
            }
        }       
        for a in prev_conns{
            self.addrs_for.insert(a.address.clone());
            if self.append_new_state(db, &a.address, &a.listener_topic){
                self.addrs_new.lock().unwrap().push(a);
            }            
        }
        if !self.addrs_new.lock().unwrap().is_empty(){
            *self.is_new_addr.lock().unwrap() = true;
            wakeupfd_notify(self.wakeup_fd);
        }   
    }
}

fn send_mess_to_listener(epoll_fd: RawFd, addr_to: &str, 
                           message_buffer: &Arc<Mutex<MessList>>,
                           messages: &Arc<Mutex<MessList>>,
                           streams_fd: &Arc<Mutex<HashMap<String, RawFd>>>){
    let buff = message_buffer.lock().unwrap().get_mut(addr_to).unwrap().take();
    if buff.is_none(){
        return; // already send from delay write cycle
    }
    if let Ok(mut mess_lock) = messages.lock(){
        if let Some(mess_for_send) = mess_lock.get_mut(addr_to).unwrap().as_mut(){
            mess_for_send.append(&mut buff.unwrap());
        }else{
            *mess_lock.get_mut(addr_to).unwrap() = buff;
        }
    }
    if let Some(strm_fd) = streams_fd.lock().unwrap().get(addr_to){
        continue_write_stream(epoll_fd, *strm_fd);
    }
}

fn check_available_stream(is_new_addr: &Arc<Mutex<bool>>, prev_time: &mut u64)->bool{
    let ct = common::current_time_ms();
    if *is_new_addr.lock().unwrap() || ct - *prev_time > settings::CHECK_AVAILABLE_STREAM_TIMEOUT_MS{
        *is_new_addr.lock().unwrap() = false;
        *prev_time = ct;
        return true;
    }
    false
}

fn wait(epoll_fd: RawFd, events: &mut Vec<libc::epoll_event>)->bool{
    match syscall!(epoll_wait(
        epoll_fd,
        events.as_mut_ptr(),
        settings::EPOLL_LISTEN_EVENTS_COUNT as i32,
        settings::CHECK_AVAILABLE_STREAM_TIMEOUT_MS as i32,
    )){
        Ok(ready_count)=>{
            unsafe { events.set_len(ready_count as usize) };
            true
        },
        Err(err)=>{
            unsafe { events.set_len(0); };
            err.kind() == std::io::ErrorKind::Interrupted           
        }
    }    
}

fn append_streams(epoll_fd: RawFd,
                  addrs: &mut Arc<Mutex<Vec<Address>>>, 
                  streams: &mut HashMap<RawFd, ArcWriteStream>,
                  streams_fd: &mut Arc<Mutex<HashMap<String, RawFd>>>,
                  messages: &Arc<Mutex<MessList>>,
                  db: &Arc<Mutex<redis::Connect>>){
    let mut addrs_lost: Vec<Address> = Vec::new();
    for addr in &addrs.lock().unwrap().clone(){
        if !addr.is_new_addr && messages.lock().unwrap()[&addr.address].as_ref().unwrap().is_empty(){
            addrs_lost.push(addr.clone());
            continue;
        }
        let listener_name;
        if let Ok(name) = db.lock().unwrap().get_listener_unique_name(&addr.listener_topic, &addr.address){
            listener_name = name;                    
        }else{
            addrs_lost.push(addr.clone());
            print_error!(format!("couldn't db.get_listener_unique_name {}", addr.address));
            return;
        }
        match TcpStream::connect(&addr.address){
            Ok(stream)=>{
                stream.set_nonblocking(true).expect("couldn't stream set_nonblocking");
                let stm_fd = stream.as_raw_fd();

                match db.lock().unwrap().load_messages_for_sender(&listener_name, &addr.listener_topic){
                    Ok(mut mess_from_db) =>{
                        if let Ok(mut mess_lock) = messages.lock(){
                            if let Some(mut mess_for_send) = mess_lock.get_mut(&addr.address).unwrap().take(){
                                mess_from_db.append(&mut mess_for_send);
                            }
                            *mess_lock.get_mut(&addr.address).unwrap() = Some(mess_from_db);
                        }
                    },
                    Err(err)=>{
                        print_error!(&format!("db.load_messages_for_sender, {} {}", addr.address, err));
                    }
                }
                let mut last_send_mess_number: u64 = 0;
                if let Ok(num) = db.lock().unwrap().get_last_mess_number_for_sender(&listener_name, &addr.listener_topic){
                    last_send_mess_number = num;
                }else{
                    print_error!(format!("couldn't db.get_last_mess_number_for_sender {}", addr.address));
                }
                let wstream = WriteStream{address: addr.address.clone(),
                                                       listener_topic: addr.listener_topic.clone(), 
                                                       listener_name: listener_name.clone(),
                                                       stream, last_send_mess_number,
                                                       is_active: false, is_close: false};
                streams.insert(wstream.stream.as_raw_fd(), Arc::new(Mutex::new(wstream)));
                add_write_stream(epoll_fd, stm_fd);
                streams_fd.lock().unwrap().insert(addr.address.clone(), stm_fd);
            },
            Err(err)=>{
                addrs_lost.push(addr.clone());
                print_error!(&format!("tcp connect, {} {}", err, addr.address));
                if let Some(mess) = messages.lock().unwrap().get_mut(&addr.address).unwrap().take(){
                    save_mess_to_db(mess, db, &listener_name, &addr.listener_topic);
                }
            }
        }
    }
    *addrs.lock().unwrap() = addrs_lost;
}

fn write_stream(stream_fd: RawFd,
                streams: &HashMap<RawFd, Arc<Mutex<WriteStream>>>, 
                messages: Arc<Mutex<HashMap<String, Option<Vec<Message>>>>>,
                db: Arc<Mutex<redis::Connect>>){
    if let Some(stream) = streams.get(&stream_fd){
        let stream = stream.clone();
        if let Ok(mut stream) = stream.try_lock(){
            if !stream.is_active && !stream.is_close{
                stream.is_active = true;
            }else{
                return;
            }
        }else{
            return;
        }
        rayon::spawn(move || {
            let mut stream = stream.lock().unwrap();
            let addr_to = stream.address.to_string();
            let last_mess_number = db.lock().unwrap().get_last_mess_number_for_sender(&stream.listener_name, &stream.listener_topic);
            if let Err(err) = last_mess_number{
                print_error!(&format!("get_last_mess_number_for_sender from db, {}", err));
                return;
            }
            let last_mess_number = last_mess_number.unwrap();
            let mut last_send_mess_number = stream.last_send_mess_number;
            {
                let mut buff: Vec<Message> = Vec::new();
                let mut writer = BufWriter::with_capacity(settings::WRITE_BUFFER_CAPASITY, stream.stream.by_ref()); 
                loop{  
                    let mut mess_for_send = None;
                    if let Ok(mut mess_lock) = messages.lock(){
                        mess_for_send = mess_lock.get_mut(&addr_to).unwrap().take();
                        if mess_for_send.is_none(){
                            *mess_lock.get_mut(&addr_to).unwrap() = Some(buff);
                            break;
                        }
                    }   
                    let mess_for_send = mess_for_send.unwrap();
                    for mess in &mess_for_send{
                        if last_send_mess_number < mess.number_mess{
                            last_send_mess_number = mess.number_mess;
                            if !mess.to_stream(&mut writer){
                                break;
                            }
                        }
                    }
                    for mess in mess_for_send{
                        if mess.at_least_once_delivery() && last_mess_number < mess.number_mess{
                            buff.push(mess);
                        }
                    }
                }
                if let Err(err) = writer.flush(){
                    print_error!(&format!("writer.flush, {}", err));
                }
            }
            stream.last_send_mess_number = last_send_mess_number;
            stream.is_active = false;            
        });
    }
}

fn remove_stream(epoll_fd: i32,
                 strm_fd: RawFd, 
                 streams: &mut HashMap<RawFd, ArcWriteStream>,
                 streams_fd: &Arc<Mutex<HashMap<String, RawFd>>>,
                 messages: &Arc<Mutex<MessList>>,
                 addrs_new: &Arc<Mutex<Vec<Address>>>,
                 db: &Arc<Mutex<redis::Connect>>){
    if let Some(stream) = streams.get(&strm_fd){
        remove_write_stream(epoll_fd, strm_fd);
        if let Ok(mut stream) = stream.lock(){
            stream.is_close = true;
        }
        let address = stream.lock().unwrap().address.clone();
        let listener_topic = stream.lock().unwrap().listener_topic.clone();
        let listener_name = stream.lock().unwrap().listener_name.clone();
        
        streams.remove(&strm_fd);
        streams_fd.lock().unwrap().remove(&address);

        let mess = messages.lock().unwrap().get_mut(&address).unwrap().take();
        if let Some(mess) = mess{
            save_mess_to_db(mess, db, &listener_name, &listener_topic);            
        }
        addrs_new.lock().unwrap().push(Address{address, listener_topic, is_new_addr: false});
    }     
}

fn save_mess_to_db(mess: Vec<Message>, db: &Arc<Mutex<redis::Connect>>, listener_name: &str, listener_topic: &str){
    let mut last_send_mess_number: u64 = 0;
    if let Ok(num) = db.lock().unwrap().get_last_mess_number_for_sender(listener_name, listener_topic){
        last_send_mess_number = num;
    }else{
        print_error!(format!("couldn't db.get_last_mess_number_for_sender, {}:{}", listener_name, listener_topic));
    }
    let mess: Vec<Message> = mess.into_iter()
                                 .filter(|m|
                                        m.at_least_once_delivery() && m.number_mess > last_send_mess_number)
                                 .collect();
    if !mess.is_empty(){
        if let Err(err) = db.lock().unwrap().save_messages_from_sender(listener_name, listener_topic, mess){
            print_error!(&format!("db.save_messages_from_sender, {}:{}, err {}", listener_name, listener_topic, err));
        }
    }
}

fn add_write_stream(epoll_fd: i32, fd: RawFd){
    if let Err(err) = regist_event(epoll_fd, fd, libc::EPOLL_CTL_ADD){
        print_error!(format!("couldn't add_write_stream, {}", err));
    }
}
fn continue_write_stream(epoll_fd: i32, fd: RawFd){
    if let Err(err) = regist_event(epoll_fd, fd, libc::EPOLL_CTL_MOD){
        print_error!(format!("couldn't continue_write_stream, {}", err));
    }
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
        print_error!(format!("couldn't wakeupfd_notify, {}", err));
    }
}
fn wakeupfd_reset(event_fd: i32){
    let b: u64 = 0;
    if let Err(err) = syscall!(read(event_fd, &b as *const u64 as *mut c_void, std::mem::size_of::<u64>())){
        print_error!(format!("couldn't wakeupfd_reset, {}", err));
    }
}
fn remove_write_stream(epoll_fd: i32, fd: RawFd){
    if let Err(err) = syscall!(epoll_ctl(epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut())){  
        print_error!(format!("couldn't remove_write_stream, {}", err));
    }
}

fn close_streams(messages: &Arc<Mutex<MessList>>,
                 streams: &mut HashMap<RawFd, ArcWriteStream>,
                 db: &Arc<Mutex<redis::Connect>>){
    for stream in streams.values(){
        if let Ok(mut stream) = stream.lock(){
            stream.is_close = true;
        }
    }
    for kv in messages.lock().unwrap().deref_mut(){
        if let Some(mess_for_send) = kv.1.take(){
            if mess_for_send.is_empty(){
                continue;
            }
            let addr = kv.0;
            let listener_topic = mess_for_send[0].topic_to.clone();
            let listener_name;
            if let Ok(name) = db.lock().unwrap().get_listener_unique_name(&listener_topic, addr){
                listener_name = name;
            }else{
                print_error!(&format!("db.get_listener_unique_name, {}", addr));
                continue;
            }
            save_mess_to_db(mess_for_send, db, &listener_name, &listener_topic);            
        }
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
                
        *self.is_close.lock().unwrap() = true;
        self.wdelay_thread_notify();
        if let Err(err) = self.wdelay_thread.take().unwrap().join(){
            print_error!(&format!("wdelay_thread.join, {:?}", err));
        }
        if let Ok(mut delay_write_list) = self.delay_write_list.lock(){
            for addr in &delay_write_list.clone(){
                send_mess_to_listener(self.epoll_fd, addr, &self.message_buffer, &self.messages, &self.streams_fd);
            }
            delay_write_list.clear();
        } 
        syscall!(close(self.epoll_fd)).expect("couldn't close epoll");
        wakeupfd_notify(self.wakeup_fd);
        if let Err(err) = self.stream_thread.take().unwrap().join(){
            print_error!(&format!("stream_thread.join, {:?}", err));
        }
    }
}