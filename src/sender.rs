use crate::mempool::Mempool;
use crate::message::Message;
use crate::redis;
use crate::print_error;
use crate::redis::Connect;
use crate::settings;
use crate::common;

use std::net::TcpStream;
use std::os::raw::c_void;
use std::thread::JoinHandle;
use std::time::Duration;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    last_mess_number: u64,
    is_active: bool,
    is_close: bool,
}

#[derive(Clone)]
struct Address{
    address: String,
    listener_topic: String,
    is_new_addr: bool,
}

type MempoolList = HashMap<String, Arc<Mutex<Mempool>>>; // key - addr
type MessList = HashMap<String, Arc<Mutex<Option<Vec<Message>>>>>; 
type MessBuffList = HashMap<String, Option<Vec<Message>>>; 
type WriteStreamList = HashMap<RawFd, Arc<Mutex<WriteStream>>>; 

pub struct Sender{
    epoll_fd: RawFd,
    wakeup_fd: RawFd,
    unique_name: String,
    addrs_for: HashSet<String>,
    addrs_new: Arc<Mutex<Vec<Address>>>,
    message_buffer: Arc<Mutex<MessBuffList>>, // key - addr
    messages: Arc<Mutex<MessList>>, // key - addr
    mempools: Arc<Mutex<MempoolList>>, 
    last_mess_number: HashMap<String, u64>,
    is_new_addr: Arc<AtomicBool>,
    is_close: Arc<AtomicBool>,
    ctime: Arc<AtomicU64>,
    delay_write_cvar: Arc<(Mutex<bool>, Condvar)>,
    stream_thread: Option<JoinHandle<()>>,
    wdelay_thread: Option<JoinHandle<()>>,
}

impl Sender {
    pub fn new(unique_name: &str, redis_path: &str, source_topic: &str)->Sender{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(HashMap::new()));
        let messages_ = messages.clone();
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(HashMap::new()));
        let mempools_ = mempools.clone(); 
        let addrs_for: HashSet<String> = HashSet::new();
        let addrs_new: Arc<Mutex<Vec<Address>>> = Arc::new(Mutex::new(Vec::new()));
        let addrs_new_ = addrs_new.clone();
        let mut streams: Arc<Mutex<WriteStreamList>> = Arc::new(Mutex::new(HashMap::new()));
        let mut streams_ = streams.clone();    
        let streams_fd: Arc<Mutex<HashMap<String, RawFd>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut streams_fd_ = streams_fd.clone();
        let wakeup_fd = wakeupfd_create(epoll_fd);
        let is_new_addr = Arc::new(AtomicBool::new(false));
        let is_new_addr_ = is_new_addr.clone();
        let db_conn = redis::Connect::new(&unique_name, &redis_path).expect("couldn't redis::Connect");
        let db = Arc::new(Mutex::new(db_conn));
        db.lock().unwrap().set_source_topic(&source_topic);
        let db_ = db.clone();
        let stream_thread = thread::spawn(move|| {
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(settings::EPOLL_LISTEN_EVENTS_COUNT);
            loop{ // stream cycle
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
                        let mut is_close = false;
                        if let Some(stream) = streams_.lock().unwrap().get(&stream_fd){
                            if !stream.lock().unwrap().is_close{
                                write_stream(stream, &messages_, &mempools_);
                            }else{
                                is_close = true;
                            }
                        }
                        if is_close{
                            remove_stream(epoll_fd, stream_fd, &mut streams_, &streams_fd,
                                &messages_, &addrs_new_, &db_, &mempools_);
                        }
                    }else if ev.events as i32 & (libc::EPOLLHUP | libc::EPOLLERR) > 0{
                        remove_stream(epoll_fd, stream_fd, &mut streams_, &streams_fd,
                                        &messages_, &addrs_new_, &db_, &mempools_);
                    }else{
                        print_error!(format!("unknown event {}", stream_fd as RawFd));
                    }
                }                
            }
            close_streams(&messages_, &mut streams_, &streams_fd, &db_, &mempools_);
        });

        let delay_write_cvar = Arc::new((Mutex::new(false), Condvar::new()));
        let delay_write_cvar_ = delay_write_cvar.clone();
        let messages_ = messages.clone();
        let message_buffer: Arc<Mutex<MessBuffList>> = Arc::new(Mutex::new(HashMap::new()));
        let message_buffer_ = message_buffer.clone();
        let is_close = Arc::new(AtomicBool::new(false));
        let is_close_ = is_close.clone();
        let ctime = Arc::new(AtomicU64::new(common::current_time_ms()));
        let ctime_ = ctime.clone();
        let mut addrs_new_ = addrs_new.clone();
        let mempools_ = mempools.clone(); 
        let wdelay_thread = thread::spawn(move||{
            let mut once_again = true;
            let mut prev_time: [u64; 2] = [common::current_time_ms(); 2];
            while !is_close_.load(Ordering::Relaxed){ // write delay cycle
                if !once_again{
                    let (lock, cvar) = &*delay_write_cvar_;
                    if let Ok(mut _started) = lock.lock(){
                        if !message_buffer_.lock().unwrap().iter().any(|m: (&String, &Option<Vec<Message>>)| m.1.is_some()){
                            *_started = false;
                            _started = cvar.wait(_started).unwrap();
                        }
                    }
                }
                once_again = !once_again;
                  
                std::thread::sleep(Duration::from_millis(settings::WRITE_MESS_DELAY_MS));

                if send_mess_to_listener(epoll_fd, &message_buffer_, &messages_, &streams_fd_){
                    once_again = true;
                }
                let ctime = common::current_time_ms();
                ctime_.store(ctime, Ordering::Relaxed);
                if check_available_stream(&is_new_addr_, ctime, &mut prev_time[0]) {
                    append_streams(epoll_fd, &mut addrs_new_, &mut streams, 
                                   &mut streams_fd_, &messages_, &db, &mempools_);
                }
                if timeout_update_last_mess_number(ctime, &mut prev_time[1]){
                    update_last_mess_number(&streams, &db);
                }
            }
        });
        Self{
            unique_name: unique_name.to_string(),
            epoll_fd,
            addrs_for,
            addrs_new,
            message_buffer,
            messages,
            mempools,
            last_mess_number: HashMap::new(),
            wakeup_fd,
            is_new_addr,
            is_close,
            ctime,
            delay_write_cvar,
            stream_thread: Some(stream_thread),
            wdelay_thread: Some(wdelay_thread),
        }
    }
    
    pub fn send_to(&mut self, db: &mut redis::Connect, addr_to: &str, to: &str, from: &str, 
                   data: &[u8], at_least_once_delivery: bool)->bool{
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
       
        let mempool = self.mempools.lock().unwrap().get_mut(addr_to).unwrap().clone();
        let mess = Message::new(&mut mempool.lock().unwrap(), to,
                                         from, &self.unique_name,
                                         number_mess, data, at_least_once_delivery);
        self.send_mess_to_buff(mess, addr_to);
               
        if is_new_addr{
            self.addrs_new.lock().unwrap().push(Address{address: addr_to.to_string(), 
                                                        listener_topic: to.to_string(),
                                                        is_new_addr: true});
            self.is_new_addr.store(true, Ordering::Relaxed);
        }
        true
    }
    
    fn send_mess_to_buff(&mut self, mess: Message, addr_to: &str){
        let (lock, cvar) = &*self.delay_write_cvar;
        if let Ok(mut _started) = lock.lock(){
            if let Ok(mut message_buffer_lock) = self.message_buffer.lock(){
                if let Some(mbuff) = message_buffer_lock.get_mut(addr_to).unwrap(){
                    mbuff.push(mess);
                }else{
                    *message_buffer_lock.get_mut(addr_to).unwrap() = Some(vec![mess]);
                }
            }    
            if !*_started{
                *_started = true;
                cvar.notify_one();
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
        self.mempools.lock().unwrap().insert(addr_to.to_string(), Arc::new(Mutex::new(Mempool::new())));
        let mempool = self.mempools.lock().unwrap().get_mut(addr_to).unwrap().clone();
        if let Ok(last_mess) = db.load_last_message_for_sender(&mempool, &listener_name, listener_topic){
            if let Some(mess) = last_mess{
                let mess_num = mess.number_mess;
                if mess_num > self.last_mess_number[addr_to]{
                    *self.last_mess_number.get_mut(addr_to).unwrap() = mess_num;
                }
            }
        }else {
            print_error!("db.load_last_message_for_sender");
        }
        if let Err(err) = db.save_listener_for_sender(addr_to, listener_topic){
            print_error!(&format!("db.save_listener_for_sender {}", err));
        }

        self.messages.lock().unwrap().insert(addr_to.to_string(), Arc::new(Mutex::new(Some(Vec::new()))));
        self.message_buffer.lock().unwrap().insert(addr_to.to_string(), Some(Vec::new()));
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
            self.is_new_addr.store(true, Ordering::Relaxed);            
        }   
    }
    pub fn get_ctime(&self)->u64{
        self.ctime.load(Ordering::Relaxed)
    }
}

fn send_mess_to_listener(epoll_fd: RawFd,
                         message_buffer: &Arc<Mutex<MessBuffList>>,
                         messages: &Arc<Mutex<MessList>>,
                         streams_fd: &Arc<Mutex<HashMap<String, RawFd>>>)->bool{
    let mut has_mess = false;
    for m in message_buffer.lock().unwrap().iter_mut(){
        if let Some(mut buff) = m.1.take(){
            let addr_to = m.0;
            let mess_lock = messages.lock().unwrap()[addr_to].clone();
            let mut mess_for_send = mess_lock.lock().unwrap();
            if let Some(mess) = mess_for_send.as_mut(){
                mess.append(&mut buff);
            }else{
                *mess_for_send = Some(buff);
            }
            if let Some(strm_fd) = streams_fd.lock().unwrap().get(addr_to){
                continue_write_stream(epoll_fd, *strm_fd);
            }
            has_mess = true;
        }
    }
    has_mess
}

fn check_available_stream(is_new_addr: &Arc<AtomicBool>, ctime: u64, prev_time: &mut u64)->bool{
    if is_new_addr.load(Ordering::Relaxed) || (ctime - *prev_time) > settings::CHECK_AVAILABLE_STREAM_TIMEOUT_MS{
        is_new_addr.store(false, Ordering::Relaxed);
        *prev_time = ctime;
        true
    }else{
        false
    }
}

fn timeout_update_last_mess_number(ctime: u64, prev_time: &mut u64)->bool{
    if ctime - *prev_time > settings::UPDATE_LAST_MESS_NUMBER_TIMEOUT_MS{
        *prev_time = ctime;
        true
    }else{
        false
    }
}

fn update_last_mess_number(streams: &Arc<Mutex<WriteStreamList>>,
                           db: &Arc<Mutex<redis::Connect>>){
    let mut snames: Vec<(RawFd, String, String)> = Vec::new();
    for stream_lock in streams.lock().unwrap().iter(){
        if let Ok(stream) = stream_lock.1.lock(){
            snames.push((*stream_lock.0, stream.listener_name.clone(),
                                         stream.listener_topic.clone()));
        }
    }
    for sname in snames{    
        match db.lock().unwrap().get_last_mess_number_for_sender(&sname.1, &sname.2){
            Ok(last_mess_number)=>{
                streams.lock().unwrap().get_mut(&sname.0).unwrap().lock().unwrap().last_mess_number = last_mess_number;
            },
            Err(err)=>{
                print_error!(&format!("get_last_mess_number_for_sender from db, {}", err));
            }
        }
    }
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
                  streams: &mut Arc<Mutex<WriteStreamList>>,
                  streams_fd: &mut Arc<Mutex<HashMap<String, RawFd>>>,
                  messages: &Arc<Mutex<MessList>>,
                  db: &Arc<Mutex<redis::Connect>>,
                  mempools: &Arc<Mutex<MempoolList>>){
    let mut addrs_lost: Vec<Address> = Vec::new();
    for addr in addrs.lock().unwrap().iter(){
        if !addr.is_new_addr && messages.lock().unwrap()[&addr.address].lock().unwrap().is_none(){
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
                
                let mempool = mempools.lock().unwrap()[&addr.address].clone();
                match db.lock().unwrap().load_messages_for_sender(&mempool, &listener_name, &addr.listener_topic){
                    Ok(mut mess_from_db) =>{
                        let mess_lock = messages.lock().unwrap()[&addr.address].clone();
                        if let Some(mut mess_for_send) = mess_lock.lock().unwrap().take(){
                            mess_from_db.append(&mut mess_for_send);
                        }
                        *mess_lock.lock().unwrap() = Some(mess_from_db);
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
                let stm_fd = stream.as_raw_fd();
                let wstream = WriteStream{address: addr.address.clone(),
                                                       listener_topic: addr.listener_topic.clone(), 
                                                       listener_name: listener_name.clone(),
                                                       stream, last_send_mess_number, last_mess_number: 0,
                                                       is_active: false, is_close: false};
                streams.lock().unwrap().insert(stm_fd, Arc::new(Mutex::new(wstream)));
                streams_fd.lock().unwrap().insert(addr.address.clone(), stm_fd);
                add_write_stream(epoll_fd, stm_fd);
            },
            Err(err)=>{
                addrs_lost.push(addr.clone());
                print_error!(&format!("tcp connect, {} {}", err, addr.address));
                let mess = messages.lock().unwrap()[&addr.address].lock().unwrap().take();
                if let Some(mess) = mess{
                    save_mess_to_db(mess, db, &listener_name, &addr.listener_topic, &addr.address, mempools);
                }
            }
        }
    }
    *addrs.lock().unwrap() = addrs_lost;
}

fn write_stream(stream: &Arc<Mutex<WriteStream>>,
                messages: &Arc<Mutex<MessList>>,
                mempools: &Arc<Mutex<MempoolList>>){
    if let Ok(mut stream) = stream.try_lock(){
        if !stream.is_active && !stream.is_close{
            stream.is_active = true;
        }else{
            return;
        }
    }else{
        return;
    }
    let stream = stream.clone();
    let messages = messages.clone();
    let mempools = mempools.clone();
    
    rayon::spawn(move || {
        let mut is_shutdown = false;
        let mut stream = stream.lock().unwrap();
        let addr_to = stream.address.to_string();
        let mut last_send_mess_number = stream.last_send_mess_number;
        let last_mess_number = stream.last_mess_number;
        {
            let mut buff: Vec<Message> = Vec::new();
            let mut writer = BufWriter::with_capacity(settings::WRITE_BUFFER_CAPASITY, stream.stream.by_ref()); 
            let mempool = mempools.lock().unwrap()[&addr_to].clone();
            let messages = messages.lock().unwrap()[&addr_to].clone();
            loop{
                let mut mess_for_send = None;
                if let Ok(mut mess_lock) = messages.lock(){
                    mess_for_send = mess_lock.take();
                    if mess_for_send.is_none(){
                        *mess_lock = Some(buff);
                        break;
                    }
                }   
                let mess_for_send = mess_for_send.unwrap();
                for mess in &mess_for_send{
                    let num_mess = mess.number_mess;
                    if last_send_mess_number < num_mess{
                        last_send_mess_number = num_mess;
                        if !mess.to_stream(&mempool, &mut writer){
                            is_shutdown = true;
                            break;
                        }
                    }
                }
                for mess in mess_for_send{
                    let num_mess = mess.number_mess;
                    let at_least_once_delivery = mess.at_least_once_delivery();
                    if at_least_once_delivery && last_mess_number < num_mess{
                        buff.push(mess);
                    }else{
                        mess.free(&mut mempool.lock().unwrap());
                    }
                }
            }
            if let Err(err) = writer.flush(){
                print_error!(&format!("writer.flush, {}", err));
                is_shutdown = true;
            }
            //mempool.lock().unwrap()._print_size();
        }
        stream.last_send_mess_number = last_send_mess_number;
        stream.is_active = false;
        if is_shutdown{ 
            let _ = stream.stream.shutdown(std::net::Shutdown::Write);
            stream.is_close = true;
        }
    });
}

fn remove_stream(epoll_fd: i32,
                 strm_fd: RawFd, 
                 streams: &mut Arc<Mutex<WriteStreamList>>,
                 streams_fd: &Arc<Mutex<HashMap<String, RawFd>>>,
                 messages: &Arc<Mutex<MessList>>,
                 addrs_new: &Arc<Mutex<Vec<Address>>>,
                 db: &Arc<Mutex<redis::Connect>>,
                 mempools: &Arc<Mutex<MempoolList>>){
    if let Some(stream) = streams.lock().unwrap().get(&strm_fd){
        remove_write_stream(epoll_fd, strm_fd);
        if let Ok(mut stream) = stream.lock(){
            stream.is_close = true;
        }
        let address = stream.lock().unwrap().address.clone();
        let listener_topic = stream.lock().unwrap().listener_topic.clone();
        let listener_name = stream.lock().unwrap().listener_name.clone();
        
        streams.lock().unwrap().remove(&strm_fd);
        streams_fd.lock().unwrap().remove(&address);

        let mess = messages.lock().unwrap()[&address].lock().unwrap().take();
        if let Some(mess) = mess{
            save_mess_to_db(mess, db, &listener_name, &listener_topic, &address, mempools);            
        }
        addrs_new.lock().unwrap().push(Address{address, listener_topic, is_new_addr: false});
    }     
}

fn save_mess_to_db(mess: Vec<Message>, db: &Arc<Mutex<redis::Connect>>, listener_name: &str, listener_topic: &str,
                   addr: &str, mempools: &Arc<Mutex<MempoolList>>){
    let mut last_send_mess_number: u64 = 0;
    if let Ok(num) = db.lock().unwrap().get_last_mess_number_for_sender(listener_name, listener_topic){
        last_send_mess_number = num;
    }else{
        print_error!(format!("couldn't db.get_last_mess_number_for_sender, {}:{}", listener_name, listener_topic));
    }
    let mess: Vec<Message> = mess.into_iter()
                                 .filter(|m|
                                        m.at_least_once_delivery() &&
                                        m.number_mess > last_send_mess_number)
                                 .collect();
    let mempool = mempools.lock().unwrap()[addr].clone();
    if !mess.is_empty(){
        if let Err(err) = db.lock().unwrap().save_messages_from_sender(&mempool, listener_name, listener_topic, mess){
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
                 streams: &mut Arc<Mutex<WriteStreamList>>,
                 streams_fd: &Arc<Mutex<HashMap<String, RawFd>>>,
                 db: &Arc<Mutex<redis::Connect>>,
                 mempools: &Arc<Mutex<MempoolList>>){
    for stream in streams.lock().unwrap().values(){
        if let Ok(mut stream) = stream.lock(){
            stream.is_close = true;
        }
    }
    for kv in messages.lock().unwrap().iter(){
        if let Some(mess_for_send) = kv.1.lock().unwrap().take(){
            if mess_for_send.is_empty(){
                continue;
            }
            let addr = kv.0;

            let fd = streams_fd.lock().unwrap()[addr];
            let streams = streams.lock().unwrap();
            let stream = streams.get(&fd).unwrap().lock().unwrap();
            let listener_topic = &stream.listener_topic;
            let listener_name = &stream.listener_name;           
            save_mess_to_db(mess_for_send, db, listener_name, listener_topic, addr, mempools);            
        }
    }
}

impl Drop for Sender {
    fn drop(&mut self) {                
        self.is_close.store(true, Ordering::Relaxed);
        self.wdelay_thread_notify();
        if let Err(err) = self.wdelay_thread.take().unwrap().join(){
            print_error!(&format!("wdelay_thread.join, {:?}", err));
        }
        
        syscall!(close(self.epoll_fd)).expect("couldn't close epoll");
        wakeupfd_notify(self.wakeup_fd);
        if let Err(err) = self.stream_thread.take().unwrap().join(){
            print_error!(&format!("stream_thread.join, {:?}", err));
        }
    }
}