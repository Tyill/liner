use crate::message::Message;
use crate::message::MessageForReceiver;
use crate::mempool::Mempool;
use crate::redis;
use crate::settings;
use crate::common;
use crate::UCback;
use crate::print_error;

use std::collections::{HashMap, BTreeMap};
use std::io::Read;
use std::os::raw::c_void;
use std::thread::JoinHandle;
use std::thread;
use std::net::{TcpStream, TcpListener};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::{Arc, Mutex};
use std::io::{self,BufReader};


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

type MessList = HashMap<RawFd, Option<Vec<Message>>>; 
type MempoolList = HashMap<RawFd, Arc<Mutex<Mempool>>>; 
type SenderList = HashMap<RawFd, Sender>; 

struct ReadStream{
    stream: TcpStream,
    is_active: bool,
    is_close: bool
}

struct Sender{
    sender_topic: String,
    sender_name: String,
    last_mess_num: u64,
}

pub struct Listener{
    epoll_fd: RawFd,
    wakeup_fd: RawFd,
    stream_thread: Option<JoinHandle<()>>,
    receive_thread: Option<JoinHandle<()>>,
}

impl Listener {
    pub fn new(listener: TcpListener,
               unique_name: String, redis_path: String, source_topic: String, receive_cb: UCback)->Listener{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        let wakeup_fd = wakeupfd_create(epoll_fd);
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(HashMap::new()));
        let mut messages_ = messages.clone();
        let mempool: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(HashMap::new()));
        let mut mempool_= mempool.clone();
        let senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(HashMap::new()));
        let mut senders_ = senders.clone();
        let stream_thread = thread::spawn(move|| {
            listener.set_nonblocking(true).expect("couldn't listener set_nonblocking");
            let listener_fd = listener.as_raw_fd();
            regist_event(epoll_fd, listener_fd, libc::EPOLL_CTL_ADD).expect("couldn't event regist");

            let mut streams: HashMap<RawFd, Arc<Mutex<ReadStream>>> = HashMap::new();
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(settings::EPOLL_LISTEN_EVENTS_COUNT);
            let mut prev_time: [u64; 1] = [common::current_time_ms(); 1];
            if let Ok(db) = redis::Connect::new(&unique_name, &redis_path){
                let db = Arc::new(Mutex::new(db));
                db.lock().unwrap().set_source_topic(&source_topic);
                loop{    
                    if !wait(epoll_fd, &mut events){
                        break;
                    }
                    let ctime = common::current_time_ms();
                    if timeout_update_last_mess_number(ctime, &mut prev_time[0]){                    
                        update_last_mess_number(&senders_, &db);
                    }                                  
                    for ev in &events {
                        let stream_fd = ev.u64 as RawFd;
                        if stream_fd == listener_fd{
                            listener_accept(epoll_fd, &mut streams, &mut senders_, &listener, 
                                            &mut mempool_, &mut messages_);
                        }else if ev.events as i32 & libc::EPOLLIN > 0{
                            if stream_fd == wakeup_fd{
                                wakeupfd_reset(wakeup_fd);
                            }else{
                                read_stream(epoll_fd, stream_fd, &streams, &senders_,
                                            db.clone(), &mempool_, &messages_);
                            }
                        }else if ev.events as i32 & (libc::EPOLLHUP | libc::EPOLLERR) > 0{
                            remove_stream(epoll_fd, stream_fd, &db, &mut streams, &mut senders_);
                        }else{
                            print_error!(format!("unknown event {}", stream_fd));
                        }
                    }
                }
                close_streams(&mut streams, &senders_, &db);
            }
        });

        let receive_thread = thread::spawn(move||{ 
            loop {
                let mut mess_buff: BTreeMap<RawFd, Vec<Message>> = BTreeMap::new();
                for mess in messages.lock().unwrap().iter_mut(){
                    let m = mess.1.take();
                    if m.is_some(){
                        mess_buff.insert(*mess.0, m.unwrap());
                    }
                }
                let mut mess_for_receive: BTreeMap<RawFd, Vec<MessageForReceiver>> = BTreeMap::new();
                for mess in mess_buff{
                    let mempool_lock = mempool.lock().unwrap()[&mess.0].clone();
                    let mempool = mempool_lock.lock().unwrap();
                    let mut mess_buff: Vec<MessageForReceiver> = Vec::with_capacity(mess.1.len());
                    for m in mess.1{
                        mess_buff.push(MessageForReceiver::new(&m, &mempool));
                    }
                    mess_for_receive.insert(mess.0, mess_buff);
                }
                for mess in mess_for_receive{
                    let mut last_mess_num = 0;
                    for m in mess.1{
                        receive_cb(m.topic_to, 
                            m.topic_from, 
                            m.uuid, 
                            m.timestamp, 
                            m.data, m.data_len);
                        m.free(&mut mempool.lock().unwrap()[&mess.0].lock().unwrap());
                        if m.number_mess > last_mess_num{
                            last_mess_num = m.number_mess;
                        }
                    }
                    let mut senders = senders.lock().unwrap();
                    if senders[&mess.0].last_mess_num < last_mess_num{
                        senders.get_mut(&mess.0).unwrap().last_mess_num = last_mess_num;
                    }
                }
            }           
        });  
        Self{
            epoll_fd,
            wakeup_fd,
            stream_thread: Some(stream_thread),
            receive_thread: Some(receive_thread),
        }
    }   
   
}

fn wait(epoll_fd: RawFd, events: &mut Vec<libc::epoll_event>)->bool{
    match syscall!(epoll_wait(
        epoll_fd,
        events.as_mut_ptr(),
        settings::EPOLL_LISTEN_EVENTS_COUNT as i32,
        -1,
    )){
        Ok(ready_count)=>{
            unsafe { 
                events.set_len(ready_count as usize)
            };
            true
        },
        Err(err)=>{
            unsafe {
                events.set_len(0); 
            };            
            err.kind() == std::io::ErrorKind::Interrupted 
        }
    }    
}

fn listener_accept(epoll_fd: RawFd, 
                   streams: &mut HashMap<RawFd, Arc<Mutex<ReadStream>>>,
                   senders: &mut Arc<Mutex<SenderList>>,
                   listener: &TcpListener,
                   mempool: &mut Arc<Mutex<MempoolList>>,
                   messages: &mut Arc<Mutex<MessList>>){
    match listener.accept() {
        Ok((stream, _addr)) => {
            stream.set_nonblocking(true).expect("couldn't listener set_nonblocking");
            let stream_fd = stream.as_raw_fd();
            streams.insert(stream_fd, Arc::new(Mutex::new(ReadStream{stream, is_active: false, is_close: false})));    
            senders.lock().unwrap().insert(stream_fd, Sender{sender_topic: "".to_string(), sender_name: "".to_string(), last_mess_num: 0});
            mempool.lock().unwrap().insert(stream_fd, Arc::new(Mutex::new(Mempool::new())));
            messages.lock().unwrap().insert(stream_fd, None);
            add_read_stream(epoll_fd, stream_fd);
            continue_read_stream(epoll_fd, listener.as_raw_fd());
        }
        Err(err) => print_error!(&format!("couldn't listener accept: {}", err)),
    };
}

fn read_stream(epoll_fd: RawFd,
               stream_fd: RawFd,
               streams: &HashMap<RawFd, Arc<Mutex<ReadStream>>>,
               senders: &Arc<Mutex<SenderList>>,
               db: Arc<Mutex<redis::Connect>>,
               mempool: &Arc<Mutex<MempoolList>>,
               messages: &Arc<Mutex<MessList>>){
    if let Some(stream) = streams.get(&stream_fd){        
        if let Ok(mut stream) = stream.try_lock(){
            if !stream.is_active && !stream.is_close{
                stream.is_active = true;
            }else{
                return;
            }
        }else{
            continue_read_stream(epoll_fd, stream_fd);
            return;
        }
        let stream = stream.clone();
        let senders = senders.clone();
        let mempool = mempool.lock().unwrap()[&stream_fd].clone();
        let messages = messages.clone();
    
        rayon::spawn(move || {
            let mut mempool = mempool.lock().unwrap();
            let mut stream = stream.lock().unwrap();
            let mut reader = BufReader::with_capacity(settings::READ_BUFFER_CAPASITY, stream.stream.by_ref());

            let mut mess_buff: Vec<Message> = Vec::new();      
            let mut last_mess_num = 0;
            if let Ok(senders) = senders.lock(){
                last_mess_num = senders[&stream_fd].last_mess_num;
            }
            while let Some(mess) = Message::from_stream(&mut mempool, reader.by_ref()){
                if last_mess_num == 0{
                    let senders = &mut senders.lock().unwrap();
                    let sender = senders.get_mut(&stream_fd).unwrap();
                    if sender.sender_name.is_empty(){
                        mess.sender_topic(&mempool, &mut sender.sender_topic);
                        mess.sender_name(&mempool, &mut sender.sender_name);
                    } 
                    last_mess_num = get_last_mess_number(&db, &sender.sender_name, &sender.sender_topic, 0);                    
                }
                if mess.number_mess > last_mess_num{
                    last_mess_num = mess.number_mess;
                    mess_buff.push(mess);
                }else{
                    mess.free(&mut mempool);
                }
            }            
            if let Ok(mut mess_lock) = messages.lock(){
                if let Some(mess_for_receive) = mess_lock.get_mut(&stream_fd).unwrap().as_mut(){
                    mess_for_receive.append(&mut mess_buff);
                }else{
                    *mess_lock.get_mut(&stream_fd).unwrap() = Some(mess_buff);
                }
            }
            stream.is_active = false;
            continue_read_stream(epoll_fd, stream_fd);  
        });
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

fn update_last_mess_number(senders: &Arc<Mutex<SenderList>>,
                           db: &Arc<Mutex<redis::Connect>>){
    for sender in senders.lock().unwrap().iter(){
        let sender = sender.1;
        if !sender.sender_name.is_empty() && sender.last_mess_num > 0{
            set_last_mess_number(db, &sender.sender_name, &sender.sender_topic, sender.last_mess_num);
        }
    }
}

fn set_last_mess_number(db: &Arc<Mutex<redis::Connect>>, sender_name: &str, sender_topic: &str, last_mess_num: u64){
    if let Err(err) = db.lock().unwrap().set_last_mess_number_from_listener(sender_name, sender_topic, last_mess_num){
        print_error!(&format!("couldn't db.set_last_mess_number: {}", err));
    }
}

fn remove_stream(epoll_fd: i32, stream_fd: RawFd, db: &Arc<Mutex<redis::Connect>>,
                streams: &mut HashMap<RawFd, Arc<Mutex<ReadStream>>>,
                senders: &mut Arc<Mutex<SenderList>>,){
    remove_read_stream(epoll_fd, stream_fd);
    {
        let senders = senders.lock().unwrap();
        let sender = &senders[&stream_fd];
        if !sender.sender_name.is_empty() && sender.last_mess_num > 0{
            set_last_mess_number(db, &sender.sender_name, &sender.sender_topic, sender.last_mess_num);
        }
    }
    streams.remove(&stream_fd);
    senders.lock().unwrap().remove(&stream_fd);
}

fn get_last_mess_number(db: &Arc<Mutex<redis::Connect>>, sender_name: &str, sender_topic: &str, default_mess_number: u64)->u64{
    match db.lock().unwrap().get_last_mess_number_for_listener(sender_name, sender_topic){
        Ok(num)=>{
            num
        },
        Err(err)=>{
            print_error!(&format!("couldn't get_last_mess_number_for_listener: {}", err));
            default_mess_number
        }
    }
}

fn add_read_stream(epoll_fd: i32, fd: RawFd){
    if let Err(err) = regist_event(epoll_fd, fd, libc::EPOLL_CTL_ADD){
        print_error!(format!("couldn't add_read_stream, {}", err));
    }
}    
fn continue_read_stream(epoll_fd: i32, fd: RawFd){
    if let Err(err) = regist_event(epoll_fd, fd, libc::EPOLL_CTL_MOD){
        print_error!(format!("couldn't continue_read_stream, {}", err));
    }
}
fn regist_event(epoll_fd: i32, fd: RawFd, ctl: i32)-> io::Result<i32> {
    let mut event = libc::epoll_event {
        events: (libc::EPOLLONESHOT | libc::EPOLLRDHUP | libc::EPOLLIN | libc::EPOLLET) as u32,
        u64: fd as u64,
    };
    syscall!(epoll_ctl(epoll_fd, ctl, fd, &mut event))
}
fn remove_read_stream(epoll_fd: i32, fd: RawFd){
    if let Err(err) = syscall!(epoll_ctl(epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut())){
        print_error!(format!("couldn't remove_read_stream, {}", err));
    }
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

fn close_streams(streams: &mut HashMap<RawFd, Arc<Mutex<ReadStream>>>,
                 senders: &Arc<Mutex<SenderList>>,
                 db: &Arc<Mutex<redis::Connect>>){
    for stream in streams.values(){
        if let Ok(mut stream) = stream.lock(){
            stream.is_close = true;
        }
    }
    update_last_mess_number(senders, db);
}

impl Drop for Listener {
    fn drop(&mut self) {
        
        syscall!(close(self.epoll_fd)).expect("couldn't close epoll");
        wakeupfd_notify(self.wakeup_fd);
        if let Err(err) = self.stream_thread.take().unwrap().join(){
            print_error!(&format!("stream_thread.join, {:?}", err));
        }
        if let Err(err) = self.receive_thread.take().unwrap().join(){
            print_error!(&format!("receive_thread.join, {:?}", err));
        }  
    }
}