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
use std::sync::{Arc, Mutex, Condvar};
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

struct ReadStream{
    stream: TcpStream,
    is_active: bool,
    is_close: bool
}

struct Sender{
    sender_topic: String,
    sender_name: String,
    last_mess_num: u64,
    last_mess_num_preview: u64,
    is_deleted: bool,
}

type MessList = HashMap<RawFd, Option<Vec<Message>>>; 
type MempoolList = HashMap<RawFd, Arc<Mutex<Mempool>>>; 
type SenderList = HashMap<RawFd, Sender>; 

pub struct Listener{
    epoll_fd: RawFd,
    wakeup_fd: RawFd,
    stream_thread: Option<JoinHandle<()>>,
    receive_thread: Option<JoinHandle<()>>,
    messages: Arc<Mutex<MessList>>,
    mempools: Arc<Mutex<MempoolList>>,
    senders: Arc<Mutex<SenderList>>,
    db: Arc<Mutex<redis::Connect>>,
    receive_thread_cvar: Arc<(Mutex<bool>, Condvar)>,
    receive_cb: UCback,
    is_close: Arc<Mutex<bool>>,
}

impl Listener {
    pub fn new(listener: TcpListener,
               unique_name: String, redis_path: String, source_topic: String, receive_cb: UCback)->Listener{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        let wakeup_fd = wakeupfd_create(epoll_fd);
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(HashMap::new()));
        let mut messages_ = messages.clone();
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(HashMap::new()));
        let mut mempools_= mempools.clone();
        let senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(HashMap::new()));
        let mut senders_ = senders.clone();
        let db_conn = redis::Connect::new(&unique_name, &redis_path).expect("couldn't redis::Connect");
        let db = Arc::new(Mutex::new(db_conn));
        db.lock().unwrap().set_source_topic(&source_topic);
        let db_ = db.clone();
        let receive_thread_cvar: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));
        let receive_thread_cvar_ = receive_thread_cvar.clone();
        
        let stream_thread = thread::spawn(move|| {
            listener.set_nonblocking(true).expect("couldn't listener set_nonblocking");
            let listener_fd = listener.as_raw_fd();
            regist_event(epoll_fd, listener_fd, libc::EPOLL_CTL_ADD).expect("couldn't event regist");

            let mut streams: HashMap<RawFd, Arc<Mutex<ReadStream>>> = HashMap::new();
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(settings::EPOLL_LISTEN_EVENTS_COUNT);
            let mut prev_time: [u64; 1] = [common::current_time_ms(); 1];
            loop{    
                if !wait(epoll_fd, &mut events){
                    break;
                }
                let ctime = common::current_time_ms();
                if timeout_update_last_mess_number(ctime, &mut prev_time[0]){                    
                    update_last_mess_number(&senders_, &db_);
                }                                  
                for ev in &events {
                    let stream_fd = ev.u64 as RawFd;
                    if stream_fd == listener_fd{
                        listener_accept(epoll_fd, &mut streams, &mut senders_, &listener, 
                                        &mut mempools_, &mut messages_);
                    }else if ev.events as i32 & libc::EPOLLIN > 0{
                        if stream_fd == wakeup_fd{
                            wakeupfd_reset(wakeup_fd);
                        }else{
                            read_stream(epoll_fd, stream_fd, &streams, &senders_,
                                        db_.clone(), &mempools_, &messages_, &receive_thread_cvar_);
                        }
                    }else if ev.events as i32 & (libc::EPOLLHUP | libc::EPOLLERR) > 0{
                        remove_stream(epoll_fd, stream_fd, &mut streams, &mut senders_);
                    }else{
                        print_error!(format!("unknown event {}", stream_fd));
                    }
                }
            }
            close_streams(&mut streams, &senders_, &db_);        
        });

        let messages_ = messages.clone();
        let mempools_= mempools.clone();
        let senders_ = senders.clone();
        let db_ = db.clone();
        let receive_thread_cvar_ = receive_thread_cvar.clone();
        let is_close: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let is_close_ = is_close.clone();
        let receive_thread = thread::spawn(move|| {
            while !*is_close_.lock().unwrap(){
                let mess_buff = mess_for_receive(&messages_);
                if mess_buff.is_empty(){
                    let (lock, cvar) = &*receive_thread_cvar_;
                    let mut _started = lock.lock().unwrap();
                    _started = cvar.wait(_started).unwrap();
                }
                do_receive_cb(mess_buff, &messages_, &mempools_, &senders_, &db_, receive_cb);                
            }
        });  
        Self{
            epoll_fd,
            wakeup_fd,
            stream_thread: Some(stream_thread),
            receive_thread: Some(receive_thread),
            receive_thread_cvar,
            messages,
            mempools,
            senders,
            db,
            receive_cb,
            is_close,
        }
    }   
}

fn do_receive_cb(mess_buff: BTreeMap<RawFd, Vec<Message>>,
                 messages: &Arc<Mutex<MessList>>,
                 mempools: &Arc<Mutex<MempoolList>>,
                 senders: &Arc<Mutex<SenderList>>,
                 db: &Arc<Mutex<redis::Connect>>,
                 receive_cb: UCback){

    let mut mess_for_receive: BTreeMap<RawFd, Vec<MessageForReceiver>> = BTreeMap::new();
    for mess in mess_buff{
        let fd = mess.0;
        let mempool_lock = mempools.lock().unwrap()[&fd].clone();
        let mempool = mempool_lock.lock().unwrap();
        let mut mess_buff: Vec<MessageForReceiver> = Vec::with_capacity(mess.1.len());
        for m in mess.1{
            mess_buff.push(MessageForReceiver::new(&m, &mempool));
        }
        mess_for_receive.insert(mess.0, mess_buff);
    }
    for mess in mess_for_receive{
        let fd = mess.0;
        let mut last_mess_num = 0;
        let mempool = mempools.lock().unwrap()[&fd].clone();
        for m in mess.1{
            receive_cb(m.topic_to, 
                m.topic_from, 
                m.uuid, 
                m.timestamp, 
                m.data, m.data_len);
            m.free(&mut mempool.lock().unwrap());
            if m.number_mess > last_mess_num{
                last_mess_num = m.number_mess;
            }
        }
        let mut senders = senders.lock().unwrap();
        if senders[&mess.0].last_mess_num < last_mess_num{
            senders.get_mut(&mess.0).unwrap().last_mess_num = last_mess_num;
        }
    }
    check_has_remove_senders(&db, &senders, &mempools, &messages);
}

fn receive_thread_notify(receive_thread_cvar: &Arc<(Mutex<bool>, Condvar)>){
    let (lock, cvar) = &**receive_thread_cvar;
    *lock.lock().unwrap() = true;
    cvar.notify_one();
}

fn mess_for_receive(messages: &Arc<Mutex<MessList>>)->BTreeMap<RawFd, Vec<Message>>{
    let mut mess_buff: BTreeMap<RawFd, Vec<Message>> = BTreeMap::new();
    for mess in messages.lock().unwrap().iter_mut(){
        let fd = *mess.0;
        let m = mess.1.take();
        if m.is_some(){
            mess_buff.insert(fd, m.unwrap());
        }
    }
    mess_buff
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
                   mempools: &mut Arc<Mutex<MempoolList>>,
                   messages: &mut Arc<Mutex<MessList>>){
    match listener.accept() {
        Ok((stream, _addr)) => {
            stream.set_nonblocking(true).expect("couldn't listener set_nonblocking");
            let stream_fd = stream.as_raw_fd();
            streams.insert(stream_fd, Arc::new(Mutex::new(ReadStream{stream, is_active: false, is_close: false})));    
            senders.lock().unwrap().insert(stream_fd, Sender{sender_topic: "".to_string(), sender_name: "".to_string(),
                                                                  last_mess_num: 0, last_mess_num_preview: 0, is_deleted: false});
            mempools.lock().unwrap().insert(stream_fd, Arc::new(Mutex::new(Mempool::new())));
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
               mempools: &Arc<Mutex<MempoolList>>,
               messages: &Arc<Mutex<MessList>>,
               receive_thread_cvar: &Arc<(Mutex<bool>, Condvar)>){
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
        let mempool = mempools.lock().unwrap()[&stream_fd].clone();
        let messages = messages.clone();
        let receive_thread_cvar = receive_thread_cvar.clone();
    
        rayon::spawn(move || {           
            let mut mess_buff: Vec<Message> = Vec::new();      
            let mut last_mess_num = 0;
            if let Ok(senders) = senders.lock(){
                last_mess_num = senders[&stream_fd].last_mess_num_preview;
            }
            let mut stream = stream.lock().unwrap();
            let mut reader = BufReader::with_capacity(settings::READ_BUFFER_CAPASITY, stream.stream.by_ref());
            let mut mempool = mempool.lock().unwrap();
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
                if mess.number_mess > last_mess_num && false{
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
            if let Ok(mut senders) = senders.lock(){
                senders.get_mut(&stream_fd).unwrap().last_mess_num_preview = last_mess_num;
            }            
            stream.is_active = false;
            continue_read_stream(epoll_fd, stream_fd);
            receive_thread_notify(&receive_thread_cvar);
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

fn remove_stream(epoll_fd: i32, stream_fd: RawFd,
                streams: &mut HashMap<RawFd, Arc<Mutex<ReadStream>>>,
                senders: &mut Arc<Mutex<SenderList>>,){
    remove_read_stream(epoll_fd, stream_fd);
    senders.lock().unwrap().get_mut(&stream_fd).unwrap().is_deleted = true;
    streams.remove(&stream_fd);
}

fn check_has_remove_senders(db: &Arc<Mutex<redis::Connect>>,
                            senders: &Arc<Mutex<SenderList>>,
                            mempools: &Arc<Mutex<MempoolList>>,
                            messages: &Arc<Mutex<MessList>>){
    let mut rem_fds: Vec<RawFd> = Vec::new();
    for sender in senders.lock().unwrap().iter(){
        if sender.1.is_deleted{
            if !sender.1.sender_name.is_empty() && sender.1.last_mess_num > 0{
                set_last_mess_number(db, &sender.1.sender_name, &sender.1.sender_topic, sender.1.last_mess_num);
            }
            rem_fds.push(*sender.0);
        }
    }
    for fd in rem_fds{
        senders.lock().unwrap().remove(&fd);
        mempools.lock().unwrap().remove(&fd);
        messages.lock().unwrap().remove(&fd);
    }
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
        *self.is_close.lock().unwrap() = true;
        receive_thread_notify(&self.receive_thread_cvar);
        if let Err(err) = self.receive_thread.take().unwrap().join(){
            print_error!(&format!("wdelay_thread.join, {:?}", err));
        }
        do_receive_cb(mess_for_receive(&self.messages), &self.messages, &self.mempools, 
                                                  &self.senders, &self.db, self.receive_cb);

        syscall!(close(self.epoll_fd)).expect("couldn't close epoll");
        wakeupfd_notify(self.wakeup_fd);
        if let Err(err) = self.stream_thread.take().unwrap().join(){
            print_error!(&format!("stream_thread.join, {:?}", err));
        }
    }
}