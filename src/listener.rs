use crate::message::Message;
use crate::redis;
use crate::settings;
use crate::print_error;

use std::collections::HashMap;
use std::io::Read;
use std::{thread, sync::mpsc};
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

struct ReadStream{
    stream: TcpStream,
    is_active: bool,
}

pub struct Listener{
    _epoll_fd: i32,    
}

impl Listener {
    pub fn new(listener: TcpListener, tx: mpsc::Sender<Message>, 
               unique_name: String, redis_path: String, source_topic: String)->Listener{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        thread::spawn(move|| {
            listener.set_nonblocking(true).expect("couldn't listener set_nonblocking");
            let listener_fd = listener.as_raw_fd();
            regist_event(epoll_fd, listener_fd, libc::EPOLL_CTL_ADD).expect("couldn't event regist");
            let mut streams: HashMap<RawFd, Arc<Mutex<ReadStream>>> = HashMap::new();
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(settings::EPOLL_LISTEN_EVENTS_COUNT);
            if let Ok(db) = redis::Connect::new(&unique_name, &redis_path){
                let db = Arc::new(Mutex::new(db));
                db.lock().unwrap().set_source_topic(&source_topic);
                loop{    
                    if !wait(epoll_fd, &mut events){
                        break;
                    }               
                    for ev in &events {
                        let stream_fd = ev.u64 as RawFd;
                        if stream_fd == listener_fd{
                            listener_accept(epoll_fd, &mut streams, &listener);
                        }else if ev.events as i32 & libc::EPOLLIN > 0{
                            read_stream(epoll_fd, stream_fd, &streams, &tx, &db);
                        }else if ev.events as i32 & (libc::EPOLLHUP | libc::EPOLLERR) > 0{
                            remove_read_stream(epoll_fd, stream_fd);
                            streams.remove(&stream_fd);
                        }else{
                            print_error!(format!("unknown event {}", stream_fd));
                        }
                    }
                }
            }
        });
        Self{
            _epoll_fd: epoll_fd,
        }
    }
    
    pub fn _close(&self) {
        let _ = syscall!(close(self._epoll_fd));
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
            unsafe { 
                events.set_len(ready_count as usize)
            };
            return true;
        },
        Err(err)=>{
            unsafe {
                events.set_len(0); 
            };            
            return err.kind() == std::io::ErrorKind::Interrupted; 
        }
    }    
}

fn listener_accept(epoll_fd: RawFd, 
                   streams: &mut HashMap<RawFd, Arc<Mutex<ReadStream>>>,
                   listener: &TcpListener){
    match listener.accept() {
        Ok((stream, _addr)) => {
            stream.set_nonblocking(true).expect("couldn't listener set_nonblocking");
            let stream_fd = stream.as_raw_fd();
            streams.insert(stream_fd, Arc::new(Mutex::new(ReadStream{stream, is_active: false})));    
            add_read_stream(epoll_fd, stream_fd);
        }
        Err(err) => print_error!(&format!("couldn't listener accept: {}", err)),
    };
    continue_read_stream(epoll_fd, listener.as_raw_fd());
}

fn read_stream(epoll_fd: RawFd,
               stream_fd: RawFd,
               streams: &HashMap<RawFd, Arc<Mutex<ReadStream>>>, 
               tx: &mpsc::Sender<Message>, 
               db: &Arc<Mutex<redis::Connect>>){
    if let Some(stream) = streams.get(&stream_fd){        
        if let Ok(mut stream) = stream.try_lock(){
            if !stream.is_active{
                stream.is_active = true;
            }else{
                return;
            }
        }else{
            continue_read_stream(epoll_fd, stream_fd);
            return;
        }
        let tx = tx.clone();
        let db = db.clone();
        let stream = stream.clone();
        rayon::spawn(move || {
            let mut stream = stream.lock().unwrap();
            let mut reader = BufReader::with_capacity(settings::READ_BUFFER_CAPASITY, stream.stream.by_ref());
            let mut last_mess_num: u64 = 0;
            let mut sender_name = String::new();
            let mut sender_topic = String::new();
            let mut count = 0;
            while let Some(mess) = Message::from_stream(reader.by_ref()){
                if last_mess_num == 0{
                    last_mess_num = get_last_mess_number(&db, &mess.sender_name, &mess.topic_from);
                    sender_name = mess.sender_name.clone();
                    sender_topic = mess.topic_from.clone();
                }
                if mess.number_mess > last_mess_num{
                    last_mess_num = mess.number_mess;
                    if let Err(err) = tx.send(mess){
                        print_error!(&format!("couldn't tx.send: {}", err));
                    }
                    count += 1;
                }
            }
            println!("count {}", count);
            set_last_mess_number(&db, &sender_name, &sender_topic, last_mess_num);
            stream.is_active = false;
            continue_read_stream(epoll_fd, stream_fd);            
        });
    }
}

fn set_last_mess_number(db: &Arc<Mutex<redis::Connect>>, sender_name: &str, topic_from: &str, last_mess_num: u64){
    if let Err(err) = db.lock().unwrap().set_last_mess_number_from_listener(&sender_name, &topic_from, last_mess_num){
        print_error!(&format!("couldn't db.set_last_mess_number: {}", err));
    }
}

fn get_last_mess_number(db: &Arc<Mutex<redis::Connect>>, sender_name: &str, topic_from: &str)->u64{
    match db.lock().unwrap().get_last_mess_number_for_listener(sender_name, topic_from){
        Ok(num)=>{
            num
        },
        Err(err)=>{
            print_error!(&format!("couldn't get_last_mess_number_for_listener: {}", err));
            0
        }
    }
}

fn add_read_stream(epoll_fd: i32, fd: RawFd){
    regist_event(epoll_fd, fd, libc::EPOLL_CTL_ADD).expect("couldn't add_read_stream");
}    
fn continue_read_stream(epoll_fd: i32, fd: RawFd){
    regist_event(epoll_fd, fd, libc::EPOLL_CTL_MOD).expect("couldn't continue_read_stream");
}
fn regist_event(epoll_fd: i32, fd: RawFd, ctl: i32)-> io::Result<i32> {
    let mut event = libc::epoll_event {
        events: (libc::EPOLLONESHOT | libc::EPOLLRDHUP | libc::EPOLLIN | libc::EPOLLET) as u32,
        u64: fd as u64,
    };
    syscall!(epoll_ctl(epoll_fd, ctl, fd, &mut event))
}
fn remove_read_stream(epoll_fd: i32, fd: RawFd){
    syscall!(epoll_ctl(epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut())).expect("couldn't continue_read_stream");  
}