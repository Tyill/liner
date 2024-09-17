use crate::message::Message;
use crate::redis;
use crate::print_error;

use std::collections::HashMap;
use std::ops::Deref;
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

pub struct Listener{
    _epoll_fd: i32,    
}

impl Listener {
    pub fn new(listener: TcpListener, tx: mpsc::Sender<Message>, db: Arc<Mutex<redis::Connect>>)->Listener{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        thread::spawn(move|| {
            listener.set_nonblocking(true).expect("couldn't listener set_nonblocking");
            let listener_fd = listener.as_raw_fd();
            regist_event(epoll_fd, listener_fd, libc::EPOLL_CTL_ADD).expect("couldn't event regist");
            let mut streams: HashMap<RawFd, Arc<Mutex<TcpStream>>> = HashMap::new();
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(128);
            loop{    
                if !wait(epoll_fd, &mut events){
                    break;
                }               
                for ev in &events {  
                    if ev.u64 == listener_fd as u64{
                        listener_accept(epoll_fd, &mut streams, &listener);
                    }else if ev.events as i32 & libc::EPOLLIN > 0{
                        let stream_fd = ev.u64 as RawFd;
                        read_stream(epoll_fd, stream_fd, &streams, &tx, &db);
                    }else if ev.events as i32 & (libc::EPOLLHUP | libc::EPOLLERR) > 0{
                        let stream_fd = ev.u64 as RawFd;
                        remove_read_stream(epoll_fd, stream_fd);
                        streams.remove(&stream_fd);
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
        128,
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
                   streams: &mut HashMap<RawFd, Arc<Mutex<TcpStream>>>,
                   listener: &TcpListener){
    match listener.accept() {
        Ok((stream, _addr)) => {
            stream.set_nonblocking(true).expect("couldn't listener set_nonblocking");
            let stream_fd = stream.as_raw_fd();            
            add_read_stream(epoll_fd, stream_fd);
            streams.insert(stream_fd, Arc::new(Mutex::new(stream)));
        }
        Err(err) => print_error(&format!("couldn't accept: {}", err), file!(), line!()),
    };
    continue_read_stream(epoll_fd, listener.as_raw_fd());
}

fn read_stream(epoll_fd: RawFd,
               stream_fd: RawFd,
               streams: &HashMap<RawFd, Arc<Mutex<TcpStream>>>, 
               tx: &mpsc::Sender<Message>, 
               db: &Arc<Mutex<redis::Connect>>){
    if let Some(stream) = streams.get(&stream_fd){
        let tx = tx.clone();
        let stream = stream.clone();
        let db = db.clone();
        rayon::spawn(move || {
            let stream = stream.lock().unwrap();
            let mut reader = BufReader::new(stream.deref());
            let mut last_mess_num: u64 = 0;
            let mut sender_name = String::new();
            let mut sender_topic = String::new();
            while let Some(m) = Message::from_stream(&mut reader){
                if last_mess_num == 0{
                    match db.lock().unwrap().get_last_mess_number_for_listener(&m.sender_name, &m.topic_from){
                        Ok(num)=>{
                            last_mess_num = num;
                        },
                        Err(err)=>{
                            print_error(&format!("couldn't get_last_mess_number_for_listener: {}", err), file!(), line!());
                            last_mess_num = 0;
                        }
                    }
                    sender_name = m.sender_name.clone();
                    sender_topic = m.topic_from.clone();
                }
                if m.number_mess > last_mess_num{
                    last_mess_num = m.number_mess;
                    if let Err(err) = tx.send(m){
                        print_error(&format!("couldn't tx.send: {}", err), file!(), line!());
                    }
                }else{
                    print_error(&format!("receive old mess num{}", m.number_mess), file!(), line!());
                }
            }
            if let Err(err) = db.lock().unwrap().set_last_mess_number_from_listener(&sender_name, &sender_topic, last_mess_num){
                print_error(&format!("couldn't db.set_last_mess_number: {}", err), file!(), line!());
            }
            continue_read_stream(epoll_fd, stream_fd);
        });
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