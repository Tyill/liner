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

pub struct EPollListener{
    _epoll_fd: i32,    
}

impl EPollListener {
    pub fn new(listener: TcpListener, tx: mpsc::Sender<Message>, db: Arc<Mutex<redis::Connect>>)->EPollListener{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        thread::spawn(move|| {
            listener.set_nonblocking(true).expect("couldn't listener set_nonblocking");
            let listener_fd = listener.as_raw_fd();
            regist_event(epoll_fd, listener_fd, libc::EPOLL_CTL_ADD).expect("couldn't event regist");
            let mut streams: HashMap<RawFd, Arc<Mutex<TcpStream>>> = HashMap::new();
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(128);
            let mut last_mess_number: HashMap<RawFd, Arc<Mutex<u64>>> = HashMap::new();
            loop{    
                if !wait(epoll_fd, &mut events){
                    break;
                }               
                for ev in &events {  
                    if ev.u64 == listener_fd as u64{
                        listener_accept(epoll_fd, &mut streams, &listener, &db, &mut last_mess_number);
                    }else if ev.events as i32 & libc::EPOLLIN > 0{
                        let stream_fd = ev.u64 as RawFd;
                        read_stream(epoll_fd, stream_fd, &streams, &tx, &db, &mut last_mess_number);
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
                   listener: &TcpListener,
                   db: &Arc<Mutex<redis::Connect>>,
                   last_mess_number: &mut HashMap<RawFd, Arc<Mutex<u64>>>){
    match listener.accept() {
        Ok((stream, _addr)) => {
            stream.set_nonblocking(true).expect("couldn't listener set_nonblocking");
            let stream_fd = stream.as_raw_fd();
            if !last_mess_number.contains_key(&stream_fd){
                let addr_rem = stream.peer_addr().unwrap().to_string();
                if let Ok(lmn) = db.lock().unwrap().get_last_mess_number_for_listener(&addr_rem){
                    last_mess_number.insert(stream_fd, Arc::new(Mutex::new(lmn)));
                }else{
                    print_error(&format!("error get_last_mess_number_for_listener"));
                    last_mess_number.insert(stream_fd, Arc::new(Mutex::new(0)));
                }
            }
            add_read_stream(epoll_fd, stream_fd);
            streams.insert(stream_fd, Arc::new(Mutex::new(stream)));
        }
        Err(err) => print_error(&format!("couldn't accept: {}", err)),
    };
    continue_read_stream(epoll_fd, listener.as_raw_fd());
}

fn read_stream(epoll_fd: RawFd,
               stream_fd: RawFd,
               streams: &HashMap<RawFd, Arc<Mutex<TcpStream>>>, 
               tx: &mpsc::Sender<Message>, 
               db: &Arc<Mutex<redis::Connect>>,
               last_mess_number: &mut HashMap<RawFd, Arc<Mutex<u64>>>){
    if let Some(stream) = streams.get(&stream_fd){
        let tx = tx.clone();
        let stream = stream.clone();
        let last_mess_num = last_mess_number.get(&stream_fd).unwrap().clone();
        let db = db.clone();
        rayon::spawn(move || {
            let stream = stream.lock().unwrap();
            let mut reader = BufReader::new(stream.deref());
            let mut last_mess_num = last_mess_num.lock().unwrap();
            while let Some(m) = Message::from_stream(&mut reader){
                if m.number_mess > *last_mess_num{
                    *last_mess_num = m.number_mess;
                    if let Err(err) = tx.send(m){
                        print_error(&format!("couldn't tx.send: {}", err));
                    }
                }else{
                    print_error(&format!("receive old mess num{}", m.number_mess));
                }
            }
            let addr_rem = stream.peer_addr().unwrap().to_string();
            if let Err(err) = db.lock().unwrap().set_last_mess_number_from_listener(&addr_rem, *last_mess_num){
                print_error(&format!("couldn't db.set_last_mess_number: {}", err));
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