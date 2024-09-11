use crate::message::Message;
use crate::redis;

use std::collections::HashMap;
use std::{thread, sync::mpsc};
use std::net::{TcpStream, TcpListener};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::{Arc, Mutex};
use std::io;


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
    epoll_fd: i32,
}

impl EPollListener {
    pub fn new(listener: TcpListener, tx: mpsc::Sender<Message>, db: &Arc<Mutex<redis::Connect>>)->EPollListener{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("couldn't create epoll queue");
        let db = db.clone();
        thread::spawn(move|| {
            listener.set_nonblocking(true).expect("couldn't listener set_nonblocking");
            let listener_fd = listener.as_raw_fd();
            regist_event(epoll_fd, listener_fd, libc::EPOLL_CTL_ADD).expect("couldn't event regist");
            let mut streams: HashMap<RawFd, Arc<Mutex<TcpStream>>> = HashMap::new();
            let mut events: Vec<libc::epoll_event> = Vec::with_capacity(128);
            loop{    
                let mut ready_count = 0;  
                match syscall!(epoll_wait(
                    epoll_fd,
                    events.as_mut_ptr() as *mut libc::epoll_event,
                    128,
                    -1,
                )){
                    Ok(res)=>ready_count = res,
                    Err(err)=>{
                        if err.kind() == std::io::ErrorKind::Interrupted{
                            continue;
                        }else{
                            eprintln!("couldn't epoll_wait: {}", err);
                            break;
                        }
                    }
                }  
                unsafe { events.set_len(ready_count as usize) };
        
                for ev in &events {  
                    if ev.u64 == listener_fd as u64{
                        match listener.accept() {
                            Ok((stream, addr)) => {
                                stream.set_nonblocking(true).expect("couldn't listener set_nonblocking");
                                let stream_fd = stream.as_raw_fd();
                                add_read_stream(epoll_fd, stream_fd).expect("couldn't add_read_stream");
                                streams.insert(stream_fd, Arc::new(Mutex::new(stream)));

                                read_last_from_db(&db);
                            }
                            Err(err) => eprintln!("couldn't accept: {}", err),
                        };
                        continue_read_stream(epoll_fd, listener_fd).expect("couldn't event continue_read_stream");
                    }else if ev.events as i32 & libc::EPOLLIN > 0{
                        let stream_fd = ev.u64 as RawFd;
                        if let Some(stream) = streams.get(&stream_fd){
                            let tx = tx.clone();
                            let stream = stream.clone();
                            rayon::spawn(move || {
                                let mut stream = stream.lock().unwrap();
                                while let Some(m) = Message::from_stream(&mut stream){
                                    let _ = tx.send(m);
                                }
                                continue_read_stream(epoll_fd, stream_fd).expect("couldn't event continue_read_stream");
                            });
                        }
                    }else if ev.events as i32 & (libc::EPOLLHUP | libc::EPOLLERR) > 0{
                        let stream_fd = ev.u64 as RawFd;
                        let _ = remove_read_stream(epoll_fd, stream_fd);
                        streams.remove(&stream_fd);
                    }else{
                        eprintln!("unexpected events: {}", ev.events as i32);
                    }
                }
            }
        });
        Self{
            epoll_fd,
        }
    }
    
    pub fn close(&self) {
        let _ = syscall!(close(self.epoll_fd));
    }
}

fn read_last_from_db(db: &Arc<Mutex<redis::Connect>>){
    rayon::spawn(move || {
        // while let Some(m) = Message::from_stream(&stream){
        //     let _ = tx.send(m);
        // }
    });
}
fn add_read_stream(epoll_fd: i32, fd: RawFd)->io::Result<i32>{
    regist_event(epoll_fd, fd, libc::EPOLL_CTL_ADD)
}    
fn continue_read_stream(epoll_fd: i32, fd: RawFd) -> io::Result<i32> {
    regist_event(epoll_fd, fd, libc::EPOLL_CTL_MOD)
}
fn regist_event(epoll_fd: i32, fd: RawFd, ctl: i32)-> io::Result<i32> {
    let mut event = libc::epoll_event {
        events: (libc::EPOLLONESHOT | libc::EPOLLRDHUP | libc::EPOLLIN | libc::EPOLLET) as u32,
        u64: fd as u64,
    };
    syscall!(epoll_ctl(epoll_fd, ctl, fd, &mut event))
}
fn remove_read_stream(epoll_fd: i32, fd: RawFd) -> io::Result<(i32)> {
    syscall!(epoll_ctl(epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut()))    
}