use std::sync::mpsc::Sender;
use std::{thread, sync::mpsc};
use std::net::{TcpStream, TcpListener};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::{Arc, Mutex};
use std::io;

use crate::message::Message;

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

pub struct EPoll{
    epoll_fd: i32,
}

impl EPoll {
    pub fn new(listener: TcpListener, tx: mpsc::Sender<Message>)->EPoll{
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("can create epoll queue");
        thread::spawn(move|| {
            listener.set_nonblocking(true).unwrap();
            regist_event(epoll_fd, 0, listener.as_raw_fd(), libc::EPOLL_CTL_ADD).expect("can event regist");
            let mut stream_index = 1;
            let mut streams: Vec<Arc<Mutex<TcpStream>>> = Vec::new();
            loop{
                let mut events: Vec<libc::epoll_event> = Vec::with_capacity(1024);
                let res = syscall!(epoll_wait(
                    epoll_fd,
                    events.as_mut_ptr() as *mut libc::epoll_event,
                    1024,
                    -1,
                )).expect("error during epoll wait");
                unsafe { events.set_len(res as usize) };
        
                for ev in events {  
                    if ev.u64 == 0{
                        match listener.accept() {
                            Ok((stream, addr)) => {
                                stream.set_nonblocking(true).unwrap();
                                add_read_stream(epoll_fd, stream_index, stream.as_raw_fd()).unwrap();
                                streams.push(Arc::new(Mutex::new(stream)));
                                stream_index += 1;
                            }
                            Err(e) => eprintln!("couldn't accept: {}", e),
                        };
                        continue_read_stream(epoll_fd, listener.as_raw_fd(), 0).unwrap();
                    }else if ev.events as i32 & libc::EPOLLIN == libc::EPOLLIN{
                        let stream_index = (ev.u64 - 1) as usize;
                        let stream = streams[stream_index].clone();
                        let stream_fd = stream.lock().unwrap().as_raw_fd();
                        let tx = tx.clone();
                        rayon::spawn(move || {
                            if let Some(m) = Message::from_stream(&stream){
                                tx.send(m).unwrap();
                            }else{
                                remove_read_stream(epoll_fd, stream_fd).unwrap();
                            }          
                        });
                        continue_read_stream(epoll_fd, stream_fd, stream_index as u64).unwrap();
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

fn add_read_stream(epoll_fd: i32, key:u64, fd: RawFd)->io::Result<i32>{
    regist_event(epoll_fd, key, fd, libc::EPOLL_CTL_ADD)
}    
fn continue_read_stream(epoll_fd: i32, fd: RawFd, key: u64) -> io::Result<i32> {
    regist_event(epoll_fd, key, fd, libc::EPOLL_CTL_MOD)
}
fn regist_event(epoll_fd: i32, key: u64, fd: RawFd, ctl: i32)-> io::Result<i32> {
    let mut event = libc::epoll_event {
        events: (libc::EPOLLONESHOT | libc::EPOLLIN) as u32,
        u64: key,
    };
    syscall!(epoll_ctl(epoll_fd, ctl, fd, &mut event))
}
fn remove_read_stream(epoll_fd: i32, fd: RawFd) -> io::Result<()> {
    syscall!(epoll_ctl(epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut()))    
}