use std::sync::mpsc::Sender;
use std::{thread, sync::mpsc};
use std::net::TcpStream;
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

pub struct EPool{
    epoll_fd: i32,
    accept_key: u64,
    tx: mpsc::Sender<Message>,
    streams: Vec<TcpStream>
}

impl EPool {
    pub fn new(tx: mpsc::Sender<Message>)->EPool{
        
        let epoll_fd = syscall!(epoll_create1(libc::EPOLL_CLOEXEC)).expect("can create epoll queue");
            
        thread::spawn(move|| loop{
            let events: Vec<libc::epoll_event> = Vec::with_capacity(1024);
            events.clear();
            let res = syscall!(epoll_wait(
                epoll_fd,
                events.as_mut_ptr() as *mut libc::epoll_event,
                1024,
                -1,
            )).expect("error during epoll wait");
    
            unsafe { events.set_len(res as usize) };
    
            for ev in events {                
                for e in ev.events{
                    if *e as i32 & libc::EPOLLIN == libc::EPOLLIN{
                        
                    }else{
                        eprintln!("unexpected events: {}", e);
                    }
                }
            }
        });
        Self{
            epoll_fd,
            accept_key: 0,
            tx,
            streams: Vec::new()
        }
    }
    pub fn add_read_stream(&mut self, stream: TcpStream)->io::Result<u64>{
        stream.set_nonblocking(true)?;  
        let key = self.accept_key;      
        let mut event = libc::epoll_event {
            events: (libc::EPOLLONESHOT | libc::EPOLLIN) as u32,
            u64: key,
        };        
        syscall!(epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_ADD, stream.as_raw_fd(), &mut event))?;
        self.accept_key += 1;
        self.streams.push(stream);
        Ok(key)
    }
    pub fn continue_read_stream(&self, stream: &mut TcpStream, key: u64) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: (libc::EPOLLONESHOT | libc::EPOLLIN) as u32,
            u64: key,
        };
        syscall!(epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_MOD, stream.as_raw_fd(), &mut event))?;
        Ok(())
    }
    pub fn remove_read_stream(&self, stream: &mut TcpStream) -> io::Result<()> {
        syscall!(epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_DEL, stream.as_raw_fd(), std::ptr::null_mut()))?;
        Ok(())
    }
    pub fn close(&self) {
        let _ = syscall!(close(self.epoll_fd));
    }    
}

