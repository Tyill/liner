//! # liner_broker
//!
//! `liner_broker` is a simple and fast redis based message serverless broker.  
//! Data transfer via TCP.
//! 
//! # Examples
//!
//! ```
//! use liner_broker::Liner;
//! 
//! fn  main() {
//! 
//!     let mut client1 = Liner::new("client1", "topic_client1", "localhost:2255", "redis://localhost/");
//!     let mut client2 = Liner::new("client2", "topic_client2", "localhost:2256", "redis://localhost/");
//!    
//!     client1.run(Box::new(|_to: &str, _from: &str, _data: &[u8]|{
//!         println!("receive_from {}", _from);
//!     }));
//!     client2.run(Box::new(|_to: &str, _from: &str, _data: &[u8]|{
//!         println!("receive_from {}", _from);
//!     }));
//!  
//!     let array = [0; 100];
//!     for _ in 0..10{
//!         client1.send_to("topic_client2", array.as_slice());
//!         println!("send_to client2");       
//!     }
//! }
//! 
//! ```

mod redis;
mod client;
mod message;
mod mempool;
mod bytestream;
mod listener;
mod sender;
mod settings;
mod common;

use crate::client::Client;
use std::ffi::CStr;
use std::ffi::CString;

type UCback = Box<dyn FnMut(&str, &str, &[u8])>;

extern "C" fn cb_(to: *const i8, from: *const i8,  data: *const u8, dsize: usize, udata: *mut libc::c_void){
    unsafe {    
        if let Some(liner) = udata.cast::<Liner>().as_mut(){
            if let Some(ucback) = liner.ucback.as_mut(){
                let to = CStr::from_ptr(to).to_str().unwrap();
                let from = CStr::from_ptr(from).to_str().unwrap();           
                (ucback)(to, from, std::slice::from_raw_parts(data, dsize));
            }
        }
    }
}

pub struct Liner{
    hclient: *mut Client,
    ucback: Option<UCback>,
}

impl Liner {
    pub fn new(unique_name: &str,
        topic: &str,
        localhost: &str,
        redis_path: &str)->Liner{
    unsafe{
        let unique = CString::new(unique_name).unwrap();
        let dbpath = CString::new(redis_path).unwrap();
        let localhost = CString::new(localhost).unwrap();
        let topic_client = CString::new(topic).unwrap();
        let hclient = ln_new_client(unique.as_ptr(),
                                                        topic_client.as_ptr(),
                                                        localhost.as_ptr(),
                                                        dbpath.as_ptr());
        if hclient != std::ptr::null_mut(){
            Self{hclient, ucback: None}
        }else{
            panic!("error create client");
        }
    }   
    }
    pub fn run(&mut self, ucback: UCback)->bool{        
        unsafe{
            self.ucback = Some(ucback);
            let ud = self as *const Self as *mut libc::c_void;
            ln_run(self.hclient, cb_, ud)
        }
    }
    pub fn send_to(&mut self, topic: &str, data: &[u8])->bool{
        unsafe{
            let topic = CString::new(topic).unwrap();
            ln_send_to(self.hclient, topic.as_ptr(), data.as_ptr(), data.len(), true)
        }
    }
    pub fn send_all(&mut self, topic: &str, data: &[u8])->bool{
        unsafe{
            let topic = CString::new(topic).unwrap();
            ln_send_all(self.hclient, topic.as_ptr(), data.as_ptr(), data.len(), true)
        }
    }
    pub fn subscribe(&mut self, topic: &str)->bool{
        unsafe{
            let topic = CString::new(topic).unwrap();
            ln_subscribe(self.hclient, topic.as_ptr())
        }
    }
    pub fn unsubscribe(&mut self, topic: &str)->bool{
        unsafe{
            let topic = CString::new(topic).unwrap();
            ln_unsubscribe(self.hclient, topic.as_ptr())
        }
    }
    pub fn clear_stored_messages(&mut self)->bool{
        unsafe{
            ln_clear_stored_messages(self.hclient)
        }
    }
    pub fn clear_addresses_of_topic(&mut self)->bool{
        unsafe{
            ln_clear_addresses_of_topic(self.hclient)
        }
    }
}

impl Drop for Liner {
    fn drop(&mut self) {
        ln_delete_client(self.hclient);
    }
}

/// Create new client for transfer data.
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_new_client(unique_name: *const i8,
                       topic: *const i8,
                       localhost: *const i8,
                       redis_path: *const i8,
                       )->*mut Client{
    let unique_name = CStr::from_ptr(unique_name).to_str().unwrap();
    let topic = CStr::from_ptr(topic).to_str().unwrap();
    let localhost = CStr::from_ptr(localhost).to_str().unwrap();
    let redis_path = CStr::from_ptr(redis_path).to_str().unwrap();
    
    if unique_name.is_empty(){
        print_error!("unique_name empty");
        return std::ptr::null_mut();
    }
    if topic.is_empty(){
        print_error!("topic empty");
        return std::ptr::null_mut();
    }
    if localhost.is_empty(){
        print_error!("localhost empty");
        return std::ptr::null_mut();
    }
    if redis_path.is_empty(){
        print_error!("redis_path empty");
        return std::ptr::null_mut();
    }
    if let Some(c) = Client::new(unique_name, topic, localhost, redis_path){
        let bx = Box::new(c);
        Box::into_raw(bx)
    }else{
        std::ptr::null_mut()
    }
    
}

pub struct UData(*mut libc::c_void);
type UCbackIntern = extern "C" fn(to: *const i8, from: *const i8, data: *const u8, dsize: usize, udata: *mut libc::c_void);

unsafe impl Send for UData {}

/// Launching a client to send messages and listen for incoming messages. 
/// 
/// Possible errors when launching a client:
/// - no connection to redis
/// - the address for the client is busy
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_run(client: *mut Client, receive_cb: UCbackIntern, udata: *mut libc::c_void)->bool{
    if !has_client(client){
        return false;
    }
    let udata: UData = UData(udata);
    (*client).run(receive_cb, udata)
}

/// Send message to other client.
/// Call only when the client is already running. 
/// 
/// Possible errors:
/// - no connection to redis
/// - no other client with this topic
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_send_to(client: *mut Client,
                          topic: *const i8,
                          data: *const u8, data_size: usize,
                          at_least_once_delivery: bool)->bool{
    let topic = CStr::from_ptr(topic).to_str().unwrap();  
    let data = std::slice::from_raw_parts(data, data_size);

    if !has_client(client){
        return false;
    }
    if topic.is_empty(){
        print_error!("topic name empty");
        return false;
    }
    if data_size == 0{
        print_error!("data_size empty");
        return false;
    }
    (*client).send_to(topic, data, at_least_once_delivery)
}

/// Send message to other clients. 
/// Call only when the client is already running.
/// 
/// Possible errors:
/// - no connection to redis
/// - no other client with this topic
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_send_all(client: *mut Client,
                          topic: *const i8,
                          data: *const u8, data_size: usize,
                          at_least_once_delivery: bool)->bool{
    let topic = CStr::from_ptr(topic).to_str().unwrap();    
    let data = std::slice::from_raw_parts(data, data_size);

    if !has_client(client){
        return false;
    }
    if topic.is_empty(){
        print_error!("topic.is_empty()");
        return false;
    }
    if data_size == 0{
        print_error!("data_size == 0");
        return false;
    }
    (*client).send_all(topic, data, at_least_once_delivery)
}

/// Subscribe to the topic and receive messages from other clients.
/// Call only when the client is not running yet.
/// 
/// Possible errors:
/// - no connection to redis
/// - no other client with this topic
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_subscribe(client: *mut Client,
                          topic: *const i8)->bool{
    let topic = CStr::from_ptr(topic).to_str().unwrap();
    
    if !has_client(client){
        return false;
    }
    if topic.is_empty(){
        print_error!("topic.is_empty()");
        return false;
    }
    (*client).subscribe(topic)
}

/// Unsubscribe from the topic and do not receive messages from other clients.
/// Call only when the client is not running yet.
/// 
/// Possible errors:
/// - no connection to redis
/// - no other client with this topic
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_unsubscribe(client: *mut Client,
                          topic: *const i8)->bool{
    let topic = CStr::from_ptr(topic).to_str().unwrap();
    
    if !has_client(client){
        return false;
    }
    if topic.is_empty(){
        print_error!("topic.is_empty()");
        return false;
    }
    (*client).unsubscribe(topic)
}

/// Clearing messages that were not previously sent for some reason.
/// Call only when the client is not running yet.
/// 
/// Possible errors:
/// - no connection to redis
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_clear_stored_messages(client: *mut Client)->bool{
    if !has_client(client){
        return false;
    }
    (*client).clear_stored_messages()
}

/// Cleaning client addresses.
/// Call only when the client is not running yet.
/// 
/// Possible errors:
/// - no connection to redis
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_clear_addresses_of_topic(client: *mut Client)->bool{
    if !has_client(client){
        return false;
    }
    (*client).clear_addresses_of_topic()
}

/// Deleting a client.
#[no_mangle]
pub extern "C" fn ln_delete_client(client: *mut Client)->bool{
    if !has_client(client){
        return false;
    }
    unsafe{
        drop(Box::from_raw(client));
    }
    true
}

fn has_client(client: *mut Client)->bool{
    if client != std::ptr::null_mut(){
        true
    }else{
        print_error!("client was not created");
        false
    }
}

#[macro_export]
macro_rules! print_error {
    ($arg:expr) => { eprintln!("Error {}:{}: {}", file!(), line!(), $arg) }
}

// The debug version
#[cfg(feature = "liner_debug")]
#[macro_export]
macro_rules! print_debug {
    ($( $args:expr ),*) => { println!("Debug", $( $args ),* ) }
}

// Non-debug version
#[cfg(not(feature = "liner_debug"))]
#[macro_export]
macro_rules! print_debug {
    ($( $args:expr ),*) => {}
}