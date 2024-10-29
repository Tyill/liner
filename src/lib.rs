//! # liner_broker
//!
//! `liner_broker` is a simple and fast redis based message serverless broker.  
//! Data transfer via TCP.
//! 
//! # Examples
//!
//! ```
//! mod liner;
//! use liner::Liner;
//! 
//! fn cb1(_to: &str, _from: &str,  _data: &[u8]){
//!     println!("receive_from {}", _from);
//! }
//! 
//! fn cb2(_to: &str, _from: &str,  _data: &[u8]){
//!     println!("receive_from {}", _from);
//! }
//! 
//! fn  main() {
//! 
//!     let mut client1 = Liner::new("client1", "topic_client1", "localhost:2255", "redis://localhost/", cb1);
//!     let mut client2 = Liner::new("client2", "topic_client2", "localhost:2256", "redis://localhost/", cb2);
//!    
//!     client1.run();
//!     client2.run();
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
pub use crate::client::Client;

use std::ffi::CStr;

/// Create new client for transfer data.
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_new_client(unique_name: *const i8,
                       topic: *const i8,
                       localhost: *const i8,
                       redis_path: *const i8,
                       )->Box<Option<Client>>{
    let unique_name = CStr::from_ptr(unique_name).to_str().unwrap();
    let topic = CStr::from_ptr(topic).to_str().unwrap();
    let localhost = CStr::from_ptr(localhost).to_str().unwrap();
    let redis_path = CStr::from_ptr(redis_path).to_str().unwrap();
    
    if unique_name.is_empty(){
        print_error!("unique_name empty");
        return Box::new(None);
    }
    if topic.is_empty(){
        print_error!("topic empty");
        return Box::new(None);
    }
    if localhost.is_empty(){
        print_error!("localhost empty");
        return Box::new(None);
    }
    if redis_path.is_empty(){
        print_error!("redis_path empty");
        return Box::new(None);
    }
    Box::new(Client::new(unique_name, topic, localhost, redis_path))
}

/// Сhecks that the client was created successfully. 
/// 
/// Possible errors when creating a client:
/// - no connection to redis
/// - the address for the client is busy
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_has_client(client: &Box<Option<Client>>)->bool{
    has_client(client)
}

pub struct UData(*const libc::c_void);
type UCback = extern "C" fn(to: *const i8, from: *const i8, data: *const u8, dsize: usize, udata: *const libc::c_void);

unsafe impl Send for UData {}

/// Launching a client to send messages and listen for incoming messages. 
/// 
/// Possible errors when launching a client:
/// - no connection to redis
/// - the address for the client is busy
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_run(client: &mut Box<Option<Client>>, receive_cb: UCback, udata: *const libc::c_void)->bool{
    if !has_client(client){
        return false;
    }    
    let c = client.as_mut();

    let udata: UData = UData(udata);
    c.as_mut().unwrap().run(receive_cb, udata)
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
pub unsafe extern "C" fn ln_send_to(client: &mut Box<Option<Client>>,
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
    let c = client.as_mut();
    c.as_mut().unwrap().send_to(topic, data, at_least_once_delivery)
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
pub unsafe extern "C" fn ln_send_all(client: &mut Box<Option<Client>>,
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
    let c = client.as_mut();
    c.as_mut().unwrap().send_all(topic, data, at_least_once_delivery)
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
pub unsafe extern "C" fn ln_subscribe(client: &mut Box<Option<Client>>,
                          topic: *const i8)->bool{
    let topic = CStr::from_ptr(topic).to_str().unwrap();
    
    if !has_client(client){
        return false;
    }
    if topic.is_empty(){
        print_error!("topic.is_empty()");
        return false;
    }
    let c = client.as_mut();
    c.as_mut().unwrap().subscribe(topic)
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
pub unsafe extern "C" fn ln_unsubscribe(client: &mut Box<Option<Client>>,
                          topic: *const i8)->bool{
    let topic = CStr::from_ptr(topic).to_str().unwrap();
    
    if !has_client(client){
        return false;
    }
    if topic.is_empty(){
        print_error!("topic.is_empty()");
        return false;
    }
    let c = client.as_mut();
    c.as_mut().unwrap().unsubscribe(topic)
}

/// Clearing messages that were not previously sent for some reason.
/// Call only when the client is not running yet.
/// 
/// Possible errors:
/// - no connection to redis
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_clear_stored_messages(client: &mut Box<Option<Client>>)->bool{
    if !has_client(client){
        return false;
    }
    let c = client.as_mut();
    c.as_mut().unwrap().clear_stored_messages()
}

/// Cleaning client addresses.
/// Call only when the client is not running yet.
/// 
/// Possible errors:
/// - no connection to redis
/// 
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_clear_addresses_of_topic(client: &mut Box<Option<Client>>)->bool{
    if !has_client(client){
        return false;
    }
    let c = client.as_mut();
    c.as_mut().unwrap().clear_addresses_of_topic()
}

/// Deleting a client.
#[no_mangle]
pub extern "C" fn ln_delete_client(client: Box<Option<Client>>)->bool{
    if !has_client(&client){
        return false;
    }
    drop(client.unwrap());
    true
}

fn has_client(client: &Option<Client>)->bool{
    if client.is_some(){
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