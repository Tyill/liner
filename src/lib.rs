//! # liner_broker
//!
//! `liner_broker` is a simple and fast redis based message serverless broker.  
//! Data transfer via TCP.
//! 
//! # Examples
//!
//! ```
//! use std::ffi::CString;
//! unsafe {
//!     let redis_path = CString::new("redis://localhost/").unwrap();
//!         
//!     let client1 = CString::new("client1").unwrap();               // must be a unique name
//!     let localhost1 = CString::new("localhost:2255").unwrap();
//!     let topic_client1 = CString::new("topic_client1").unwrap();   // may coincide with other clients
//!     
//!     let hclient1 = liner_broker::ln_new_client(client1.as_ptr(),
//!                                                    topic_client1.as_ptr(),
//!                                                    localhost1.as_ptr(),
//!                                                    redis_path.as_ptr());
//!     let has_client = liner_broker::ln_has_client(&hclient1){
//!         panic!("hclient1 error create, check redis path");
//!     }
//!     
//!     let client2 = CString::new("client2").unwrap();               // must be a unique name
//!     let localhost2 = CString::new("localhost:2256").unwrap();
//!     let topic_client2 = CString::new("topic_client2").unwrap();   // may coincide with other clients
//!     
//!     let hclient2 = liner_broker::ln_new_client(client2.as_ptr(),
//!                                                topic_client2.as_ptr(),
//!                                                localhost2.as_ptr(),
//!                                                redis_path.as_ptr());
//!     if !liner_broker::ln_has_client(&hclient2){
//!         panic!("hclient2 error create, check redis path");
//!     }
//! 
//!     if !liner_broker::ln_run(&mut hclient1, cb1){
//!         panic!("hclient1 ln_run not run!");
//!     }
//!     if !liner_broker::ln_run(&mut hclient2, cb2){
//!         panic!("hclient2 ln_run not run!");
//!     }
//!     let array = [0; MESS_SIZE];
//!     if !liner_broker::ln_send_to(&mut hclient1, topic_client2.as_ptr(), array.as_ptr(), array.len(), true){
//!         panic!("hclient1 ln_send_to error!");
//!     }         
//! }
//! 
//! unsafe fn cb1(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
//!     unsafe {
//!         let from = CStr::from_ptr(_from);
//!         println!("cb1 receive_from {}", from.to_str().unwrap());
//!     }
//! }
//! 
//! unsafe fn cb2(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
//!     unsafe {
//!         let from = CStr::from_ptr(_from);
//!         println!("cb2 receive_from {}", from.to_str().unwrap());
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

/// Create new client for transfer data.
/// 
/// # Examples
///
/// ```
/// use std::ffi::CString;
/// unsafe {
///     let client1 = CString::new("client1").unwrap();               // must be a unique name
///     let redis_path = CString::new("redis://localhost/").unwrap();
///     let localhost1 = CString::new("localhost:2255").unwrap();
///     let topic_client1 = CString::new("topic_client1").unwrap();   // may coincide with other clients
///     
///     let hclient1 = liner_broker::ln_new_client(client1.as_ptr(),
///                                                    topic_client1.as_ptr(),
///                                                    localhost1.as_ptr(),
///                                                    redis_path.as_ptr());
///     let has_client = liner_broker::ln_has_client(&hclient1);
///         panic!("hclient1 error create, check redis path");
///     }
/// }
/// ```
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
/// # Examples
///
/// ```
/// use std::ffi::CString;
/// unsafe {
///     let client1 = CString::new("client1").unwrap();               // must be a unique name
///     let redis_path = CString::new("redis://localhost/").unwrap();
///     let localhost1 = CString::new("localhost:2255").unwrap();
///     let topic_client1 = CString::new("topic_client1").unwrap();   // may coincide with other clients
///     
///     let hclient1 = liner_broker::ln_new_client(client1.as_ptr(),
///                                                    topic_client1.as_ptr(),
///                                                    localhost1.as_ptr(),
///                                                    redis_path.as_ptr());
///     let has_client = liner_broker::ln_has_client(&hclient1);
///         panic!("hclient1 error create, check redis path");
///     }
/// }
/// ```
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_has_client(client: &Box<Option<Client>>)->bool{
    has_client(client)
}

type UCback = extern "C" fn(to: *const i8, from: *const i8, data: *const u8, dsize: usize);

/// Launching a client to send messages and listen for incoming messages. 
/// 
/// Possible errors when launching a client:
/// - no connection to redis
/// - the address for the client is busy
/// 
/// # Examples
///
/// ```
/// use std::ffi::CString;
/// unsafe {
///     let client1 = CString::new("client1").unwrap();               // must be a unique name
///     let redis_path = CString::new("redis://localhost/").unwrap();
///     let localhost1 = CString::new("localhost:2255").unwrap();
///     let topic_client1 = CString::new("topic_client1").unwrap();   // may coincide with other clients
///     
///     let hclient1 = liner_broker::ln_new_client(client1.as_ptr(),
///                                                    topic_client1.as_ptr(),
///                                                    localhost1.as_ptr(),
///                                                    redis_path.as_ptr());
///     let has_client = liner_broker::ln_has_client(&hclient1);
///         panic!("hclient1 error create, check redis path");
///     }
///     if !liner_broker::ln_run(&mut hclient1, cb1){
///         panic!("hclient1 ln_run not run!");
///     }
/// }
/// ```
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_run(client: &mut Box<Option<Client>>, receive_cb: UCback)->bool{
    if !has_client(client){
        return false;
    }    
    let c = client.as_mut();
    c.as_mut().unwrap().run(receive_cb)
}
/// Send message to other client.
/// Call only when the client is already running. 
/// 
/// Possible errors:
/// - no connection to redis
/// - no other client with this topic
/// 
/// # Examples
///
/// ```
/// use std::ffi::CString;
/// unsafe {
///     let redis_path = CString::new("redis://localhost/").unwrap();
///         
///     let client1 = CString::new("client1").unwrap();               // must be a unique name
///     let localhost1 = CString::new("localhost:2255").unwrap();
///     let topic_client1 = CString::new("topic_client1").unwrap();   // may coincide with other clients
///     
///     let hclient1 = liner_broker::ln_new_client(client1.as_ptr(),
///                                                topic_client1.as_ptr(),
///                                                localhost1.as_ptr(),
///                                                redis_path.as_ptr());
///     let has_client = liner_broker::ln_has_client(&hclient1);
///         panic!("hclient1 error create, check redis path");
///     }
///     if !liner_broker::ln_run(&mut hclient1, cb1){
///         panic!("hclient1 ln_run not run!");
///     }
/// 
///     let topic_client2 = CString::new("topic_client2").unwrap();   // may coincide with other clients
///     let array = [0; MESS_SIZE];
///     if !liner_broker::ln_send_to(&mut hclient1, topic_client2.as_ptr(), array.as_ptr(), array.len(), true){
///         panic!("hclient1 ln_send_to error!");
///     }  
/// }
/// ```
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
/// # Examples
///
/// ```
/// use std::ffi::CString;
/// unsafe {
///     let redis_path = CString::new("redis://localhost/").unwrap();
///         
///     let client1 = CString::new("client1").unwrap();               // must be a unique name
///     let localhost1 = CString::new("localhost:2255").unwrap();
///     let topic_client1 = CString::new("topic_client1").unwrap();   // may coincide with other clients
///     
///     let hclient1 = liner_broker::ln_new_client(client1.as_ptr(),
///                                                topic_client1.as_ptr(),
///                                                localhost1.as_ptr(),
///                                                redis_path.as_ptr());
///     let has_client = liner_broker::ln_has_client(&hclient1);
///         panic!("hclient1 error create, check redis path");
///     }
///     if !liner_broker::ln_run(&mut hclient1, cb1){
///         panic!("hclient1 ln_run not run!");
///     }
/// 
///     let topic_other_clients = CString::new("topic_other_clients").unwrap();   // may coincide with other clients
///     let array = [0; MESS_SIZE];
///     if !liner_broker::ln_send_all(&mut hclient1, topic_other_clients.as_ptr(), array.as_ptr(), array.len(), true){
///         panic!("hclient1 ln_send_all error!");
///     }  
/// }
/// ```
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
/// # Examples
///
/// ```
/// use std::ffi::CString;
/// unsafe {
///     let redis_path = CString::new("redis://localhost/").unwrap();
///         
///     let client1 = CString::new("client1").unwrap();               // must be a unique name
///     let localhost1 = CString::new("localhost:2255").unwrap();
///     let topic_client1 = CString::new("topic_client1").unwrap();   // may coincide with other clients
///     
///     let hclient1 = liner_broker::ln_new_client(client1.as_ptr(),
///                                                topic_client1.as_ptr(),
///                                                localhost1.as_ptr(),
///                                                redis_path.as_ptr());
///     let has_client = liner_broker::ln_has_client(&hclient1);
///         panic!("hclient1 error create, check redis path");
///     }
///     if !liner_broker::ln_run(&mut hclient1, cb1){
///         panic!("hclient1 ln_run not run!");
///     }
/// 
///     let topic_subscr = CString::new("topic_subscr").unwrap();
///     if !liner_broker::ln_subscribe(&mut hclient1, topic_subscr.as_ptr()){
///         panic!("hclient1 ln_subscribe error!");
///     }  
/// }
/// ```
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
/// # Examples
///
/// ```
/// use std::ffi::CString;
/// unsafe {
///     let redis_path = CString::new("redis://localhost/").unwrap();
///         
///     let client1 = CString::new("client1").unwrap();               // must be a unique name
///     let localhost1 = CString::new("localhost:2255").unwrap();
///     let topic_client1 = CString::new("topic_client1").unwrap();   // may coincide with other clients
///     
///     let hclient1 = liner_broker::ln_new_client(client1.as_ptr(),
///                                                topic_client1.as_ptr(),
///                                                localhost1.as_ptr(),
///                                                redis_path.as_ptr());
///     let has_client = liner_broker::ln_has_client(&hclient1);
///         panic!("hclient1 error create, check redis path");
///     }
///     if !liner_broker::ln_run(&mut hclient1, cb1){
///         panic!("hclient1 ln_run not run!");
///     }
/// 
///     let topic_subscr = CString::new("topic_subscr").unwrap();
///     if !liner_broker::ln_unsubscribe(&mut hclient1, topic_subscr.as_ptr()){
///         panic!("hclient1 ln_unsubscribe error!");
///     }  
/// }
/// ```
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