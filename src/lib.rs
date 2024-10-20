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

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_has_client(client: &mut Box<Option<Client>>)->bool{
    has_client(client)
}

type UCback = extern "C" fn(to: *const i8, from: *const i8, data: *const u8, dsize: usize);

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_run(client: &mut Box<Option<Client>>, receive_cb: UCback)->bool{
    if !has_client(client){
        return false;
    }    
    let c = client.as_mut();
    c.as_mut().unwrap().run(receive_cb)
}
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

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_clear_stored_messages(client: &mut Box<Option<Client>>)->bool{
    if !has_client(client){
        return false;
    }
    let c = client.as_mut();
    c.as_mut().unwrap().clear_stored_messages()
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn ln_clear_addresses_of_topic(client: &mut Box<Option<Client>>)->bool{
    if !has_client(client){
        return false;
    }
    let c = client.as_mut();
    c.as_mut().unwrap().clear_addresses_of_topic()
}

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