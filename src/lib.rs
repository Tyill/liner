mod redis;
mod client;
mod message;
mod bytestream;
mod listener;
mod sender;
mod settings;
mod common;
use crate::client::Client;

use std::ffi::CStr;


#[no_mangle]
pub extern "C" fn init(unique_name: *const i8,
                       redis_path: *const i8,
                       )->Box<Option<Client>>{
    unsafe{
        let unique_name = CStr::from_ptr(unique_name).to_str().unwrap();
        let redis_path = CStr::from_ptr(redis_path).to_str().unwrap();
        
        Box::new(Client::new(unique_name, redis_path))
    }    
}

type UCback = extern "C" fn(to: *const i8, from: *const i8, uuid: *const i8, timestamp: u64, data: *const u8, dsize: usize);

#[no_mangle]
pub extern "C" fn run(client: &mut Box<Option<Client>>, 
                      topic: *const i8, 
                      localhost: *const i8,
                      receive_cb: UCback)->bool{
    unsafe{
        let topic = CStr::from_ptr(topic).to_str().unwrap();
        let localhost = CStr::from_ptr(localhost).to_str().unwrap();
            
        let c = client.as_mut();
        c.as_mut().unwrap().run(topic, localhost, receive_cb)
    }
}

#[no_mangle]
pub extern "C" fn send_to(client: &mut Box<Option<Client>>,
                          topic: *const i8,
                          uuid: *const i8,
                          data: *const u8, data_size: usize)->bool{
    unsafe {
        let topic = CStr::from_ptr(topic).to_str().unwrap();
        let uuid = CStr::from_ptr(uuid).to_str().unwrap();       
        let data = std::slice::from_raw_parts(data, data_size);

        let c = client.as_mut();
        c.as_mut().unwrap().send_to(topic, uuid, data)
    }    
}

#[no_mangle]
pub extern "C" fn set_internal_thread_pool_size(_client: &mut Box<Option<Client>>, _num: u32){
    
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