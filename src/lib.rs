mod topic;
mod redis;
mod client;
mod message;
use crate::client::Client;

use std::ffi::CStr;

#[no_mangle]
pub extern "C" fn init(topic: *const u8, 
                       localhost: *const u8,
                       redis_path: *const u8,
                       cb: extern "C" fn(*const u8, usize))->Box<Option<Client>>{
    unsafe{
        let topic_ = String::from_raw_parts(topic as *mut u8, topic_length, 256);
        let localhost_ = String::from_raw_parts(localhost as *mut u8, localhost_length, 256);
        let redis_path_ = String::from_raw_parts(redis_path as *mut u8, redis_path_length, 256);
        
        Box::new(Client::new(&topic_, &localhost_, &redis_path_, cb))
    }
    
}  

#[no_mangle]
pub extern "C" fn send_to(client: &mut Box<Option<Client>>, topic_name: &str, data: *const::std::os::raw::c_uchar, data_size: usize)->bool{
    unsafe {
        let data = std::slice::from_raw_parts(data, data_size);
    true 
   //     return topic::Topic::send_to(topic_name, data);
    }    
}  

