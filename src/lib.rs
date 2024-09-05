mod topic;
mod redis;
mod client;
mod message;
use crate::client::Client;

use std::ffi::CStr;

#[no_mangle]
pub extern "C" fn init(topic: *const i8, 
                       localhost: *const i8,
                       redis_path: *const i8,
                       cb: extern "C" fn(*const u8, usize))->Box<Option<Client>>{
    unsafe{
        let topic_ = CStr::from_ptr(topic).to_str().unwrap();
        let localhost_ = CStr::from_ptr(localhost).to_str().unwrap();
        let redis_path_ = CStr::from_ptr(redis_path).to_str().unwrap();
        
        Box::new(Client::new(topic_, localhost_, redis_path_, cb))
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

