mod topic;
mod redis;
mod client;
use std::sync::{Mutex, OnceLock};
use crate::client::Client;

static mut CLIENT: OnceLock<Mutex<Client>> = OnceLock::new();
    
fn client()-> &'static Mutex<Client> {
    unsafe {
        CLIENT.get_mut().unwrap()
    }
}

#[no_mangle]
pub extern "C" fn init(topic_name: &str, localhost: &str, redis_path: &str)->bool{
    unsafe {
        if let Some(_) = CLIENT.get_mut(){
            assert!(false)
        }
    }
    match Client::new(topic_name, localhost, redis_path) {
        Some(c)=>{ 
            unsafe {
                CLIENT.get_or_init(|| Mutex::new(c));
            }
            return true;
        },
        None=>return false
    }
}  

#[no_mangle]
pub extern "C" fn send_to(topic_name: &str, data: *const::std::os::raw::c_uchar, data_size: usize)->bool{
    unsafe {
        let data = std::slice::from_raw_parts(data, data_size);
    true 
   //     return topic::Topic::send_to(topic_name, data);
    }    
}  

