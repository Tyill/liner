mod topic;
mod redis;
mod client;
mod message;
mod bytestream;
use crate::client::Client;

use std::ffi::CStr;


#[no_mangle]
pub extern "C" fn init(topic: *const i8, 
                       redis_path: *const i8,
                       )->Box<Option<Client>>{
    unsafe{
        let topic_ = CStr::from_ptr(topic).to_str().unwrap();
        let redis_path_ = CStr::from_ptr(redis_path).to_str().unwrap();
        
        Box::new(Client::new(topic_, redis_path_))
    }    
}

type UCback = extern "C" fn(to: *const i8, from: *const i8, uuid: *const i8, timestamp: u64, data: *const u8, dsize: usize);

#[no_mangle]
pub extern "C" fn run(client: &mut Box<Option<Client>>, 
                      localhost: *const i8,
                      receive_cb: UCback)->bool{
    unsafe{
        let localhost_ = CStr::from_ptr(localhost).to_str().unwrap();
            
        let c = client.as_mut();
        c.as_mut().unwrap().run(localhost_, receive_cb)
    }
}


#[no_mangle]
pub extern "C" fn send_to(client: &mut Box<Option<Client>>,
                          to: *const i8, // topic_name
                          uuid: *const i8,
                          data: *const u8, data_size: usize)->bool{
    unsafe {
        let to_ = CStr::from_ptr(to).to_str().unwrap();
        let uuid_ = CStr::from_ptr(uuid).to_str().unwrap();       
        let data = std::slice::from_raw_parts(data, data_size);

        let c = client.as_mut();
        c.as_mut().unwrap().send_to(to_, uuid_, data)
    }    
}  

