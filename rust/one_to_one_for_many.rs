use std::time::SystemTime;
use std::ffi::CString;
//use std::ffi::CStr;
use std::{thread, time};


const MESS_SEND_COUNT: usize = 10;
const MESS_SIZE: usize = 100;
const SEND_CYCLE_COUNT: usize = 10;

static mut RECEIVE_COUNT_1: u64 = 0;
static mut RECEIVE_COUNT_2: u64 = 0;
static mut RECEIVE_COUNT_3: u64 = 0;
static mut SEND_BEGIN: u64 = 0;
static mut SEND_END: u64 = 0;

extern "C" fn cb1(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize, _udata: *const libc::c_void){
    unsafe {    
        RECEIVE_COUNT_1 += 1;
    }
}

extern "C" fn cb2(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize, _udata: *const libc::c_void){
    unsafe {    
        RECEIVE_COUNT_2 += 1;
    }
}

extern "C" fn cb3(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize, _udata: *const libc::c_void){
    unsafe {    
        RECEIVE_COUNT_3 += 1;
    }
}

extern "C" fn cb_server(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize, _udata: *const libc::c_void){
    
}

fn  main() {
    unsafe {
        let client1 = CString::new("client1").unwrap();
        let client2 = CString::new("client2").unwrap();
        let client3 = CString::new("client3").unwrap();
        let server1 = CString::new("server1").unwrap();
        let dbpath = CString::new("redis://localhost/").unwrap();
        let localhost1 = CString::new("localhost:2255").unwrap();
        let localhost2 = CString::new("localhost:2256").unwrap();
        let localhost3 = CString::new("localhost:2257").unwrap();
        let localhost4 = CString::new("localhost:2258").unwrap();
       
        let topic_client = CString::new("topic_client").unwrap();
        let mut hclient1 = liner_broker::ln_new_client(client1.as_ptr(),
                                                               topic_client.as_ptr(),
                                                               localhost1.as_ptr(),
                                                               dbpath.as_ptr());
        
        let mut hclient2 = liner_broker::ln_new_client(client2.as_ptr(), 
                                                               topic_client.as_ptr(),
                                                               localhost2.as_ptr(),
                                                               dbpath.as_ptr());
    
        let mut hclient3 = liner_broker::ln_new_client(client3.as_ptr(), 
                                                               topic_client.as_ptr(),
                                                               localhost3.as_ptr(),
                                                               dbpath.as_ptr());
    
        let topic_server1 = CString::new("topic_server1").unwrap();
        let mut hserver1 = liner_broker::ln_new_client(server1.as_ptr(), 
                                                               topic_server1.as_ptr(),
                                                               localhost4.as_ptr(),
                                                               dbpath.as_ptr());
    
        liner_broker::ln_clear_stored_messages(&mut hserver1);
        liner_broker::ln_clear_stored_messages(&mut hclient1);
        liner_broker::ln_clear_stored_messages(&mut hclient2);
        liner_broker::ln_clear_stored_messages(&mut hclient3);
    
        liner_broker::ln_clear_addresses_of_topic(&mut hserver1);
        liner_broker::ln_clear_addresses_of_topic(&mut hclient1);
        liner_broker::ln_clear_addresses_of_topic(&mut hclient2);
        liner_broker::ln_clear_addresses_of_topic(&mut hclient3);
            
        liner_broker::ln_run(&mut hclient1, cb1, std::ptr::null_mut());
        liner_broker::ln_run(&mut hclient2, cb2, std::ptr::null_mut());
        liner_broker::ln_run(&mut hclient3, cb3, std::ptr::null_mut());
    
        liner_broker::ln_run(&mut hserver1, cb_server, std::ptr::null_mut());
    
    let array = [0; MESS_SIZE];
    for _ in 0..SEND_CYCLE_COUNT{
        SEND_BEGIN = current_time_ms();
        for _ in 0..MESS_SEND_COUNT{
            liner_broker::ln_send_to(&mut hserver1, topic_client.as_ptr(), array.as_ptr(), array.len(), true);
        }
        SEND_END = current_time_ms();
        println!("send_to {} ms", SEND_END - SEND_BEGIN);       
      
        thread::sleep(time::Duration::from_millis(1000));
    }
    println!("RECEIVE_COUNT_1 {}", RECEIVE_COUNT_1);       
    println!("RECEIVE_COUNT_2 {}", RECEIVE_COUNT_2);       
    println!("RECEIVE_COUNT_3 {}", RECEIVE_COUNT_3);       
      
}
}

fn current_time_ms()->u64{ 
    SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}
