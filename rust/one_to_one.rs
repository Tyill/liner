use std::time::SystemTime;
use std::ffi::CString;
use std::ffi::CStr;
use std::{thread, time};


const MESS_SEND_COUNT: usize = 10000;
const MESS_SIZE: usize = 100;
const SEND_CYCLE_COUNT: usize = 10;

static mut receive_count: i32 = 0;
static mut send_begin: u64 = 0;
static mut send_end: u64 = 0;

extern "C" fn cb1(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
    unsafe {
        let from = CStr::from_ptr(_from);
        println!("receive_from {}", from.to_str().unwrap());
    }
}

extern "C" fn cb2(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
    unsafe {
        receive_count += 1;

        if receive_count == MESS_SEND_COUNT as i32{
            receive_count = 0;
            println!("receive_from {} ms", current_time_ms() - send_end);
        }
    }
}

fn  main() {
    unsafe {
    let client1 = CString::new("client1").unwrap();
    let client2 = CString::new("client2").unwrap();
    let dbpath = CString::new("redis://localhost/").unwrap();
    let localhost1 = CString::new("localhost:2255").unwrap();
    let localhost2 = CString::new("localhost:2256").unwrap();
   
    let topic_1 = CString::new("topic_client1").unwrap();
    let mut hclient1 = liner_broker::ln_new_client(client1.as_ptr(),
                                                           topic_1.as_ptr(),
                                                           localhost1.as_ptr(),
                                                           dbpath.as_ptr());
    
    let topic_2 = CString::new("topic_client2").unwrap();
    let mut hclient2 = liner_broker::ln_new_client(client2.as_ptr(), 
                                                           topic_2.as_ptr(),
                                                           localhost2.as_ptr(),
                                                           dbpath.as_ptr());

    liner_broker::ln_clear_stored_messages(&mut hclient1);
    liner_broker::ln_clear_stored_messages(&mut hclient2);

    liner_broker::ln_clear_addresses_of_topic(&mut hclient1);
    liner_broker::ln_clear_addresses_of_topic(&mut hclient2);

    liner_broker::ln_run(&mut hclient1, cb1);
    liner_broker::ln_run(&mut hclient2, cb2);

    let array = [0; MESS_SIZE];
    for _ in 0..SEND_CYCLE_COUNT{
        send_begin = current_time_ms();
        for _ in 0..MESS_SEND_COUNT{
            liner_broker::ln_send_to(&mut hclient1, topic_2.as_ptr(), array.as_ptr(), array.len(), true);
        }
        send_end = current_time_ms();
        println!("send_to {} ms", send_end - send_begin);       
      
        thread::sleep(time::Duration::from_millis(1000));
    }
}
}

fn current_time_ms()->u64{ 
    SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}
