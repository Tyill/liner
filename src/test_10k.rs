use std::time::SystemTime;
use std::ffi::CString;
//use std::ffi::CStr;
use std::{thread, time};


extern "C" fn cb1(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
   // unsafe {
      //  let from = CStr::from_ptr(from).to_str().unwrap();
    
      //  print!("{}", from);
    //}
}

extern "C" fn cb2(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
    //   unsafe {
    //       let to = CStr::from_ptr(_to).to_str().unwrap();
    //       let from = CStr::from_ptr(_from).to_str().unwrap();
         
    //       print!("{}", from);
    //   }
}

fn  main() {
    unsafe {
    let client1 = CString::new("client1").unwrap();
    let client2 = CString::new("client2").unwrap();
    let localhost1 = CString::new("localhost:2255").unwrap();
    let localhost2 = CString::new("localhost:2256").unwrap();   
    let dbpath = CString::new("redis://127.0.0.1/").unwrap();

    let topic_1 = CString::new("topic_client1").unwrap();
    let mut hclient1 = liner_broker::ln_new_client(client1.as_ptr(), topic_1.as_ptr(), localhost1.as_ptr(), dbpath.as_ptr());
    
    let topic_2 = CString::new("topic_client2").unwrap();
    let mut hclient2 = liner_broker::ln_new_client(client2.as_ptr(), topic_2.as_ptr(), localhost2.as_ptr(), dbpath.as_ptr());
   
    liner_broker::ln_run(&mut hclient1, cb1);
    liner_broker::ln_run(&mut hclient2, cb2);
       
    let array = [0; 100];
    for _ in 0..10{
        println!("{} begin send_to", current_time_ms());       
        for _ in 0..10000{
            liner_broker::ln_send_to(&mut hclient1,   
                topic_2.as_ptr(),
                array.as_ptr(), array.len(), true);
        }
        println!("{} end send_to", current_time_ms());       
      
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
