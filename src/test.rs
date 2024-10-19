use std::time::SystemTime;
use std::ffi::{CString, CStr};
use std::{thread, time};

//extern crate liner;  

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
    let unique1 = CString::new("client1").unwrap();
    let unique2 = CString::new("client2").unwrap();
    let dbpath = CString::new("redis://127.0.0.1/").unwrap();
    let localhost1 = CString::new("localhost:2255").unwrap();
    let localhost2 = CString::new("localhost:2256").unwrap();
   
    let topic_1 = CString::new("topic_client1").unwrap();
    let mut c1 = liner::ln_new_client(unique1.as_ptr(),
                                                           topic_1.as_ptr(),
                                                           localhost1.as_ptr(),
                                                           dbpath.as_ptr());
    
    let topic_2 = CString::new("topic_client2").unwrap();
    let mut c2 = liner::ln_new_client(unique2.as_ptr(), 
                                                           topic_2.as_ptr(),
                                                           localhost2.as_ptr(),
                                                           dbpath.as_ptr());
    liner::ln_run(&mut c1, cb1);
    liner::ln_run(&mut c2, cb2);
    let array = [0; 100];
    for _ in 0..10{
        println!("{} begin send_to", current_time_ms());       
        for _ in 0..1{
            liner::ln_send_to(&mut c1,   
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
