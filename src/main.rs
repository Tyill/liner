use liner;
use std::time::SystemTime;
use std::ffi::CString;
use std::{thread, time};
       

extern "C" fn cb1(_to: *const i8, _from: *const i8, _uuid: *const i8, _timestamp: u64, _data: *const u8, _dsize: usize){
   // unsafe {
      //  let from = CStr::from_ptr(from).to_str().unwrap();
    
      //  print!("{}", from);
    //}
}

extern "C" fn cb2(_to: *const i8, _from: *const i8, _uuid: *const i8, _timestamp: u64, _data: *const u8, _dsize: usize){
  //  unsafe {
      //  let from = CStr::from_ptr(from).to_str().unwrap();
    
      //  print!("{}", from);
  //  }
}

fn main() {
    let unique1 = CString::new("unique1").unwrap();
    let dbpath = CString::new("redis://127.0.0.1/").unwrap();
    let mut c1 = liner::new_client(unique1.as_ptr(), dbpath.as_ptr());

    let topic_1 = CString::new("1").unwrap();
    let localhost = CString::new("localhost:2255").unwrap();
    liner::run(&mut c1, topic_1.as_ptr(), localhost.as_ptr(), cb1);

     let unique2 = CString::new("unique2").unwrap();
     let dbpath = CString::new("redis://127.0.0.1/").unwrap();
     let mut c2 = liner::new_client(unique2.as_ptr(), dbpath.as_ptr());

    let topic_2 = CString::new("2").unwrap();
    let localhost = CString::new("localhost:2256").unwrap();
    liner::run(&mut c2, topic_2.as_ptr(), localhost.as_ptr(), cb2);

    let array = vec![0; 100];
    let uuid = CString::new("1234").unwrap();
    ////loop {
        println!("{} begin send_to", current_time_ms());       
        for _ in 0..100{
            liner::send_to(&mut c1, 
                topic_2.as_ptr(),
                uuid.as_ptr(),
                array.as_ptr(), array.len(), true);
        }
        println!("{} end send_to", current_time_ms());       
      
        thread::sleep(time::Duration::from_millis(100000));
   // }
}

fn current_time_ms()->u64{ 
    SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}
