use std::time::SystemTime;
use std::ffi::CString;
//use std::ffi::CStr;
use std::{thread, time};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        unsafe {
            let client1 = CString::new("client1").unwrap();
            let server1 = CString::new("server1").unwrap();
            let dbpath = CString::new("redis://localhost/").unwrap();
            let localhost1 = CString::new("localhost:2255").unwrap();
            let localhost2 = CString::new("localhost:2256").unwrap();
           
            let topic_1 = CString::new("topic_client1").unwrap();
            let mut hclient1 = liner_broker::ln_new_client(client1.as_ptr(),
                                                                   topic_1.as_ptr(),
                                                                   localhost1.as_ptr(),
                                                                   dbpath.as_ptr());
        
            let topic_server1 = CString::new("topic_server1").unwrap();
            let mut hserver1 = liner_broker::ln_new_client(server1.as_ptr(), 
                                                                topic_server1.as_ptr(),
                                                                localhost2.as_ptr(),
                                                                dbpath.as_ptr());
        
            liner_broker::ln_clear_stored_messages(&mut hserver1);
            liner_broker::ln_clear_stored_messages(&mut hclient1);
            
            liner_broker::ln_clear_addresses_of_topic(&mut hserver1);
            liner_broker::ln_clear_addresses_of_topic(&mut hclient1);
                        
            liner_broker::ln_run(&mut hclient1, cb1);
            liner_broker::ln_run(&mut hserver1, cb_server1);
        
            let array = [0; 100];
            for _ in 0..10{
                println!("{} begin send_to", current_time_ms());       
                for _ in 0..10{
                    //ln_send_to(&hserver1, "topic_client1", data, sizeof(data), TRUE);
                    //ln_send_to(&hserver1, "topic_client2", data, sizeof(data), TRUE);
                    //ln_send_to(&hserver1, "topic_client3", data, sizeof(data), TRUE);
                   
                    liner_broker::ln_send_to(&mut hserver1, topic_for_subscr.as_ptr(), array.as_ptr(), array.len(), true);
                }
                println!("{} end send_to", current_time_ms());       
              
                thread::sleep(time::Duration::from_millis(1000));
            }
        }

        assert_eq!(result, 4);
    }
}

extern "C" fn cb1(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
   // println!("cb1");
   // unsafe {
      //  let from = CStr::from_ptr(from).to_str().unwrap();
    
      //  print!("{}", from);
    //}
}

extern "C" fn cb2(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
   // println!("cb2");
    //   unsafe {
    //       let to = CStr::from_ptr(_to).to_str().unwrap();
    //       let from = CStr::from_ptr(_from).to_str().unwrap();
         
    //       print!("{}", from);
    //   }
}

extern "C" fn cb3(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
   // println!("cb3");
    //   unsafe {
    //       let to = CStr::from_ptr(_to).to_str().unwrap();
    //       let from = CStr::from_ptr(_from).to_str().unwrap();
         
    //       print!("{}", from);
    //   }
}

extern "C" fn cb_server1(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
    //   unsafe {
    //       let to = CStr::from_ptr(_to).to_str().unwrap();
    //       let from = CStr::from_ptr(_from).to_str().unwrap();
         
    //       print!("{}", from);
    //   }
}


fn current_time_ms()->u64{ 
    SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}
