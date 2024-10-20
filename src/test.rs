use std::time::SystemTime;
use std::ffi::{CString, CStr};
use std::{thread, time};

//extern crate liner;  

extern "C" fn cb1(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
    println!("cb1");
   // unsafe {
      //  let from = CStr::from_ptr(from).to_str().unwrap();
    
      //  print!("{}", from);
    //}
}

extern "C" fn cb2(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
    println!("cb2");
    //   unsafe {
    //       let to = CStr::from_ptr(_to).to_str().unwrap();
    //       let from = CStr::from_ptr(_from).to_str().unwrap();
         
    //       print!("{}", from);
    //   }
}

extern "C" fn cb3(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
    println!("cb3");
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

fn  main() {
    unsafe {
    let client1 = CString::new("client1").unwrap();
    let client2 = CString::new("client2").unwrap();
    let client3 = CString::new("client3").unwrap();
    let server1 = CString::new("server1").unwrap();
    let dbpath = CString::new("redis://127.0.0.1/").unwrap();
    let localhost1 = CString::new("localhost:2255").unwrap();
    let localhost2 = CString::new("localhost:2256").unwrap();
    let localhost3 = CString::new("localhost:2257").unwrap();
    let localhost4 = CString::new("localhost:2258").unwrap();
   
    let topic_1 = CString::new("topic_client1").unwrap();
    let mut hclient1 = liner::ln_new_client(client1.as_ptr(),
                                                           topic_1.as_ptr(),
                                                           localhost1.as_ptr(),
                                                           dbpath.as_ptr());
    
    let topic_2 = CString::new("topic_client2").unwrap();
    let mut hclient2 = liner::ln_new_client(client2.as_ptr(), 
                                                           topic_2.as_ptr(),
                                                           localhost2.as_ptr(),
                                                           dbpath.as_ptr());

    let topic_3 = CString::new("topic_client3").unwrap();
    let mut hclient3 = liner::ln_new_client(client3.as_ptr(), 
                                                        topic_3.as_ptr(),
                                                        localhost3.as_ptr(),
                                                        dbpath.as_ptr());

    let topic_server1 = CString::new("topic_server1").unwrap();
    let mut hserver1 = liner::ln_new_client(server1.as_ptr(), 
                                                        topic_server1.as_ptr(),
                                                        localhost4.as_ptr(),
                                                        dbpath.as_ptr());

    liner::ln_clear_stored_messages(&mut hserver1);
    liner::ln_clear_stored_messages(&mut hclient1);
    liner::ln_clear_stored_messages(&mut hclient2);
    liner::ln_clear_stored_messages(&mut hclient3);

    liner::ln_clear_addresses_of_topic(&mut hserver1);
    liner::ln_clear_addresses_of_topic(&mut hclient1);
    liner::ln_clear_addresses_of_topic(&mut hclient2);
    liner::ln_clear_addresses_of_topic(&mut hclient3);

    let topic_for_subscr = CString::new("topic_for_subscr").unwrap();
    liner::ln_subscribe(&mut hclient1, topic_for_subscr.as_ptr());
    liner::ln_subscribe(&mut hclient2, topic_for_subscr.as_ptr());
    liner::ln_subscribe(&mut hclient3, topic_for_subscr.as_ptr());

    liner::ln_run(&mut hclient1, cb1);
    liner::ln_run(&mut hclient2, cb2);
    liner::ln_run(&mut hclient3, cb3);

    liner::ln_run(&mut hserver1, cb_server1);

    let array = [0; 100];
    for _ in 0..10{
        println!("{} begin send_to", current_time_ms());       
        for _ in 0..10{
            //ln_send_to(&hserver1, "topic_client1", data, sizeof(data), TRUE);
            //ln_send_to(&hserver1, "topic_client2", data, sizeof(data), TRUE);
            //ln_send_to(&hserver1, "topic_client3", data, sizeof(data), TRUE);
           
            liner::ln_send_to(&mut hserver1, topic_for_subscr.as_ptr(), array.as_ptr(), array.len(), true);
        }
        println!("{} end send_to", current_time_ms());       
      
        thread::sleep(time::Duration::from_millis(1000));
    }

//     auto hclient1 = ln_new_client("client1", "topic_client1", "localhost:2255", "redis://127.0.0.1/");
//     auto hclient2 = ln_new_client("client2", "topic_client2", "localhost:2256", "redis://127.0.0.1/");
//     auto hclient3 = ln_new_client("client3", "topic_client3", "localhost:2257", "redis://127.0.0.1/");
//     auto hserver1 = ln_new_client("server1", "topic_server1", "localhost:2258", "redis://127.0.0.1/");
    
//     ln_clear_addresses_of_topic(&hclient1);
//     ln_clear_addresses_of_topic(&hclient2);
//     ln_clear_addresses_of_topic(&hclient3);
    
//     ln_clear_addresses_of_topic(&hserver1);
//     ln_clear_stored_messages(&hserver1);

//     ln_subscribe(&hclient1, "topic_for_subscr");
//     ln_subscribe(&hclient2, "topic_for_subscr");
//     ln_subscribe(&hclient3, "topic_for_subscr");

//     ln_run(&hclient1, cb_client1);
//     ln_run(&hclient2, cb_client2);
//     ln_run(&hclient3, cb_client3);

//     ln_run(&hserver1, cb_server);
  
//     char data[MESS_SIZE];
//     for (int i = 0; i < SEND_CYCLE_COUNT; ++i){
//         send_begin = clock();
//         for (int j = 0; j < MESS_SEND_COUNT; ++j){
//             //ln_send_to(&hserver1, "topic_client1", data, sizeof(data), TRUE);
//             // ln_send_to(&hserver1, "topic_client2", data, sizeof(data), TRUE);
//             // ln_send_to(&hserver1, "topic_client3", data, sizeof(data), TRUE);
//             ln_send_to(&hserver1, "topic_for_subscr", data, sizeof(data), TRUE);
//         }
//         send_end = clock();
//         std::cout << "send_to " << 1000.0 * (send_end - send_begin) / CLOCKS_PER_SEC << " ms" << std::endl;
//         std::this_thread::sleep_for(std::chrono::milliseconds(1000));
//     }
//     ln_delete_client(hclient1);
//     ln_delete_client(hclient2);
//     ln_delete_client(hclient3);
    
//     ln_delete_client(hserver1);

//     std::cout << "client1 receive_count_1 " << receive_count_1 << " receive_count_subscr_1 " << receive_count_subscr_1 << std::endl;
//     std::cout << "client2 receive_count_2 " << receive_count_2 << " receive_count_subscr_2 " << receive_count_subscr_2 << std::endl;
//     std::cout << "client3 receive_count_3 " << receive_count_3 << " receive_count_subscr_3 " << receive_count_subscr_3 << std::endl;
// }
}
}

fn current_time_ms()->u64{ 
    SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}
