use std::time::SystemTime;
use std::ffi::CString;
//use std::ffi::CStr;
use std::{thread, time};


type UCback = fn(to: &str, from: &str, data: &[u8]);

pub struct Liner{
    hclient: Box<Option<liner_broker::Client>>,
    ucback: UCback,
}

impl Liner {
    pub fn new(unique_name: &str,
        topic: &str,
        localhost: &str,
        redis_path: &str,
        ucback: UCback)->Liner{
    unsafe{
        let unique = CString::new(unique_name).unwrap();
        let dbpath = CString::new(redis_path).unwrap();
        let localhost = CString::new(localhost).unwrap();
        let topic_client = CString::new(topic).unwrap();
        let hclient = liner_broker::ln_new_client(unique.as_ptr(),
                                                        topic_client.as_ptr(),
                                                        localhost.as_ptr(),
                                                        dbpath.as_ptr());
        if liner_broker::ln_has_client(&hclient){
            Self{hclient, ucback}
        }else{
            panic!("error create client");
        }
    }   
    }
    pub fn run(&mut self)->bool{
        unsafe{
        let topic = CString::new(topic).unwrap();
        liner_broker::ln_send_to(&mut self.hclient, topic.as_ptr(), data.as_ptr(), data.len(), true)
        }
    }
    pub fn send_to(&mut self, topic: &str, data: &[u8])->bool{
        unsafe{
        let topic = CString::new(topic).unwrap();
        liner_broker::ln_send_to(&mut self.hclient, topic.as_ptr(), data.as_ptr(), data.len(), true)
        }
    }
    pub fn send_all(&mut self, topic: &str, data: &[u8])->bool{
        unsafe{
        let topic = CString::new(topic).unwrap();
        liner_broker::ln_send_all(&mut self.hclient, topic.as_ptr(), data.as_ptr(), data.len(), true)
        }
    }
    fn cb1(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
    
    }
}
}


extern "C" fn cb2(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
    unsafe {    
        receive_count_2 += 1;
    }
}

extern "C" fn cb3(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
    unsafe {    
        receive_count_3 += 1;
    }
}

extern "C" fn cb_server(_to: *const i8, _from: *const i8,  _data: *const u8, _dsize: usize){
    
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
            
        liner_broker::ln_run(&mut hclient1, cb1);
        liner_broker::ln_run(&mut hclient2, cb2);
        liner_broker::ln_run(&mut hclient3, cb3);
    
        liner_broker::ln_run(&mut hserver1, cb_server);
    
    let array = [0; MESS_SIZE];
    for _ in 0..SEND_CYCLE_COUNT{
        send_begin = current_time_ms();
        for _ in 0..MESS_SEND_COUNT{
            liner_broker::ln_send_all(&mut hserver1, topic_client.as_ptr(), array.as_ptr(), array.len(), true);
        }
        send_end = current_time_ms();
        println!("send_to {} ms", send_end - send_begin);       
      
        thread::sleep(time::Duration::from_millis(1000));
    }
    println!("receive_count_1 {}", receive_count_1);       
    println!("receive_count_2 {}", receive_count_2);       
    println!("receive_count_3 {}", receive_count_3);       
      
}
}

fn current_time_ms()->u64{ 
    SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}
