use std::time::SystemTime;
use std::{thread, time};

mod liner;
use liner::Liner;

const MESS_SEND_COUNT: usize = 10;
const MESS_SIZE: usize = 100;
const SEND_CYCLE_COUNT: usize = 10;

static mut RECEIVE_COUNT_1: u64 = 0;
static mut RECEIVE_COUNT_2: u64 = 0;
static mut RECEIVE_COUNT_3: u64 = 0;
static mut SEND_BEGIN: u64 = 0;
static mut SEND_END: u64 = 0;

fn cb1(_to: &str, _from: &str,  _data: &[u8]){
    unsafe {    
        RECEIVE_COUNT_1 += 1;
    }
}

fn cb2(_to: &str, _from: &str,  _data: &[u8]){    
    unsafe {    
        RECEIVE_COUNT_2 += 1;
    }
}

fn cb3(_to: &str, _from: &str,  _data: &[u8]){    
    unsafe {    
        RECEIVE_COUNT_3 += 1;
    }
}

fn cb_server(_to: &str, _from: &str,  _data: &[u8]){    
    
}

fn  main() {

    let mut client1 = Liner::new("client1", "topic_client", "localhost:2255", "redis://localhost/", cb1);
    let mut client2 = Liner::new("client2", "topic_client", "localhost:2256", "redis://localhost/", cb2);
    let mut client3 = Liner::new("client3", "topic_client", "localhost:2257", "redis://localhost/", cb3);
    let mut server1 = Liner::new("server1", "topic_server1", "localhost:2258", "redis://localhost/", cb_server);
    
    server1.clear_stored_messages();
    client1.clear_stored_messages();
    client2.clear_stored_messages();
    client3.clear_stored_messages();

    server1.clear_addresses_of_topic();
    client1.clear_addresses_of_topic();
    client2.clear_addresses_of_topic();
    client3.clear_addresses_of_topic();

    client1.run();
    client2.run();
    client3.run();
    server1.run();
    
    unsafe{
        let array = [0; MESS_SIZE];
        for _ in 0..SEND_CYCLE_COUNT{
            SEND_BEGIN = current_time_ms();
            for _ in 0..MESS_SEND_COUNT{
                server1.send_to("topic_client", array.as_slice());
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
