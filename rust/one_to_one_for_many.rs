use std::time::SystemTime;
use std::{thread, time};
use std::sync::{ Arc, Mutex};

use liner_broker::Liner;

fn  main() {

    let mut client1 = Liner::new("client1", "topic_client", "localhost:2255", "redis://localhost/");
    let mut client2 = Liner::new("client2", "topic_client", "localhost:2256", "redis://localhost/");
    let mut client3 = Liner::new("client3", "topic_client", "localhost:2257", "redis://localhost/");
    let mut server1 = Liner::new("server1", "topic_server1", "localhost:2258", "redis://localhost/");
    
    server1.clear_stored_messages();
    client1.clear_stored_messages();
    client2.clear_stored_messages();
    client3.clear_stored_messages();

    server1.clear_addresses_of_topic();
    client1.clear_addresses_of_topic();
    client2.clear_addresses_of_topic();
    client3.clear_addresses_of_topic();


    const MESS_SEND_COUNT: usize = 10;
    const MESS_SIZE: usize = 100;
    const SEND_CYCLE_COUNT: usize = 10;

    let receive_count_1 = Arc::new(Mutex::new(0));
    let receive_count_2 = Arc::new(Mutex::new(0));
    let receive_count_3 = Arc::new(Mutex::new(0));

    let _receive_count_1 = receive_count_1.clone();
    let _receive_count_2 = receive_count_2.clone();
    let _receive_count_3 = receive_count_3.clone();
    
    client1.run(Box::new(move |_to: &str, _from: &str,  _data: &[u8]|{
        *_receive_count_1.lock().unwrap() += 1;
    }));
    client2.run(Box::new(move |_to: &str, _from: &str,  _data: &[u8]|{
        *_receive_count_2.lock().unwrap() += 1;
    }));
    client3.run(Box::new(move |_to: &str, _from: &str,  _data: &[u8]|{
        *_receive_count_3.lock().unwrap() += 1;
    }));
    server1.run(Box::new(move |_to: &str, _from: &str,  _data: &[u8]|{}));
    
    let array = [0; MESS_SIZE];
    for _ in 0..SEND_CYCLE_COUNT{
        let send_begin = current_time_ms();
        for _ in 0..MESS_SEND_COUNT{
            server1.send_to("topic_client", array.as_slice());
        }
        let send_end = current_time_ms();
        println!("send_to {} ms", send_end - send_begin);       
    
        thread::sleep(time::Duration::from_millis(1000));
    }
    println!("receive_count_1 {}", receive_count_1.lock().unwrap());       
    println!("receive_count_2 {}", receive_count_2.lock().unwrap());       
    println!("receive_count_3 {}", receive_count_3.lock().unwrap());      
}

fn current_time_ms()->u64{ 
    SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}
