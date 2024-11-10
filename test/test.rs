use std::time::SystemTime;
use std::{thread, time};
use std::sync::{ Arc, Mutex};

use liner_broker::Liner;

fn  main() {

    let mut client1 = Liner::new("client1", "topic_client1", "localhost:2255", "redis://localhost/");
    let mut client2 = Liner::new("client2", "topic_client2", "localhost:2256", "redis://localhost/");
   
    client1.clear_stored_messages();
    client2.clear_stored_messages();

    client1.clear_addresses_of_topic();
    client2.clear_addresses_of_topic();

    const MESS_SEND_COUNT: usize = 1000;
    const MESS_SIZE: usize = 100;
    const SEND_CYCLE_COUNT: usize = 10;

    let mut receive_count: i32 = 0;
    let send_end = Arc::new(Mutex::new(0));
    let _send_end = send_end.clone();

    client1.run(Box::new(|_to: &str, _from: &str,  _data: &[u8]|{
        println!("receive_from {}", _from);
    }));
    client2.run(Box::new(move |_to: &str, _from: &str,  _data: &[u8]|{
        receive_count += 1;    
        if receive_count == MESS_SEND_COUNT as i32{
            receive_count = 0;
            println!("receive_from {} ms", current_time_ms() - *_send_end.lock().unwrap());
        }
    }));

    let array = [0; MESS_SIZE];
    for _ in 0..SEND_CYCLE_COUNT{
        let send_begin = current_time_ms();
        for _ in 0..MESS_SEND_COUNT{
            client1.send_to("topic_client2", array.as_slice());
        }
        let send = current_time_ms();
        println!("send_to {} ms", send - send_begin);  
        *send_end.lock().unwrap() = send;     
    
        thread::sleep(time::Duration::from_millis(1000));
    }
}

fn current_time_ms()->u64{ 
    SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}
