use std::time::SystemTime;
use std::{thread, time};

mod liner;
use liner::Liner;

const MESS_SEND_COUNT: usize = 1000;
const MESS_SIZE: usize = 100;
const SEND_CYCLE_COUNT: usize = 10;

static mut RECEIVE_COUNT: i32 = 0;
static mut SEND_BEGIN: u64 = 0;
static mut SEND_END: u64 = 0;

fn cb1(_to: &str, _from: &str,  _data: &[u8]){
    println!("receive_from {}", _from);
}

fn cb2(_to: &str, _from: &str,  _data: &[u8]){
    unsafe {
        RECEIVE_COUNT += 1;

        if RECEIVE_COUNT == MESS_SEND_COUNT as i32{
            RECEIVE_COUNT = 0;
            println!("receive_from {} ms", current_time_ms() - SEND_END);
        }
    }
}

fn  main() {

    let mut client1 = Liner::new("client1", "topic_client1", "localhost:2255", "redis://localhost/", cb1);
    let mut client2 = Liner::new("client2", "topic_client2", "localhost:2256", "redis://localhost/", cb2);
   
    client1.clear_stored_messages();
    client2.clear_stored_messages();

    client1.clear_addresses_of_topic();
    client2.clear_addresses_of_topic();

    client1.run();
    client2.run();

    unsafe{
        let array = [0; MESS_SIZE];
        for _ in 0..SEND_CYCLE_COUNT{
            SEND_BEGIN = current_time_ms();
            for _ in 0..MESS_SEND_COUNT{
                client1.send_to("topic_client2", array.as_slice());
            }
            SEND_END = current_time_ms();
            println!("send_to {} ms", SEND_END - SEND_BEGIN);       
        
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
