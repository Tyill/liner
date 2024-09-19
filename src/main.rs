use liner;
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
    let mut c1 = liner::init(unique1.as_ptr(), dbpath.as_ptr());

    let topic_1 = CString::new("1").unwrap();
    let localhost = CString::new("localhost:2255").unwrap();
    liner::run(&mut c1, topic_1.as_ptr(), localhost.as_ptr(), cb1);

    let unique2 = CString::new("unique2").unwrap();
    let dbpath = CString::new("redis://127.0.0.1/").unwrap();
    let mut c2 = liner::init(unique2.as_ptr(), dbpath.as_ptr());

    let topic_2 = CString::new("2").unwrap();
    let localhost = CString::new("localhost:2256").unwrap();
    liner::run(&mut c2, topic_2.as_ptr(), localhost.as_ptr(), cb2);

    let mut array: [u8; 3] = [0; 3];
    array[0] = 1;
    array[1] = 2;
    array[2] = 3;

    let uuid = CString::new("1234").unwrap();
   
    loop {        
        for _ in 0..100{
            liner::send_to(&mut c1, 
                topic_2.as_ptr(),
                uuid.as_ptr(),
                array.as_ptr(), array.len());
        }

        thread::sleep(time::Duration::from_millis(1000));
    }
}


// use std::sync::{Arc, Mutex};

// fn main() {
//     let num_of_threads = 4;
//     let mut array_of_threads = vec![];
//     let counter = Arc::new(Mutex::new(false));

//     for id in 0..num_of_threads {
//         let counter_clone = counter.clone();
//         array_of_threads.push(std::thread::spawn(move || print_lots(id, counter_clone)));
//     }

//     for t in array_of_threads {
//         t.join().expect("Thread join failure");
//     }
// }

// fn print_lots(id: u32, c: Arc<Mutex<bool>>) {
//     println!("Begin [{}]", id);
//     let _guard = c.lock().unwrap();
//     for _i in 0..1000 {
//         print!("{} ", id);
//     }
//     println!("\nEnd [{}]", id);
// }