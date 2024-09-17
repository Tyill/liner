use liner;
use std::ffi::CString;
use std::ffi::CStr;

extern "C" fn cb1(_to: *const i8, from: *const i8, _uuid: *const i8, _timestamp: u64, _data: *const u8, _dsize: usize){
    unsafe {
        let from = CStr::from_ptr(from).to_str().unwrap();
    
        print!("{}", from);
    }
}

extern "C" fn cb2(_to: *const i8, from: *const i8, _uuid: *const i8, _timestamp: u64, _data: *const u8, _dsize: usize){
    unsafe {
        let from = CStr::from_ptr(from).to_str().unwrap();
    
        print!("{}", from);
    }
}

fn main() {
    let topic_1 = CString::new("1").unwrap();
    let dbpath = CString::new("redis://127.0.0.1/").unwrap();
    let mut c1 = liner::init(topic_1.as_ptr(), dbpath.as_ptr());

    let localhost = CString::new("localhost:2255").unwrap();
    liner::run(&mut c1, localhost.as_ptr(), cb1);

    let topic_2 = CString::new("2").unwrap();
    let dbpath = CString::new("redis://127.0.0.1/").unwrap();
    let mut c2 = liner::init(topic_2.as_ptr(), dbpath.as_ptr());

    let localhost = CString::new("localhost:2256").unwrap();
    liner::run(&mut c2, localhost.as_ptr(), cb2);

    let mut array: [u8; 3] = [0; 3];
    array[0] = 1;
    array[1] = 2;
    array[2] = 3;

    let uuid = CString::new("1234").unwrap();
   
    liner::send_to(&mut c1, 
                   topic_2.as_ptr(),
                   uuid.as_ptr(),
                   array.as_ptr(), array.len());

    loop {
        use std::{thread, time};

        let ten_millis = time::Duration::from_millis(1000);
        
        thread::sleep(ten_millis);
    }
}
