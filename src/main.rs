use liner;
use std::ffi::CString;
use std::ffi::CStr;

extern "C" fn cb1(to: *const i8, from: *const i8, uuid: *const i8, timestamp: u64, data: *const u8, dsize: usize){
    unsafe {
        let from = CStr::from_ptr(from).to_str().unwrap();
    
        print!("{}", from);
    }
}

extern "C" fn cb2(to: *const i8, from: *const i8, uuid: *const i8, timestamp: u64, data: *const u8, dsize: usize){
    unsafe {
        let from = CStr::from_ptr(from).to_str().unwrap();
    
        print!("{}", from);
    }
}

fn main() {
    let mut c1 = liner::init(CString::new("1").unwrap().as_ptr(),
                                                   CString::new("redis://127.0.0.1/").unwrap().as_ptr());

    liner::run(&mut c1, CString::new("localhost:2255").unwrap().as_ptr(), cb1);

    let mut c2 = liner::init(CString::new("2").unwrap().as_ptr(),
                                                  CString::new("redis://127.0.0.1/").unwrap().as_ptr());

    liner::run(&mut c2, CString::new("localhost:2256").unwrap().as_ptr(), cb2);

    let mut array: [u8; 3] = [0; 3];
    array[0] = 1;
    array[1] = 2;
    array[2] = 3;
    liner::send_to(&mut c1, CString::new("2").unwrap().as_ptr(),
    CString::new("1234").unwrap().as_ptr(), array.as_ptr(), array.len());


    loop {
        use std::{thread, time};

        let ten_millis = time::Duration::from_millis(1000);
        
        thread::sleep(ten_millis);
    }
}
