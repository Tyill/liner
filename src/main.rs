use liner;

extern "C" fn cb1(to: *const i8, from: *const i8, uuid: *const i8, timestamp: u64, data: *const u8, dsize: usize){

}

extern "C" fn cb2(to: *const i8, from: *const i8, uuid: *const i8, timestamp: u64, data: *const u8, dsize: usize){

}

fn main() {
    let mut c1 = liner::init("1".as_ptr() as *const i8,
                                             "redis://127.0.0.1/".as_ptr()as *const i8);

    liner::run(&mut c1, "localhost:4455".as_ptr() as *const i8, cb1);

    let mut c2 = liner::init("2".as_ptr() as *const i8,
                                              "redis://127.0.0.1/".as_ptr()as *const i8);

    liner::run(&mut c2, "localhost:4456".as_ptr() as *const i8, cb2);

    loop {
        use std::{thread, time};

        let ten_millis = time::Duration::from_millis(10);
        
        thread::sleep(ten_millis);
    }
}
