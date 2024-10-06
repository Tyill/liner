use std::time::SystemTime;

pub fn current_time_ms()->u64{ 
    SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}

pub fn delta_time_ms(prev_ns: &i64)->i64{ 
    (clock_ns() - prev_ns)/1E6 as i64
}

pub fn clock_ns()->i64{
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let ret = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_COARSE, &mut time) };
    assert!(ret == 0);
    time.tv_nsec
}