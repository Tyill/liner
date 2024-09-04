use std::os::raw::c_void;
use std::thread;
use crate::redis;


pub struct Message{
    data: Vec<u8>,
}


