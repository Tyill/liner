mod topic;
mod redis;
use std::sync::{Mutex, OnceLock};

struct Config{
    redis_path: String
}
impl Default for Config {
    fn default() -> Config {
        Config {
            redis_path: String::from("redis://127.0.0.1/"),
        }
    }
}
fn config() -> &'static Mutex<Config> {
    static CONFIG: OnceLock<Mutex<Config>> = OnceLock::new();
    CONFIG.get_or_init(|| Mutex::new(Config::default()))
}

#[no_mangle]
pub extern "C" fn init(redis_path: &str){    
    config().lock().unwrap().redis_path = String::from(redis_path);
}  

#[no_mangle]
pub extern "C" fn create_topic(name: &str, addr: &str)-> Box<Option<topic::Topic>>{
    let t = topic::Topic::new(name, addr);
    Box::new(t)
}  

#[no_mangle]
pub extern "C" fn send_to(topic_name: &str, data: *const::std::os::raw::c_uchar, data_size: usize)->bool{
    unsafe {
        let data = std::slice::from_raw_parts(data, data_size);

        return topic::Topic::send_to(topic_name, data);
    }    
}  

