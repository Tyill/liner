mod topic;

#[no_mangle]
pub extern "C" fn create_topic(name: &str, addr: &str)-> Box<topic::Topic>{
    
    Box::new(topic::Topic::new(name, addr))
}  
