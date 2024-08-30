mod topic;

#[no_mangle]
pub extern "C" fn create_topic(name: &str, addr: &str)-> Box<Option<topic::Topic>>{
    let t = topic::Topic::new(name, addr);
    Box::new(t)
}  

