use std::ffi::CString;
use std::ffi::CStr;


type UCback = Box<dyn FnMut(&str, &str, &[u8])>;

extern "C" fn _cb(to: *const i8, from: *const i8,  data: *const u8, dsize: usize, udata: *mut libc::c_void){
    unsafe {    
        if let Some(liner) = udata.cast::<Liner>().as_mut(){
            if let Some(ucback) = liner.ucback.as_mut(){
                let to = CStr::from_ptr(to).to_str().unwrap();
                let from = CStr::from_ptr(from).to_str().unwrap();           
                (ucback)(to, from, std::slice::from_raw_parts(data, dsize));
            }
        }
    }
}

pub struct Liner{
    hclient: Box<Option<liner_broker::Client>>,
    ucback: Option<UCback>,
}

impl Liner {
    pub fn new(unique_name: &str,
        topic: &str,
        localhost: &str,
        redis_path: &str)->Liner{
    unsafe{
        let unique = CString::new(unique_name).unwrap();
        let dbpath = CString::new(redis_path).unwrap();
        let localhost = CString::new(localhost).unwrap();
        let topic_client = CString::new(topic).unwrap();
        let hclient = liner_broker::ln_new_client(unique.as_ptr(),
                                                        topic_client.as_ptr(),
                                                        localhost.as_ptr(),
                                                        dbpath.as_ptr());
        if liner_broker::ln_has_client(&hclient){
            Self{hclient, ucback: None}
        }else{
            panic!("error create client");
        }
    }   
    }
    pub fn run(&mut self, ucback: UCback)->bool{        
        unsafe{
            self.ucback = Some(ucback);
            let ud = self as *const Self as *mut libc::c_void;
            liner_broker::ln_run(&mut self.hclient, _cb, ud)
        }
    }
    pub fn send_to(&mut self, topic: &str, data: &[u8])->bool{
        unsafe{
            let topic = CString::new(topic).unwrap();
            liner_broker::ln_send_to(&mut self.hclient, topic.as_ptr(), data.as_ptr(), data.len(), true)
        }
    }
    pub fn send_all(&mut self, topic: &str, data: &[u8])->bool{
        unsafe{
            let topic = CString::new(topic).unwrap();
            liner_broker::ln_send_all(&mut self.hclient, topic.as_ptr(), data.as_ptr(), data.len(), true)
        }
    }
    pub fn subscribe(&mut self, topic: &str)->bool{
        unsafe{
            let topic = CString::new(topic).unwrap();
            liner_broker::ln_subscribe(&mut self.hclient, topic.as_ptr())
        }
    }
    pub fn unsubscribe(&mut self, topic: &str)->bool{
        unsafe{
            let topic = CString::new(topic).unwrap();
            liner_broker::ln_unsubscribe(&mut self.hclient, topic.as_ptr())
        }
    }
    pub fn clear_stored_messages(&mut self)->bool{
        unsafe{
            liner_broker::ln_clear_stored_messages(&mut self.hclient)
        }
    }
    pub fn clear_addresses_of_topic(&mut self)->bool{
        unsafe{
            liner_broker::ln_clear_addresses_of_topic(&mut self.hclient)
        }
    }
}

impl Drop for Liner {
    fn drop(&mut self) {
        liner_broker::ln_delete_client(Box::new(self.hclient.take()));
    }
}

