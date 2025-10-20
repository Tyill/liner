use crate::mempool::Mempool;
use crate::message::Message;
use crate::redis;
use crate::{print_error, print_debug};
use crate::redis::Connect;
use crate::settings;
use crate::common;

use std::thread::JoinHandle;
use std::time::Duration;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::collections::HashMap;
use std::thread;
use std::io::{BufWriter, Write};
use std::net::TcpStream;

struct WriteStream{
    ix: usize,
    connection_key: i32,
    address: String,
    stream: Arc<Option<TcpStream>>,
    last_send_mess_number: u64,
    last_mess_number: u64,
    is_active: bool,
    has_close_request: bool,
    is_closed: bool,
}
impl WriteStream {
    pub fn new() -> WriteStream {          
        Self{
            ix: 0,
            connection_key: 0,
            address: "".to_string(),
            stream: Arc::new(None),
            last_send_mess_number: 0,
            last_mess_number: 0,
            is_active: false,
            has_close_request: true,
            is_closed: true,
        }
    }
}

#[derive(Clone)]
struct Address{
    ix: usize,
    connection_key: i32,
    address: String,
}

type MempoolList = Vec<Arc<Mutex<Mempool>>>;
type MempoolBuffList = Vec<Mempool>;
type MessList = Vec<Arc<Mutex<Option<Vec<Message>>>>>; 
type MessBuffList = Vec<Option<Vec<Message>>>; 
type WriteStreamList = Vec<Arc<Mutex<WriteStream>>>; 

pub struct Sender{
    addrs_for: HashMap<String, usize>, // key addr, value addr_index
    addrs_new: Arc<Mutex<Vec<Address>>>,
    message_buffer: Arc<Mutex<MessBuffList>>,
    messages: Arc<Mutex<MessList>>, 
    mempools: Arc<Mutex<MempoolList>>, 
    mempool_buffer: Arc<Mutex<MempoolBuffList>>, 
    last_mess_number: Vec<u64>,
    connection_key: Vec<i32>,
    is_new_addr: Arc<AtomicBool>,
    is_close: Arc<AtomicBool>,
    ctime: Arc<AtomicU64>,
    delay_write_cvar: Arc<(Mutex<bool>, Condvar)>,
    wdelay_thread: Option<JoinHandle<()>>,
}

impl Sender {
    pub fn new(unique_name: &str, redis_path: &str, source_topic: &str)->Sender{
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(Vec::new()));
        let messages_ = messages.clone();
        let message_buffer: Arc<Mutex<MessBuffList>> = Arc::new(Mutex::new(Vec::new()));
        let message_buffer_ = message_buffer.clone();
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(Vec::new()));
        let mempools_ = mempools.clone();
        let mempool_buffer: Arc<Mutex<MempoolBuffList>> = Arc::new(Mutex::new(Vec::new()));
        let mempool_buffer_ = mempool_buffer.clone();  
        let addrs_for: HashMap<String, usize> = HashMap::new();
        let addrs_new: Arc<Mutex<Vec<Address>>> = Arc::new(Mutex::new(Vec::new()));
        let mut addrs_new_ = addrs_new.clone();
        let db_conn = redis::Connect::new(unique_name, redis_path).expect("couldn't redis::Connect");
        let db = Arc::new(Mutex::new(db_conn));
        db.lock().unwrap().set_source_topic(source_topic);
        
        let delay_write_cvar = Arc::new((Mutex::new(false), Condvar::new()));
        let delay_write_cvar_ = delay_write_cvar.clone();
        let is_new_addr = Arc::new(AtomicBool::new(false));
        let is_new_addr_ = is_new_addr.clone();
        let is_close = Arc::new(AtomicBool::new(false));
        let is_close_ = is_close.clone();
        let ctime = Arc::new(AtomicU64::new(common::current_time_ms()));
        let ctime_ = ctime.clone();
        let wdelay_thread = thread::spawn(move||{
            let mut streams: WriteStreamList = Vec::new();
            let mut prev_time: [u64; 2] = [common::current_time_ms(); 2];
            let mut ppl_wait = false;
            while !is_close_.load(Ordering::Relaxed){ // write delay cycle
                let (lock, cvar) = &*delay_write_cvar_;
                let mut has_new_mess = false;
                let mut has_old_mess = false;
                if let Ok(mut _started) = lock.lock(){
                    has_new_mess = message_buffer_.lock().unwrap().iter().any(|m: &Option<Vec<Message>>| m.is_some());
                    has_old_mess = has_new_mess || messages_.lock().unwrap().iter().any(|m: &Arc<Mutex<Option<Vec<Message>>>>| m.lock().unwrap().is_some());
                    if !has_new_mess && !has_old_mess && !ppl_wait{
                        *_started = false;
                        _started = cvar.wait_timeout(_started, Duration::from_millis(settings::SENDER_THREAD_WAIT_TIMEOUT_MS)).unwrap().0;
                        has_new_mess = *_started || message_buffer_.lock().unwrap().iter().any(|m: &Option<Vec<Message>>| m.is_some());
                    }                    
                }
                if settings::SENDER_THREAD_WRITE_MESS_DELAY_MS > 0 && !has_new_mess && !has_old_mess{
                    std::thread::sleep(Duration::from_millis(settings::SENDER_THREAD_WRITE_MESS_DELAY_MS));
                    has_new_mess = message_buffer_.lock().unwrap().iter().any(|m: &Option<Vec<Message>>| m.is_some());
                }
                ppl_wait = has_new_mess;
                if has_new_mess || has_old_mess{
                    send_mess_to_listener(&streams, &messages_, &message_buffer_, &mempools_, &mempool_buffer_);
                }
                let ctime = common::current_time_ms();
                ctime_.store(ctime, Ordering::Relaxed);
                if check_available_stream(&is_new_addr_, ctime, &mut prev_time[0]) {
                    append_streams(&mut streams, &mut addrs_new_, &db, &messages_, &mempools_);
                }
                if timeout_update_last_mess_number(ctime, &mut prev_time[1]){
                    update_last_mess_number(&mut streams, &db, &messages_, &mempools_);
                }
                check_streams_close(&mut streams, &addrs_new_, &db, &messages_, &mempools_);
            }
            close_streams(&streams, &db, &messages_, &mempools_);
        });
        Self{
            addrs_for,
            addrs_new,
            message_buffer,
            messages,
            mempools,
            mempool_buffer,
            last_mess_number: Vec::new(),
            connection_key: Vec::new(),
            is_new_addr,
            is_close,
            ctime,
            delay_write_cvar,
            wdelay_thread: Some(wdelay_thread),
        }
    }
    
    pub fn send_to(&mut self, db: &mut redis::Connect, addr_to: &str, listener_topic: &str, 
                   data: &[u8], at_least_once_delivery: bool)->bool{
        let mut is_new_addr = false;
        if !self.addrs_for.contains_key(addr_to){
            if !self.append_new_state(db, addr_to, listener_topic){
                return false;
            }
            self.addrs_for.insert(addr_to.to_string(), self.addrs_for.len());
            is_new_addr = true;
        }
        let ix = self.addrs_for[addr_to];
        let connection_key = self.connection_key[ix];
        let listener_topic_key;
        match db.get_topic_key(listener_topic){
            Ok(key)=>{
                listener_topic_key = key;
            },
            Err(err)=>{
                print_error!(format!("couldn't db.get_topic_key {}, err {}", listener_topic, err));
                return false;
            }
        }
        let number_mess = self.last_mess_number[ix] + 1;
        *self.last_mess_number.get_mut(ix).unwrap() = number_mess;
       
        let mess = Message::new(self.mempool_buffer.lock().unwrap().get_mut(ix).unwrap(),
                                         connection_key, listener_topic_key, number_mess, data, at_least_once_delivery);
        self.send_mess_to_buff(mess, ix);
               
        if is_new_addr{
            self.addrs_new.lock().unwrap().push(Address{ix,
                                                        connection_key,
                                                        address: addr_to.to_string()});
            self.is_new_addr.store(true, Ordering::Relaxed);
        }
        true
    }
    
    fn send_mess_to_buff(&mut self, mess: Message, ix: usize){
        let (lock, cvar) = &*self.delay_write_cvar;
        if let Ok(mut _started) = lock.lock(){
            if let Ok(mut message_buffer_lock) = self.message_buffer.lock(){
                if let Some(mbuff) = message_buffer_lock.get_mut(ix).unwrap(){
                    mbuff.push(mess);
                }else{
                    *message_buffer_lock.get_mut(ix).unwrap() = Some(vec![mess]);
                }
            }    
            if !*_started{
                *_started = true;
                cvar.notify_one();
            }
        }
    }
     
    fn append_new_state(&mut self, db: &mut redis::Connect, addr_to: &str, listener_topic: &str)->bool{
        let listener_name;
        if let Ok(name) = db.get_listener_unique_name(listener_topic, addr_to){
            listener_name = name;
        }else{
            print_error!(format!("couldn't db.get_listener_unique_name {}", addr_to));
            return false;
        }
        let connection_key;
        match db.get_connection_key_for_sender(&listener_name){
            Ok(ck) =>{
                self.connection_key.push(ck);
                connection_key = ck;
            },
            Err(err)=>{
                print_error!(&format!("get_connection_key_for_sender from db: {}", err));
                return false;
            }
        }
        let last_mess_num = db.get_last_mess_number_for_sender(connection_key);
        if let Ok(last_mess_num) = last_mess_num{
            self.last_mess_number.push(last_mess_num);
        }else {
            if let Err(err) = db.init_last_mess_number_from_sender(connection_key){            
                print_error!(&format!("init_last_mess_number_from_sender from db: {}", err));
                return false;
            }
            self.last_mess_number.push(0);
        }
        let ix = self.mempools.lock().unwrap().len();    
        self.mempools.lock().unwrap().push(Arc::new(Mutex::new(Mempool::new())));
        self.mempool_buffer.lock().unwrap().push(Mempool::new());
        let mempool = self.mempools.lock().unwrap().get_mut(ix).unwrap().clone();
        if let Ok(last_mess) = db.load_last_message_for_sender(&mempool, connection_key){
            if let Some(mess) = last_mess{
                let mess_num = mess.number_mess;
                if mess_num > self.last_mess_number[ix]{
                    *self.last_mess_number.get_mut(ix).unwrap() = mess_num;
                }
            }
        }else {
            print_error!("db.load_last_message_for_sender");
        }
        if let Err(err) = db.save_listener_for_sender(addr_to, listener_topic){
            print_error!(&format!("db.save_listener_for_sender {}", err));
        }
        if let Err(err) = db.set_sender_topic_by_connection_key_from_sender(connection_key){
            print_error!(&format!("db.set_sender_topic_by_connection_key_from_sender {}", err));
        }
        self.messages.lock().unwrap().push(Arc::new(Mutex::new(Some(Vec::new()))));
        self.message_buffer.lock().unwrap().push(Some(Vec::new()));
        true
    }

    pub fn load_prev_connects(&mut self, db: &mut Connect){       
        match db.get_listeners_of_sender() {
            Ok(addr_topic) => {
                for t in addr_topic{
                    if self.append_new_state(db, &t.0, &t.1){
                        let ix = self.addrs_for.len();
                        let connection_key = self.connection_key[ix];
                        self.addrs_new.lock().unwrap().push(Address{ix, 
                                                                    connection_key,
                                                                    address: t.0.clone()});
                        self.addrs_for.insert(t.0, ix);
                    }
                }
            },
            Err(err)=>{
                print_error!(&format!("db.get_listeners_of_sender {}", err));
            }
        }       
        if !self.addrs_new.lock().unwrap().is_empty(){
            self.is_new_addr.store(true, Ordering::Relaxed);            
        }   
    }
    pub fn get_ctime(&self)->u64{
        self.ctime.load(Ordering::Relaxed)
    }
    fn wdelay_thread_notify(&self){
        let (lock, cvar) = &*self.delay_write_cvar;
        *lock.lock().unwrap() = true;
        cvar.notify_one();
    }
}

fn send_mess_to_listener(streams: &WriteStreamList, 
                         messages: &Arc<Mutex<MessList>>,
                         message_buffer: &Arc<Mutex<MessBuffList>>,
                         mempools: &Arc<Mutex<MempoolList>>,
                         mempool_buffer: &Arc<Mutex<MempoolBuffList>>){
    let mut mess_from_buff: Vec<Option<Vec<Message>>> = Vec::new();
    for m in message_buffer.lock().unwrap().iter_mut(){
        mess_from_buff.push(m.take());
    }
    for (ix, buff) in mess_from_buff.into_iter().enumerate(){
        if let Some(mut buff) = buff{
            {
                let mempool_dst_lock = mempools.lock().unwrap().get_mut(ix).unwrap().clone();
                let mut mempool_buffer_lock = mempool_buffer.lock().unwrap();
                let mempool_buffer = mempool_buffer_lock.get_mut(ix).unwrap();
                let mempool_dst = &mut mempool_dst_lock.lock().unwrap();
                for m in &mut buff{
                    m.change_mempool(mempool_buffer,
                                     mempool_dst);
                }
            }   
            if let Ok(mut mess_lock) = messages.lock().unwrap()[ix].lock(){
                if let Some(mess) = mess_lock.as_mut(){
                    mess.append(&mut buff);
                }else{
                    *mess_lock = Some(buff);
                }
            }
        }
        if let Some(stream) = streams.get(ix){
            let mut has_new_mess = false;
            if let Ok(mess_lock) = messages.lock().unwrap()[ix].lock(){
                if let Some(mess) = mess_lock.as_ref(){
                    has_new_mess = mess.last().unwrap().number_mess > stream.lock().unwrap().last_send_mess_number;
                }
            }
            if has_new_mess{
                write_stream(stream, messages, mempools);
            }
        }
    }
}

fn check_available_stream(is_new_addr: &Arc<AtomicBool>, ctime: u64, prev_time: &mut u64)->bool{
    if is_new_addr.load(Ordering::Relaxed) || (ctime - *prev_time) > settings::CHECK_AVAILABLE_STREAM_TIMEOUT_MS{
        is_new_addr.store(false, Ordering::Relaxed);
        *prev_time = ctime;
        true
    }else{
        false
    }
}

fn timeout_update_last_mess_number(ctime: u64, prev_time: &mut u64)->bool{
    if ctime - *prev_time > settings::UPDATE_LAST_MESS_NUMBER_TIMEOUT_MS{
        *prev_time = ctime;
        true
    }else{
        false
    }
}

fn update_last_mess_number(streams: &mut WriteStreamList,
                           db: &Arc<Mutex<redis::Connect>>,
                           messages: &Arc<Mutex<MessList>>,
                           mempools: &Arc<Mutex<MempoolList>>){
    let mut connection_keys: Vec<i32> = Vec::new();
    for stream_lock in streams.iter(){
        if let Ok(stream) = stream_lock.lock(){
            connection_keys.push(stream.connection_key);
        }
    }
    for (ix, connection_key) in connection_keys.iter().enumerate(){
        match db.lock().unwrap().get_last_mess_number_for_sender(*connection_key){
            Ok(last_mess_number)=>{
                streams.get_mut(ix).unwrap().lock().unwrap().last_mess_number = last_mess_number;

                let mempool = mempools.lock().unwrap()[ix].clone();
                if let Ok(mut mess_lock ) = messages.lock().unwrap()[ix].lock(){
                    if let Some(mess) = mess_lock.take(){
                        let mut mess_for_send = Vec::new();
                        for m in mess{
                            if last_mess_number < m.number_mess{
                                mess_for_send.push(m);
                            }else{
                                m.free(&mut mempool.lock().unwrap());
                            }
                        }
                        if !mess_for_send.is_empty(){
                            *mess_lock = Some(mess_for_send);
                        }          
                    }
                }
            },
            Err(err)=>{
                print_error!(&format!("get_last_mess_number_for_sender from db, {}", err));
            }
        }
    }
}

fn append_streams(streams: &mut WriteStreamList,
                  addrs: &mut Arc<Mutex<Vec<Address>>>,
                  db: &Arc<Mutex<redis::Connect>>,
                  messages: &Arc<Mutex<MessList>>,
                  mempools: &Arc<Mutex<MempoolList>>){
    let mut addrs_lost: Vec<Address> = Vec::new();
    for addr in addrs.lock().unwrap().iter(){
        match TcpStream::connect(&addr.address){
            Ok(stream)=>{
                let mempool = mempools.lock().unwrap()[addr.ix].clone();
                match db.lock().unwrap().load_messages_for_sender(&mempool, addr.connection_key){
                    Ok(mut mess_from_db) =>{
                        let mess_lock = messages.lock().unwrap()[addr.ix].clone();
                        if let Some(mut mess_for_send) = mess_lock.lock().unwrap().take(){
                            mess_from_db.append(&mut mess_for_send);
                        }
                        *mess_lock.lock().unwrap() = Some(mess_from_db);
                    },
                    Err(err)=>{
                        print_error!(&format!("db.load_messages_for_sender, {} {}", addr.address, err));
                    }
                }
                let mut last_send_mess_number: u64 = 0;
                if let Ok(num) = db.lock().unwrap().get_last_mess_number_for_sender(addr.connection_key){
                    last_send_mess_number = num;
                }else{
                    print_error!(format!("couldn't db.get_last_mess_number_for_sender {}", addr.address));
                }
                let wstream = WriteStream{ix: addr.ix,
                                                       connection_key: addr.connection_key,  
                                                       address: addr.address.clone(),
                                                       stream: Arc::new(Some(stream)), 
                                                       last_send_mess_number, last_mess_number: 0,
                                                       is_active: false, has_close_request: false, is_closed: false};
                while addr.ix >= streams.len() {
                    streams.push(Arc::new(Mutex::new(WriteStream::new())));
                }
                streams[addr.ix] = Arc::new(Mutex::new(wstream));                
            },
            Err(_err)=>{
                addrs_lost.push(addr.clone());
                print_debug!(&format!("tcp connect, {} {}", _err, addr.address));
                let mess = messages.lock().unwrap()[addr.ix].lock().unwrap().take();
                if let Some(mess) = mess{
                    save_mess_to_db(mess, db, addr.ix, addr.connection_key, mempools);
                }
            }
        }
    }
    *addrs.lock().unwrap() = addrs_lost;
}

fn write_stream(stream: &Arc<Mutex<WriteStream>>,
                messages: &Arc<Mutex<MessList>>,
                mempools: &Arc<Mutex<MempoolList>>){
    if let Ok(mut stream) = stream.lock(){
        if !stream.is_active && !stream.has_close_request{
            stream.is_active = true;
        }else{
            return;
        }
    }else{
        return;
    }
    let stream = stream.clone();
    let messages = messages.clone();
    let mempools = mempools.clone();
    
    rayon::spawn(move || {
        let mut is_shutdown = false;
        let mut ix = 0;
        let mut last_send_mess_number = 0;
        let mut arc_stream = Arc::new(None);
        if let Ok(stream) = stream.lock(){
            ix = stream.ix;
            last_send_mess_number = stream.last_send_mess_number;
            arc_stream = stream.stream.clone();
        }
        let messages = messages.lock().unwrap()[ix].clone();
        if let Some(tcp_stream) = arc_stream.as_ref(){
            let mut buff: Vec<Message> = Vec::new();
            let mut writer = BufWriter::with_capacity(settings::WRITE_BUFFER_CAPASITY, tcp_stream); 
            let mempool = mempools.lock().unwrap()[ix].clone();
            loop{
                let mut mess_for_send = None;
                if let Ok(mut mess_lock) = messages.lock(){
                    mess_for_send = mess_lock.take();                    
                } 
                let mess_for_send_is_none = mess_for_send.is_none();
                if mess_for_send_is_none || is_shutdown{
                    if !mess_for_send_is_none{
                        buff.append(&mut mess_for_send.unwrap());
                    }
                    break;
                }  
                for mess in mess_for_send.unwrap(){                    
                    let num_mess = mess.number_mess;
                    if !is_shutdown && last_send_mess_number < num_mess{
                        last_send_mess_number = num_mess;
                        if !mess.to_stream(&mempool, &mut writer){
                            is_shutdown = true;
                        }
                    }
                    buff.push(mess);
                }                
            }
            while let Err(err) = writer.flush() {
                print_error!(&format!("writer.flush, {}, {}", err, err.kind()));
                if err.kind() != std::io::ErrorKind::WouldBlock && err.kind() != std::io::ErrorKind::Interrupted{
                    is_shutdown = true;
                    break;
                }
            }
            let mut last_mess_number = 0;
            if let Ok(stream) = stream.lock(){
                last_mess_number = stream.last_mess_number;
            }
            let mut no_send_mess: Vec<Message> = Vec::new();
            for mess in buff{
                let num_mess = mess.number_mess;
                let at_least_once_delivery = mess.at_least_once_delivery();
                if (is_shutdown || at_least_once_delivery) && last_mess_number < num_mess{
                    no_send_mess.push(mess);
                }else{
                    mess.free(&mut mempool.lock().unwrap());
                }
            }
            if let Ok(mut mess_lock) = messages.lock(){
                if let Some(mut mess_for_send) = mess_lock.take(){
                    no_send_mess.append(&mut mess_for_send);
                }
                if !no_send_mess.is_empty(){
                    *mess_lock = Some(no_send_mess);
                }
            }
        }
        if let Ok(mut stream) = stream.lock(){
            stream.last_send_mess_number = last_send_mess_number;
            stream.is_active = false;
            if is_shutdown{ 
                stream.has_close_request = true;
            }
        }
    });
}

fn check_streams_close(streams: &mut WriteStreamList,
                       addrs_new: &Arc<Mutex<Vec<Address>>>,
                       db: &Arc<Mutex<redis::Connect>>,
                       messages: &Arc<Mutex<MessList>>,
                       mempools: &Arc<Mutex<MempoolList>>){
    for stream in streams.iter(){
        if let Ok(mut stream) = stream.lock(){
            if stream.has_close_request && !stream.is_closed && !stream.is_active {
                if let Some(stream) = stream.stream.as_ref(){
                    let _ = stream.shutdown(std::net::Shutdown::Write);
                }
                let ix = stream.ix;
                let connection_key = stream.connection_key;
                let address = stream.address.clone();
               
                let mess = messages.lock().unwrap()[ix].lock().unwrap().take();
                if let Some(mess) = mess{
                    save_mess_to_db(mess, db, ix, connection_key, mempools);            
                }
                addrs_new.lock().unwrap().push(Address{ix, connection_key, address});
            
                stream.is_closed = true;
            }
        }
    }  
}

fn save_mess_to_db(mess: Vec<Message>, db: &Arc<Mutex<redis::Connect>>,
                   ix: usize, connection_key: i32, mempools: &Arc<Mutex<MempoolList>>){                    
    let mut last_send_mess_number: u64 = 0;
    if let Ok(num) = db.lock().unwrap().get_last_mess_number_for_sender(connection_key){
        last_send_mess_number = num;
    }else{
        print_error!(format!("couldn't db.get_last_mess_number_for_sender, connection_key {}", connection_key));
    }
    let mess: Vec<Message> = mess.into_iter()
                                 .filter(|m|
                                        m.at_least_once_delivery() &&
                                        m.number_mess > last_send_mess_number)
                                 .collect();
    if !mess.is_empty(){
        let mempool = mempools.lock().unwrap()[ix].clone();
        if let Err(err) = db.lock().unwrap().save_messages_from_sender(&mempool, connection_key, mess){
            print_error!(&format!("db.save_messages_from_sender, connection_key {}, err {}", connection_key, err));
        }
    }
}

fn close_streams(streams: &WriteStreamList,
                 db: &Arc<Mutex<redis::Connect>>,
                 messages: &Arc<Mutex<MessList>>,
                 mempools: &Arc<Mutex<MempoolList>>){
    for stream in streams.iter(){
        if let Ok(mut stream) = stream.lock(){
            stream.has_close_request = true;
        }
    }
    for (ix, mess) in messages.lock().unwrap().iter().enumerate(){
        if let Some(mess_for_send) = mess.lock().unwrap().take(){
            if !mess_for_send.is_empty(){
                let stream = streams.get(ix).unwrap().lock().unwrap();
                save_mess_to_db(mess_for_send, db, ix, stream.connection_key,  mempools);
            }          
        }
    }
}

impl Drop for Sender {
    fn drop(&mut self) {                
        self.is_close.store(true, Ordering::Relaxed);
        self.wdelay_thread_notify();
        if let Err(err) = self.wdelay_thread.take().unwrap().join(){
            print_error!(&format!("wdelay_thread.join, {:?}", err));
        }
    }
}