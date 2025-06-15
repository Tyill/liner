use crate::message::Message;
use crate::message::MessageForReceiver;
use crate::mempool::Mempool;
use crate::redis;
use crate::settings;
use crate::common;
use crate::{UCbackIntern, UData};
use crate::{print_error, print_debug};

use std::collections::HashMap;
use std::io::Read;
use std::thread::JoinHandle;
use std::thread;
use std::time::Duration;
use std::sync::{ Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::io::{self,BufReader};
use std::ffi::CString;

use std::net::SocketAddr;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token, Waker};

struct ReadStream{
    stream: Arc<Option<TcpStream>>,
    is_active: bool,
    is_close: bool
}

struct Sender{
    sender_topic: String,
    connection_key: i32,
    last_mess_num: u64,
    last_mess_num_preview: u64,
    last_mess_num_saved: u64,
}

type MessList = Vec<Option<Vec<Message>>>; 
type MempoolList = Vec<Arc<Mutex<Mempool>>>; 
type SenderList = Vec<Sender>;
type ReadStreamList = Vec<Arc<Mutex<ReadStream>>>; 

pub struct Listener{
    stream_thread: Option<JoinHandle<()>>,
    receive_thread: Option<JoinHandle<()>>,
    receive_thread_cvar: Arc<(Mutex<bool>, Condvar)>,
    listener_topic: Arc<Mutex<HashMap<i32, String>>>,
    is_close: Arc<AtomicBool>,
    waker: Arc<Waker>,
}

impl Listener {
    pub fn new(mut listener: TcpListener,
               unique_name: &str, redis_path: &str, source_topic: &str, subscriptions: &HashMap<i32, String>, receive_cb: UCbackIntern, udata: UData)->Listener{
        let mut poll = Poll::new().expect("couldn't create poll queue");
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(Vec::new()));
        let mut messages_ = messages.clone();
        let mut mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(Vec::new()));
        let mempools_= mempools.clone();
        let mut senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(Vec::new()));
        let senders_ = senders.clone();
        let db_conn = redis::Connect::new(unique_name, redis_path).expect("couldn't redis::Connect");
        let db = Arc::new(Mutex::new(db_conn));
        db.lock().unwrap().set_source_topic(source_topic);
        let db_ = db.clone();
      
        let listener_topic: Arc<Mutex<HashMap<i32, String>>> = Arc::new(Mutex::new(HashMap::new())); // key listener_topic, value key
        let topic_key = db.lock().unwrap().get_topic_key(source_topic).expect("couldn't db.get_topic_key");
        if let Ok(mut lr) = listener_topic.lock(){ 
            lr.insert(topic_key, source_topic.to_owned());
            for s in subscriptions.iter(){
                lr.insert(*s.0, s.1.clone());
            }
        }

        let receive_thread_cvar: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));
        let receive_thread_cvar_ = receive_thread_cvar.clone();
      
        const SERVER: Token = Token(usize::MAX);
        const WAKER: Token = Token(usize::MAX - 1);
        poll.registry().register(&mut listener, SERVER, Interest::READABLE).expect("couldn't register listener");
        let waker = Arc::new(Waker::new(poll.registry(), WAKER).expect("couldn't create Waker"));
        let stream_thread = thread::spawn(move|| {            
            let mut streams: ReadStreamList = Vec::new();
            let mut address: HashMap<SocketAddr, usize> = HashMap::new();
            let mut events = Events::with_capacity(settings::EPOLL_LISTEN_EVENTS_COUNT);
            loop{ 
                if let Err(err) = poll.poll(&mut events, None){
                    if err.kind() != std::io::ErrorKind::Interrupted{
                        print_error!(&format!("couldn't poll.poll: {}", err));
                        break;
                    }
                }
                let mut has_wake = false;
                for ev in &events {
                    match ev.token() {                    
                        SERVER => {
                            listener_accept(&poll, &mut address, &mut streams, &mut senders, &listener, 
                                &mut mempools, &mut messages_);
                        }
                        WAKER => {
                            has_wake = true;
                            break;
                        }
                        client =>{
                            if let Some(stream) = streams.get(client.0){
                                read_stream(client, stream, &senders,
                                            db.clone(), &mempools, &messages_, &receive_thread_cvar_);
                            }
                        }                        
                    }
                }
                if has_wake{
                    break;
                }
            }
            update_last_mess_number(&senders, &db);       
        });

        let receive_thread_cvar_ = receive_thread_cvar.clone();
        let is_close = Arc::new(AtomicBool::new(false));
        let is_close_ = is_close.clone();
        let listener_topic_ = listener_topic.clone();
        let receive_thread = thread::spawn(move|| {
            let mut prev_time: [u64; 1] = [common::current_time_ms(); 1];
            let mut temp_mempool: Mempool = Mempool::new();
            let mut rep_count = 0;
            while !is_close_.load(Ordering::Relaxed){
                let (lock, cvar) = &*receive_thread_cvar_;
                let mut has_new_mess = false;
                if let Ok(mut _started) = lock.lock(){
                    has_new_mess = messages.lock().unwrap().iter().any(|m: &Option<Vec<Message>>| m.is_some());                 
                    if !has_new_mess && rep_count > settings::LISTENER_THREAD_READ_MESS_DELAY_REPEATE_COUNT{
                        *_started = false;
                        _started = cvar.wait_timeout(_started, Duration::from_millis(settings::LISTENER_THREAD_WAIT_TIMEOUT_MS)).unwrap().0;
                        has_new_mess = *_started;
                    }
                    if has_new_mess{
                        rep_count = 0;
                    }else{
                        rep_count += 1;
                    }
                }
                if settings::LISTENER_THREAD_READ_MESS_DELAY_MS > 0 && !has_new_mess{
                    std::thread::sleep(Duration::from_millis(settings::LISTENER_THREAD_READ_MESS_DELAY_MS));
                }
                if has_new_mess{
                    do_receive_cb(&messages, &mut temp_mempool, &mempools_, &senders_, &listener_topic_, receive_cb, &udata); 
                } 
                let ctime = common::current_time_ms();
                if timeout_update_last_mess_number(ctime, &mut prev_time[0]){                    
                    update_last_mess_number(&senders_, &db_);
                }
            }
        });        
        Self{            
            stream_thread: Some(stream_thread),
            receive_thread: Some(receive_thread),
            receive_thread_cvar,
            listener_topic,
            is_close,
            waker
        }
    }
    pub fn subscribe(&mut self, topic: &str, topic_key: i32){
        self.listener_topic.lock().unwrap().insert(topic_key, topic.to_owned());
    }   
    pub fn unsubscribe(&mut self, topic_key: i32){
        self.listener_topic.lock().unwrap().remove(&topic_key);
    }
}

fn do_receive_cb(message_buffer: &Arc<Mutex<MessList>>,
                 temp_mempool: &mut Mempool,
                 mempools: &Arc<Mutex<MempoolList>>,
                 senders: &Arc<Mutex<SenderList>>,
                 listener_topic: &Arc<Mutex<HashMap<i32, String>>>,
                 receive_cb: UCbackIntern,
                 udata: &UData){

    let mut mess_from_buff: Vec<Option<Vec<Message>>> = Vec::new();
    for m in message_buffer.lock().unwrap().iter_mut(){
        mess_from_buff.push(m.take());
    }
    for (ix, mess) in mess_from_buff.into_iter().enumerate(){
        if let Some(mess) = mess{
            let mempool_lock = mempools.lock().unwrap()[ix].clone();
            let mut mess_for_receive: Vec<Message> = Vec::new();    
            if let Ok(mut mempool) = mempool_lock.lock(){
                for mut m in mess{
                    m.change_mempool(&mut mempool, temp_mempool);
                    mess_for_receive.push(m);
                }
            }
            let topic_from = CString::new(senders.lock().unwrap()[ix].sender_topic.as_bytes()).unwrap();
            let mut last_mess_num = 0;
            for m in mess_for_receive{
                let mut topic_to = None;
                if let Some(topic) = listener_topic.lock().unwrap().get(&m.listener_topic_key){
                    topic_to = Some(CString::new(topic.as_bytes()).unwrap());
                }else{
                    print_debug!(&format!("unsubscribe on topic_key {}", m.listener_topic_key));
                }
                if let Some(topic_to) = topic_to{
                    let m = MessageForReceiver::new(&m, temp_mempool);
                    receive_cb(topic_to.as_c_str().as_ptr(), 
                               topic_from.as_c_str().as_ptr(), 
                               m.data, m.data_len, 
                               udata.0);      
                }
                m.free(temp_mempool);
                if m.number_mess > last_mess_num{
                    last_mess_num = m.number_mess;
                }             
            }
            let mut senders = senders.lock().unwrap();
            if senders[ix].last_mess_num < last_mess_num{
                senders.get_mut(ix).unwrap().last_mess_num = last_mess_num;
            }
        }
    }
}

fn listener_accept(poll: &Poll, 
                   address: &mut HashMap<SocketAddr, usize>,
                   streams: &mut ReadStreamList,
                   senders: &mut Arc<Mutex<SenderList>>,
                   listener: &TcpListener,
                   mempools: &mut Arc<Mutex<MempoolList>>,
                   messages: &mut Arc<Mutex<MessList>>){
    
    loop {
        match listener.accept() {
            Ok((mut stream, addr)) => {
                let mut token = Token(address.len());
                let mut ix = usize::MAX;
                if address.contains_key(&addr){
                    ix = *address.get(&addr).unwrap();
                    token = Token(ix);
                }          
                if let Ok(()) = poll.registry().register(&mut stream, token, Interest::READABLE){
                    if ix == usize::MAX{
                        streams.push(Arc::new(Mutex::new(ReadStream{stream: Arc::new(Some(stream)), is_active: false, is_close: false})));    
                        senders.lock().unwrap().push(Sender{sender_topic: "".to_owned(), connection_key: -1, last_mess_num: 0, last_mess_num_preview: 0, last_mess_num_saved: 0});
                        mempools.lock().unwrap().push(Arc::new(Mutex::new(Mempool::new())));
                        messages.lock().unwrap().push(None);
                        address.insert(addr, address.len());
                    }else{
                        streams[ix] = Arc::new(Mutex::new(ReadStream{stream: Arc::new(Some(stream)), is_active: false, is_close: false}));    
                    }
                }else{
                    print_error!(format!("couldn't poll.registry() stream"));
                }                
            }
            Err(err) => {
                if err.kind() != io::ErrorKind::WouldBlock && err.kind() != std::io::ErrorKind::Interrupted {
                    print_error!(&format!("couldn't listener accept: {}", err));
                }
                if err.kind() != std::io::ErrorKind::Interrupted{
                    break;
                }
            } 
        }
    }
}

fn read_stream(token: Token,
               stream: &Arc<Mutex<ReadStream>>,
               senders: &Arc<Mutex<SenderList>>,
               db: Arc<Mutex<redis::Connect>>,
               mempools: &Arc<Mutex<MempoolList>>,
               messages: &Arc<Mutex<MessList>>,
               receive_thread_cvar: &Arc<(Mutex<bool>, Condvar)>){
    if let Ok(mut stream) = stream.lock(){
        if !stream.is_active && !stream.is_close{
            stream.is_active = true;
        }else{
            return;
        }
    }else{
        return;
    }
    let stream = stream.clone();
    let senders = senders.clone();
    let mempool = mempools.lock().unwrap()[token.0].clone();
    let messages = messages.clone();
    let receive_thread_cvar = receive_thread_cvar.clone();

    rayon::spawn(move || {           
        let mut mess_buff: Vec<Message> = Vec::new();      
        let mut last_mess_num = 0;
        if let Ok(senders) = senders.lock(){
            last_mess_num = senders[token.0].last_mess_num_preview;
        }
        let mut arc_stream = Arc::new(None);
        if let Ok(stream) = stream.lock(){
            arc_stream = stream.stream.clone();
        }
        let mut is_shutdown = false;
        if let Some(stream) = arc_stream.as_ref(){
            let mut reader = BufReader::with_capacity(settings::READ_BUFFER_CAPASITY, stream);
            while let Some(mess) = Message::from_stream(&mempool, reader.by_ref(), &mut is_shutdown){
                if last_mess_num == 0{
                    let senders = &mut senders.lock().unwrap();
                    let sender = senders.get_mut(token.0).unwrap();
                    if sender.connection_key == -1{
                        sender.connection_key = mess.connection_key(&mempool);
                        sender.sender_topic = get_sender_topic(&db, sender.connection_key);
                    } 
                    last_mess_num = get_last_mess_number(&db, sender.connection_key, 0);                    
                }
                if mess.number_mess > last_mess_num{
                    last_mess_num = mess.number_mess;
                    mess_buff.push(mess);
                }else{
                    mess.free(&mut mempool.lock().unwrap());
                }
            }
        }
        if !mess_buff.is_empty(){ 
            let (lock, cvar) = &*receive_thread_cvar;
            if let Ok(mut _started) = lock.lock(){
                if let Ok(mut mess_lock) = messages.lock(){
                    if let Some(mess_for_receive) = mess_lock[token.0].as_mut(){
                        mess_for_receive.append(&mut mess_buff);
                    }else{
                        mess_lock[token.0] = Some(mess_buff);
                    }
                }
                if !*_started{
                    *_started = true;
                    cvar.notify_one();
                }
            }
        }
        if let Ok(mut senders) = senders.lock(){
            if let Some(sender) = senders.get_mut(token.0){
                sender.last_mess_num_preview = last_mess_num;
            }    
        }
        if let Ok(mut stream) = stream.lock(){
            if is_shutdown{
                if let Some(stream) = stream.stream.as_ref(){
                    let _ = stream.shutdown(std::net::Shutdown::Read);
                }
                stream.is_close = true;
            }            
            stream.is_active = false;
        }
    });
}

fn timeout_update_last_mess_number(ctime: u64, prev_time: &mut u64)->bool{
    if ctime - *prev_time > settings::UPDATE_LAST_MESS_NUMBER_TIMEOUT_MS{
        *prev_time = ctime;
        true
    }else{
        false
    }
}

fn update_last_mess_number(senders: &Arc<Mutex<SenderList>>,
                           db: &Arc<Mutex<redis::Connect>>){
    for sender in senders.lock().unwrap().iter_mut(){
        if sender.connection_key >= 0 && sender.last_mess_num_saved < sender.last_mess_num{
            set_last_mess_number(db, sender.connection_key, sender.last_mess_num);
            sender.last_mess_num_saved = sender.last_mess_num;
        }
    }
}

fn set_last_mess_number(db: &Arc<Mutex<redis::Connect>>, connection_key: i32, last_mess_num: u64){
    if let Err(err) = db.lock().unwrap().set_last_mess_number_from_listener(connection_key, last_mess_num){
        print_error!(&format!("couldn't db.set_last_mess_number: {}", err));
    }
}

fn get_last_mess_number(db: &Arc<Mutex<redis::Connect>>, connection_key: i32, default_mess_number: u64)->u64{
    match db.lock().unwrap().get_last_mess_number_for_listener(connection_key){
        Ok(num)=>{
            num
        },
        Err(err)=>{
            print_error!(&format!("couldn't get_last_mess_number_for_listener: {}", err));
            default_mess_number
        }
    }
}

fn get_sender_topic(db: &Arc<Mutex<redis::Connect>>, connection_key: i32)->String{
    match db.lock().unwrap().get_sender_topic_by_connection_key(connection_key){
        Ok(v)=>{
            v
        },
        Err(err)=>{
            print_error!(&format!("couldn't get_sender_topic, conn_key {}, err {}", connection_key, err));
            "".to_string()
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {        
        self.is_close.store(true, Ordering::Relaxed);
        receive_thread_notify(&self.receive_thread_cvar);
        if let Err(err) = self.receive_thread.take().unwrap().join(){
            print_error!(&format!("wdelay_thread.join, {:?}", err));
        }      
        self.waker.wake().expect("unable to wake");
        if let Err(err) = self.stream_thread.take().unwrap().join(){
            print_error!(&format!("stream_thread.join, {:?}", err));
        }
    }
}

fn receive_thread_notify(receive_thread_cvar: &Arc<(Mutex<bool>, Condvar)>){
    let (lock, cvar) = &**receive_thread_cvar;
    *lock.lock().unwrap() = true;
    cvar.notify_one();
}