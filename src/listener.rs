use crate::message::Message;
use crate::message::MessageForReceiver;
use crate::mempool::Mempool;
use crate::redis;
use crate::settings;
use crate::common;
use crate::{UCbackIntern, UData};
use crate::print_error;

use std::collections::{HashMap};
use std::io::Read;
use std::thread::JoinHandle;
use std::thread;
use std::time::Duration;
use std::sync::{ Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::io::{self,BufReader};

use std::net::SocketAddr;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

struct ReadStream{
    stream: TcpStream,
    is_active: bool,
    is_close: bool
}

struct Sender{
    sender_topic: String,
    sender_name: String,
    listener_topic: String,
    last_mess_num: u64,
    last_mess_num_preview: u64,
    is_deleted: bool,
}

type MessList = Vec<Option<Vec<Message>>>; 
type MempoolList = Vec<Arc<Mutex<Mempool>>>; 
type SenderList = Vec<Sender>;
type ReadStreamList = Vec<Arc<Mutex<ReadStream>>>; 

pub struct Listener{
    stream_thread: Option<JoinHandle<()>>,
    receive_thread: Option<JoinHandle<()>>,
    receive_thread_cvar: Arc<(Mutex<bool>, Condvar)>,
    is_close: Arc<AtomicBool>,
}

impl Listener {
    pub fn new(mut listener: TcpListener,
               unique_name: &str, redis_path: &str, source_topic: &str, receive_cb: UCbackIntern, udata: UData)->Listener{
        let mut poll = Poll::new().expect("couldn't create poll queue");
        //let wakeup_fd = wakeupfd_create(epoll_fd);
        let mut messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(Vec::new()));
        let messages_ = messages.clone();
        let mut mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(Vec::new()));
        let mempools_= mempools.clone();
        let mut senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(Vec::new()));
        let senders_ = senders.clone();
        let db_conn = redis::Connect::new(unique_name, redis_path).expect("couldn't redis::Connect");
        let db = Arc::new(Mutex::new(db_conn));
        db.lock().unwrap().set_source_topic(source_topic);
        let db_ = db.clone();
        let receive_thread_cvar: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));
        let receive_thread_cvar_ = receive_thread_cvar.clone();
      
        const SERVER: Token = Token(usize::MAX);
        poll.registry().register(&mut listener, SERVER, Interest::READABLE).expect("couldn't register listener");
   
        let stream_thread = thread::spawn(move|| {            
            let mut streams: ReadStreamList = Vec::new();
            let mut address: HashMap<SocketAddr, usize> = HashMap::new();
            let mut events = Events::with_capacity(settings::EPOLL_LISTEN_EVENTS_COUNT);
            loop{ 
                if let Err(_) = poll.poll(&mut events, None){
                    break;
                }
                for ev in &events {
                    match ev.token() {                    
                        SERVER => {
                            listener_accept(&poll, &mut address, &mut streams, &mut senders, &listener, 
                                &mut mempools, &mut messages);
                        }
                        CLIENT =>{
                            // if stream_fd == wakeup_fd{
                            //     wakeupfd_reset(wakeup_fd);
                            // }else 
                            if let Some(stream) = streams.get(CLIENT.0){
                                if !stream.lock().unwrap().is_close{
                                    read_stream(&poll, CLIENT, stream, &senders,
                                                db.clone(), &mempools, &messages, &receive_thread_cvar_);
                                }else{
                                    remove_stream(&poll, CLIENT, &mut streams, &mut senders);
                                }
                            }
                        }
                        _ => {
                            print_error!(format!("unknown event"));
                        }
                    }
                }
            }
            update_last_mess_number(&senders, &db);       
        });

        let receive_thread_cvar_ = receive_thread_cvar.clone();
        let is_close = Arc::new(AtomicBool::new(false));
        let is_close_ = is_close.clone();
        let receive_thread = thread::spawn(move|| {
            let mut once_again = true;
            let mut prev_time: [u64; 1] = [common::current_time_ms(); 1];
            let mut temp_mempool: Mempool = Mempool::new();
            while !is_close_.load(Ordering::Relaxed){
                if !once_again{
                    let (lock, cvar) = &*receive_thread_cvar_;
                    if let Ok(mut _started) = lock.lock(){                    
                        if !messages_.lock().unwrap().iter().any(|m: &Option<Vec<Message>>| m.is_some()){
                            *_started = false;
                            _started = cvar.wait(_started).unwrap();
                        }
                    }
                }
                once_again = !once_again;
                
                if settings::READ_MESS_DELAY_MS > 0{
                    std::thread::sleep(Duration::from_millis(settings::READ_MESS_DELAY_MS));
                }    
                let mut mess_buff: Vec<Option<Vec<Message>>> = Vec::new();
                let mut has_mess = false;
                for mess in messages.lock().unwrap().iter_mut(){
                    if let Some(m) = mess.take(){
                        mess_buff.push(Some(m));
                        has_mess = true;
                    }else{
                        mess_buff.push(None);
                    }
                }
                if has_mess{
                    do_receive_cb(mess_buff, &mut temp_mempool, &mempools_, &senders_, receive_cb, &udata); 
                    once_again = true;
                } 
                let ctime = common::current_time_ms();
                if timeout_update_last_mess_number(ctime, &mut prev_time[0]){                    
                    update_last_mess_number(&senders_, &db_);
                    check_has_remove_senders(&db_, &senders_);
                }
            }
        });  
        Self{            
            stream_thread: Some(stream_thread),
            receive_thread: Some(receive_thread),
            receive_thread_cvar,
            is_close,
        }
    }
}

fn do_receive_cb(mess_buff: Vec<Option<Vec<Message>>>,
                 temp_mempool: &mut Mempool,
                 mempools: &Arc<Mutex<MempoolList>>,
                 senders: &Arc<Mutex<SenderList>>,
                 receive_cb: UCbackIntern,
                 udata: &UData){

    let mut ix: usize = 0;
    for mess in mess_buff{
        if let Some(mess) = mess{
            let mempool_lock = mempools.lock().unwrap()[ix].clone();
            let mut mess_for_receive: Vec<MessageForReceiver> = Vec::new();    
            if let Ok(mut mempool) = mempool_lock.lock(){
                for mut m in mess{
                    m.change_mempool(&mut mempool, temp_mempool);
                    let m = MessageForReceiver::new(&m, temp_mempool);
                    mess_for_receive.push(m);
                }
            }
            let mut last_mess_num = 0;
            for m in mess_for_receive{
                receive_cb(m.topic_to, 
                    m.topic_from, 
                    m.data, m.data_len, 
                    udata.0);
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
        ix += 1;
    }
}

fn receive_thread_notify(receive_thread_cvar: &Arc<(Mutex<bool>, Condvar)>){
    let (lock, cvar) = &**receive_thread_cvar;
    *lock.lock().unwrap() = true;
    cvar.notify_one();
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
                        streams.push(Arc::new(Mutex::new(ReadStream{stream, is_active: false, is_close: false})));    
                        senders.lock().unwrap().push(Sender{sender_topic: "".to_string(), sender_name: "".to_string(), listener_topic: "".to_string(),
                                                            last_mess_num: 0, last_mess_num_preview: 0, is_deleted: false});
                        mempools.lock().unwrap().push(Arc::new(Mutex::new(Mempool::new())));
                        messages.lock().unwrap().push(None);
                    }else{
                        streams[ix] = Arc::new(Mutex::new(ReadStream{stream, is_active: false, is_close: false}));    
                        senders.lock().unwrap()[ix] = Sender{sender_topic: "".to_string(), sender_name: "".to_string(), listener_topic: "".to_string(),
                                                             last_mess_num: 0, last_mess_num_preview: 0, is_deleted: false};
                        messages.lock().unwrap()[ix] = None;
                    }
                }else{
                    print_error!(format!("couldn't poll.registry() stream"));
                }                
            }
            Err(err) => {
                if err.kind() != io::ErrorKind::WouldBlock {
                    print_error!(&format!("couldn't listener accept: {}", err));
                }
                break;
            } 
        }
    }
}

fn read_stream(poll: &Poll,
               token: Token,
               stream: &Arc<Mutex<ReadStream>>,
               senders: &Arc<Mutex<SenderList>>,
               db: Arc<Mutex<redis::Connect>>,
               mempools: &Arc<Mutex<MempoolList>>,
               messages: &Arc<Mutex<MessList>>,
               receive_thread_cvar: &Arc<(Mutex<bool>, Condvar)>){
    if let Ok(mut stream) = stream.try_lock(){
        if !stream.is_active && !stream.is_close{
            stream.is_active = true;
        }else{
            return;
        }
    }else{
       // continue_read_stream(poll, stream_token.0);
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
        let mut stream = stream.lock().unwrap();
        let mut reader = BufReader::with_capacity(settings::READ_BUFFER_CAPASITY, stream.stream.by_ref());
        let mut is_shutdown = false;
        while let Some(mess) = Message::from_stream(&mempool, reader.by_ref(), &mut is_shutdown){
            if last_mess_num == 0{
                let senders = &mut senders.lock().unwrap();
                let sender = senders.get_mut(token.0).unwrap();
                if sender.sender_name.is_empty(){
                    mess.sender_topic(&mempool.lock().unwrap(), &mut sender.sender_topic);
                    mess.sender_name(&mempool.lock().unwrap(), &mut sender.sender_name);
                    mess.listener_topic(&mempool.lock().unwrap(), &mut sender.listener_topic);
                } 
                last_mess_num = get_last_mess_number(&db, &sender.sender_name, &sender.sender_topic, &sender.listener_topic, 0);                    
            }
            if mess.number_mess > last_mess_num{
                last_mess_num = mess.number_mess;
                mess_buff.push(mess);
            }else{
                mess.free(&mut mempool.lock().unwrap());
            }
        }  
       // mempool.lock().unwrap()._print_size();
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
            senders.get_mut(token.0).unwrap().last_mess_num_preview = last_mess_num;
        }            
        if is_shutdown{
            let _ = stream.stream.shutdown(std::net::Shutdown::Read);
            stream.is_close = true;
        }            
        stream.is_active = false;
        //continue_read_stream(poll, stream_fd);
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
    for sender in senders.lock().unwrap().iter(){
        if !sender.sender_name.is_empty() && sender.last_mess_num > 0{
            set_last_mess_number(db, &sender.sender_name, &sender.sender_topic, &sender.listener_topic, sender.last_mess_num);
        }
    }
}

fn set_last_mess_number(db: &Arc<Mutex<redis::Connect>>, sender_name: &str, sender_topic: &str, listener_topic: &str, last_mess_num: u64){
    if let Err(err) = db.lock().unwrap().set_last_mess_number_from_listener(sender_name, sender_topic, listener_topic,last_mess_num){
        print_error!(&format!("couldn't db.set_last_mess_number: {}", err));
    }
}

fn remove_stream(poll: &Poll, 
                 token: Token,
                 streams: &mut ReadStreamList,
                 senders: &mut Arc<Mutex<SenderList>>,){
    senders.lock().unwrap().get_mut(token.0).unwrap().is_deleted = true;
}

fn check_has_remove_senders(db: &Arc<Mutex<redis::Connect>>,
                            senders: &Arc<Mutex<SenderList>>){
    for sender in senders.lock().unwrap().iter(){
        if sender.is_deleted{
            if !sender.sender_name.is_empty() && sender.last_mess_num > 0{
                set_last_mess_number(db, &sender.sender_name, &sender.sender_topic, &sender.listener_topic, sender.last_mess_num);
            }
        }
    }    
}

fn get_last_mess_number(db: &Arc<Mutex<redis::Connect>>, sender_name: &str, sender_topic: &str, listener_topic: &str, default_mess_number: u64)->u64{
    match db.lock().unwrap().get_last_mess_number_for_listener(sender_name, sender_topic, listener_topic){
        Ok(num)=>{
            num
        },
        Err(err)=>{
            print_error!(&format!("couldn't get_last_mess_number_for_listener: {}", err));
            default_mess_number
        }
    }
}

// fn wakeupfd_create(epoll_fd: RawFd)->RawFd{
//     let event_fd = syscall!(eventfd(0, 0)).expect("couldn't eventfd");
//     let mut event = libc::epoll_event {
//         events: (libc::EPOLLIN) as u32,
//         u64: event_fd as u64,
//     };
//     syscall!(epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, event_fd, &mut event)).expect("couldn't eventfd_create");
//     event_fd
// }
// fn wakeupfd_notify(event_fd: RawFd){
//     let b: u64 = 1;
//     if let Err(err) = syscall!(write(event_fd, &b as *const u64 as *const c_void, std::mem::size_of::<u64>())){
//         print_error!(format!("couldn't wakeupfd_notify, {}", err));
//     }
// }
// fn wakeupfd_reset(event_fd: i32){
//     let b: u64 = 0;
//     if let Err(err) = syscall!(read(event_fd, &b as *const u64 as *mut c_void, std::mem::size_of::<u64>())){
//         print_error!(format!("couldn't wakeupfd_reset, {}", err));
//     }
// }

impl Drop for Listener {
    fn drop(&mut self) {        
        self.is_close.store(true, Ordering::Relaxed);
        receive_thread_notify(&self.receive_thread_cvar);
        if let Err(err) = self.receive_thread.take().unwrap().join(){
            print_error!(&format!("wdelay_thread.join, {:?}", err));
        }
      
       // wakeupfd_notify(self.wakeup_fd);
        if let Err(err) = self.stream_thread.take().unwrap().join(){
            print_error!(&format!("stream_thread.join, {:?}", err));
        }
    }
}