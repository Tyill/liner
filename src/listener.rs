use crate::message::Message;
use crate::mempool::Mempool;
use crate::store::Store;
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
    stream: Option<TcpStream>,
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

/// Accept/token index `ix` is shared across `streams[ix]`, `senders[ix]`, `mempools[ix]`,
/// and `messages[ix]`, and is owned by one `SocketAddr` in the accept map for the life of
/// that peer identity. Do **not** recycle `ix` into a free-list for a different address:
/// the mempool and `last_mess_num` / `connection_key` on that slot belong to the original peer.
/// Same `SocketAddr` reconnecting must reuse its own `ix` (see `listener_accept`).
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
               db: Arc<Mutex<dyn Store>>, source_topic: &str, subscriptions: &HashMap<i32, String>, receive_cb: UCbackIntern, udata: UData)->Listener{
        let mut poll = Poll::new().expect("couldn't create poll queue");
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(Vec::new()));
        let mut messages_ = messages.clone();
        let mut mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(Vec::new()));
        let mempools_= mempools.clone();
        let mut senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(Vec::new()));
        let senders_ = senders.clone();
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
            // Sticky SocketAddr → slot index. Kept across TCP close so reconnect reuses the
            // same streams/senders/mempools/messages index (never hand a freed ix to another addr).
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
                cleanup_closed_streams(&poll, &mut streams);
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
            let mut buff_data: Vec<u8> = vec![0; 4086];
            while !is_close_.load(Ordering::Relaxed){
                let (lock, cvar) = &*receive_thread_cvar_;
                let mut has_new_mess = false;
                if let Ok(mut _started) = lock.lock(){
                    has_new_mess = messages.lock().unwrap().iter().any(|m: &Option<Vec<Message>>| m.is_some());                 
                    if !has_new_mess{
                        *_started = false;
                        _started = cvar.wait_timeout(_started, Duration::from_millis(settings::LISTENER_THREAD_WAIT_TIMEOUT_MS)).unwrap().0;
                        has_new_mess = *_started || messages.lock().unwrap().iter().any(|m: &Option<Vec<Message>>| m.is_some());
                    }
                }
                if has_new_mess{
                    do_receive_cb(&messages, &mempools_, &senders_, &listener_topic_, receive_cb, &mut buff_data, &udata); 
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
                 mempools: &Arc<Mutex<MempoolList>>,
                 senders: &Arc<Mutex<SenderList>>,
                 listener_topic: &Arc<Mutex<HashMap<i32, String>>>,
                 receive_cb: UCbackIntern,
                 buff_data: &mut Vec<u8>,
                 udata: &UData){

    let mut mess_from_buff: Vec<Option<Vec<Message>>> = Vec::new();
    for m in message_buffer.lock().unwrap().iter_mut(){
        mess_from_buff.push(m.take());
    }
    for (ix, mess) in mess_from_buff.into_iter().enumerate(){
        if let Some(mess) = mess{
            let sender_topic = match senders.lock() {
                Ok(s) => match s.get(ix) {
                    Some(sender) => sender.sender_topic.clone(),
                    None => {
                        print_error!(&format!("do_receive_cb: sender index out of bounds: {}", ix));
                        continue;
                    }
                },
                Err(_) => {
                    print_error!("do_receive_cb: senders lock poisoned");
                    continue;
                }
            };
            let topic_from = CString::new(sender_topic.as_bytes()).unwrap_or_else(|_| CString::new("").unwrap());
            let mut last_mess_num = 0;
            let mut topic_cstr_cache: HashMap<i32, CString> = HashMap::new();
            let mempool = match mempools.lock() {
                Ok(mp) => match mp.get(ix) {
                    Some(m) => m.clone(),
                    None => {
                        print_error!(&format!("do_receive_cb: mempool index out of bounds: {}", ix));
                        continue;
                    }
                },
                Err(_) => {
                    print_error!("do_receive_cb: mempools lock poisoned");
                    continue;
                }
            };
            for m in mess{
                // Important: don't hold the `listener_topic` lock while calling `receive_cb`:
                // callback can be slow, and we don't want to block subscribe/unsubscribe.
                if !topic_cstr_cache.contains_key(&m.listener_topic_key) {
                    let topic = match listener_topic.lock() {
                        Ok(lt) => lt.get(&m.listener_topic_key).cloned(),
                        Err(_) => {
                            print_error!("do_receive_cb: listener_topic lock poisoned");
                            None
                        }
                    };
                    if let Some(topic) = topic {
                        let topic_to = CString::new(topic.as_bytes())
                            .unwrap_or_else(|_| CString::new("").unwrap());
                        topic_cstr_cache.insert(m.listener_topic_key, topic_to);
                    }
                }
                if let Some(topic_to) = topic_cstr_cache.get(&m.listener_topic_key) {
                    let mlen = m.get_data(&mempool, buff_data);
                    m.free(&mempool);
                    receive_cb(topic_to.as_c_str().as_ptr(), 
                            topic_from.as_c_str().as_ptr(), 
                            buff_data[..mlen].as_ptr(), mlen, 
                            udata.0);
                } else {
                    print_debug!(&format!("unsubscribe on topic_key {}", m.listener_topic_key));
                    // Important: always free the message, even if it won't be delivered.
                    m.free(&mempool);
                }
                if m.number_mess > last_mess_num {
                    last_mess_num = m.number_mess;
                }
            }
            if let Ok(mut senders) = senders.lock() {
                if let Some(sender) = senders.get_mut(ix) {
                    if sender.last_mess_num < last_mess_num {
                        sender.last_mess_num = last_mess_num;
                    }
                } else {
                    print_error!(&format!("do_receive_cb: sender index out of bounds at update: {}", ix));
                }
            } else {
                print_error!("do_receive_cb: senders lock poisoned at update");
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
                        streams.push(Arc::new(Mutex::new(ReadStream{
                            stream: Some(stream),
                            is_active: false,
                            is_close: false,
                        })));    
                        if let Ok(mut s) = senders.lock() {
                            s.push(Sender{sender_topic: "".to_owned(), connection_key: -1, last_mess_num: 0, last_mess_num_preview: 0, last_mess_num_saved: 0});
                        } else {
                            print_error!("listener_accept: senders lock poisoned");
                        }
                        if let Ok(mut mp) = mempools.lock() {
                            mp.push(Arc::new(Mutex::new(Mempool::new())));
                        } else {
                            print_error!("listener_accept: mempools lock poisoned");
                        }
                        if let Ok(mut mb) = messages.lock() {
                            mb.push(None);
                        } else {
                            print_error!("listener_accept: messages lock poisoned");
                        }
                        address.insert(addr, streams.len() - 1);
                    }else{
                        // Same SocketAddr reconnects: keep index + mempool/sender/`last_mess_num`.
                        // Only replace the TCP stream — do not reset parallel slot vectors.
                        streams[ix] = Arc::new(Mutex::new(ReadStream{
                            stream: Some(stream),
                            is_active: false,
                            is_close: false,
                        }));    
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
               db: Arc<Mutex<dyn Store>>,
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
    let mempool = match mempools.lock() {
        Ok(mp) => match mp.get(token.0) {
            Some(m) => m.clone(),
            None => {
                print_error!(&format!("read_stream: mempool index out of bounds for token {}", token.0));
                if let Ok(mut s) = stream.lock() {
                    s.is_active = false;
                }
                return;
            }
        },
        Err(_) => {
            print_error!("read_stream: mempools lock poisoned");
            if let Ok(mut s) = stream.lock() {
                s.is_active = false;
            }
            return;
        }
    };
    let tcp_stream = match stream.lock() {
        Ok(mut s) => match s.stream.take() {
            Some(ts) => ts,
            None => {
                s.is_active = false;
                return;
            }
        },
        Err(_) => {
            print_error!("read_stream: stream lock poisoned");
            return;
        }
    };
    let messages = messages.clone();
    let receive_thread_cvar = receive_thread_cvar.clone();

    rayon::spawn(move || {           
        let mut mess_buff: Vec<Message> = Vec::new();      
        let mut last_mess_num = 0;
        if let Ok(senders) = senders.lock(){
            if let Some(sender) = senders.get(token.0) {
                last_mess_num = sender.last_mess_num_preview;
            } else {
                print_error!(&format!("read_stream: sender index out of bounds for token {}", token.0));
            }
        }
        let reader_stream = tcp_stream;
        let mut is_shutdown = false;
        {
            let mut reader = BufReader::with_capacity(settings::READ_BUFFER_CAPASITY, &reader_stream);
            while let Some(mess) = Message::from_stream(&mempool, reader.by_ref(), &mut is_shutdown){
                if last_mess_num == 0{
                    let mut connection_key = None;
                    if let Ok(mut senders) = senders.lock() {
                        if let Some(sender) = senders.get_mut(token.0) {
                            if sender.connection_key == -1{
                                sender.connection_key = mess.connection_key(&mempool);
                                sender.sender_topic = get_sender_topic(&db, sender.connection_key);
                            }
                            connection_key = Some(sender.connection_key);
                        } else {
                            print_error!(&format!("read_stream: sender index out of bounds for token {}", token.0));
                        }
                    } else {
                        print_error!("read_stream: senders lock poisoned (init)");
                    }
                    if let Some(connection_key) = connection_key {
                        last_mess_num = get_last_mess_number(&db, connection_key, 0);
                    }
                }
                if mess.number_mess > last_mess_num{
                    last_mess_num = mess.number_mess;
                    mess_buff.push(mess);
                }else{
                    mess.free(&mempool);
                }
            }
        }
        if !mess_buff.is_empty(){
            let (lock, cvar) = &*receive_thread_cvar;
            if let Ok(mut _started) = lock.lock(){
                if let Ok(mut mess_lock) = messages.lock(){
                    if let Some(slot) = mess_lock.get_mut(token.0) {
                        if let Some(mess_for_receive) = slot.as_mut() {
                            mess_for_receive.append(&mut mess_buff);
                        } else {
                            *slot = Some(mess_buff);
                        }
                    } else {
                        print_error!(&format!("read_stream: messages index out of bounds for token {}", token.0));
                        // Free to avoid leaking mempool allocations.
                        for m in mess_buff.drain(..) {
                            m.free(&mempool);
                        }
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
            } else {
                print_error!(&format!("read_stream: sender index out of bounds for token {}", token.0));
            }    
        }
        if is_shutdown {
            let _ = reader_stream.shutdown(std::net::Shutdown::Read);
        }
        if let Ok(mut stream) = stream.lock() {
            // Put stream back so the poll thread can deregister it if needed.
            stream.stream = Some(reader_stream);
            stream.is_close = is_shutdown;
            stream.is_active = false;
        }
    });
}

fn cleanup_closed_streams(poll: &Poll, streams: &mut ReadStreamList) {
    // Drop the TCP fd only. Leave `address` → index and mempool/sender slots intact so a later
    // accept from the same SocketAddr reclaims its own index (see module docs on `Listener`).
    for stream_lock in streams.iter() {
        let mut to_deregister: Option<TcpStream> = None;
        if let Ok(mut s) = stream_lock.lock() {
            if s.is_close {
                to_deregister = s.stream.take();
            }
        }
        if let Some(mut stream) = to_deregister {
            let _ = poll.registry().deregister(&mut stream);
        }
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

fn update_last_mess_number(senders: &Arc<Mutex<SenderList>>,
                           db: &Arc<Mutex<dyn Store>>){
    let updates: Vec<(usize, i32, u64)> = {
        let senders = senders.lock().unwrap();
        let mut out = Vec::new();
        for (ix, sender) in senders.iter().enumerate() {
            if sender.connection_key >= 0 && sender.last_mess_num_saved < sender.last_mess_num {
                out.push((ix, sender.connection_key, sender.last_mess_num));
            }
        }
        out
    };
    if updates.is_empty() {
        return;
    }
    let mut succeeded: Vec<(usize, u64)> = Vec::with_capacity(updates.len());
    {
        let mut db = db.lock().unwrap();
        for (ix, connection_key, last_mess_num) in updates {
            match db.set_last_mess_number_from_listener(connection_key, last_mess_num) {
                Ok(()) => succeeded.push((ix, last_mess_num)),
                Err(err) => {
                    print_error!(&format!("couldn't db.set_last_mess_number: {}", err));
                }
            }
        }
    }
    if succeeded.is_empty() {
        return;
    }
    let mut senders = senders.lock().unwrap();
    for (ix, last_mess_num) in succeeded {
        if let Some(sender) = senders.get_mut(ix) {
            if sender.connection_key >= 0 && sender.last_mess_num_saved < last_mess_num {
                sender.last_mess_num_saved = last_mess_num;
            }
        }
    }
}

fn get_last_mess_number(db: &Arc<Mutex<dyn Store>>, connection_key: i32, default_mess_number: u64)->u64{
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

fn get_sender_topic(db: &Arc<Mutex<dyn Store>>, connection_key: i32)->String{
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
        if let Err(err) = self.waker.wake() {
            print_error!(&format!("unable to wake stream thread: {}", err));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;
    use std::sync::OnceLock;

    #[test]
    fn timeout_update_last_mess_number_updates_only_after_timeout() {
        let mut prev = 1_000u64;
        let timeout = settings::UPDATE_LAST_MESS_NUMBER_TIMEOUT_MS;

        // exactly equal => no update (function uses strict `>`).
        assert!(!timeout_update_last_mess_number(prev + timeout, &mut prev));
        assert_eq!(prev, 1_000u64);

        // greater => update and prev changes.
        assert!(timeout_update_last_mess_number(1_000u64 + timeout + 1, &mut prev));
        assert_eq!(prev, 1_000u64 + timeout + 1);
    }

    #[test]
    fn receive_thread_notify_sets_flag_and_wakes_waiter() {
        let pair: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));
        let pair_waiter = pair.clone();

        let jh = std::thread::spawn(move || {
            let (lock, cvar) = &*pair_waiter;
            let started = lock.lock().unwrap();
            let (started, _timeout) = cvar
                .wait_timeout(started, Duration::from_millis(250))
                .unwrap();
            *started
        });

        // Let the waiter get to the wait point.
        std::thread::sleep(Duration::from_millis(20));
        receive_thread_notify(&pair);

        let observed = jh.join().unwrap();
        assert!(observed);
    }

    type CbRecord = (String, String, Vec<u8>);
    static CB_RECORDS: OnceLock<Mutex<Vec<CbRecord>>> = OnceLock::new();

    extern "C" fn test_receive_cb(
        to: *const i8,
        from: *const i8,
        data: *const u8,
        dsize: usize,
        udata: *mut libc::c_void,
    ) {
        let to = unsafe { CStr::from_ptr(to) }.to_string_lossy().to_string();
        let from = unsafe { CStr::from_ptr(from) }.to_string_lossy().to_string();
        let data = unsafe { std::slice::from_raw_parts(data, dsize) }.to_vec();

        // Prefer the passed udata storage (more isolated per-test).
        if !udata.is_null() {
            let storage = unsafe { &*(udata as *const Mutex<Vec<CbRecord>>) };
            storage.lock().unwrap().push((to, from, data));
            return;
        }

        CB_RECORDS
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap()
            .push((to, from, data));
    }

    fn make_udata_ptr() -> (*mut libc::c_void, *mut Mutex<Vec<CbRecord>>) {
        let bx: Box<Mutex<Vec<CbRecord>>> = Box::new(Mutex::new(Vec::new()));
        let raw_mutex: *mut Mutex<Vec<CbRecord>> = Box::into_raw(bx);
        (raw_mutex as *mut libc::c_void, raw_mutex)
    }

    unsafe fn udata_from_ptr(ptr: *mut libc::c_void) -> UData {
        // UData's inner field is private; transmute is fine for tests.
        std::mem::transmute::<*mut libc::c_void, UData>(ptr)
    }

    #[test]
    fn do_receive_cb_calls_callback_for_subscribed_topic() {
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(vec![None]));
        let mempool: Arc<Mutex<Mempool>> = Arc::new(Mutex::new(Mempool::new()));
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(vec![mempool.clone()]));
        let senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(vec![Sender {
            sender_topic: "from_topic".to_string(),
            connection_key: 1,
            last_mess_num: 0,
            last_mess_num_preview: 0,
            last_mess_num_saved: 0,
        }]));
        let listener_topic: Arc<Mutex<HashMap<i32, String>>> =
            Arc::new(Mutex::new(HashMap::from([(7, "to_topic".to_string())])));

        let data = b"hello";
        let msg = Message::new(&mempool, 1, 7, 1, data, false).unwrap();
        messages.lock().unwrap()[0] = Some(vec![msg]);

        let (udata_ptr, raw_mutex) = make_udata_ptr();
        let udata = unsafe { udata_from_ptr(udata_ptr) };
        let mut buff = vec![0u8; 16];

        do_receive_cb(
            &messages,
            &mempools,
            &senders,
            &listener_topic,
            test_receive_cb,
            &mut buff,
            &udata,
        );

        let records = unsafe { &*raw_mutex }.lock().unwrap().clone();
        unsafe { drop(Box::from_raw(raw_mutex)); }

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, "to_topic");
        assert_eq!(records[0].1, "from_topic");
        assert_eq!(records[0].2, data);
    }

    #[test]
    fn do_receive_cb_does_not_call_callback_for_unsubscribed_topic() {
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(vec![None]));
        let mempool: Arc<Mutex<Mempool>> = Arc::new(Mutex::new(Mempool::new()));
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(vec![mempool.clone()]));
        let senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(vec![Sender {
            sender_topic: "from_topic".to_string(),
            connection_key: 1,
            last_mess_num: 0,
            last_mess_num_preview: 0,
            last_mess_num_saved: 0,
        }]));
        let listener_topic: Arc<Mutex<HashMap<i32, String>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let msg = Message::new(&mempool, 1, 123, 1, b"hello", false).unwrap();
        messages.lock().unwrap()[0] = Some(vec![msg]);

        let (udata_ptr, raw_mutex) = make_udata_ptr();
        let udata = unsafe { udata_from_ptr(udata_ptr) };
        let mut buff = vec![0u8; 16];

        do_receive_cb(
            &messages,
            &mempools,
            &senders,
            &listener_topic,
            test_receive_cb,
            &mut buff,
            &udata,
        );

        let records = unsafe { &*raw_mutex }.lock().unwrap().clone();
        unsafe { drop(Box::from_raw(raw_mutex)); }

        assert!(records.is_empty());
    }

    #[test]
    fn do_receive_cb_does_not_panic_on_nul_in_topics() {
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(vec![None]));
        let mempool: Arc<Mutex<Mempool>> = Arc::new(Mutex::new(Mempool::new()));
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(vec![mempool.clone()]));
        let senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(vec![Sender {
            sender_topic: "from\0topic".to_string(),
            connection_key: 1,
            last_mess_num: 0,
            last_mess_num_preview: 0,
            last_mess_num_saved: 0,
        }]));
        let listener_topic: Arc<Mutex<HashMap<i32, String>>> =
            Arc::new(Mutex::new(HashMap::from([(7, "to\0topic".to_string())])));

        let msg = Message::new(&mempool, 1, 7, 1, b"hello", false).unwrap();
        messages.lock().unwrap()[0] = Some(vec![msg]);

        let (udata_ptr, raw_mutex) = make_udata_ptr();
        let udata = unsafe { udata_from_ptr(udata_ptr) };
        let mut buff = vec![0u8; 16];

        do_receive_cb(
            &messages,
            &mempools,
            &senders,
            &listener_topic,
            test_receive_cb,
            &mut buff,
            &udata,
        );

        let records = unsafe { &*raw_mutex }.lock().unwrap().clone();
        unsafe { drop(Box::from_raw(raw_mutex)); }

        assert_eq!(records.len(), 1);
        // we fall back to empty strings on NUL
        assert_eq!(records[0].0, "");
        assert_eq!(records[0].1, "");
    }

    #[test]
    fn concurrent_producers_and_consumer_do_not_deadlock_and_deliver_all() {
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(vec![None]));
        let mempool: Arc<Mutex<Mempool>> = Arc::new(Mutex::new(Mempool::new()));
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(vec![mempool.clone()]));
        let senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(vec![Sender {
            sender_topic: "from_topic".to_string(),
            connection_key: 1,
            last_mess_num: 0,
            last_mess_num_preview: 0,
            last_mess_num_saved: 0,
        }]));
        let listener_topic: Arc<Mutex<HashMap<i32, String>>> =
            Arc::new(Mutex::new(HashMap::from([(7, "to_topic".to_string())])));

        let receive_thread_cvar: Arc<(Mutex<bool>, Condvar)> =
            Arc::new((Mutex::new(false), Condvar::new()));

        let (udata_ptr, raw_mutex) = make_udata_ptr();
        let udata = unsafe { udata_from_ptr(udata_ptr) };
        let raw_mutex_addr = raw_mutex as usize;

        let producers = 4usize;
        let per_producer = 100usize;
        let expected = producers * per_producer;

        // Consumer thread: wait for notifications and drain via do_receive_cb.
        let messages_c = messages.clone();
        let mempools_c = mempools.clone();
        let senders_c = senders.clone();
        let listener_topic_c = listener_topic.clone();
        let cvar_c = receive_thread_cvar.clone();
        let consumer = std::thread::spawn(move || {
            let mut buff = vec![0u8; 64];
            let start = std::time::Instant::now();
            loop {
                let raw_mutex = raw_mutex_addr as *const Mutex<Vec<CbRecord>>;
                let delivered = unsafe { &*raw_mutex }.lock().unwrap().len();
                if delivered >= expected {
                    break;
                }
                if start.elapsed() > Duration::from_secs(2) {
                    break;
                }

                // Wait until we likely have new messages.
                let (lock, cvar) = &*cvar_c;
                let mut started = lock.lock().unwrap();
                let has_pending = messages_c
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|m: &Option<Vec<Message>>| m.is_some());
                if !*started && !has_pending {
                    let (s, _timeout) = cvar.wait_timeout(started, Duration::from_millis(50)).unwrap();
                    started = s;
                }
                *started = false;
                drop(started);

                do_receive_cb(
                    &messages_c,
                    &mempools_c,
                    &senders_c,
                    &listener_topic_c,
                    test_receive_cb,
                    &mut buff,
                    &udata,
                );
            }
        });

        // Producers: push messages concurrently and notify consumer.
        let mut joins = Vec::new();
        for p in 0..producers {
            let messages_p = messages.clone();
            let mempool_p = mempool.clone();
            let cvar_p = receive_thread_cvar.clone();
            joins.push(std::thread::spawn(move || {
                for i in 0..per_producer {
                    let number = (p * per_producer + i) as u64 + 1;
                    let msg = Message::new(&mempool_p, 1, 7, number, b"x", false).unwrap();
                    let mut guard = messages_p.lock().unwrap();
                    if let Some(slot) = guard.get_mut(0) {
                        if let Some(v) = slot.as_mut() {
                            v.push(msg);
                        } else {
                            *slot = Some(vec![msg]);
                        }
                    }
                    drop(guard);

                    // Notify consumer.
                    let (lock, cvar) = &*cvar_p;
                    let mut started = lock.lock().unwrap();
                    *started = true;
                    cvar.notify_one();
                }
            }));
        }
        for j in joins {
            j.join().unwrap();
        }

        consumer.join().unwrap();

        let records = unsafe { &*raw_mutex }.lock().unwrap().clone();
        unsafe { drop(Box::from_raw(raw_mutex)); }

        assert_eq!(records.len(), expected);
    }
}