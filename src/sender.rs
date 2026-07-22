use crate::mempool::Mempool;
use crate::message::Message;
use crate::store::Store;
use crate::{print_error, print_debug};
use crate::settings;
use crate::common;

use std::thread::JoinHandle;
use std::time::Duration;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::{HashMap, HashSet};
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
type MessList = Vec<Option<Vec<Message>>>; 
type WriteStreamList = Vec<Arc<Mutex<WriteStream>>>; 

pub struct Sender{
    addrs_for: HashMap<String, usize>, // key addr, value addr_index
    addrs_new: Arc<Mutex<Vec<Address>>>,
    messages: Arc<Mutex<MessList>>, 
    mempools: Arc<Mutex<MempoolList>>, 
    last_mess_number: Vec<u64>,
    connection_key: Vec<i32>,
    topic_keys: HashMap<String, i32>, // listener_topic -> wire topic_key
    failed_addrs: Arc<Mutex<HashSet<String>>>,
    has_failed_addrs: Arc<AtomicBool>,
    is_new_addr: Arc<AtomicBool>,
    is_close: Arc<AtomicBool>,
    delay_write_cvar: Arc<(Mutex<bool>, Condvar)>,
    wdelay_thread: Option<JoinHandle<()>>,
}

impl Sender {
    pub fn new(db: Arc<Mutex<dyn Store>>, source_topic: &str)->Sender{
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(Vec::new()));
        let messages_ = messages.clone();
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(Vec::new()));
        let mempools_ = mempools.clone();
        let addrs_for: HashMap<String, usize> = HashMap::new();
        let addrs_new: Arc<Mutex<Vec<Address>>> = Arc::new(Mutex::new(Vec::new()));
        let mut addrs_new_ = addrs_new.clone();
        db.lock().unwrap().set_source_topic(source_topic);
        let db_thread = db.clone();
        
        let delay_write_cvar = Arc::new((Mutex::new(false), Condvar::new()));
        let delay_write_cvar_ = delay_write_cvar.clone();
        let is_new_addr = Arc::new(AtomicBool::new(false));
        let is_new_addr_ = is_new_addr.clone();
        let is_close = Arc::new(AtomicBool::new(false));
        let is_close_ = is_close.clone();
        let failed_addrs: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let failed_addrs_ = failed_addrs.clone();
        let has_failed_addrs = Arc::new(AtomicBool::new(false));
        let has_failed_addrs_ = has_failed_addrs.clone();
        let wdelay_thread = thread::spawn(move||{
            let mut streams: WriteStreamList = Vec::new();
            let mut prev_time: [u64; 2] = [common::current_time_ms(); 2];
            let mut ppl_wait = false;
            while !is_close_.load(Ordering::Relaxed){ // write delay cycle
                let (lock, cvar) = &*delay_write_cvar_;
                let mut has_new_mess = false;
                if let Ok(mut _started) = lock.lock(){
                    has_new_mess = check_has_messages(&streams, &messages_);
                    // Don't sleep if we have queued messages but streams aren't ready yet,
                    // or if we were just active in the previous loop.
                    if !has_new_mess && !ppl_wait && !is_new_addr_.load(Ordering::Relaxed){
                        *_started = false;
                        // Wake-on-work via cvar; long timeout only for periodic maintenance.
                        match cvar.wait_timeout(
                            _started,
                            Duration::from_millis(settings::SENDER_THREAD_WAIT_TIMEOUT_MS),
                        ) {
                            Ok((guard, _timeout)) => {
                                _started = guard;
                                has_new_mess = *_started;
                            }
                            Err(_) => {
                                print_error!("wdelay_thread: delay_write_cvar poisoned");
                            }
                        }
                    } 
                }
                if settings::SENDER_THREAD_IDLE_BACKOFF_MS > 0 && !has_new_mess{
                    std::thread::sleep(Duration::from_millis(settings::SENDER_THREAD_IDLE_BACKOFF_MS));
                    has_new_mess = check_has_messages(&streams, &messages_);
                }
                ppl_wait = has_new_mess;
                if has_new_mess{
                    send_mess_to_listener(&streams, &messages_, &mempools_);
                }
                let ctime = common::current_time_ms();
                if check_available_stream(&is_new_addr_, ctime, &mut prev_time[0]) {
                    append_streams(
                        &mut streams,
                        &mut addrs_new_,
                        &db_thread,
                        &messages_,
                        &mempools_,
                        &failed_addrs_,
                        &has_failed_addrs_,
                    );
                }
                if timeout_update_last_mess_number(ctime, &mut prev_time[1]){
                    update_last_mess_number(&mut streams, &db_thread, &messages_, &mempools_);
                }
                check_streams_close(&mut streams, &addrs_new_, &db_thread, &messages_, &mempools_);
            }
            close_streams(&streams, &addrs_new_, &db_thread, &messages_, &mempools_);
        });
        Self{
            addrs_for,
            addrs_new,
            messages,
            mempools,
            last_mess_number: Vec::new(),
            connection_key: Vec::new(),
            topic_keys: HashMap::new(),
            failed_addrs,
            has_failed_addrs,
            is_new_addr,
            is_close,
            delay_write_cvar,
            wdelay_thread: Some(wdelay_thread),
        }
    }

    pub fn drain_failed_addrs(&mut self) -> HashSet<String> {
        if !self.has_failed_addrs.load(Ordering::Relaxed) {
            return HashSet::new();
        }
        let taken = self
            .failed_addrs
            .lock()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default();
        // Only clear the flag if nothing new arrived during the take.
        if let Ok(g) = self.failed_addrs.lock() {
            if g.is_empty() {
                self.has_failed_addrs.store(false, Ordering::Relaxed);
            }
        }
        taken
    }

    /// True when enqueue needs the shared store (unknown addr and/or topic_key).
    pub fn needs_store_for_send(&self, addr_to: &str, listener_topic: &str) -> bool {
        !self.addrs_for.contains_key(addr_to) || !self.topic_keys.contains_key(listener_topic)
    }

    /// Warm `addrs_for` / `topic_keys` (and kick connect for new addr). Hold store only here —
    /// not across `Message::new` / compress.
    pub fn ensure_send_route(
        &mut self,
        db: &mut dyn Store,
        addr_to: &str,
        listener_topic: &str,
    ) -> bool {
        if !self.addrs_for.contains_key(addr_to) {
            if !self.append_new_state(db, addr_to, listener_topic) {
                return false;
            }
            let ix = self.addrs_for.len();
            let connection_key = match self.connection_key.get(ix) {
                Some(&ck) => ck,
                None => {
                    print_error!(&format!(
                        "ensure_send_route: connection_key missing after append, addr={}",
                        addr_to
                    ));
                    return false;
                }
            };
            self.addrs_for.insert(addr_to.to_string(), ix);
            self.addrs_new.lock().unwrap().push(Address {
                ix,
                connection_key,
                address: addr_to.to_string(),
            });
            self.is_new_addr.store(true, Ordering::Relaxed);
        }
        if !self.topic_keys.contains_key(listener_topic) {
            match db.get_topic_key(listener_topic) {
                Ok(key) => {
                    self.topic_keys.insert(listener_topic.to_owned(), key);
                }
                Err(err) => {
                    print_error!(format!(
                        "couldn't db.get_topic_key {}, err {}",
                        listener_topic, err
                    ));
                    return false;
                }
            }
        }
        true
    }

    /// Enqueue message. Caller must have warm route (`ensure_send_route` or prior sends).
    pub fn send_to(
        &mut self,
        addr_to: &str,
        listener_topic: &str,
        data: &[u8],
        at_least_once_delivery: bool,
    ) -> bool {
        let Some(&ix) = self.addrs_for.get(addr_to) else {
            print_error!(&format!("send_to: address not prepared: {}", addr_to));
            return false;
        };
        let connection_key = match self.connection_key.get(ix) {
            Some(v) => *v,
            None => {
                print_error!(&format!(
                    "send_to: connection_key index out of bounds: ix={}, addr={}",
                    ix, addr_to
                ));
                return false;
            }
        };
        let Some(&listener_topic_key) = self.topic_keys.get(listener_topic) else {
            print_error!(&format!(
                "send_to: topic_key not prepared: {}",
                listener_topic
            ));
            return false;
        };
        let number_mess = match self.last_mess_number.get(ix) {
            Some(slot) => {
                // u64 wrap is effectively unreachable; checked fail would permanently
                // wedge this connection_key with no recovery short of peer restart.
                slot.wrapping_add(1)
            }
            None => {
                print_error!(&format!(
                    "send_to: last_mess_number index out of bounds: ix={}, addr={}",
                    ix, addr_to
                ));
                return false;
            }
        };

        let mempool = match self.mempools.lock() {
            Ok(mps) => match mps.get(ix) {
                Some(mp) => mp.clone(),
                None => {
                    print_error!(&format!(
                        "send_to: mempool index out of bounds: ix={}, addr={}",
                        ix, addr_to
                    ));
                    return false;
                }
            },
            Err(_) => {
                print_error!("send_to: mempools lock poisoned");
                return false;
            }
        };
        let Some(mess) = Message::new(
            &mempool,
            connection_key,
            listener_topic_key,
            number_mess,
            data,
            at_least_once_delivery,
        ) else {
            return false;
        };
        // Commit sequence only after encode succeeded.
        self.last_mess_number[ix] = number_mess;
        self.send_mess_notify(mess, ix);
        true
    }
    
    fn send_mess_notify(&mut self, mess: Message, ix: usize){
        let (lock, cvar) = &*self.delay_write_cvar;
        if let Ok(mut _started) = lock.lock(){
            if let Ok(mut mess_lock) = self.messages.lock(){
                if let Some(slot) = mess_lock.get_mut(ix) {
                    if let Some(mbuff) = slot.as_mut() {
                        mbuff.push(mess);
                    } else {
                        *slot = Some(vec![mess]);
                    }
                } else {
                    print_error!(&format!(
                        "send_mess_notify: messages index out of bounds: {}",
                        ix
                    ));
                    drop(mess); // Drop → free_inner
                }
            } else {
                print_error!("send_mess_notify: messages lock poisoned");
                drop(mess);
            }
            if !*_started{
                *_started = true;
                cvar.notify_one();
            }
        } else {
            print_error!("send_mess_notify: delay_write_cvar lock poisoned");
            drop(mess);
        }
    }
     
    fn append_new_state(&mut self, db: &mut dyn Store, addr_to: &str, listener_topic: &str)->bool{
        let listener_name = match db.get_listener_unique_name(listener_topic, addr_to) {
            Ok(name) => name,
            Err(_) => {
                print_error!(format!("couldn't db.get_listener_unique_name {}", addr_to));
                return false;
            }
        };
        let connection_key = match db.get_connection_key_for_sender(&listener_name) {
            Ok(ck) => ck,
            Err(err) => {
                print_error!(&format!("get_connection_key_for_sender from db: {}", err));
                return false;
            }
        };
        let mut last_mess_num = match db.get_last_mess_number_for_sender(connection_key) {
            Ok(n) => n,
            Err(err) => {
                print_error!(&format!(
                    "get_last_mess_number_for_sender from db (connection_key {}): {}",
                    connection_key, err
                ));
                return false;
            }
        };

        let mempool = Arc::new(Mutex::new(Mempool::new()));
        if let Ok(last_mess) = db.load_last_message_for_sender(&mempool, connection_key) {
            if let Some(mess) = last_mess {
                if mess.number_mess > last_mess_num {
                    last_mess_num = mess.number_mess;
                }
                mess.free(&mempool);
            }
        } else {
            print_error!("db.load_last_message_for_sender");
        }
        if let Err(err) = db.save_listener_for_sender(addr_to, listener_topic, &listener_name) {
            print_error!(&format!("db.save_listener_for_sender {}", err));
        }
        if let Err(err) = db.set_sender_topic_by_connection_key_from_sender(connection_key) {
            print_error!(&format!("db.set_sender_topic_by_connection_key_from_sender {}", err));
        }

        // Commit related index vectors together so a mid-function Err cannot leave them desynced.
        self.connection_key.push(connection_key);
        self.last_mess_number.push(last_mess_num);
        if let Ok(mut mps) = self.mempools.lock() {
            mps.push(mempool);
        } else {
            print_error!("append_new_state: mempools lock poisoned at push");
            self.connection_key.pop();
            self.last_mess_number.pop();
            return false;
        }
        if let Ok(mut messages) = self.messages.lock() {
            messages.push(Some(Vec::new()));
        } else {
            print_error!("append_new_state: messages lock poisoned at push");
            self.connection_key.pop();
            self.last_mess_number.pop();
            if let Ok(mut mps) = self.mempools.lock() {
                mps.pop();
            }
            return false;
        }
        true
    }

    pub fn load_prev_connects(&mut self, db: &mut dyn Store){
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
    fn wdelay_thread_notify(&self){
        let (lock, cvar) = &*self.delay_write_cvar;
        *lock.lock().unwrap() = true;
        cvar.notify_one();
    }
}

fn send_mess_to_listener(streams: &WriteStreamList, 
                         messages: &Arc<Mutex<MessList>>,
                         mempools: &Arc<Mutex<MempoolList>>){
    for (ix, mess) in messages.lock().unwrap().iter().enumerate(){
        if let Some(stream) = streams.get(ix){            
            if let Some(mess) = mess.as_ref(){
                if let Some(last) = mess.last() {
                    let has_mess =
                        last.number_mess > stream.lock().unwrap().last_send_mess_number;
                    if has_mess {
                        write_stream(stream, messages, mempools);
                    }
                }
            }
        }
    }
}

fn check_has_messages(streams: &WriteStreamList, messages: &Arc<Mutex<MessList>>)->bool{
    let mut has_mess = false;
    for (ix, mess) in messages.lock().unwrap().iter().enumerate(){
        if let Some(stream) = streams.get(ix){            
            if let Some(mess) = mess.as_ref(){
                let Some(last) = mess.last() else {
                    continue;
                };
                if let Ok(stream) = stream.lock(){
                    has_mess = !stream.is_active && last.number_mess > stream.last_send_mess_number;
                    if has_mess{
                        break;
                    }
                }
            }
        }
    }
    has_mess
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
                           db: &Arc<Mutex<dyn Store>>,
                           messages: &Arc<Mutex<MessList>>,
                           mempools: &Arc<Mutex<MempoolList>>){
    let mut connection_keys: Vec<i32> = Vec::new();
    for stream_lock in streams.iter(){
        if let Ok(stream) = stream_lock.lock(){
            connection_keys.push(stream.connection_key);
        }
    }
    let last_numbers: Vec<Result<u64, String>> = {
        let mut db = db.lock().unwrap();
        connection_keys
            .iter()
            .map(|ck| {
                db.get_last_mess_number_for_sender(*ck)
                    .map_err(|err| err.to_string())
            })
            .collect()
    };
    for (ix, result) in last_numbers.into_iter().enumerate(){
        match result{
            Ok(last_mess_number)=>{
                if let Some(stream_lock) = streams.get_mut(ix) {
                    if let Ok(mut s) = stream_lock.lock() {
                        s.last_mess_number = last_mess_number;
                    }
                } else {
                    print_error!(&format!("update_last_mess_number: stream index out of bounds {}", ix));
                    continue;
                }

                let mut mess_for_free = Vec::new();
                if let Ok(mut mess_lock) = messages.lock(){
                    if let Some(slot) = mess_lock.get_mut(ix) {
                        if let Some(mess) = slot.take() {
                        let mut mess_for_send = Vec::new();
                        for m in mess{
                            if last_mess_number < m.number_mess{
                                mess_for_send.push(m);
                            }else{
                                mess_for_free.push(m);
                            }
                        }
                        if !mess_for_send.is_empty(){
                            *slot = Some(mess_for_send);
                        }
                        }
                    }
                }
                if !mess_for_free.is_empty(){
                    let mempool = match mempools.lock() {
                        Ok(mps) => match mps.get(ix) {
                            Some(mp) => mp.clone(),
                            None => {
                                print_error!(&format!("update_last_mess_number: mempool index out of bounds {}", ix));
                                continue;
                            }
                        },
                        Err(_) => {
                            print_error!("update_last_mess_number: mempools lock poisoned");
                            continue;
                        }
                    };
                    for m in mess_for_free {
                        m.free(&mempool);
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
                  db: &Arc<Mutex<dyn Store>>,
                  messages: &Arc<Mutex<MessList>>,
                  mempools: &Arc<Mutex<MempoolList>>,
                  failed_addrs: &Arc<Mutex<HashSet<String>>>,
                  has_failed_addrs: &Arc<AtomicBool>){
    // Take the queue so we don't hold `addrs` across connect/db (ensure_send_route also pushes here).
    let pending: Vec<Address> = std::mem::take(&mut *addrs.lock().unwrap());
    let mut addrs_lost: Vec<Address> = Vec::new();
    for addr in pending {
        match TcpStream::connect(&addr.address){
            Ok(stream)=>{
                // Use blocking sockets for the sender side.
                // Our bytestream writer expects blocking semantics (no WouldBlock on write/flush).
                let _ = stream.set_nonblocking(false);
                let mempool = match mempools.lock() {
                    Ok(mps) => match mps.get(addr.ix) {
                        Some(mp) => mp.clone(),
                        None => {
                            print_error!(&format!("append_streams: mempool index out of bounds {}", addr.ix));
                            addrs_lost.push(addr);
                            continue;
                        }
                    },
                    Err(_) => {
                        print_error!("append_streams: mempools lock poisoned");
                        addrs_lost.push(addr);
                        continue;
                    }
                };
                match db.lock().unwrap().load_messages_for_sender(&mempool, addr.connection_key){
                    Ok(mut mess_from_db) =>{
                        if let Ok(mut mess_lock) = messages.lock(){
                            if let Some(slot) = mess_lock.get_mut(addr.ix) {
                                if let Some(mut mess_for_send) = slot.take() {
                                    mess_from_db.append(&mut mess_for_send);
                                }
                                *slot = Some(mess_from_db);
                            } else {
                                print_error!(&format!("append_streams: messages index out of bounds {}", addr.ix));
                            }
                        }
                    },
                    Err(err)=>{
                        print_error!(&format!("db.load_messages_for_sender, {} {}", addr.address, err));
                    }
                }
                let mut last_ack_mess_number: u64 = 0;
                if let Ok(num) = db.lock().unwrap().get_last_mess_number_for_sender(addr.connection_key){
                    last_ack_mess_number = num;
                }else{
                    print_error!(format!("couldn't db.get_last_mess_number_for_sender {}", addr.address));
                }
                // Drop already-ACKed messages loaded from the store so reconnect cannot
                // re-inflate the mempool with persisted at-least-once payloads.
                if last_ack_mess_number > 0 {
                    if let Ok(mut mess_lock) = messages.lock() {
                        if let Some(slot) = mess_lock.get_mut(addr.ix) {
                            if let Some(mut queued) = slot.take() {
                                let mut keep = Vec::new();
                                for m in queued.drain(..) {
                                    if last_ack_mess_number < m.number_mess {
                                        keep.push(m);
                                    } else {
                                        m.free(&mempool);
                                    }
                                }
                                if !keep.is_empty() {
                                    *slot = Some(keep);
                                }
                            }
                        }
                    }
                }
                let wstream = WriteStream{ix: addr.ix,
                                                       connection_key: addr.connection_key,  
                                                       address: addr.address.clone(),
                                                       stream: Arc::new(Some(stream)), 
                                                       last_send_mess_number: last_ack_mess_number,
                                                       last_mess_number: last_ack_mess_number,
                                                       is_active: false, has_close_request: false, is_closed: false};
                while addr.ix >= streams.len() {
                    streams.push(Arc::new(Mutex::new(WriteStream::new())));
                }
                streams[addr.ix] = Arc::new(Mutex::new(wstream));                
            },
            Err(_err)=>{
                if let Ok(mut failed) = failed_addrs.lock() {
                    failed.insert(addr.address.clone());
                }
                has_failed_addrs.store(true, Ordering::Relaxed);
                print_debug!(&format!("tcp connect, {} {}", _err, addr.address));
                if let Ok(mut mess_lock) = messages.lock() {
                    if let Some(slot) = mess_lock.get_mut(addr.ix) {
                        if let Some(mess) = slot.take() {
                            // Release messages before db — append_new_state/emit hold db then messages.
                            drop(mess_lock);
                            save_mess_to_db(mess, db, addr.ix, addr.connection_key, mempools);
                        }
                    } else {
                        print_error!(&format!("append_streams: messages index out of bounds on connect fail {}", addr.ix));
                    }
                } else {
                    print_error!("append_streams: messages lock poisoned");
                }
                addrs_lost.push(addr);
            }
        }
    }
    // Re-queue failures; keep any addresses pushed by ensure_send_route while we were connecting.
    if !addrs_lost.is_empty() {
        addrs.lock().unwrap().extend(addrs_lost);
    }
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
        if let Some(tcp_stream) = arc_stream.as_ref(){
            let mut buff: Vec<Message> = Vec::new();
            let mut writer = BufWriter::with_capacity(settings::WRITE_BUFFER_CAPASITY, tcp_stream); 
            let mempool = match mempools.lock() {
                Ok(mps) => match mps.get(ix) {
                    Some(mp) => mp.clone(),
                    None => {
                        print_error!(&format!("write_stream: mempool index out of bounds {}", ix));
                        if let Ok(mut s) = stream.lock() {
                            s.is_active = false;
                            s.has_close_request = true;
                        }
                        return;
                    }
                },
                Err(_) => {
                    print_error!("write_stream: mempools lock poisoned");
                    if let Ok(mut s) = stream.lock() {
                        s.is_active = false;
                        s.has_close_request = true;
                    }
                    return;
                }
            };
            loop{
                let mess_for_send = match messages.lock() {
                    Ok(mut ml) => ml.get_mut(ix).and_then(|s| s.take()),
                    Err(_) => None,
                };
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
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                is_shutdown = true;
                break;
            }
            let mut last_mess_number = 0;
            if let Ok(stream) = stream.lock(){
                last_mess_number = stream.last_mess_number;
            }
            let mut mess_no_send: Vec<Message> = Vec::new();
            let mut mess_for_free: Vec<Message> = Vec::new();
            for mess in buff{
                let num_mess = mess.number_mess;
                let at_least_once_delivery = mess.at_least_once_delivery();
                if (is_shutdown || at_least_once_delivery) && last_mess_number < num_mess{
                    mess_no_send.push(mess);
                }else{
                    mess_for_free.push(mess);
                }
            }
            if !mess_for_free.is_empty(){
                for mess in mess_for_free{
                    mess.free(&mempool);
                }
            }
            if let Ok(mut mess_lock) = messages.lock(){
                if let Some(slot) = mess_lock.get_mut(ix) {
                    if let Some(mut mess) = slot.take(){
                        mess_no_send.append(&mut mess);
                    }
                    if !mess_no_send.is_empty(){
                        *slot = Some(mess_no_send);
                    }
                } else {
                    print_error!(&format!("write_stream: messages index out of bounds {}", ix));
                    for m in mess_no_send {
                        m.free(&mempool);
                    }
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
                       db: &Arc<Mutex<dyn Store>>,
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
               
                if let Ok(mut ml) = messages.lock() {
                    if let Some(slot) = ml.get_mut(ix) {
                        if let Some(mess) = slot.take() {
                            drop(ml);
                            save_mess_to_db(mess, db, ix, connection_key, mempools);
                        }
                    } else {
                        print_error!(&format!("check_streams_close: messages index out of bounds {}", ix));
                    }
                } else {
                    print_error!("check_streams_close: messages lock poisoned");
                }
                addrs_new.lock().unwrap().push(Address{ix, connection_key, address});
            
                stream.is_closed = true;
            }
        }
    }  
}

fn save_mess_to_db(mess: Vec<Message>, db: &Arc<Mutex<dyn Store>>,
                   ix: usize, connection_key: i32, mempools: &Arc<Mutex<MempoolList>>){                    
    let mut last_send_mess_number: u64 = 0;
    if let Ok(num) = db.lock().unwrap().get_last_mess_number_for_sender(connection_key){
        last_send_mess_number = num;
    }else{
        print_error!(format!("couldn't db.get_last_mess_number_for_sender, connection_key {}", connection_key));
    }
    let mempool = match mempools.lock() {
        Ok(mps) => match mps.get(ix) {
            Some(mp) => mp.clone(),
            None => {
                print_error!(&format!("save_mess_to_db: mempool index out of bounds {}", ix));
                return;
            }
        },
        Err(_) => {
            print_error!("save_mess_to_db: mempools lock poisoned");
            return;
        }
    };

    // Important: free messages we are not going to persist. The remaining ones are freed by
    // `save_messages_from_sender` on the store (frees encoded messages internally).
    let mut to_save: Vec<Message> = Vec::new();
    let mut to_free: Vec<Message> = Vec::new();
    for m in mess {
        if m.at_least_once_delivery() && m.number_mess > last_send_mess_number {
            to_save.push(m);
        } else {
            to_free.push(m);
        }
    }

    if !to_save.is_empty() {
        if let Err(err) = db
            .lock()
            .unwrap()
            .save_messages_from_sender(&mempool, connection_key, to_save)
        {
            print_error!(&format!(
                "db.save_messages_from_sender, connection_key {}, err {}",
                connection_key, err
            ));
        }
    }

    for m in to_free {
        m.free(&mempool);
    }
}

fn close_streams(streams: &WriteStreamList,
                 addrs_new: &Arc<Mutex<Vec<Address>>>,
                 db: &Arc<Mutex<dyn Store>>,
                 messages: &Arc<Mutex<MessList>>,
                 mempools: &Arc<Mutex<MempoolList>>){
    for stream in streams.iter(){
        if let Ok(mut stream) = stream.lock(){
            stream.has_close_request = true;
        }
    }
    let mut pending: Vec<(usize, i32, Vec<Message>)> = Vec::new();
    {
        let mut messages = messages.lock().unwrap();
        for (ix, mess) in messages.iter_mut().enumerate() {
            if let Some(mess_for_send) = mess.take() {
                if mess_for_send.is_empty() {
                    continue;
                }
                let connection_key = streams
                    .get(ix)
                    .and_then(|s| s.lock().ok().map(|s| s.connection_key))
                    .or_else(|| {
                        addrs_new
                            .lock()
                            .ok()
                            .and_then(|addrs| addrs.iter().find(|a| a.ix == ix).map(|a| a.connection_key))
                    });
                if let Some(connection_key) = connection_key {
                    pending.push((ix, connection_key, mess_for_send));
                }
            }
        }
    }
    for (ix, connection_key, mess_for_send) in pending {
        save_mess_to_db(mess_for_send, db, ix, connection_key, mempools);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if cond() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        false
    }

    #[test]
    fn write_stream_sends_message_and_clears_queue_for_at_most_once() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).unwrap();
        client.set_read_timeout(Some(Duration::from_millis(500))).unwrap();

        let (server, _) = listener.accept().unwrap();
        server.set_write_timeout(Some(Duration::from_millis(500))).unwrap();

        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(vec![None]));
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(vec![mempool.clone()]));

        let mess = Message::new(&mempool, 777, 42, 1, b"hello", false).unwrap();
        messages.lock().unwrap()[0] = Some(vec![mess]);

        let ws = WriteStream {
            ix: 0,
            connection_key: 777,
            address: addr.to_string(),
            stream: Arc::new(Some(server)),
            last_send_mess_number: 0,
            last_mess_number: 0,
            is_active: false,
            has_close_request: false,
            is_closed: false,
        };
        let stream = Arc::new(Mutex::new(ws));

        write_stream(&stream, &messages, &mempools);

        assert!(
            wait_until(Duration::from_secs(1), || {
                stream.lock().unwrap().last_send_mess_number >= 1
            }),
            "write_stream didn't update last_send_mess_number in time"
        );

        let recv_mempool = Arc::new(Mutex::new(Mempool::new()));
        let mut shutdown = false;
        let received = Message::from_stream(&recv_mempool, &mut client.try_clone().unwrap(), &mut shutdown)
            .expect("expected one message");
        assert!(!shutdown);
        assert_eq!(received.number_mess, 1);
        assert_eq!(received.listener_topic_key, 42);
        assert_eq!(received.connection_key(&recv_mempool), 777);

        let mut out = Vec::new();
        let len = received.get_data(&recv_mempool, &mut out);
        assert_eq!(&out[..len], b"hello");

        // For "at most once" we should not requeue the message after successful send.
        assert!(
            messages.lock().unwrap()[0].is_none(),
            "expected queue to be empty after send"
        );
    }

    #[test]
    fn write_stream_keeps_message_in_queue_for_at_least_once_until_confirmed() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).unwrap();
        client.set_read_timeout(Some(Duration::from_millis(500))).unwrap();

        let (server, _) = listener.accept().unwrap();
        server.set_write_timeout(Some(Duration::from_millis(500))).unwrap();

        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(vec![None]));
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(vec![mempool.clone()]));

        let mess = Message::new(&mempool, 888, 123, 1, b"payload", true).unwrap();
        messages.lock().unwrap()[0] = Some(vec![mess]);

        let ws = WriteStream {
            ix: 0,
            connection_key: 888,
            address: addr.to_string(),
            stream: Arc::new(Some(server)),
            last_send_mess_number: 0,
            // Not yet confirmed by receiver (db update would set this later).
            last_mess_number: 0,
            is_active: false,
            has_close_request: false,
            is_closed: false,
        };
        let stream = Arc::new(Mutex::new(ws));

        write_stream(&stream, &messages, &mempools);

        assert!(
            wait_until(Duration::from_secs(1), || {
                stream.lock().unwrap().last_send_mess_number >= 1
            }),
            "write_stream didn't update last_send_mess_number in time"
        );

        // The data is actually sent over the wire.
        let recv_mempool = Arc::new(Mutex::new(Mempool::new()));
        let mut shutdown = false;
        let received = Message::from_stream(&recv_mempool, &mut client.try_clone().unwrap(), &mut shutdown)
            .expect("expected one message");
        assert!(!shutdown);
        assert_eq!(received.number_mess, 1);

        // But for at-least-once delivery, message stays queued until confirmed.
        let queued = messages.lock().unwrap()[0].as_ref().map(|v| v.len()).unwrap_or(0);
        assert_eq!(queued, 1, "expected message to remain queued for at-least-once");
    }

    #[test]
    fn write_stream_clears_at_least_once_after_confirmed_last_mess_number() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).unwrap();
        client.set_read_timeout(Some(Duration::from_millis(500))).unwrap();

        let (server, _) = listener.accept().unwrap();
        server.set_write_timeout(Some(Duration::from_millis(500))).unwrap();

        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(vec![None]));
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(vec![mempool.clone()]));

        let mess = Message::new(&mempool, 999, 7, 1, b"ack-me", true).unwrap();
        messages.lock().unwrap()[0] = Some(vec![mess]);

        let ws = WriteStream {
            ix: 0,
            connection_key: 999,
            address: addr.to_string(),
            stream: Arc::new(Some(server)),
            last_send_mess_number: 0,
            last_mess_number: 0,
            is_active: false,
            has_close_request: false,
            is_closed: false,
        };
        let stream = Arc::new(Mutex::new(ws));

        // First attempt sends the message but keeps it queued (at-least-once, not yet confirmed).
        write_stream(&stream, &messages, &mempools);
        assert!(
            wait_until(Duration::from_secs(1), || {
                let s = stream.lock().unwrap();
                s.last_send_mess_number >= 1 && !s.is_active
            }),
            "first write_stream didn't finish in time"
        );

        // Drain the sent message from the client side (not strictly necessary for the queue logic,
        // but keeps the TCP socket state clean).
        let recv_mempool = Arc::new(Mutex::new(Mempool::new()));
        let mut shutdown = false;
        let received =
            Message::from_stream(&recv_mempool, &mut client.try_clone().unwrap(), &mut shutdown)
                .expect("expected one message");
        assert!(!shutdown);
        assert_eq!(received.number_mess, 1);

        // Simulate confirmation from receiver (Redis update).
        stream.lock().unwrap().last_mess_number = 1;

        // Second call should free + clear the queued message without re-sending (last_send already 1).
        write_stream(&stream, &messages, &mempools);
        assert!(
            wait_until(Duration::from_secs(1), || !stream.lock().unwrap().is_active),
            "second write_stream didn't finish in time"
        );

        assert!(
            messages.lock().unwrap()[0].is_none(),
            "expected queue to be empty after confirmation"
        );
    }

    #[test]
    fn check_has_messages_does_not_panic_on_empty_message_vec() {
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(vec![Some(Vec::new())]));

        let ws = WriteStream {
            ix: 0,
            connection_key: 0,
            address: "127.0.0.1:0".to_string(),
            stream: Arc::new(None),
            last_send_mess_number: 0,
            last_mess_number: 0,
            is_active: false,
            has_close_request: false,
            is_closed: false,
        };
        let streams: WriteStreamList = vec![Arc::new(Mutex::new(ws))];

        assert!(!check_has_messages(&streams, &messages));
    }
}