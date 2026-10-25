use crate::bytestream::{FeedOutcome, FrameReadState};
use crate::message::{self, Message};
use crate::mempool::Mempool;
use crate::store::Store;
use crate::settings::{self, INTERNAL_CHANNEL_TOPIC};
use crate::common;
use crate::{UCbackIntern, UData};
use crate::{print_error, print_debug};
use crate::status::{StatusEmitter, StatusMsg, LNR_LISTENER_STORE_ERROR};

use std::collections::HashMap;
use std::io::Read;
use std::thread::JoinHandle;
use std::thread;
use std::time::Duration;
use std::sync::{ Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::io;
use std::ffi::CString;

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token, Waker};

struct Sender{
    sender_topic: Arc<CString>,
    connection_key: i32,
    /// Highest number delivered to the callback: the ack to save.
    last_mess_num: u64,
    last_mess_num_saved: u64,
    /// Bumped on a store reset: a batch taken before it no longer moves `last_mess_num`.
    epoch: u64,
    /// The ack write found the stored number gone or lowered; the next session resets the slot.
    store_reset: bool,
}

type MessList = Vec<Option<Vec<Message>>>;
type MempoolList = Vec<Arc<Mutex<Mempool>>>;
type SenderList = Vec<Sender>;

const SERVER: Token = Token(usize::MAX);
const WAKER: Token = Token(usize::MAX - 1);

/// One accepted TCP. Bound to a slot once its first frame header names the sender.
/// A slot may hold several: after a reconnect the old TCP keeps draining to EOF
/// next to the new one.
struct Conn {
    tcp: TcpStream,
    bound: Option<Bound>,
}

/// Read state of a TCP bound to slot `ix`.
struct Bound {
    ix: usize,
    mempool: Arc<Mutex<Mempool>>,
    frame: FrameReadState,
}

/// Open TCPs by poll token (`0..WAKER`).
#[derive(Default)]
struct Conns {
    map: HashMap<Token, Conn>,
    seq: usize,
}

/// Stream-thread side of the slots.
#[derive(Default)]
struct Slots {
    /// Sticky connection_key → slot index. Kept across TCP close so reconnect reuses the
    /// same senders/mempools/messages index (never hand a freed ix to another peer).
    by_key: HashMap<i32, usize>,
    /// Replay filter by slot: the highest number accepted from any of its TCPs, so a resend on
    /// a new TCP is not delivered twice while the stored ack lags. Lowered only by a store reset.
    cursor: Vec<u64>,
}

/// Shared state the stream thread delivers into.
struct ReadCtx {
    senders: Arc<Mutex<SenderList>>,
    messages: Arc<Mutex<MessList>>,
    mempools: Arc<Mutex<MempoolList>>,
    db: Arc<Mutex<dyn Store>>,
    receive_thread_cvar: Arc<(Mutex<bool>, Condvar)>,
    internal_topic_key: Arc<AtomicI32>,
    status_emitter: StatusEmitter,
}

/// Slot index `ix` is shared across `senders[ix]`, `mempools[ix]` and `messages[ix]`,
/// and is owned by one `connection_key` for the life of the listener.
/// Do **not** recycle `ix` into a free-list for a different peer:
/// the mempool and `last_mess_num` / `connection_key` on that slot belong to the original peer.
/// The sender connects from an ephemeral port, so a reconnect is recognised by the
/// `connection_key` in its first frame header, not by `SocketAddr` (see [`Conns::on_ready`]).
pub struct Listener{
    stream_thread: Option<JoinHandle<()>>,
    receive_thread: Option<JoinHandle<()>>,
    receive_thread_cvar: Arc<(Mutex<bool>, Condvar)>,
    listener_topic: Arc<Mutex<HashMap<i32, Arc<CString>>>>,
    internal_topic_key: Arc<AtomicI32>,
    is_close: Arc<AtomicBool>,
    waker: Arc<Waker>,
}

fn topic_cstring(topic: &str) -> Arc<CString> {
    Arc::new(CString::new(topic.as_bytes()).unwrap_or_else(|_| CString::new("").unwrap()))
}

impl Listener {
    pub fn new(mut listener: TcpListener,
               db: Arc<Mutex<dyn Store>>, source_topic: &str, subscriptions: &HashMap<i32, String>, receive_cb: UCbackIntern, udata: UData,
               status_emitter: StatusEmitter)->Result<Listener, String>{
        #[cfg(test)]
        if test_force_listener_new_error_load() {
            return Err("test inject: listener new failed".to_string());
        }
        let mut poll = Poll::new().map_err(|e| format!("couldn't create poll queue: {}", e))?;
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(Vec::new()));
        let messages_ = messages.clone();
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(Vec::new()));
        let mempools_= mempools.clone();
        let senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(Vec::new()));
        let senders_ = senders.clone();
        db.lock().map_err(|_| "db lock poisoned".to_string())?.set_source_topic(source_topic);
        let db_ = db.clone();
        let status_emitter_stream = status_emitter.clone();
        let status_emitter_recv = status_emitter;
      
        let listener_topic: Arc<Mutex<HashMap<i32, Arc<CString>>>> = Arc::new(Mutex::new(HashMap::new()));
        let internal_topic_key = Arc::new(AtomicI32::new(-1));
        let topic_key = db
            .lock()
            .map_err(|_| "db lock poisoned".to_string())?
            .get_topic_key(source_topic)
            .map_err(|e| format!("couldn't db.get_topic_key: {}", e))?;
        if let Ok(mut lr) = listener_topic.lock(){ 
            lr.insert(topic_key, topic_cstring(source_topic));
            if source_topic == INTERNAL_CHANNEL_TOPIC {
                internal_topic_key.store(topic_key, Ordering::Relaxed);
            }
            for s in subscriptions.iter(){
                lr.insert(*s.0, topic_cstring(s.1));
                if s.1.as_str() == INTERNAL_CHANNEL_TOPIC {
                    internal_topic_key.store(*s.0, Ordering::Relaxed);
                }
            }
        }

        let receive_thread_cvar: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));
        let receive_thread_cvar_ = receive_thread_cvar.clone();

        poll.registry()
            .register(&mut listener, SERVER, Interest::READABLE)
            .map_err(|e| format!("couldn't register listener: {}", e))?;
        let waker = Arc::new(
            Waker::new(poll.registry(), WAKER).map_err(|e| format!("couldn't create Waker: {}", e))?,
        );
        let internal_topic_key_stream = internal_topic_key.clone();
        let stream_thread = thread::spawn(move|| {            
            let ctx = ReadCtx {
                senders,
                messages: messages_,
                mempools,
                db,
                receive_thread_cvar: receive_thread_cvar_,
                internal_topic_key: internal_topic_key_stream,
                status_emitter: status_emitter_stream,
            };
            let mut conns = Conns::default();
            let mut slots = Slots::default();
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
                            conns.accept(&poll, &listener);
                        }
                        WAKER => {
                            has_wake = true;
                            break;
                        }
                        token => {
                            conns.on_ready(&poll, token, ev.is_read_closed(), &ctx, &mut slots);
                        }
                    }
                }
                if has_wake{
                    break;
                }
            }
            update_last_mess_number(&ctx.senders, &ctx.db, &ctx.status_emitter);
        });

        let receive_thread_cvar_ = receive_thread_cvar.clone();
        let is_close = Arc::new(AtomicBool::new(false));
        let is_close_ = is_close.clone();
        let listener_topic_ = listener_topic.clone();
        let receive_thread = thread::spawn(move|| {
            let mut prev_time: [u64; 1] = [common::current_time_ms(); 1];
            let mut buff_data: Vec<u8> = vec![0; 4086];
            let mut decomp_scratch: Vec<u8> = Vec::new();
            let mut mess_from_buff: Vec<Option<Vec<Message>>> = Vec::new();
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
                    do_receive_cb(&messages, &mempools_, &senders_, &listener_topic_, receive_cb, &mut buff_data, &mut decomp_scratch, &mut mess_from_buff, &udata); 
                } 
                let ctime = common::current_time_ms();
                if timeout_update_last_mess_number(ctime, &mut prev_time[0]){                    
                    update_last_mess_number(&senders_, &db_, &status_emitter_recv);
                }
            }
        });        
        Ok(Self{            
            stream_thread: Some(stream_thread),
            receive_thread: Some(receive_thread),
            receive_thread_cvar,
            listener_topic,
            internal_topic_key,
            is_close,
            waker
        })
    }
    pub fn subscribe(&mut self, topic: &str, topic_key: i32){
        self.listener_topic.lock().unwrap().insert(topic_key, topic_cstring(topic));
        if topic == INTERNAL_CHANNEL_TOPIC {
            self.internal_topic_key.store(topic_key, Ordering::Relaxed);
        }
    }   
    pub fn unsubscribe(&mut self, topic_key: i32){
        if self.internal_topic_key.load(Ordering::Relaxed) == topic_key {
            self.internal_topic_key.store(-1, Ordering::Relaxed);
        }
        self.listener_topic.lock().unwrap().remove(&topic_key);
    }
}

#[cfg(test)]
static FORCE_LISTENER_NEW_ERR: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
fn test_force_listener_new_error_load() -> bool {
    FORCE_LISTENER_NEW_ERR.load(Ordering::SeqCst)
}

/// Clears the inject flag on drop (use with [`CLIENT_RUN_TEST_LOCK`] / `client_run_test_lock`).
#[cfg(test)]
pub struct ForceListenerNewErrorGuard;

#[cfg(test)]
impl Drop for ForceListenerNewErrorGuard {
    fn drop(&mut self) {
        FORCE_LISTENER_NEW_ERR.store(false, Ordering::SeqCst);
    }
}

/// Test-only: force [`Listener::new`] to return `Err` until the guard is dropped.
/// Caller must hold the shared client-run test lock so other `run` tests cannot race.
#[cfg(test)]
pub fn test_force_listener_new_error() -> ForceListenerNewErrorGuard {
    FORCE_LISTENER_NEW_ERR.store(true, Ordering::SeqCst);
    ForceListenerNewErrorGuard
}

fn do_receive_cb(message_buffer: &Arc<Mutex<MessList>>,
                 mempools: &Arc<Mutex<MempoolList>>,
                 senders: &Arc<Mutex<SenderList>>,
                 listener_topic: &Arc<Mutex<HashMap<i32, Arc<CString>>>>,
                 receive_cb: UCbackIntern,
                 buff_data: &mut Vec<u8>,
                 decomp_scratch: &mut Vec<u8>,
                 mess_from_buff: &mut Vec<Option<Vec<Message>>>,
                 udata: &UData){

    mess_from_buff.clear();
    let epochs: Vec<u64> = {
        let mut buffer = message_buffer.lock().unwrap();
        for m in buffer.iter_mut(){
            mess_from_buff.push(m.take());
        }
        // Under the buffer lock: a store reset empties the buffer and bumps the epoch under it too.
        match senders.lock() {
            Ok(s) => s.iter().map(|s| s.epoch).collect(),
            Err(_) => Vec::new(),
        }
    };
    for (ix, mess) in mess_from_buff.drain(..).enumerate(){
        if let Some(mess) = mess{
            let topic_from = match senders.lock() {
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
            let mut last_mess_num = 0;
            let mut topic_cstr_cache: HashMap<i32, Arc<CString>> = HashMap::new();
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
                        topic_cstr_cache.insert(m.listener_topic_key, topic);
                    }
                }
                if let Some(topic_to) = topic_cstr_cache.get(&m.listener_topic_key) {
                    let mlen = m.get_data(&mempool, buff_data, decomp_scratch);
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
            // `topic_from` must live until after `for m in mess` (as_ptr in receive_cb).
            drop(topic_from);
            if let Ok(mut senders) = senders.lock() {
                if let Some(sender) = senders.get_mut(ix) {
                    if Some(&sender.epoch) == epochs.get(ix) && sender.last_mess_num < last_mess_num {
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

/// First-header peek of an unbound TCP.
enum Peek {
    /// Header not complete yet.
    Wait,
    Key(i32),
    /// EOF, error or invalid frame length.
    Drop,
}

/// Peek the first frame header for its `connection_key` (nothing is consumed).
/// `read_closed` comes from the poll event: a peer that closes after a partial header
/// leaves those bytes peekable forever.
fn peek_connection_key(tcp: &TcpStream, read_closed: bool) -> Peek {
    let mut hdr = [0u8; message::WIRE_CONNECTION_KEY_END];
    match tcp.peek(&mut hdr) {
        Ok(n) if n == hdr.len() => match message::connection_key_from_wire(&hdr) {
            Some(connection_key) => Peek::Key(connection_key),
            None => {
                print_error!("invalid first frame header");
                Peek::Drop
            }
        },
        Ok(0) => Peek::Drop,
        Ok(_) if read_closed => Peek::Drop,
        Ok(_) => Peek::Wait,
        Err(err) if matches!(err.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) => Peek::Wait,
        Err(err) => {
            print_error!(&format!("pending stream peek: {}", err));
            Peek::Drop
        }
    }
}

impl Conns {
    fn accept(&mut self, poll: &Poll, listener: &TcpListener) {
        loop {
            match listener.accept() {
                Ok((mut tcp, _addr)) => {
                    let _ = tcp.set_nodelay(true);
                    // Skip tokens still held by live TCPs once `seq` wraps.
                    let mut token = Token(self.seq % WAKER.0);
                    self.seq = self.seq.wrapping_add(1);
                    while self.map.contains_key(&token) {
                        token = Token(self.seq % WAKER.0);
                        self.seq = self.seq.wrapping_add(1);
                    }
                    if let Ok(()) = poll.registry().register(&mut tcp, token, Interest::READABLE){
                        self.map.insert(token, Conn{tcp, bound: None});
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

    /// Readiness on a TCP: bind it to its sender's slot on the first complete header,
    /// then read what arrived. A finished TCP is dropped; its slot stays.
    fn on_ready(&mut self,
                poll: &Poll,
                token: Token,
                read_closed: bool,
                ctx: &ReadCtx,
                slots: &mut Slots) {
        let Some(conn) = self.map.get_mut(&token) else {
            return;
        };
        if conn.bound.is_none() {
            match peek_connection_key(&conn.tcp, read_closed) {
                Peek::Wait => return,
                Peek::Drop => {}
                Peek::Key(connection_key) => {
                    if let Some((ix, mempool)) = slot_for_connection_key(connection_key, slots, ctx) {
                        if start_session(ix, &mempool, &mut slots.cursor[ix], ctx) {
                            // The sender cleared its store: TCPs of its old session are stale.
                            self.close_slot(poll, ix);
                        }
                        if let Some(conn) = self.map.get_mut(&token) {
                            conn.bound = Some(Bound { ix, mempool, frame: FrameReadState::new() });
                        }
                    }
                }
            }
        }
        let Some(conn) = self.map.get_mut(&token) else {
            return;
        };
        let is_open = match conn.bound.as_mut() {
            Some(bound) => read_conn(&mut conn.tcp, bound, &mut slots.cursor[bound.ix], ctx),
            None => false,
        };
        if !is_open {
            if let Some(mut conn) = self.map.remove(&token) {
                if let Some(bound) = conn.bound.as_mut() {
                    bound.frame.abort(&bound.mempool);
                }
                let _ = poll.registry().deregister(&mut conn.tcp);
            }
        }
    }

    /// Drop every TCP bound to slot `ix`.
    fn close_slot(&mut self, poll: &Poll, ix: usize) {
        self.map.retain(|_, conn| {
            let Some(bound) = conn.bound.as_mut() else {
                return true;
            };
            if bound.ix != ix {
                return true;
            }
            bound.frame.abort(&bound.mempool);
            let _ = poll.registry().deregister(&mut conn.tcp);
            false
        });
    }
}

/// Slot owned by `connection_key`, created on first sight. Returns its index and mempool.
fn slot_for_connection_key(connection_key: i32,
                           slots: &mut Slots,
                           ctx: &ReadCtx) -> Option<(usize, Arc<Mutex<Mempool>>)> {
    let Ok(mut mempools) = ctx.mempools.lock() else {
        print_error!("slot_for_connection_key: mempools lock poisoned");
        return None;
    };
    if let Some(&ix) = slots.by_key.get(&connection_key) {
        return mempools.get(ix).map(|mp| (ix, mp.clone()));
    }
    let mempool = Arc::new(Mutex::new(Mempool::new()));
    mempools.push(mempool.clone());
    let ix = mempools.len() - 1;
    if let Ok(mut s) = ctx.senders.lock() {
        s.push(Sender{sender_topic: topic_cstring(""), connection_key, last_mess_num: 0, last_mess_num_saved: 0,
                      epoch: 0, store_reset: false});
    } else {
        print_error!("slot_for_connection_key: senders lock poisoned");
    }
    if let Ok(mut mb) = ctx.messages.lock() {
        mb.push(None);
    } else {
        print_error!("slot_for_connection_key: messages lock poisoned");
    }
    slots.by_key.insert(connection_key, ix);
    slots.cursor.push(0);
    Some((ix, mempool))
}

/// Read everything available on a bound TCP into its slot. Returns `false` once the TCP is done.
/// `last_mess_num` is the slot's replay cursor ([`Slots::cursor`]).
fn read_conn(tcp: &mut TcpStream, bound: &mut Bound, last_mess_num: &mut u64, ctx: &ReadCtx) -> bool {
    let ReadCtx { messages, receive_thread_cvar, internal_topic_key, .. } = ctx;
    let Bound { ix, mempool, frame } = bound;
    let ix = *ix;
    let mut mess_buff: Vec<Message> = Vec::with_capacity(settings::LISTENER_RECEIVE_FLUSH_BATCH);
    let mut is_shutdown = false;
    let mut scratch = [0u8; settings::BYTESTREAM_READ_BUFFER_SIZE];
    loop {
        let n = match tcp.read(&mut scratch) {
            Ok(0) => {
                is_shutdown = true;
                break;
            }
            Ok(n) => n,
            Err(err) => {
                let k = err.kind();
                if k == io::ErrorKind::Interrupted {
                    continue;
                }
                if k == io::ErrorKind::WouldBlock {
                    break;
                }
                print_error!(&format!("read_conn: {}", k));
                is_shutdown = true;
                break;
            }
        };
        let mut off = 0;
        while off < n {
            let (used, outcome) = frame.feed(&scratch[off..n], mempool);
            if used == 0 {
                break;
            }
            off += used;
            match outcome {
                FeedOutcome::NeedMore => break,
                FeedOutcome::Shutdown => {
                    is_shutdown = true;
                    break;
                }
                FeedOutcome::Frame { mem_pos, mem_len } => {
                    let Some(mess) = Message::from_alloc(mempool, mem_pos, mem_len) else {
                        is_shutdown = true;
                        break;
                    };
                    let ik = internal_topic_key.load(Ordering::Relaxed);
                    let internal = ik >= 0 && mess.listener_topic_key == ik;
                    // Replay filter is for application payloads. Internal subscribe/unsubscribe
                    // events must still run after a peer restarts (message numbers reset to 1)
                    // or the same peer reconnects with a new TCP session.
                    if internal || mess.number_mess > *last_mess_num {
                        if mess.number_mess > *last_mess_num {
                            *last_mess_num = mess.number_mess;
                        }
                        mess_buff.push(mess);
                        if mess_buff.len() >= settings::LISTENER_RECEIVE_FLUSH_BATCH {
                            flush_received_messages(ix, &mut mess_buff, messages, receive_thread_cvar, mempool);
                        }
                    } else {
                        mess.free(mempool);
                    }
                }
            }
        }
        if is_shutdown {
            break;
        }
    }
    flush_received_messages(ix, &mut mess_buff, messages, receive_thread_cvar, mempool);
    !is_shutdown
}

/// A new TCP on slot `ix`: re-read the stored ack and lift the replay `cursor` to it.
/// A stored ack below the one this listener saved (or a failed ack write) means the sender ran
/// `clear_stored_messages` and numbers from the stored ack again: the slot restarts from it,
/// queued messages are dropped and batches already taken by the receive thread no longer move
/// the ack. Returns `true` on such a reset. Resolves the sender topic on first use.
fn start_session(ix: usize, mempool: &Arc<Mutex<Mempool>>, cursor: &mut u64, ctx: &ReadCtx) -> bool {
    let (connection_key, has_topic) = {
        let Ok(senders) = ctx.senders.lock() else {
            print_error!("start_session: senders lock poisoned");
            return false;
        };
        let Some(sender) = senders.get(ix) else {
            print_error!(&format!("start_session: sender index out of bounds {}", ix));
            return false;
        };
        (sender.connection_key, !sender.sender_topic.as_bytes().is_empty())
    };
    if !has_topic {
        let topic = topic_cstring(&get_sender_topic(&ctx.db, connection_key, &ctx.status_emitter));
        if let Some(sender) = ctx.senders.lock().ok().as_mut().and_then(|s| s.get_mut(ix)) {
            sender.sender_topic = topic;
        }
    }
    // Lock order: db, messages, senders. The db stays locked through the compare: an ack
    // written in between would otherwise look like a reset.
    let Ok(mut db) = ctx.db.lock() else {
        print_error!("start_session: db lock poisoned");
        return false;
    };
    let Some(stored) = get_last_mess_number(&mut *db, ix, ctx) else {
        return false;
    };
    let Ok(mut messages) = ctx.messages.lock() else {
        print_error!("start_session: messages lock poisoned");
        return false;
    };
    let Ok(mut senders) = ctx.senders.lock() else {
        print_error!("start_session: senders lock poisoned");
        return false;
    };
    let Some(sender) = senders.get_mut(ix) else {
        return false;
    };
    if !sender.store_reset && stored >= sender.last_mess_num_saved {
        sender.last_mess_num_saved = stored;
        sender.last_mess_num = sender.last_mess_num.max(stored);
        *cursor = (*cursor).max(stored);
        return false;
    }
    print_debug!(&format!("store reset for connection_key {}: ack {} -> {}",
                          connection_key, sender.last_mess_num_saved, stored));
    if let Some(queued) = messages.get_mut(ix).and_then(Option::take) {
        for m in queued {
            m.free(mempool);
        }
    }
    sender.epoch += 1;
    sender.store_reset = false;
    sender.last_mess_num = stored;
    sender.last_mess_num_saved = stored;
    *cursor = stored;
    true
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
                           db: &Arc<Mutex<dyn Store>>,
                           status_emitter: &StatusEmitter){
    let updates: Vec<(usize, i32, u64, u64, u64, Arc<CString>)> = {
        let senders = senders.lock().unwrap();
        let mut out = Vec::new();
        for (ix, sender) in senders.iter().enumerate() {
            if sender.connection_key >= 0 && !sender.store_reset
                && sender.last_mess_num_saved < sender.last_mess_num {
                out.push((
                    ix,
                    sender.connection_key,
                    sender.last_mess_num_saved,
                    sender.last_mess_num,
                    sender.epoch,
                    sender.sender_topic.clone(),
                ));
            }
        }
        out
    };
    if updates.is_empty() {
        return;
    }
    // (ix, epoch, written ack or `None` when the store was reset under us)
    let mut succeeded: Vec<(usize, u64, Option<u64>)> = Vec::with_capacity(updates.len());
    {
        let mut db = db.lock().unwrap();
        for (ix, connection_key, saved, last_mess_num, epoch, sender_topic) in updates {
            match db.set_last_mess_number_from_listener(connection_key, saved, last_mess_num) {
                Ok(written) => succeeded.push((ix, epoch, written.then_some(last_mess_num))),
                Err(err) => {
                    print_error!(&format!("couldn't db.set_last_mess_number: {}", err));
                    if status_emitter.is_enabled() {
                        let ck = connection_key.to_string();
                        let err_s = err.to_string();
                        status_emitter.emit_msg(
                            LNR_LISTENER_STORE_ERROR,
                            sender_topic.to_str().unwrap_or(""),
                            "",
                            StatusMsg::SetLastMessNumberFromListener,
                            &[&ck, &err_s],
                        );
                    }
                }
            }
        }
    }
    if succeeded.is_empty() {
        return;
    }
    let mut senders = senders.lock().unwrap();
    for (ix, epoch, written) in succeeded {
        let Some(sender) = senders.get_mut(ix) else {
            continue;
        };
        if sender.epoch != epoch {
            continue;
        }
        match written {
            Some(last_mess_num) => {
                if sender.last_mess_num_saved < last_mess_num {
                    sender.last_mess_num_saved = last_mess_num;
                }
            }
            None => sender.store_reset = true,
        }
    }
}

/// Stored ack of slot `ix`, `None` on a store error (reported).
fn get_last_mess_number(db: &mut dyn Store, ix: usize, ctx: &ReadCtx) -> Option<u64> {
    let (connection_key, sender_topic) = {
        let senders = ctx.senders.lock().ok()?;
        let sender = senders.get(ix)?;
        (sender.connection_key, sender.sender_topic.clone())
    };
    match db.get_last_mess_number_for_listener(connection_key){
        Ok(num)=>{
            Some(num)
        },
        Err(err)=>{
            print_error!(&format!("couldn't get_last_mess_number_for_listener: {}", err));
            if ctx.status_emitter.is_enabled() {
                let err_s = err.to_string();
                ctx.status_emitter.emit_msg(
                    LNR_LISTENER_STORE_ERROR,
                    sender_topic.to_str().unwrap_or(""),
                    "",
                    StatusMsg::GetLastMessNumberForListener,
                    &[&err_s],
                );
            }
            None
        }
    }
}

fn get_sender_topic(db: &Arc<Mutex<dyn Store>>, connection_key: i32,
                    status_emitter: &StatusEmitter)->String{
    match db.lock().unwrap().get_sender_topic_by_connection_key(connection_key){
        Ok(v)=>{
            v
        },
        Err(err)=>{
            print_error!(&format!("couldn't get_sender_topic, conn_key {}, err {}", connection_key, err));
            if status_emitter.is_enabled() {
                let ck = connection_key.to_string();
                let err_s = err.to_string();
                status_emitter.emit_msg(
                    LNR_LISTENER_STORE_ERROR,
                    "",
                    "",
                    StatusMsg::GetSenderTopicByConnectionKey,
                    &[&ck, &err_s],
                );
            }
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

fn flush_received_messages(
    ix: usize,
    mess_buff: &mut Vec<Message>,
    messages: &Arc<Mutex<MessList>>,
    receive_thread_cvar: &Arc<(Mutex<bool>, Condvar)>,
    mempool: &Arc<Mutex<Mempool>>,
) {
    if mess_buff.is_empty() {
        return;
    }
    let (lock, cvar) = &**receive_thread_cvar;
    if let Ok(mut started) = lock.lock() {
        if let Ok(mut mess_lock) = messages.lock() {
            if let Some(slot) = mess_lock.get_mut(ix) {
                if let Some(mess_for_receive) = slot.as_mut() {
                    mess_for_receive.append(mess_buff);
                } else {
                    *slot = Some(std::mem::take(mess_buff));
                }
            } else {
                print_error!(&format!("read_conn: messages index out of bounds for slot {}", ix));
                for m in mess_buff.drain(..) {
                    m.free(mempool);
                }
            }
        }
        // Always wake: skipping notify when *started is already true can miss
        // the receive thread entering wait_timeout → ~LISTENER wait spikes.
        *started = true;
        cvar.notify_one();
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

    fn test_sender_slot(topic: &str) -> Sender {
        Sender {
            sender_topic: topic_cstring(topic),
            connection_key: 1,
            last_mess_num: 0,
            last_mess_num_saved: 0,
            epoch: 0,
            store_reset: false,
        }
    }

    fn call_do_receive_cb(
        messages: &Arc<Mutex<MessList>>,
        mempools: &Arc<Mutex<MempoolList>>,
        senders: &Arc<Mutex<SenderList>>,
        listener_topic: &Arc<Mutex<HashMap<i32, Arc<CString>>>>,
        buff: &mut Vec<u8>,
        udata: &UData,
    ) {
        let mut scratch = Vec::new();
        let mut mess_from_buff = Vec::new();
        do_receive_cb(
            messages,
            mempools,
            senders,
            listener_topic,
            test_receive_cb,
            buff,
            &mut scratch,
            &mut mess_from_buff,
            udata,
        );
    }

    #[test]
    fn do_receive_cb_calls_callback_for_subscribed_topic() {
        let messages: Arc<Mutex<MessList>> = Arc::new(Mutex::new(vec![None]));
        let mempool: Arc<Mutex<Mempool>> = Arc::new(Mutex::new(Mempool::new()));
        let mempools: Arc<Mutex<MempoolList>> = Arc::new(Mutex::new(vec![mempool.clone()]));
        let senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(vec![test_sender_slot("from_topic")]));
        let listener_topic: Arc<Mutex<HashMap<i32, Arc<CString>>>> =
            Arc::new(Mutex::new(HashMap::from([(7, topic_cstring("to_topic"))])));

        let data = b"hello";
        let msg = Message::new(mempool.clone(), 1, 7, 1, data, false).unwrap();
        messages.lock().unwrap()[0] = Some(vec![msg]);

        let (udata_ptr, raw_mutex) = make_udata_ptr();
        let udata = unsafe { udata_from_ptr(udata_ptr) };
        let mut buff = vec![0u8; 16];

        call_do_receive_cb(
            &messages,
            &mempools,
            &senders,
            &listener_topic,
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
        let senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(vec![test_sender_slot("from_topic")]));
        let listener_topic: Arc<Mutex<HashMap<i32, Arc<CString>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let msg = Message::new(mempool.clone(), 1, 123, 1, b"hello", false).unwrap();
        messages.lock().unwrap()[0] = Some(vec![msg]);

        let (udata_ptr, raw_mutex) = make_udata_ptr();
        let udata = unsafe { udata_from_ptr(udata_ptr) };
        let mut buff = vec![0u8; 16];

        call_do_receive_cb(
            &messages,
            &mempools,
            &senders,
            &listener_topic,
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
        let senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(vec![test_sender_slot("from\0topic")]));
        let listener_topic: Arc<Mutex<HashMap<i32, Arc<CString>>>> =
            Arc::new(Mutex::new(HashMap::from([(7, topic_cstring("to\0topic"))])));

        let msg = Message::new(mempool.clone(), 1, 7, 1, b"hello", false).unwrap();
        messages.lock().unwrap()[0] = Some(vec![msg]);

        let (udata_ptr, raw_mutex) = make_udata_ptr();
        let udata = unsafe { udata_from_ptr(udata_ptr) };
        let mut buff = vec![0u8; 16];

        call_do_receive_cb(
            &messages,
            &mempools,
            &senders,
            &listener_topic,
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
        let senders: Arc<Mutex<SenderList>> = Arc::new(Mutex::new(vec![test_sender_slot("from_topic")]));
        let listener_topic: Arc<Mutex<HashMap<i32, Arc<CString>>>> =
            Arc::new(Mutex::new(HashMap::from([(7, topic_cstring("to_topic"))])));

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
            let mut scratch = Vec::new();
            let mut mess_from_buff = Vec::new();
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
                    &mut scratch,
                    &mut mess_from_buff,
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
                    let msg = Message::new(mempool_p.clone(), 1, 7, number, b"x", false).unwrap();
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

    #[test]
    fn tcp_split_frame_does_not_spin_on_wouldblock() {
        use std::io::{ErrorKind, Write};
        use std::net::TcpListener;
        use std::time::Instant;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = std::net::TcpStream::connect(addr).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        let _ = server.set_nodelay(true);

        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let msg = Message::new(mempool.clone(), 1, 7, 1, b"split-tcp", false).unwrap();
        let mut wire = Vec::new();
        assert!(msg.to_stream(&mempool, &mut wire));
        assert!(wire.len() > 4);

        client.write_all(&wire[..2]).unwrap();
        client.flush().unwrap();

        let mut frame = FrameReadState::new();
        let mut scratch = [0u8; settings::BYTESTREAM_READ_BUFFER_SIZE];
        let mut got_partial = false;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            match server.read(&mut scratch) {
                Ok(0) => panic!("peer closed before split header"),
                Ok(n) => match frame.feed(&scratch[..n], &mempool).1 {
                    FeedOutcome::NeedMore => got_partial = true,
                    FeedOutcome::Frame { .. } => panic!("complete frame from header fragment"),
                    FeedOutcome::Shutdown => panic!("shutdown on header fragment"),
                },
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    if got_partial {
                        break;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => panic!("{err}"),
            }
        }
        assert!(got_partial, "expected 2-byte header to reach feed");
        let spin_start = Instant::now();
        match server.read(&mut scratch) {
            Err(err) if err.kind() == ErrorKind::WouldBlock => {}
            Ok(0) => panic!("eof while waiting for rest of frame"),
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) => panic!("{err}"),
        }
        assert!(
            spin_start.elapsed() < Duration::from_millis(200),
            "WouldBlock after partial header spun {:?}",
            spin_start.elapsed()
        );

        client.write_all(&wire[2..]).unwrap();
        client.flush().unwrap();

        let mut decoded = None;
        let rest_start = Instant::now();
        while rest_start.elapsed() < Duration::from_secs(2) {
            match server.read(&mut scratch) {
                Ok(0) => panic!("eof before complete frame"),
                Ok(n) => {
                    match frame.feed(&scratch[..n], &mempool).1 {
                        FeedOutcome::Frame { mem_pos, mem_len } => {
                            decoded = Message::from_alloc(&mempool, mem_pos, mem_len);
                        }
                        FeedOutcome::NeedMore => {}
                        FeedOutcome::Shutdown => panic!("shutdown finishing split frame"),
                    }
                    if decoded.is_some() {
                        break;
                    }
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => panic!("{err}"),
            }
        }
        let decoded = decoded.expect("split TCP frame should complete without 10s spin");
        assert!(rest_start.elapsed() < Duration::from_secs(2));
        let mut out = Vec::new();
        let mut decomp = Vec::new();
        let n = decoded.get_data(&mempool, &mut out, &mut decomp);
        assert_eq!(&out[..n], b"split-tcp");
    }

    fn test_ctx() -> ReadCtx {
        let db: Arc<Mutex<dyn Store>> = Arc::new(Mutex::new(
            crate::store::sqlite::Sqlite::new("listener_conns_test", ":memory:").unwrap()));
        ReadCtx {
            senders: Arc::new(Mutex::new(Vec::new())),
            messages: Arc::new(Mutex::new(Vec::new())),
            mempools: Arc::new(Mutex::new(Vec::new())),
            db,
            receive_thread_cvar: Arc::new((Mutex::new(false), Condvar::new())),
            internal_topic_key: Arc::new(AtomicI32::new(-1)),
            status_emitter: StatusEmitter::new(),
        }
    }

    fn wire_frame(connection_key: i32, number: u64) -> Vec<u8> {
        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let msg = Message::new(mempool.clone(), connection_key, 7, number, b"slot", false).unwrap();
        let mut wire = Vec::new();
        assert!(msg.to_stream(&mempool, &mut wire));
        wire
    }

    fn connect_and_send(addr: std::net::SocketAddr, connection_key: i32, number: u64) -> std::net::TcpStream {
        use std::io::Write;
        let mut client = std::net::TcpStream::connect(addr).unwrap();
        client.write_all(&wire_frame(connection_key, number)).unwrap();
        client
    }

    /// Test harness for the stream thread's poll loop.
    struct Harness {
        poll: Poll,
        listener: TcpListener,
        conns: Conns,
        slots: Slots,
        ctx: ReadCtx,
    }

    impl Harness {
        fn new() -> Self {
            let poll = Poll::new().unwrap();
            let mut listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            poll.registry().register(&mut listener, SERVER, Interest::READABLE).unwrap();
            Harness { poll, listener, conns: Conns::default(), slots: Slots::default(), ctx: test_ctx() }
        }

        fn addr(&self) -> std::net::SocketAddr {
            self.listener.local_addr().unwrap()
        }

        /// Run the poll loop until `done` holds (panics after 2 s).
        fn pump_until(&mut self, done: impl Fn(&Harness) -> bool) {
            let mut events = Events::with_capacity(16);
            let start = std::time::Instant::now();
            while !done(self) {
                assert!(start.elapsed() < Duration::from_secs(2), "pump_until timed out");
                self.poll.poll(&mut events, Some(Duration::from_millis(20))).unwrap();
                for ev in &events {
                    match ev.token() {
                        SERVER => self.conns.accept(&self.poll, &self.listener),
                        token => self.conns.on_ready(&self.poll, token, ev.is_read_closed(),
                                                     &self.ctx, &mut self.slots),
                    }
                }
            }
        }

        fn received(&self, ix: usize) -> Vec<u64> {
            self.ctx.messages.lock().unwrap().get(ix).and_then(|s| s.as_ref())
                .map(|v| v.iter().map(|m| m.number_mess).collect()).unwrap_or_default()
        }
    }

    #[test]
    fn reconnect_shares_slot_while_old_tcp_drains() {
        use std::io::Write;
        let mut h = Harness::new();
        let addr = h.addr();

        let mut first = connect_and_send(addr, 42, 1);
        h.pump_until(|h| h.received(0).len() == 1);

        // Reconnect from a new ephemeral port: same slot, both TCPs stay open. The resend of 1
        // is a duplicate even though the stored ack (0) lags.
        let mut second = connect_and_send(addr, 42, 1);
        assert_ne!(second.local_addr().unwrap().port(), first.local_addr().unwrap().port());
        h.pump_until(|h| h.conns.map.values().all(|c| c.bound.is_some()) && h.conns.map.len() == 2);
        second.write_all(&wire_frame(42, 2)).unwrap();
        h.pump_until(|h| h.received(0).len() == 2);

        // Tail written to the old TCP after the new one is bound: the slot cursor filters
        // what the new TCP already delivered.
        first.write_all(&wire_frame(42, 2)).unwrap();
        first.write_all(&wire_frame(42, 3)).unwrap();
        h.pump_until(|h| h.received(0).last() == Some(&3));
        assert_eq!(h.received(0), vec![1, 2, 3]);
        assert_eq!(h.ctx.mempools.lock().unwrap().len(), 1);

        // Old TCP closes; the slot and the new TCP stay.
        drop(first);
        h.pump_until(|h| h.conns.map.len() == 1);

        let _other = connect_and_send(addr, 7, 1);
        h.pump_until(|h| h.received(1).len() == 1);
        let s = h.ctx.senders.lock().unwrap();
        assert_eq!((s[0].connection_key, s[1].connection_key), (42, 7));
        assert_eq!(h.ctx.mempools.lock().unwrap().len(), 2);
        assert_eq!(h.slots.by_key.len(), 2);
    }

    #[test]
    fn store_reset_restarts_slot() {
        use std::io::Write;
        let mut h = Harness::new();
        let mut old = connect_and_send(h.addr(), 42, 1);
        old.write_all(&wire_frame(42, 2)).unwrap();
        h.pump_until(|h| h.received(0).len() == 2);
        // The listener saved ack 2, then the sender cleared its store (stored ack is 0 again).
        {
            let mut s = h.ctx.senders.lock().unwrap();
            s[0].last_mess_num = 2;
            s[0].last_mess_num_saved = 2;
        }

        // Restarted sender numbers from 1: the old session's TCP and queue are dropped.
        let _new = connect_and_send(h.addr(), 42, 1);
        h.pump_until(|h| h.received(0) == vec![1]);
        assert_eq!(h.conns.map.len(), 1);
        let s = h.ctx.senders.lock().unwrap();
        assert_eq!((s[0].epoch, s[0].last_mess_num, s[0].last_mess_num_saved), (1, 0, 0));
        assert_eq!(h.slots.cursor[0], 1);
    }

    #[test]
    fn ack_write_detects_store_reset() {
        let ctx = test_ctx();
        let mut db = ctx.db.lock().unwrap();
        // No row yet (bind-time read failed): counts as 0 for the first save only.
        assert!(!db.set_last_mess_number_from_listener(41, 2, 4).unwrap());
        assert!(db.set_last_mess_number_from_listener(41, 0, 4).unwrap());
        assert_eq!(db.get_last_mess_number_for_listener(41).unwrap(), 4);
        assert_eq!(db.get_last_mess_number_for_listener(42).unwrap(), 0);
        assert!(db.set_last_mess_number_from_listener(42, 0, 3).unwrap());
        assert_eq!(db.get_last_mess_number_for_listener(42).unwrap(), 3);
        drop(db);

        // Saved 5 but the store holds 3: a reset, nothing written.
        ctx.senders.lock().unwrap().push(Sender{sender_topic: topic_cstring("s"), connection_key: 42,
            last_mess_num: 7, last_mess_num_saved: 5, epoch: 0, store_reset: false});
        update_last_mess_number(&ctx.senders, &ctx.db, &ctx.status_emitter);
        assert!(ctx.senders.lock().unwrap()[0].store_reset);
        assert_eq!(ctx.db.lock().unwrap().get_last_mess_number_for_listener(42).unwrap(), 3);
    }

    #[test]
    fn replay_is_dropped() {
        use std::io::Write;
        let mut h = Harness::new();
        let mut tcp = connect_and_send(h.addr(), 42, 1);
        // Replay of 1 on the same TCP is dropped; frames arrive in order, so 2 marks the end.
        tcp.write_all(&wire_frame(42, 1)).unwrap();
        tcp.write_all(&wire_frame(42, 2)).unwrap();
        h.pump_until(|h| h.received(0).last() == Some(&2));
        assert_eq!(h.received(0), vec![1, 2]);
    }

    #[test]
    fn connection_closed_mid_header_is_dropped() {
        use std::io::Write;
        let mut h = Harness::new();
        let mut client = std::net::TcpStream::connect(h.addr()).unwrap();
        client.write_all(&[0, 0, 0]).unwrap();
        h.pump_until(|h| h.conns.map.len() == 1);
        assert!(h.conns.map.values().all(|c| c.bound.is_none()), "partial header waits unbound");

        drop(client);
        h.pump_until(|h| h.conns.map.is_empty());
        assert!(h.ctx.mempools.lock().unwrap().is_empty(), "no slot for an unidentified peer");
    }
}
