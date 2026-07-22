use crate::{message::Message, mempool::Mempool, print_error};

use super::store::{DbError, DbResult, ReceiverSeedEntry, Store};
use ::redis::{Commands, ConnectionLike, ErrorKind, RedisResult};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

fn map_db<T>(r: RedisResult<T>) -> DbResult<T> {
    r.map_err(|e| DbError::new(e.to_string()))
}

fn parse_i32_res(s: &str, ctx: &'static str) -> RedisResult<i32> {
    s.parse::<i32>()
        .map_err(|_| (ErrorKind::TypeError, ctx).into())
}

fn parse_u64_res(s: &str, ctx: &'static str) -> RedisResult<u64> {
    s.parse::<u64>()
        .map_err(|_| (ErrorKind::TypeError, ctx).into())
}

/// Escape `:` / `\` so user-controlled names cannot inject Redis key path segments.
fn redis_safe(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            ':' => out.push_str("\\c"),
            _ => out.push(c),
        }
    }
    out
}

fn cache_name_key(topic: &str, address: &str) -> String {
    format!("{topic}\x1f{address}")
}

pub struct Redis {
    unique_name: String,
    source_topic: String,
    source_localhost: String,
    conn_str: String,
    conn: ::redis::Connection,
    topic_addr_cache: HashMap<String, Vec<String>>, // key: topic, value: addrs
    topic_key_cache: HashMap<String, i32>, // key: topic, value: key
    unique_name_cache: HashMap<String, String>, // key: addr, value: uname
    last_mess_number: HashMap<i32, u64>, // key: connection_key
}
impl Redis {
    pub fn new(unique_name: &str, conn_str: &str)->RedisResult<Redis>{
        let client = ::redis::Client::open(conn_str.to_string())?;
        let conn = client.get_connection()?;
        Ok(Redis{
            unique_name: unique_name.to_string(),
            source_topic: "".to_string(),
            source_localhost: "".to_string(),
            conn_str: conn_str.to_string(),
            conn,
            topic_addr_cache: HashMap::new(), 
            topic_key_cache: HashMap::new(), 
            unique_name_cache: HashMap::new(),
            last_mess_number: HashMap::new(),
        })
    }    
    pub fn redis_path(&self)->String{
        self.conn_str.clone()
    }
    pub fn set_source_topic(&mut self, topic: &str){
        self.source_topic = topic.to_string();
    } 
    pub fn set_source_localhost(&mut self, localhost: &str){
        self.source_localhost = localhost.to_string();
    }    
    pub fn regist_topic(&mut self, topic: &str)->RedisResult<()>{
        let localhost = self.source_localhost.to_string();
        let unique: String = self.unique_name.to_string();
        let topic_k = redis_safe(topic);
        let dbconn = self.get_dbconn()?;
        let () = dbconn.hset(&format!("lnr_topic:{topic_k}:addr"), localhost, unique)?;
        self.init_addresses_of_topic(topic)?;
        Ok(())
    }
    pub fn unregist_topic(&mut self, topic: &str)->RedisResult<()>{
        let localhost = self.source_localhost.to_string();
        let topic_k = redis_safe(topic);
        let dbconn = self.get_dbconn()?;
        let () = dbconn.hdel(&format!("lnr_topic:{topic_k}:addr"), localhost)?;
        self.init_addresses_of_topic(topic)?;
        Ok(())
    }
    pub fn clear_addresses_of_topic(&mut self)->RedisResult<()>{
        let topic_k = redis_safe(&self.source_topic);
        let dbconn = self.get_dbconn()?; 
        let () = dbconn.del(&format!("lnr_topic:{topic_k}:addr"))?;
        Ok(())
    }
    pub fn clear_stored_messages(&mut self)->RedisResult<()>{
        let key = format!("{}:{}", redis_safe(&self.unique_name), redis_safe(&self.source_topic));
        let addr_topic: Vec<(String, String)>;
        let conn_key_pattern = format!(
            "lnr_connection:{}:{}:*:key",
            redis_safe(&self.unique_name),
            redis_safe(&self.source_topic)
        );
        {
            let dbconn = self.get_dbconn()?;
            addr_topic = dbconn.hgetall(&format!("lnr_sender:{key}:listener"))?;
        }
        let mut connection_keys: Vec<i32> = Vec::new();
        for t in &addr_topic {
            // Prefer persisted listener name (topic\x1fname); fall back to catalog lookup.
            let listener_topic = t.1.split('\x1f').next().unwrap_or(t.1.as_str());
            let listener_name = t
                .1
                .split_once('\x1f')
                .map(|(_, n)| n.to_string())
                .filter(|n| !n.is_empty())
                .or_else(|| self.get_listener_unique_name(listener_topic, &t.0).ok());
            if let Some(listener_name) = listener_name {
                if let Ok(connection_key) = self.get_connection_key_for_sender(&listener_name) {
                    connection_keys.push(connection_key);
                }
            }
        }
        // Also pick up connection keys from the mapping keys themselves — covers orphans
        // left after kill -9 when listener-hash lookup fails.
        {
            let dbconn = self.get_dbconn()?;
            let mapped: Vec<String> = dbconn.keys(&conn_key_pattern)?;
            for map_key in mapped {
                let res: Option<String> = dbconn.get(&map_key)?;
                if let Some(res) = res {
                    if let Ok(ck) = parse_i32_res(&res, "invalid connection key") {
                        connection_keys.push(ck);
                    }
                }
            }
        }
        connection_keys.sort_unstable();
        connection_keys.dedup();
        {
            let dbconn = self.get_dbconn()?;
            for connection_key in connection_keys {
                let () = dbconn.del(&format!("lnr_connection:{connection_key}:messages"))?;
                let () = dbconn.del(&format!("lnr_connection:{connection_key}:mess_number"))?;
            }
            let () = dbconn.del(&format!("lnr_sender:{key}:listener"))?;
        }
        Ok(())
    }
           
    pub fn save_listener_for_sender(
        &mut self,
        listener_addr: &str,
        listener_topic: &str,
        listener_name: &str,
    ) -> RedisResult<()> {
        let key = format!("{}:{}", redis_safe(&self.unique_name), redis_safe(&self.source_topic));
        let dbconn = self.get_dbconn()?;
        let value = format!("{listener_topic}\x1f{listener_name}");
        let () = dbconn.hset(
            &format!("lnr_sender:{key}:listener"),
            listener_addr,
            value,
        )?;
        Ok(())
    }
    pub fn get_listeners_of_sender(&mut self) -> RedisResult<Vec<(String, String)>> {
        let key = format!("{}:{}", redis_safe(&self.unique_name), redis_safe(&self.source_topic));
        let dbconn = self.get_dbconn()?;
        let raw: Vec<(String, String)> =
            dbconn.hgetall(&format!("lnr_sender:{key}:listener"))?;
        let mut addr_topic = Vec::new();
        for (addr, value) in raw {
            let listener_topic = value
                .split_once('\x1f')
                .map(|(t, _)| t.to_string())
                .unwrap_or(value);
            addr_topic.push((addr, listener_topic));
        }
        Ok(addr_topic)
    }
    pub fn remove_sender_listeners_on_topic(&mut self, listener_topic: &str) -> RedisResult<()> {
        let key = format!("{}:{}", redis_safe(&self.unique_name), redis_safe(&self.source_topic));
        let dbconn = self.get_dbconn()?;
        let hash_key = format!("lnr_sender:{key}:listener");
        let raw: Vec<(String, String)> = dbconn.hgetall(&hash_key)?;
        for (addr, value) in raw {
            let stored_topic = value
                .split_once('\x1f')
                .map(|(t, _)| t)
                .unwrap_or(value.as_str());
            if stored_topic == listener_topic {
                let () = dbconn.hdel(&hash_key, addr)?;
            }
        }
        Ok(())
    }
    pub fn get_addresses_of_topic(&mut self, without_cache: bool, topic: &str)->RedisResult<Vec<String>>{
        if !self.topic_addr_cache.contains_key(topic) || without_cache{
            self.init_addresses_of_topic(topic)?;
        }
        Ok(self.topic_addr_cache[topic].to_vec())
    }
    pub fn get_listener_unique_name(&mut self, topic: &str, address: &str) -> RedisResult<String> {
        if !self.topic_addr_cache.contains_key(topic) {
            self.init_addresses_of_topic(topic)?;
        }
        let ck = cache_name_key(topic, address);
        if let Some(name) = self.unique_name_cache.get(&ck) {
            return Ok(name.clone());
        }
        let key = format!("{}:{}", redis_safe(&self.unique_name), redis_safe(&self.source_topic));
        let dbconn = self.get_dbconn()?;
        let value: Option<String> = dbconn.hget(
            &format!("lnr_sender:{key}:listener"),
            address,
        )?;
        if let Some(value) = value {
            if let Some((stored_topic, listener_name)) = value.split_once('\x1f') {
                if stored_topic == topic && !listener_name.is_empty() {
                    self.unique_name_cache
                        .insert(ck, listener_name.to_string());
                    return Ok(listener_name.to_string());
                }
            } else if value == topic {
                // Legacy value: topic only, no persisted listener name.
            }
        }
        // Fall back to topic catalog (addr -> unique name).
        if let Some(name) = self.unique_name_cache.get(&ck) {
            return Ok(name.clone());
        }
        Err((ErrorKind::TypeError, "!unique_name_cache.contains_key").into())
    }
    fn init_addresses_of_topic(&mut self, topic: &str)->RedisResult<()>{
        let dbconn = self.get_dbconn()?; 
        let topic_k = redis_safe(topic);
        let addrs_names: Vec<(String, String)> = dbconn.hgetall(&format!("lnr_topic:{topic_k}:addr"))?;
        let mut addrs: Vec<String> = Vec::new();
        for an in addrs_names{
            self.unique_name_cache.insert(cache_name_key(topic, &an.0), an.1);
            addrs.push(an.0);
        }            
        self.topic_addr_cache.insert(topic.to_string(), addrs);
        Ok(())
    }
   
    pub fn get_connection_key_for_sender(&mut self, listener_name: &str)->RedisResult<i32>{
        let key = format!(
            "{}:{}:{}",
            redis_safe(&self.unique_name),
            redis_safe(&self.source_topic),
            redis_safe(listener_name)
        );
        let dbconn = self.get_dbconn()?; 
        let res: RedisResult<String> = dbconn.get(format!("lnr_connection:{key}:key"));
        if let Ok(res) = res{
            parse_i32_res(&res, "invalid connection key")
        }else{
            let mut value = 0;
            self.init_connection_key(listener_name, &mut value)?;
            Ok(value)
        }
    }        
    fn init_connection_key(&mut self, listener_name: &str, value: &mut i32)->RedisResult<()>{
        let key = format!(
            "{}:{}:{}",
            redis_safe(&self.unique_name),
            redis_safe(&self.source_topic),
            redis_safe(listener_name)
        );
        let dbconn = self.get_dbconn()?; 
        *value = dbconn.incr("lnr_unique_key", 1)?;
        dbconn.set::<_,_,()>(&format!("lnr_connection:{key}:key"), value)?;
        Ok(())
    }
        
    pub fn get_topic_key(&mut self, topic: &str)->RedisResult<i32>{
        if let Some(key) = self.topic_key_cache.get(topic){
            Ok(*key)
        }else{
            let dbconn = self.get_dbconn()?; 
            let topic_k = redis_safe(topic);
            let res: RedisResult<String> = dbconn.get(&format!("lnr_topic:{topic_k}:key"));
            if let Ok(key) = res{
                let value = parse_i32_res(&key, "invalid topic key")?;
                self.topic_key_cache.insert(topic.to_owned(), value);
                Ok(value)
            }else{
                let mut value = 0;
                self.init_topic_key(topic, &mut value)?;
                self.topic_key_cache.insert(topic.to_owned(), value);
                Ok(value)
            }
        }
    }
    fn init_topic_key(&mut self, topic: &str, value: &mut i32)->RedisResult<()>{
        let dbconn = self.get_dbconn()?; 
        *value = dbconn.incr("lnr_unique_key", 1)?;
        let topic_k = redis_safe(topic);
        dbconn.set::<_,_,()>(&format!("lnr_topic:{topic_k}:key"), value)?;
        Ok(())
    }

    pub fn set_sender_topic_by_connection_key_from_sender(&mut self, connection_key: i32)->RedisResult<()>{
        let source_topic: String = self.source_topic.clone();
        let dbconn = self.get_dbconn()?;
        let () = dbconn.set(&format!("lnr_connection:{}:sender", connection_key), source_topic)?;
        Ok(())
    }
    pub fn get_sender_topic_by_connection_key(&mut self, connection_key: i32)->RedisResult<String>{
        let dbconn = self.get_dbconn()?;
        dbconn.get(&format!("lnr_connection:{}:sender", connection_key))
    }
    
    pub fn set_last_mess_number_from_listener(&mut self, connection_key: i32, val: u64)->RedisResult<()>{
        let dbconn = self.get_dbconn()?;
        let () = dbconn.set(&format!("lnr_connection:{}:mess_number", connection_key), val)?;
        self.last_mess_number.insert(connection_key, val);
        Ok(())
    }  
    pub fn get_last_mess_number_for_listener(&mut self, connection_key: i32)->RedisResult<u64>{
        if !self.last_mess_number.contains_key(&connection_key){
            let dbconn = self.get_dbconn()?; 
            // The key may be absent for a new connection. Treat missing as 0.
            let res: Option<String> = dbconn.get(&format!("lnr_connection:{}:mess_number", connection_key))?;
            let value = match res {
                Some(res) => parse_u64_res(&res, "invalid mess_number")?,
                None => {
                    // Persist the default to avoid repeated nil reads.
                    self.init_last_mess_number_from_sender(connection_key)?;
                    0
                },
            };
            self.last_mess_number.insert(connection_key, value);
        }
        Ok(self.last_mess_number[&connection_key])              
    }
     
    pub fn init_last_mess_number_from_sender(&mut self, connection_key: i32)->RedisResult<()>{
        let dbconn = self.get_dbconn()?; 
        let () = dbconn.set_nx(&format!("lnr_connection:{}:mess_number", connection_key), 0)?;
        Ok(())
    }
    pub fn get_last_mess_number_for_sender(&mut self, connection_key: i32)->RedisResult<u64>{
        let dbconn = self.get_dbconn()?; 
        // The key may legitimately be absent for a new connection. Treat missing as 0.
        let res: Option<String> = dbconn.get(&format!("lnr_connection:{}:mess_number", connection_key))?;
        match res {
            Some(res) => parse_u64_res(&res, "invalid mess_number"),
            None => {
                // Persist the default to avoid repeated nil reads.
                self.init_last_mess_number_from_sender(connection_key)?;
                Ok(0)
            }
        }
    }

    pub fn save_messages_from_sender(&mut self, mempool: &Arc<Mutex<Mempool>>, connection_key: i32, mess: Vec<Message>)->RedisResult<()>{
        let dbconn = self.get_dbconn()?; 
        let encoded = encode_and_free_messages(mempool, mess);
        for buf in encoded {
            let () = dbconn.rpush(&format!("lnr_connection:{}:messages", connection_key), buf)?;
        }        
        Ok(())
    }

    pub fn load_messages_for_sender(&mut self, mempool: &Arc<Mutex<Mempool>>, connection_key: i32)->RedisResult<Vec<Message>>{
        let dbconn = self.get_dbconn()?; 
        let llen: Option<usize> = dbconn.llen(&format!("lnr_connection:{}:messages", connection_key))?;
        let mut out = Vec::new();
        if let Some(llen) = llen{
            let buff: Vec<Vec<u8>> = dbconn.lpop(&format!("lnr_connection:{}:messages", connection_key), core::num::NonZeroUsize::new(llen))?;
            for b in buff{
                let mut is_shutdown = false;
                if let Some(mess) = Message::from_stream(mempool, &mut &b[..], &mut is_shutdown){
                    out.push(mess);
                }else{
                    print_error!("!Message::from_stream");
                }
            }
        }        
        Ok(out)
    }

    pub fn load_last_message_for_sender(&mut self, mempool: &Arc<Mutex<Mempool>>, connection_key: i32)->RedisResult<Option<Message>>{
        let dbconn = self.get_dbconn()?; 
        let llen: Option<usize> = dbconn.llen(&format!("lnr_connection:{}:messages", connection_key))?;
        let mut out = None;
        if let Some(llen) = llen{
            if llen == 0{
                return Ok(None)
            }
            let buff: Vec<Vec<u8>> = dbconn.lrange(&format!("lnr_connection:{}:messages", connection_key), -1, -1)?;
            for b in buff{
                let mut is_shutdown = false;
                if let Some(mess) = Message::from_stream(mempool, &mut &b[..], &mut is_shutdown){
                    out = Some(mess);
                }else{
                    print_error!("!Message::from_stream");
                }
            }
        }        
        Ok(out)
    }
          
    fn get_dbconn(&mut self)->RedisResult<&mut redis::Connection>{
        if !self.conn.is_open(){
            let client = redis::Client::open(self.conn_str.clone())?;
            self.conn = client.get_connection()?;
        }
        Ok(&mut self.conn)
    }  

    /// No-op: Redis uses a shared catalog; `receivers_json` seeding (including SQLite-only
    /// `conn_sender` / first `connection_key` convention) applies only to SQLite.
    pub fn seed_receivers(&mut self, _entries: &[ReceiverSeedEntry]) -> RedisResult<()> {
        Ok(())
    }
}

impl Store for Redis {
    fn set_source_topic(&mut self, topic: &str) {
        Redis::set_source_topic(self, topic);
    }

    fn set_source_localhost(&mut self, localhost: &str) {
        Redis::set_source_localhost(self, localhost);
    }

    fn regist_topic(&mut self, topic: &str) -> DbResult<()> {
        map_db(Redis::regist_topic(self, topic))
    }

    fn unregist_topic(&mut self, topic: &str) -> DbResult<()> {
        map_db(Redis::unregist_topic(self, topic))
    }

    fn clear_addresses_of_topic(&mut self) -> DbResult<()> {
        map_db(Redis::clear_addresses_of_topic(self))
    }

    fn clear_stored_messages(&mut self) -> DbResult<()> {
        map_db(Redis::clear_stored_messages(self))
    }

    fn save_listener_for_sender(
        &mut self,
        listener_addr: &str,
        listener_topic: &str,
        listener_name: &str,
    ) -> DbResult<()> {
        map_db(Redis::save_listener_for_sender(
            self,
            listener_addr,
            listener_topic,
            listener_name,
        ))
    }

    fn get_listeners_of_sender(&mut self) -> DbResult<Vec<(String, String)>> {
        map_db(Redis::get_listeners_of_sender(self))
    }

    fn remove_sender_listeners_on_topic(&mut self, listener_topic: &str) -> DbResult<()> {
        map_db(Redis::remove_sender_listeners_on_topic(self, listener_topic))
    }

    fn get_addresses_of_topic(&mut self, without_cache: bool, topic: &str) -> DbResult<Vec<String>> {
        map_db(Redis::get_addresses_of_topic(self, without_cache, topic))
    }

    fn get_listener_unique_name(&mut self, topic: &str, address: &str) -> DbResult<String> {
        map_db(Redis::get_listener_unique_name(self, topic, address))
    }

    fn get_connection_key_for_sender(&mut self, listener_name: &str) -> DbResult<i32> {
        map_db(Redis::get_connection_key_for_sender(self, listener_name))
    }

    fn get_topic_key(&mut self, topic: &str) -> DbResult<i32> {
        map_db(Redis::get_topic_key(self, topic))
    }

    fn set_sender_topic_by_connection_key_from_sender(&mut self, connection_key: i32) -> DbResult<()> {
        map_db(Redis::set_sender_topic_by_connection_key_from_sender(
            self,
            connection_key,
        ))
    }

    fn get_sender_topic_by_connection_key(&mut self, connection_key: i32) -> DbResult<String> {
        map_db(Redis::get_sender_topic_by_connection_key(self, connection_key))
    }

    fn set_last_mess_number_from_listener(&mut self, connection_key: i32, val: u64) -> DbResult<()> {
        map_db(Redis::set_last_mess_number_from_listener(
            self,
            connection_key,
            val,
        ))
    }

    fn get_last_mess_number_for_listener(&mut self, connection_key: i32) -> DbResult<u64> {
        map_db(Redis::get_last_mess_number_for_listener(self, connection_key))
    }

    fn get_last_mess_number_for_sender(&mut self, connection_key: i32) -> DbResult<u64> {
        map_db(Redis::get_last_mess_number_for_sender(self, connection_key))
    }

    fn save_messages_from_sender(
        &mut self,
        mempool: &Arc<Mutex<Mempool>>,
        connection_key: i32,
        mess: Vec<Message>,
    ) -> DbResult<()> {
        map_db(Redis::save_messages_from_sender(
            self,
            mempool,
            connection_key,
            mess,
        ))
    }

    fn load_messages_for_sender(
        &mut self,
        mempool: &Arc<Mutex<Mempool>>,
        connection_key: i32,
    ) -> DbResult<Vec<Message>> {
        map_db(Redis::load_messages_for_sender(
            self,
            mempool,
            connection_key,
        ))
    }

    fn load_last_message_for_sender(
        &mut self,
        mempool: &Arc<Mutex<Mempool>>,
        connection_key: i32,
    ) -> DbResult<Option<Message>> {
        map_db(Redis::load_last_message_for_sender(
            self,
            mempool,
            connection_key,
        ))
    }

    fn seed_receivers(&mut self, entries: &[ReceiverSeedEntry]) -> DbResult<()> {
        map_db(Redis::seed_receivers(self, entries))
    }
}

fn encode_and_free_messages(mempool: &Arc<Mutex<Mempool>>, mess: Vec<Message>) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(mess.len());
    for m in mess {
        let mut buf: Vec<u8> = Vec::new();
        m.to_stream(mempool, &mut buf);
        m.free(mempool);
        out.push(buf);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    #[ignore]
    fn roundtrip_save_then_load_messages_via_real_redis() {
        // Requires a running Redis instance.
        let redis_url =
            std::env::var("LINER_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());

        let mut c = Redis::new("it_redis_roundtrip", &redis_url).expect("redis connect failed");
        c.set_source_topic("topic_it_redis_roundtrip");
        c.set_source_localhost("127.0.0.1:0");

        // Unique connection_key for the test run.
        let connection_key: i32 = {
            let db = c.get_dbconn().expect("get_dbconn");
            db.incr("lnr_test_unique_key", 1).expect("incr")
        };
        let list_key = format!("lnr_connection:{}:messages", connection_key);

        // Clean any leftovers.
        {
            let db = c.get_dbconn().expect("get_dbconn");
            let _: () = db.del(&list_key).expect("del");
        }

        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let free_before = mempool.lock().unwrap().debug_free_len();
        let count_before = mempool.lock().unwrap().debug_free_count();

        let m1 = Message::new(mempool.clone(), connection_key, 10, 1, b"hello", true).unwrap();
        let m2 = Message::new(mempool.clone(), connection_key, 10, 2, b"world", true).unwrap();

        c.save_messages_from_sender(&mempool, connection_key, vec![m1, m2])
            .expect("save_messages_from_sender");

        let free_after_save = mempool.lock().unwrap().debug_free_len();
        let count_after_save = mempool.lock().unwrap().debug_free_count();
        assert!(free_after_save > free_before);
        assert!(count_after_save >= count_before + 2);

        {
            let db = c.get_dbconn().expect("get_dbconn");
            let llen: usize = db.llen(&list_key).expect("llen");
            assert_eq!(llen, 2);
        }

        let loaded = c
            .load_messages_for_sender(&mempool, connection_key)
            .expect("load_messages_for_sender");
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].number_mess, 1);
        assert_eq!(loaded[1].number_mess, 2);

        {
            let db = c.get_dbconn().expect("get_dbconn");
            let llen: usize = db.llen(&list_key).expect("llen");
            assert_eq!(llen, 0);
            let _: () = db.del(&list_key).expect("del cleanup");
        }
    }

    #[test]
    fn parse_helpers_reject_invalid_numbers() {
        assert!(parse_i32_res("x", "ctx").is_err());
        assert!(parse_u64_res("x", "ctx").is_err());
        assert!(parse_u64_res("-1", "ctx").is_err());
    }

    #[test]
    fn encode_and_free_messages_frees_mempool_allocations() {
        let mempool = Arc::new(Mutex::new(Mempool::new()));
        let free_before = mempool.lock().unwrap().debug_free_len();
        let count_before = mempool.lock().unwrap().debug_free_count();

        let m1 = Message::new(mempool.clone(), 1, 10, 1, b"hello", true).unwrap();
        let m2 = Message::new(mempool.clone(), 1, 10, 2, b"world", true).unwrap();

        let encoded = encode_and_free_messages(&mempool, vec![m1, m2]);
        assert_eq!(encoded.len(), 2);

        let free_after = mempool.lock().unwrap().debug_free_len();
        let count_after = mempool.lock().unwrap().debug_free_count();
        assert!(
            free_after > free_before,
            "expected mempool free_len to increase after freeing messages"
        );
        assert!(
            count_after >= count_before + 2,
            "expected at least 2 frees to be recorded"
        );

        // Sanity: encoded payload can be decoded back into a Message.
        let recv_pool = Arc::new(Mutex::new(Mempool::new()));
        let mut shutdown = false;
        let decoded =
            Message::from_stream(&recv_pool, &mut &encoded[0][..], &mut shutdown).unwrap();
        assert!(!shutdown);
        assert_eq!(decoded.number_mess, 1);
        assert_eq!(decoded.listener_topic_key, 10);
    }
}
