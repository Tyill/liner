use crate::{message::Message, mempool::Mempool, print_error};

use redis::{Commands, ConnectionLike, RedisResult, ErrorKind};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Connect{
    unique_name: String,
    source_topic: String,
    source_localhost: String,
    conn_str: String,
    conn: redis::Connection,
    topic_addr_cache: HashMap<String, Vec<String>>, // key: topic, value: addrs
    topic_key_cache: HashMap<String, i32>, // key: topic, value: key
    unique_name_cache: HashMap<String, String>, // key: addr, value: uname
    last_mess_number: HashMap<i32, u64>, // key: connection_key
}
impl Connect {
    pub fn new(unique_name: &str, conn_str: &str)->RedisResult<Connect>{
        let client = redis::Client::open(conn_str.to_string())?;
        let conn = client.get_connection()?;
        Ok(Connect{
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
        let dbconn = self.get_dbconn()?;
        let () = dbconn.hset(&format!("topic:{}:addr", topic), localhost, unique)?;
        self.init_addresses_of_topic(topic)?;
        Ok(())
    }
    pub fn unregist_topic(&mut self, topic: &str)->RedisResult<()>{
        let localhost = self.source_localhost.to_string();
        let dbconn = self.get_dbconn()?;
        let () = dbconn.hdel(&format!("topic:{}:addr", topic), localhost)?;
        self.init_addresses_of_topic(topic)?;
        Ok(())
    }
    pub fn clear_addresses_of_topic(&mut self)->RedisResult<()>{
        let source_topic = self.source_topic.to_string();
        let dbconn = self.get_dbconn()?; 
        let () = dbconn.del(&format!("topic:{}:addr", source_topic))?;
        Ok(())
    }
    pub fn clear_stored_messages(&mut self)->RedisResult<()>{
        let key = format!("{}:{}", self.unique_name, self.source_topic);
        let addr_topic: Vec<(String, String)>;
        {
            let dbconn = self.get_dbconn()?;
            addr_topic = dbconn.hgetall(&format!("sender:{}:listener", key))?;
        }
        for t in addr_topic{
            if let Ok(listener_name) = self.get_listener_unique_name(&t.1, &t.0){
                if let Ok(connection_key) = self.get_connection_key_for_sender(&listener_name){
                    let dbconn = self.get_dbconn()?;
                    let () = dbconn.del(&format!("connection:{}:messages", connection_key))?;
                    let () = dbconn.del(&format!("connection:{}:mess_number", connection_key))?;
                }
            }
        }
        let dbconn = self.get_dbconn()?;
        let () = dbconn.del(&format!("sender:{}:listener", key))?;       
        Ok(())
    }
           
    pub fn save_listener_for_sender(&mut self, listener_addr: &str, listener_topic: &str)->RedisResult<()>{
        let key = format!("{}:{}", self.unique_name, self.source_topic);
        let dbconn = self.get_dbconn()?;
        let () = dbconn.hset(&format!("sender:{}:listener", key), listener_addr, listener_topic)?;
        Ok(())
    }
    pub fn get_listeners_of_sender(&mut self)->RedisResult<Vec<(String, String)>>{
        let key = format!("{}:{}", self.unique_name, self.source_topic);
        let dbconn = self.get_dbconn()?;
        let addr_topic: Vec<(String, String)> = dbconn.hgetall(&format!("sender:{}:listener", key))?;
        Ok(addr_topic)
    }
    pub fn get_addresses_of_topic(&mut self, without_cache: bool, topic: &str)->RedisResult<Vec<String>>{
        if !self.topic_addr_cache.contains_key(topic) || without_cache{
            self.init_addresses_of_topic(topic)?;
        }
        Ok(self.topic_addr_cache[topic].to_vec())
    }
    pub fn get_listener_unique_name(&mut self, topic: &str, address: &str)->RedisResult<String>{
        if !self.topic_addr_cache.contains_key(topic){
            self.init_addresses_of_topic(topic)?;
        }
        if self.unique_name_cache.contains_key(address){
            return Ok(self.unique_name_cache[address].to_string());
        }
        Err((ErrorKind::TypeError, "!unique_name_cache.contains_key").into())
    }
    fn init_addresses_of_topic(&mut self, topic: &str)->RedisResult<()>{
        let dbconn = self.get_dbconn()?; 
        let addrs_names: Vec<(String, String)> = dbconn.hgetall(&format!("topic:{}:addr", topic))?;
        let mut addrs: Vec<String> = Vec::new();
        for an in addrs_names{
            self.unique_name_cache.insert(an.0.clone(), an.1);
            addrs.push(an.0);
        }            
        self.topic_addr_cache.insert(topic.to_string(), addrs);
        Ok(())
    }
   
    pub fn get_connection_key_for_sender(&mut self, listener_name: &str)->RedisResult<i32>{
        let key = format!("{}:{}:{}", self.unique_name, self.source_topic, listener_name);
        let dbconn = self.get_dbconn()?; 
        let res: RedisResult<String> = dbconn.get(format!("connection:{}:key", key));
        if let Ok(res) = res{
            Ok(res.parse::<i32>().unwrap())
        }else{
            let mut value = 0;
            self.init_connection_key(listener_name, &mut value)?;
            Ok(value)
        }
    }        
    fn init_connection_key(&mut self, listener_name: &str, value: &mut i32)->RedisResult<()>{
        let key = format!("{}:{}:{}", self.unique_name, self.source_topic, listener_name);
        let dbconn = self.get_dbconn()?; 
        *value = dbconn.incr("unique_key", 1)?;
        dbconn.set(&format!("connection:{}:key", key), value)?;
        Ok(())
    }
        
    pub fn get_topic_key(&mut self, topic: &str)->RedisResult<i32>{
        if let Some(key) = self.topic_key_cache.get(topic){
            Ok(*key)
        }else{
            let dbconn = self.get_dbconn()?; 
            let res: RedisResult<String> = dbconn.get(&format!("topic:{}:key", topic));
            if let Ok(key) = res{
                let value = key.parse::<i32>().unwrap();
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
        *value = dbconn.incr("unique_key", 1)?;
        dbconn.set(&format!("topic:{}:key", topic), value)?;
        Ok(())
    }

    pub fn set_sender_topic_by_connection_key_from_sender(&mut self, connection_key: i32)->RedisResult<()>{
        let source_topic: String = self.source_topic.clone();
        let dbconn = self.get_dbconn()?;
        let () = dbconn.set(&format!("connection:{}:sender", connection_key), source_topic)?;
        Ok(())
    }
    pub fn get_sender_topic_by_connection_key(&mut self, connection_key: i32)->RedisResult<String>{
        let dbconn = self.get_dbconn()?;
        dbconn.get(&format!("connection:{}:sender", connection_key))
    }
    
    pub fn set_last_mess_number_from_listener(&mut self, connection_key: i32, val: u64)->RedisResult<()>{
        let dbconn = self.get_dbconn()?;
        let () = dbconn.set(&format!("connection:{}:mess_number", connection_key), val)?;
        self.last_mess_number.insert(connection_key, val);
        Ok(())
    }  
    pub fn get_last_mess_number_for_listener(&mut self, connection_key: i32)->RedisResult<u64>{
        if !self.last_mess_number.contains_key(&connection_key){
            let dbconn = self.get_dbconn()?; 
            let res: String = dbconn.get(&format!("connection:{}:mess_number", connection_key))?;
            self.last_mess_number.insert(connection_key, res.parse::<u64>().unwrap());
        }
        Ok(self.last_mess_number[&connection_key])              
    }
     
    pub fn init_last_mess_number_from_sender(&mut self, connection_key: i32)->RedisResult<()>{
        let dbconn = self.get_dbconn()?; 
        let () = dbconn.set_nx(&format!("connection:{}:mess_number", connection_key), 0)?;
        Ok(())
    }
    pub fn get_last_mess_number_for_sender(&mut self, connection_key: i32)->RedisResult<u64>{
        let dbconn = self.get_dbconn()?; 
        let res: String = dbconn.get(&format!("connection:{}:mess_number", connection_key))?;
        Ok(res.parse::<u64>().unwrap())
    }

    pub fn save_messages_from_sender(&mut self, mempool: &Arc<Mutex<Mempool>>, connection_key: i32, mess: Vec<Message>)->RedisResult<()>{
        let dbconn = self.get_dbconn()?; 
        for m in mess{
            let mut buf: Vec<u8> = Vec::new(); 
            m.to_stream(mempool, &mut buf);                
            let () = dbconn.rpush(&format!("connection:{}:messages", connection_key), buf)?;
        }
        Ok(())
    }

    pub fn load_messages_for_sender(&mut self, mempool: &Arc<Mutex<Mempool>>, connection_key: i32)->RedisResult<Vec<Message>>{
        let dbconn = self.get_dbconn()?; 
        let llen: Option<usize> = dbconn.llen(&format!("connection:{}:messages", connection_key))?;
        let mut out = Vec::new();
        if let Some(llen) = llen{
            let buff: Vec<Vec<u8>> = dbconn.lpop(&format!("connection:{}:messages", connection_key), core::num::NonZeroUsize::new(llen))?;
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
        let llen: Option<usize> = dbconn.llen(&format!("connection:{}:messages", connection_key))?;
        let mut out = None;
        if let Some(llen) = llen{
            if llen == 0{
                return Ok(None)
            }
            let buff: Vec<Vec<u8>> = dbconn.lrange(&format!("connection:{}:messages", connection_key), -1, -1)?;
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
}