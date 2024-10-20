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
    unique_name_cache: HashMap<String, String>, // key: addr, value: uname
    last_mess_number: HashMap<String, u64>, // key: sender_name,sender_topic,listener_name,listener_topic
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
        dbconn.hset(&format!("topic:{}:addr", topic), localhost, unique)?;
        self.init_addresses_of_topic(topic)?;
        Ok(())
    }
    pub fn unregist_topic(&mut self, topic: &str)->RedisResult<()>{
        let localhost = self.source_localhost.to_string();
        let dbconn = self.get_dbconn()?;
        dbconn.hdel(&format!("topic:{}:addr", topic), localhost)?;
        self.init_addresses_of_topic(topic)?;
        Ok(())
    }
    pub fn clear_addresses_of_topic(&mut self)->RedisResult<()>{
        let source_topic = self.source_topic.to_string();
        let dbconn = self.get_dbconn()?; 
        dbconn.del(&format!("topic:{}:addr", source_topic))?;
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
                let listener_topic = t.1;
                let key = format!("{}:{}:{}", key, listener_name, listener_topic);
                let dbconn = self.get_dbconn()?;
                dbconn.del(&format!("connection:{}:messages", key))?;
                dbconn.del(&format!("connection:{}:mess_number", key))?;
            }
        }
        let dbconn = self.get_dbconn()?;
        dbconn.del(&format!("sender:{}:listener", key))?;       
        Ok(())
    }
           
    pub fn save_listener_for_sender(&mut self, listener_addr: &str, listener_topic: &str)->RedisResult<()>{
        let key = format!("{}:{}", self.unique_name, self.source_topic);
        let dbconn = self.get_dbconn()?;
        dbconn.hset(&format!("sender:{}:listener", key), listener_addr, listener_topic)?;
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
   
    pub fn set_last_mess_number_from_listener(&mut self, sender_name: &str, sender_topic: &str, listener_topic: &str, val: u64)->RedisResult<()>{
        let key = format!("{}:{}:{}:{}", sender_name, sender_topic, self.unique_name, listener_topic);
        let dbconn = self.get_dbconn()?;
        dbconn.set(&format!("connection:{}:mess_number", key), val)?;
        self.last_mess_number.insert(key, val);
        Ok(())
    }  
    pub fn get_last_mess_number_for_listener(&mut self, sender_name: &str, sender_topic: &str, listener_topic: &str)->RedisResult<u64>{
        let key = format!("{}:{}:{}:{}", sender_name, sender_topic, self.unique_name, listener_topic);
        if !self.last_mess_number.contains_key(&key){
            let dbconn = self.get_dbconn()?; 
            let res: String = dbconn.get(&format!("connection:{}:mess_number", key))?;
            self.last_mess_number.insert(key.clone(), res.parse::<u64>().unwrap());
        }
        Ok(self.last_mess_number[&key])              
    }
     
    pub fn init_last_mess_number_from_sender(&mut self, listener_name: &str, listener_topic: &str)->RedisResult<()>{
        let key = format!("{}:{}:{}:{}", self.unique_name, self.source_topic, listener_name, listener_topic);
        let dbconn = self.get_dbconn()?; 
        dbconn.set_nx(&format!("connection:{}:mess_number", key), 0)?;
        Ok(())
    }
    pub fn get_last_mess_number_for_sender(&mut self, listener_name: &str, listener_topic: &str)->RedisResult<u64>{
        let key = format!("{}:{}:{}:{}", self.unique_name, self.source_topic, listener_name, listener_topic);
        let dbconn = self.get_dbconn()?; 
        let res: String = dbconn.get(&format!("connection:{}:mess_number", key))?;
        Ok(res.parse::<u64>().unwrap())
    }

    pub fn save_messages_from_sender(&mut self, mempool: &Arc<Mutex<Mempool>>, listener_name: &str, listener_topic: &str, mess: Vec<Message>)->RedisResult<()>{
        let key = format!("{}:{}:{}:{}", self.unique_name, self.source_topic, listener_name, listener_topic);
        let dbconn = self.get_dbconn()?; 
        for m in mess{
            let mut buf: Vec<u8> = Vec::new(); 
            m.to_stream(mempool, &mut buf);                
            dbconn.rpush(&format!("connection:{}:messages", key), buf)?;
        }
        Ok(())
    }

    pub fn load_messages_for_sender(&mut self, mempool: &Arc<Mutex<Mempool>>, listener_name: &str, listener_topic: &str)->RedisResult<Vec<Message>>{
        let key = format!("{}:{}:{}:{}", self.unique_name, self.source_topic, listener_name, listener_topic);
        let dbconn = self.get_dbconn()?; 
        let llen: Option<usize> = dbconn.llen(&format!("connection:{}:messages", key.clone()))?;
        let mut out = Vec::new();
        if let Some(llen) = llen{
            let buff: Vec<Vec<u8>> = dbconn.lpop(&format!("connection:{}:messages", key), core::num::NonZeroUsize::new(llen))?;
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

    pub fn load_last_message_for_sender(&mut self, mempool: &Arc<Mutex<Mempool>>, listener_name: &str, listener_topic: &str)->RedisResult<Option<Message>>{
        let key = format!("{}:{}:{}:{}", self.unique_name, self.source_topic, listener_name, listener_topic);
        let dbconn = self.get_dbconn()?; 
        let llen: Option<usize> = dbconn.llen(&format!("connection:{}:messages", key.clone()))?;
        let mut out = None;
        if let Some(llen) = llen{
            if llen == 0{
                return Ok(None)
            }
            let buff: Vec<Vec<u8>> = dbconn.lrange(&format!("connection:{}:messages", key), -1, -1)?;
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