use crate::{message::Message, print_error};

use redis::{Commands, ConnectionLike, RedisResult, ErrorKind};
use std::collections::HashMap;

pub struct Connect{
    unique_name: String,
    source_topic: String,
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
            conn_str: conn_str.to_string(),
            conn,
            topic_addr_cache: HashMap::new(),  
            unique_name_cache: HashMap::new(),
            last_mess_number: HashMap::new(),
        })
    }    
    pub fn redis_path(&self)->String{
        return self.conn_str.clone();
    }
    pub fn regist_topic(&mut self, topic: &str, addr: &str)->RedisResult<()>{
        self.source_topic = topic.to_string();
        let unique: String = self.unique_name.to_string();
        let dbconn = self.get_dbconn()?;
        dbconn.hset(&format!("topic:{}:addr", topic), addr, unique)?;
        Ok(())
    }
    pub fn set_source_topic(&mut self, topic: &str){
        self.source_topic = topic.to_string();
    }    
    pub fn get_addresses_of_topic(&mut self, topic: &str)->RedisResult<Vec<String>>{
        if !self.topic_addr_cache.contains_key(topic){
            self.init_addresses_of_topic(topic)?;
        }
        Ok(self.topic_addr_cache.get(topic).unwrap().to_vec())
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
   
    pub fn set_last_mess_number_from_listener(&mut self, sender_name: &str, sender_topic: &str, val: u64)->RedisResult<()>{
        let conn = format!("{}_{}_{}_{}", sender_name, sender_topic, self.unique_name, self.source_topic);
        let dbconn = self.get_dbconn()?;
        dbconn.set(&format!("connection_{}:mess_number", conn), val)?;
        if self.last_mess_number.contains_key(&conn){
            *self.last_mess_number.get_mut(&conn).unwrap() = val;
        }else{
            self.last_mess_number.insert(conn, val);
        }
        Ok(())
    }  
    pub fn get_last_mess_number_for_listener(&mut self, sender_name: &str, sender_topic: &str)->RedisResult<u64>{
        let conn = format!("{}_{}_{}_{}", sender_name, sender_topic, self.unique_name, self.source_topic);
        if !self.last_mess_number.contains_key(&conn){
            let dbconn = self.get_dbconn()?; 
            let res: String = dbconn.get(&format!("connection_{}:mess_number", conn))?;
            self.last_mess_number.insert(conn.clone(), res.parse::<u64>().unwrap());
        }
        Ok(*self.last_mess_number.get(&conn).unwrap())              
    }
     
    pub fn init_last_mess_number_from_sender(&mut self, listener_name: &str, listener_topic: &str)->RedisResult<()>{
        let conn = format!("{}_{}_{}_{}", self.unique_name, self.source_topic, listener_name, listener_topic);
        let dbconn = self.get_dbconn()?; 
        dbconn.set_nx(&format!("connection_{}:mess_number", conn), 0)?;
        Ok(())
    }
    pub fn get_last_mess_number_for_sender(&mut self, listener_name: &str, listener_topic: &str)->RedisResult<u64>{
        let conn = format!("{}_{}_{}_{}", self.unique_name, self.source_topic, listener_name, listener_topic);
        let dbconn = self.get_dbconn()?; 
        let res: String = dbconn.get(&format!("connection_{}:mess_number", conn))?;
        Ok(res.parse::<u64>().unwrap())
    }

    pub fn save_messages_from_sender(&mut self, listener_name: &str, listener_topic: &str, mess: Vec<Message>)->RedisResult<()>{
        let conn = format!("{}_{}_{}_{}", self.unique_name, self.source_topic, listener_name, listener_topic);
        let dbconn = self.get_dbconn()?; 
        for m in mess{
            let mut buf: Vec<u8> = Vec::new(); 
            m.to_stream(&mut buf);                
            dbconn.rpush(&format!("connection_{}:messages", conn), buf)?;
        }
        Ok(())
    }

    pub fn load_messages_for_sender(&mut self, listener_name: &str, listener_topic: &str)->RedisResult<Vec<Message>>{
        let conn = format!("{}_{}_{}_{}", self.unique_name, self.source_topic, listener_name, listener_topic);
        let dbconn = self.get_dbconn()?; 
        let llen: Option<usize> = dbconn.llen(&format!("connection_{}:messages", conn.clone()))?;
        let mut out = Vec::new();
        if let Some(llen) = llen{
            let buff: Vec<Vec<u8>> = dbconn.lpop(&format!("connection_{}:messages", conn), core::num::NonZeroUsize::new(llen))?;
            for b in buff{
                if let Some(mess) = Message::from_stream(&mut &b[..]){
                    out.push(mess);
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