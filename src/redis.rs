use redis::{Commands, ConnectionLike, RedisResult};
use std::collections::HashMap;

pub struct Connect{
    unique_name: String,
    source_topic: String,
    conn_str: String,
    conn: redis::Connection,
    topic_addr_cache: HashMap<String, Vec<String>>,
    last_mess_number: HashMap<String, u64>, // key: sender_name,sender_topic,listener_topic 
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
            last_mess_number: HashMap::new()
        })
    }    
    pub fn regist_topic(&mut self, topic: &str, addr: &str)->RedisResult<()>{
        self.source_topic = topic.to_string();
        let dbconn = self.get_dbconn()?;
        dbconn.hset(&format!("topic:{}:addr", topic), addr, "")?;
        Ok(())
    }
    pub fn get_addresses_of_topic(&mut self, topic: &str)->RedisResult<Vec<String>>{
        if !self.topic_addr_cache.contains_key(topic){
            let dbconn = self.get_dbconn()?; 
            let res = dbconn.hkeys(&format!("topic:{}:addr", topic))?;
            self.topic_addr_cache.insert(topic.to_string(), res);
        }
        Ok(self.topic_addr_cache.get(topic).unwrap().to_vec())
    }
    pub fn get_topic_by_address(&self, address: &str)->Option<String>{
        for topic in &self.topic_addr_cache{
            if topic.1.contains(&address.to_string()){
                return Some(topic.0.to_string())
            }
        }
        None
    }
    pub fn get_unique_name(&self)->String{
        self.unique_name.to_string()
    }

    pub fn set_last_mess_number_from_listener(&mut self, sender_name: &str, sender_topic: &str, val: u64)->RedisResult<()>{
        let conn = format!("{}_{}_{}", sender_name, sender_topic, self.source_topic);
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
        let conn = format!("{}_{}_{}", sender_name, sender_topic, self.source_topic);
        if !self.last_mess_number.contains_key(&conn){
            let dbconn = self.get_dbconn()?; 
            let res: String = dbconn.get(&format!("connection_{}:mess_number", conn))?;
            self.last_mess_number.insert(conn.clone(), res.parse::<u64>().unwrap());
        }
        Ok(*self.last_mess_number.get(&conn).unwrap())              
    }
     
    pub fn init_last_mess_number_from_sender(&mut self, listener_topic: &str)->RedisResult<()>{
        let conn = format!("{}_{}_{}", self.unique_name, self.source_topic, listener_topic);
        let dbconn = self.get_dbconn()?; 
        dbconn.set_nx(&format!("connection_{}:mess_number", conn), 0)?;
        Ok(())
    }
    pub fn get_last_mess_number_for_sender(&mut self, listener_topic: &str)->RedisResult<u64>{
        let conn = format!("{}_{}_{}", self.unique_name, self.source_topic, listener_topic);
        let dbconn = self.get_dbconn()?; 
        let res: String = dbconn.get(&format!("connection_{}:mess_number", conn))?;
        Ok(res.parse::<u64>().unwrap())
    }
        
    fn get_dbconn(&mut self)->RedisResult<&mut redis::Connection>{
        if !self.conn.is_open(){
            let client = redis::Client::open(self.conn_str.clone())?;
            self.conn = client.get_connection()?;
        }
        Ok(&mut self.conn)
    }  
}