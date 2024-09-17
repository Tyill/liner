use redis::{Commands, ConnectionLike, RedisResult};
use std::collections::HashMap;

pub struct Connect{
    conn_str: String,
    conn: redis::Connection,
    topic_addr_local: String,
    topic_addr_cache: HashMap<String, Vec<String>>,
}
impl Connect {
    pub fn new(conn_str: &str)->RedisResult<Connect>{
        let client = redis::Client::open(conn_str.to_string())?;
        let conn = client.get_connection()?;
        Ok(Connect{
            conn_str: conn_str.to_string(),
            conn,
            topic_addr_local: "".to_string(),
            topic_addr_cache: HashMap::new(),
        })
    }    
    pub fn regist_topic(&mut self, name: &str, addr: &str)->RedisResult<()>{
        self.topic_addr_local = addr.to_string();
        let dbconn = self.get_dbconn()?;
        dbconn.hset(&format!("topic:{}:addr", name), addr, "")?;
        Ok(())
    }
    pub fn get_topic_addresses(&mut self, name: &str)->RedisResult<Vec<String>>{
        if !self.topic_addr_cache.contains_key(name){
            let dbconn = self.get_dbconn()?; 
            let res = dbconn.hkeys(&format!("topic:{}:addr", name))?;
            self.topic_addr_cache.insert(name.to_string(), res);
        }
        Ok(self.topic_addr_cache.get(name).unwrap().to_vec())
    }
        
    pub fn get_last_mess_number_for_listener(&mut self, addr_rem: &str)->RedisResult<u64>{
        let conn = format!("{}_{}", self.topic_addr_local, addr_rem);
        let dbconn = self.get_dbconn()?; 
        let res: String = dbconn.get(&format!("connection_{}:mess_number", conn))?;
        Ok(res.parse::<u64>().unwrap())              
    }
    pub fn get_last_mess_number_for_sender(&mut self, addr_rem: &str)->RedisResult<u64>{
        let conn = format!("{}_{}", addr_rem, self.topic_addr_local);
        let dbconn = self.get_dbconn()?; 
        let res: String = dbconn.get(&format!("connection_{}:mess_number", conn))?;
        Ok(res.parse::<u64>().unwrap())
    }
    pub fn set_last_mess_number_from_listener(&mut self, addr_rem: &str, val: u64)->RedisResult<()>{
        let conn = format!("{}_{}", addr_rem, self.topic_addr_local);
        let dbconn = self.get_dbconn()?; 
        let _: String = dbconn.set(&format!("connection_{}:mess_number", conn), val)?;
        Ok(())
    }
    pub fn set_last_mess_number_from_sender(&mut self, addr_rem: &str, val: u64)->RedisResult<()>{
        let conn = format!("{}_{}", self.topic_addr_local, addr_rem);
        let dbconn = self.get_dbconn()?; 
        let _: String = dbconn.set(&format!("connection_{}:mess_number", conn), val)?;
        Ok(())
    }
   
        
    fn get_dbconn(&mut self)->RedisResult<&mut redis::Connection>{
        if !self.conn.is_open(){
            let client = redis::Client::open(self.conn_str.clone())?;
            self.conn = client.get_connection()?;
        }
        Ok(&mut self.conn)
    }  
}