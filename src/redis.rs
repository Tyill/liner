use redis::{Commands, ConnectionLike, RedisResult};
use std::collections::HashMap;

pub struct Connect{
    conn_str: String,
    conn: redis::Connection,
    topic_addr_cache: HashMap<String, Vec<String>>,
}
impl Connect {
    pub fn new(conn_str: &str)->RedisResult<Connect>{
        let client = redis::Client::open(conn_str.to_string())?;
        let conn = client.get_connection()?;
        Ok(Connect{
            conn_str: conn_str.to_string(),
            conn,
            topic_addr_cache: HashMap::new(),
        })
    }    
    pub fn regist_topic(&mut self, name: &str, addr: &str)->RedisResult<()>{
        let conn = self.get_conn()?; 
        conn.hset(&format!("topic:{}:addr", name), addr, "")?;
        Ok(())
    }
    pub fn get_topic_addresses(&mut self, name: &str)->RedisResult<Vec<String>>{
        if !self.topic_addr_cache.contains_key(name){
            let conn = self.get_conn()?; 
            let res = conn.hkeys(&format!("topic:{}:addr", name))?;
            self.topic_addr_cache.insert(name.to_string(), res);
        }
        Ok(self.topic_addr_cache.get(name).unwrap().to_vec())
    }
    pub fn get_last_mess_number(&mut self, addr_from: &str, addr_to: &str)->RedisResult<u64>{
        let conn = self.get_conn()?; 
        let res: String = conn.get(&format!("mess_from_{}_to_{}:number", addr_from, addr_to))?;
        Ok(res.parse::<u64>().unwrap())
    }
    fn get_conn(&mut self)->RedisResult<&mut redis::Connection>{
        if !self.conn.is_open(){
            let client = redis::Client::open(self.conn_str.clone())?;
            self.conn = client.get_connection()?;
        }
        Ok(&mut self.conn)
    }  
}