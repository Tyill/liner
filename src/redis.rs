use redis::{Commands, RedisResult};

use crate::config;


pub fn regist_topic(name: &str, addr: &str)->RedisResult<()>{
    
    let redis_path = config().lock().unwrap().redis_path.clone();
    
    let client = redis::Client::open(redis_path)?;
    let mut con = client.get_connection()?;
    let _: () = con.hset(format!("topic:{}:addr", name), addr, "").unwrap_or_default();
  
    Ok(())
}

pub fn get_topic_addr(name: &str)->RedisResult<Vec<String>>{
    
    let redis_path = config().lock().unwrap().redis_path.clone();
    
    let client = redis::Client::open(redis_path)?;
    let mut con = client.get_connection()?;
    let res = con.hkeys(format!("topic:{}:addr", name)).unwrap_or_default();
  
    Ok(res)
}