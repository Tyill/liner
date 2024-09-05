

pub struct Message{
   to: String,
   from: String,
   uuid: String,
   timestamp: u64,
   data: Vec<u8>,
}

impl Message {
   pub fn new(data: Vec<u8>) -> Message {
       Self{
         to: "".to_string(),
         from: "".to_string(),
         uuid: "".to_string(),
         timestamp: 123,
         data
       }
   }
}


