use crate::bytestream::ToBeBytes;



pub struct Memarea{
    buff: Vec <u8>,
}
pub struct Span{
    pos: usize,
    size: usize, 
}

impl Memarea{
    pub fn alloc_str(& mut self, value: &str)->Span{
        let csz = self.buff.len();
        self.buff.extend_from_slice(&value.as_bytes());
        Span{pos: csz, size: value.len()}
    }
    pub fn alloc_num<T>(&mut self, value: T)->Span
    where 
    T: ToBeBytes{
        let csz = self.buff.len();
        self.buff.extend_from_slice(&value.to_be_bytes().as_ref());
        &self.buff[csz..std::mem::size_of::<T>()]
    }
    pub fn alloc_array(&mut self, value: &[u8])->Span{
        let csz = self.buff.len();
        self.buff.extend_from_slice(value);
        &self.buff[csz..value.len()]
    }   
}
