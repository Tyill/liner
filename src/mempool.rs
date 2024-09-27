use crate::bytestream::ToBeBytes;

pub struct Mempool{
    buff: Vec<u8>,
}
pub struct Span{
    pos: usize,
    size: usize,
}
impl Span{
    pub fn new(pos: usize, size: usize)->Span{
        Self{pos, size}
    }
    pub fn new_with_offs(pos: usize, size: usize, offs: &mut usize)->Span{
        *offs += size;
        Self{pos, size}
    }
    pub fn pos(&self)->usize{
        self.pos
    }
    pub fn size(&self)->usize{
        self.size
    }
    pub fn clone(&self)->Span{
        Span{pos: self.pos, size: self.size}
    }
}

impl Mempool{
    pub fn alloc(&mut self, size: usize)->Span{
        let csz = self.buff.len();
        self.buff.resize(csz + size, 0);
        Span{pos: csz, size}
    }
    pub fn write_str(&mut self, mut pos: usize, value: &str){
        &self.buff[pos.. pos + std::mem::size_of::<u32>()].clone_from_slice((value.len() as u32).to_be_bytes().as_ref());
        pos += std::mem::size_of::<u32>();
        &self.buff[pos.. pos + value.len()].clone_from_slice(value.as_bytes());
    } 
    pub fn write_num<T>(&mut self, pos: usize, value: T)
    where T: ToBeBytes{
        &self.buff[pos.. pos + std::mem::size_of::<T>()].clone_from_slice(value.to_be_bytes().as_ref());
    } 
    pub fn write_array(&mut self, mut pos: usize, value: &[u8]){
        &self.buff[pos.. pos + std::mem::size_of::<u32>()].clone_from_slice((value.len() as u32).to_be_bytes().as_ref());
        pos += std::mem::size_of::<u32>();
        &self.buff[pos.. pos + value.len()].clone_from_slice(value);
    }
}
