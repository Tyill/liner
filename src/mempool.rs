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
        &self.buff[pos.. pos + std::mem::size_of::<u32>()].copy_from_slice((value.len() as u32).to_be_bytes().as_ref());
        pos += std::mem::size_of::<u32>();
        &self.buff[pos.. pos + value.len()].copy_from_slice(value.as_bytes());
    } 
    pub fn write_num<T>(&mut self, pos: usize, value: T)
    where T: ToBeBytes{
        &self.buff[pos.. pos + std::mem::size_of::<T>()].copy_from_slice(value.to_be_bytes().as_ref());
    } 
    pub fn write_array(&mut self, mut pos: usize, value: &[u8]){
        &self.buff[pos.. pos + std::mem::size_of::<u32>()].copy_from_slice((value.len() as u32).to_be_bytes().as_ref());
        pos += std::mem::size_of::<u32>();
        &self.buff[pos.. pos + value.len()].copy_from_slice(value);
    }
    pub fn read_string(&mut self, mut pos: usize)->String{
        let sz: usize = i32::from_be_bytes(u8_4(&self.buff[pos.. pos + std::mem::size_of::<u32>()])) as usize;
        pos += std::mem::size_of::<u32>();
        String::from_utf8_lossy(&self.buff[pos.. pos + sz]).to_string()
    }
    pub fn read_u64(&mut self, pos: usize)->u64{
        u64::from_be_bytes(u8_8(&self.buff[pos.. pos + std::mem::size_of::<u64>()]))
    }
    pub fn read_u8(&mut self, pos: usize)->u8{
        self.buff[pos]
    }
    pub fn read_array(&mut self, mut pos: usize)->&[u8]{
        let sz: usize = i32::from_be_bytes(u8_4(&self.buff[pos.. pos + 4])) as usize;
        &self.buff[pos.. pos + sz]
    }
}

fn u8_4(b: &[u8]) -> [u8; 4] {
    b.try_into().unwrap()
}
fn u8_8(b: &[u8]) -> [u8; 8] {
    b.try_into().unwrap()
}