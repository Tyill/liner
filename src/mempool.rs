
use std::collections::BTreeMap;

pub struct Mempool{
    buff: Vec<u8>,
    free_mem: BTreeMap<usize, MemSpan>, // key: size
    pos_mem: BTreeMap<i32, MemSpan>, // key: pos
}

struct MemSpan{
    pos: i32,
    prev_pos: i32, // another position with the same length
    size: usize,
}

impl Mempool{
    pub fn new()->Mempool{
        Mempool{
            buff: Vec::new(),
            free_mem: BTreeMap::new(),
            pos_mem: BTreeMap::new(),
        }
    }

    pub fn alloc(&mut self, mut req_size: usize)->usize{     
        {
            let keys: Vec<&usize> = self.free_mem.keys().collect();
            let mut ix;
            match keys.binary_search(&&req_size){   
                Ok(ix_) =>{
                    ix = ix_;
                },
                Err(ix_)=>{
                    if ix_ == keys.len(){
                        return self.new_mem(req_size);
                    }
                    ix = ix_;
                }
            }
            let req_size_mem = req_size;
            while ix < keys.len(){
                req_size = *keys[ix];
                let fmp = &self.free_mem[&req_size];
                let mut cpos = fmp.pos;
                let mut prev_pos = fmp.prev_pos;
                while cpos < 0 && prev_pos >= 0{
                    let pp = &self.pos_mem[&prev_pos];
                    cpos = pp.pos;
                    prev_pos = pp.prev_pos;
                }
                if cpos >= 0{
                    break;
                }else{
                    ix += 1;
                }
            }
            if ix == keys.len(){
                return self.new_mem(req_size_mem);
            }
        }
        let fmp = self.free_mem.get_mut(&req_size).unwrap();
        let cpos = fmp.pos;
        self.pos_mem.get_mut(&fmp.pos).unwrap().pos = -1;
        fmp.pos = -1;
        cpos as usize
    }
    
    pub fn free(&mut self, pos: usize){
      
    }
    pub fn write_str(&mut self, mut pos: usize, value: &str){
        let _ = &self.buff[pos.. pos + std::mem::size_of::<u32>()].copy_from_slice((value.len() as u32).to_be_bytes().as_ref());
        pos += std::mem::size_of::<u32>();
        let _ = &self.buff[pos.. pos + value.len()].copy_from_slice(value.as_bytes());
    } 
    pub fn write_num<T>(&mut self, pos: usize, value: T)
    where T: ToBeBytes{
        let _ = &self.buff[pos.. pos + std::mem::size_of::<T>()].copy_from_slice(value.to_be_bytes().as_ref());
    } 
    pub fn write_array(&mut self, mut pos: usize, value: &[u8]){
        let _ = &self.buff[pos.. pos + std::mem::size_of::<u32>()].copy_from_slice((value.len() as u32).to_be_bytes().as_ref());
        pos += std::mem::size_of::<u32>();
        let _ = &self.buff[pos.. pos + value.len()].copy_from_slice(value);
    }
    pub fn read_string(&self, mut pos: usize)->String{
        let sz: usize = i32::from_be_bytes(u8_4(&self.buff[pos.. pos + std::mem::size_of::<u32>()])) as usize;
        pos += std::mem::size_of::<u32>();
        String::from_utf8_lossy(&self.buff[pos.. pos + sz]).to_string()
    }
    pub fn read_u64(&self, pos: usize)->u64{
        u64::from_be_bytes(u8_8(&self.buff[pos.. pos + std::mem::size_of::<u64>()]))
    }
    pub fn read_u32(&self, pos: usize)->u32{
        u32::from_be_bytes(u8_4(&self.buff[pos.. pos + std::mem::size_of::<u32>()]))
    }
    pub fn read_u8(&self, pos: usize)->u8{
        self.buff[pos]
    }
    pub fn read_array(&self, mut pos: usize)->&[u8]{
        let sz: usize = i32::from_be_bytes(u8_4(&self.buff[pos.. pos + 4])) as usize;
        pos += std::mem::size_of::<u32>();
        &self.buff[pos.. pos + sz]
    }

    fn new_mem(&mut self, req_size: usize)->usize{
        let csz = self.buff.len();
        self.buff.resize(csz + req_size, 0);
        self.pos_mem.insert(csz as i32, MemSpan{pos: csz as i32, prev_pos: 0, size: req_size});
        csz
    }
}

fn u8_4(b: &[u8]) -> [u8; 4] {
    b.try_into().unwrap()
}
fn u8_8(b: &[u8]) -> [u8; 8] {
    b.try_into().unwrap()
}

pub trait ToBeBytes {
    type ByteArray: AsRef<[u8]>;
    fn to_be_bytes(&self) -> Self::ByteArray;
}
impl ToBeBytes for u8 {
    type ByteArray = [u8; 1];
    fn to_be_bytes(&self) -> Self::ByteArray {
        u8::to_be_bytes(*self)
    }
}
impl ToBeBytes for i32 {
    type ByteArray = [u8; 4];
    fn to_be_bytes(&self) -> Self::ByteArray {
        i32::to_be_bytes(*self)
    }
}
impl ToBeBytes for u64 {
    type ByteArray = [u8; 8];
    fn to_be_bytes(&self) -> Self::ByteArray {
        u64::to_be_bytes(*self)
    }
}