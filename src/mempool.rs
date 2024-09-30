
use std::{collections::BTreeMap, usize};


pub struct Mempool{
    buff: Vec<u8>,
    pos_mem: BTreeMap<usize, MemSpan>, // key: pos
    free_mem: BTreeMap<usize, MemFreeSpan>, // key: size
}
#[derive(Clone, Copy)]
struct MemSpan{
    pos: usize,
    length: usize,
    great_pos: usize, // another position with the great length
    is_free: bool,
    has_great: bool,
}

#[derive(Clone, Copy)]
struct MemFreeSpan{
    pos: usize,
}

impl Mempool{
    pub fn new()->Mempool{
        Mempool{
            buff: Vec::new(),
            free_mem: BTreeMap::new(),
            pos_mem: BTreeMap::new(),
        }
    }
    pub fn alloc(&mut self, req_size: usize)->usize{     
        let keys: Vec<&usize> = self.free_mem.keys().collect();
        let mut ix;
        match keys.binary_search(&&req_size){   
            Ok(ix_) =>{
                ix = ix_;
            },
            Err(ix_)=>{
                if ix_ == keys.len(){
                    return self.new_mem(req_size, usize::MAX, false);
                }
                ix = ix_;
            }
        }
        let ix_mem = ix;
        while ix < keys.len(){
            let fm = &self.free_mem[keys[ix]];
            let mut pm = self.pos_mem[&fm.pos];
            while !pm.is_free && pm.has_great{
                pm = self.pos_mem[&pm.great_pos];
            }
            if pm.is_free{
                self.pos_mem.get_mut(&pm.pos).unwrap().is_free = false;
                return pm.pos;
            }else{
                ix += 1;
            }
        }
        let fm = self.free_mem[keys[ix_mem]];
        self.new_mem(req_size, fm.pos, true)
    }
    
    pub fn free(&mut self, pos: usize){
        let pm = self.pos_mem.get_mut(&pos).unwrap();
        assert!(!pm.is_free);
        pm.is_free = true;
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
    pub fn read_mess(&self, pos: usize, sz: usize)->&[u8]{
        &self.buff[pos.. pos + sz]
    }

    fn new_mem(&mut self, req_size: usize, great_pos: usize, has_great: bool)->usize{
        let csz = self.buff.len();
        self.buff.resize(csz + req_size, 0);
        self.pos_mem.insert(csz, MemSpan{pos: csz, great_pos, length: req_size, is_free: false, has_great});
        if !has_great{
            self.free_mem.insert(req_size, MemFreeSpan{pos: csz});
        }
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