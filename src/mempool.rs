
use std::collections::BTreeMap;
use std::collections::btree_map;


pub struct Mempool{
    buff: Vec<u8>,
    free_mem: BTreeMap<usize, Vec<usize>>, // key: size, value: free pos
    median_size: usize,
}

impl Mempool{   
    pub fn new()->Mempool{
        Mempool{
            buff: Vec::new(),
            free_mem: BTreeMap::new(),
            median_size: 0,
        }
    }
    pub fn _print_size(&self){
        println!("mempool sz {}", self.buff.len());
    }
    pub fn alloc(&mut self, req_size: usize)->(usize, usize){    
        let mut length = 0;
        {           
            let keys: Vec<&usize> = self.free_mem.keys().collect();
            let mut ix;
            match keys.binary_search(&&req_size){   
                Ok(ix_) =>{
                    ix = ix_;
                },
                Err(ix_)=>{
                    if ix_ == keys.len(){
                        return (self.new_mem(req_size), req_size);
                    }
                    ix = ix_;
                }
            }            
            while ix < keys.len(){
                if !self.free_mem[keys[ix]].is_empty(){
                    length = *keys[ix];
                    break;
                }else{
                    ix += 1;
                }
            }
        }
        if length > 0{
            (self.free_mem.get_mut(&length).unwrap().pop().unwrap(), length)
        }else{
            (self.new_mem(req_size), req_size)
        }
    }
    fn new_mem(&mut self, req_size: usize)->usize{
        let csz = self.buff.len();
        self.buff.resize(csz + req_size, 0);
        if let btree_map::Entry::Vacant(e) = self.free_mem.entry(req_size) {
            e.insert(Vec::new());
            let keys: Vec<&usize> = self.free_mem.keys().collect();
            self.median_size = *keys[self.free_mem.len()/2];
        }
        csz
    }
    pub fn alloc_with_write(&mut self, value: &[u8])->(usize, usize){
        let (pos, sz) = self.alloc(value.len());
        self.write_data(pos, value);
        (pos, sz)
    }
    pub fn free(&mut self, mut pos: usize, length: usize){
        let mut endlen = length;
        while endlen > (self.median_size as f32 * 1.5) as usize {
            self.free_mem.get_mut(&self.median_size).unwrap().push(pos);
            endlen -= self.median_size;
            pos += self.median_size;
        }
        if endlen > 0{
            if endlen < length && !self.free_mem.contains_key(&endlen){
                self.free_mem.insert(endlen, Vec::new());
            }
            self.free_mem.get_mut(&endlen).unwrap().push(pos);
        }
    }
    pub fn write_str(&mut self, mut pos: usize, value: &str){
        self.buff[pos.. pos + std::mem::size_of::<u32>()].copy_from_slice((value.len() as u32).to_be_bytes().as_ref());
        pos += std::mem::size_of::<u32>();
        self.buff[pos.. pos + value.len()].copy_from_slice(value.as_bytes());
    } 
    pub fn write_num<T>(&mut self, pos: usize, value: T)
    where T: ToBeBytes{
        self.buff[pos.. pos + std::mem::size_of::<T>()].copy_from_slice(value.to_be_bytes().as_ref());
    } 
    pub fn write_array(&mut self, mut pos: usize, value: &[u8]){
        self.buff[pos.. pos + std::mem::size_of::<u32>()].copy_from_slice((value.len() as u32).to_be_bytes().as_ref());
        pos += std::mem::size_of::<u32>();
        self.buff[pos.. pos + value.len()].copy_from_slice(value);
    }
    pub fn write_data(&mut self, pos: usize, value: &[u8]){
        self.buff[pos.. pos + value.len()].copy_from_slice(value);
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
    pub fn _read_array(&self, mut pos: usize)->&[u8]{
        let sz: usize = i32::from_be_bytes(u8_4(&self.buff[pos.. pos + 4])) as usize;
        pos += std::mem::size_of::<u32>();
        &self.buff[pos.. pos + sz]
    }
    pub fn read_data(&self, pos: usize, sz: usize)->&[u8]{
        &self.buff[pos.. pos + sz]
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