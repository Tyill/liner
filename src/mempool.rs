
use std::collections::BTreeMap;
use std::collections::btree_map;


pub struct Mempool{
    buff: Vec<u8>,
    free_mem: BTreeMap<usize, Vec<usize>>, // key: size, value: free pos
}

impl Mempool{   
    pub fn new()->Mempool{
        Mempool{
            buff: Vec::new(),
            free_mem: BTreeMap::new(),
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
                        return self.new_mem(req_size);
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
            let pos = self.free_mem.get_mut(&length).unwrap().pop().unwrap();
            let endlen = length - req_size;
            if endlen > 0 {
                if let btree_map::Entry::Vacant(e) = self.free_mem.entry(endlen) {
                    e.insert(Vec::new());
                }
                self.free_mem.get_mut(&endlen).unwrap().push(pos + req_size);
            }
            (pos, req_size)
        }else{
            self.new_mem(req_size)
        }
    }
    fn new_mem(&mut self, req_size: usize)->(usize, usize){
        let mut free_mem_pos: BTreeMap<usize, usize> = BTreeMap::new(); // pos, len
        for m in &self.free_mem{
            if !m.1.is_empty(){
                for pos in m.1{
                    free_mem_pos.insert(*pos, *m.0);
                }
            }
        }
        if !free_mem_pos.is_empty(){
            let mut prev_free_len: usize = 0;
            let mut start_free_pos: usize = 0;
            let mut new_free_len: usize = 0;
            let mut count = 0;
            let len = free_mem_pos.len();
            for m in &free_mem_pos{
                let has_new_len = start_free_pos + new_free_len == *m.0 && new_free_len > 0;
                if has_new_len{
                    new_free_len += m.1;
                    count += 1;
                    if count < len{
                        continue;
                    }
                }
                if new_free_len > prev_free_len{
                    for pm in &free_mem_pos{
                        if *pm.0 == *m.0 && !has_new_len{
                            break;
                        }
                        if *pm.0 >= start_free_pos{
                            if let Some(index) = self.free_mem[&pm.1].iter().position(|v| *v == *pm.0) {
                                self.free_mem.get_mut(&pm.1).unwrap().swap_remove(index);
                            }
                        }
                    }
                    if new_free_len >= req_size{
                        if let btree_map::Entry::Vacant(e) = self.free_mem.entry(req_size) {
                            e.insert(Vec::new());
                        }
                        let endlen = new_free_len - req_size;
                        if endlen > 0 {
                            if let btree_map::Entry::Vacant(e) = self.free_mem.entry(endlen) {
                                e.insert(Vec::new());
                            }
                            self.free_mem.get_mut(&endlen).unwrap().push(start_free_pos + req_size);
                        }
                        return (start_free_pos, req_size);
                    }
                    if let btree_map::Entry::Vacant(e) = self.free_mem.entry(new_free_len) {
                        e.insert(Vec::new());
                    }
                    self.free_mem.get_mut(&new_free_len).unwrap().push(start_free_pos);
                }
                start_free_pos = *m.0;
                prev_free_len = *m.1;
                new_free_len = prev_free_len;
                count += 1;
            }
        }
        let csz = self.buff.len();
        self.buff.resize(csz + req_size, 0);
        if let btree_map::Entry::Vacant(e) = self.free_mem.entry(req_size) {
            e.insert(Vec::new());
        }
        (csz, req_size)
    }
    pub fn alloc_with_write(&mut self, value: &[u8])->(usize, usize){
        let (pos, sz) = self.alloc(value.len());
        self.write_data(pos, value);
        (pos, sz)
    }
    pub fn free(&mut self, pos: usize, length: usize){
        self.free_mem.get_mut(&length).unwrap().push(pos);
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