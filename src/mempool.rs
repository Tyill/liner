
use std::collections::BTreeMap;
use std::collections::btree_map;

use crate::settings;


pub struct Mempool{
    buff: Vec<Vec<u8>>,
    free_mem: BTreeMap<usize, (usize, Vec<usize>)>, // key: size, value: count, free pos
    free_len: usize,
    free_count: usize,
}

impl Mempool{   
    pub fn new()->Mempool{
        Mempool{
            buff: Vec::new(),
            free_mem: BTreeMap::new(),
            free_len: 0,
            free_count: 0,
        }
    }
    pub fn alloc(&mut self, req_size: usize)->(usize, usize){    
        let Some((&length, _)) = self
            .free_mem
            .range(req_size..)
            .find(|(_, entry)| !entry.1.is_empty())
        else {
            return self.new_mem(req_size);
        };
        let pos = self.free_mem.get_mut(&length).unwrap().1.pop().unwrap();
        let endlen = length - req_size;
        if endlen > 0 {
            // We split a free block of size `length` into:
            // - an allocated block of size `req_size`
            // - a free tail block of size `endlen`
            // The original `length` block no longer exists as a segment.
            self.free_mem_remove_len(length);
            if !self.free_mem.contains_key(&req_size){
                self.free_mem.insert(req_size, (1, Vec::new()));
            }else{
                let count = &mut self.free_mem.get_mut(&req_size).unwrap().0;
                *count += 1;
            }
            self.free_mem_insert_pos(endlen, pos + req_size);
        }
        self.free_mem_len_decrease(req_size);
        (pos, req_size)
    }
    fn new_mem(&mut self, req_size: usize)->(usize, usize){
        if self.free_len > req_size{
            if let Some(fm) = self.check_free_mem(req_size){
                self.free_mem_len_decrease(req_size);
                return fm;
            } 
        }
        let csz = self.buff.len() * settings::MEMPOOL_CHUNK_SIZE_BYTE;
        for  _ in 0..req_size / settings::MEMPOOL_CHUNK_SIZE_BYTE + 1{
            self.buff.push(vec![0; settings::MEMPOOL_CHUNK_SIZE_BYTE]);
        }
        self.free_mem_insert_empty_pos(req_size);
        let endlen = (req_size / settings::MEMPOOL_CHUNK_SIZE_BYTE + 1) * settings::MEMPOOL_CHUNK_SIZE_BYTE - req_size;
        if endlen > 0{
            let nsz = self.buff.len() * settings::MEMPOOL_CHUNK_SIZE_BYTE;
            self.free_mem_insert_pos(endlen, nsz - endlen);
            self.free_len += endlen;
        }
        (csz, req_size)
    }
    fn check_free_mem(&mut self, req_size: usize)->Option<(usize, usize)>{
        let csz = self.buff.len() * settings::MEMPOOL_CHUNK_SIZE_BYTE;
        if self.free_len < req_size || 
           (self.free_len - req_size) < (settings::MEMPOOL_MIN_PERCENT_FOR_COMPRESS * csz as f32) as usize{
            return None;
        }
        let mut free_len_all = 0;
        let mut free_mem_pos: BTreeMap<usize, usize> = BTreeMap::new(); // pos, len
        for m in &self.free_mem{
            if !m.1.1.is_empty(){
                for pos in &m.1.1{
                    free_mem_pos.insert(*pos, *m.0);
                    free_len_all += m.0;
                }
            }
        }
        self.free_len = free_len_all;     // на всякий обновим
        let mut min_free_len: usize = 0;
        let mut free_mem: Vec<(usize, usize)> = Vec::new();
        {
            let mut prev_free_len: usize = 0;
            let mut start_free_pos: usize = 0;
            let mut new_free_len: usize = 0;
            let mut count = 0;
            let len = free_mem_pos.len();
            for m in free_mem_pos{
                let (free_pos, free_len) = m;
                let has_new_len = start_free_pos + new_free_len == free_pos && count > 0;
                if has_new_len{
                    if new_free_len == prev_free_len{
                        if let Some(index) = self.free_mem[&prev_free_len].1.iter().position(|v| *v == start_free_pos) {
                            self.free_mem_remove_pos(prev_free_len, index);
                        }
                    }
                    if let Some(index) = self.free_mem[&free_len].1.iter().position(|v| *v == free_pos) {
                        self.free_mem_remove_pos(free_len, index);
                    }
                    new_free_len += free_len;
                    count += 1;
                    if count < len{
                        continue;
                    }
                }
                if new_free_len > prev_free_len{
                    if new_free_len >= req_size && (new_free_len < min_free_len || min_free_len == 0){
                        min_free_len = new_free_len;
                    }
                    free_mem.push((start_free_pos, new_free_len));                                     
                }
                start_free_pos = free_pos;
                prev_free_len = free_len;
                new_free_len = free_len;
                count += 1;
            }
        }
        let mut has_req_mem = false;
        let mut req_pos: usize = 0;
        for m in free_mem{
            let (free_pos, free_len) = m;
            if !has_req_mem && req_size > 0 && free_len == min_free_len && min_free_len >= req_size{
                self.free_mem_insert_empty_pos(req_size);
                let endlen = free_len - req_size;
                if endlen > 0 {                    
                    self.free_mem_insert_pos(endlen, free_pos + req_size);
                }
                req_pos = free_pos;
                has_req_mem = true;        
            }else{
                self.free_mem_insert_pos(free_len, free_pos);
            }                
        }
        let buff_len_bytes = self.buff.len() * settings::MEMPOOL_CHUNK_SIZE_BYTE;
        if buff_len_bytes > 0
            && self.free_len > (settings::MEMPOOL_MIN_PERCENT_FOR_RESIZE * buff_len_bytes as f32) as usize
            && buff_len_bytes > settings::MEMPOOL_OVER_SIZE_MB * 1024 * 1024
        {
            let reserved_end = if has_req_mem { req_pos + req_size } else { 0 };
            let _ = self.shrink_free_tail(buff_len_bytes, reserved_end, has_req_mem);
        }
        if has_req_mem{    
            Some((req_pos, req_size))
        }else{
            None
        }     
    }
    fn free_mem_insert_pos(&mut self, free_len: usize, free_pos: usize){
        if let btree_map::Entry::Vacant(e) = self.free_mem.entry(free_len) {
            e.insert((1, vec![free_pos]));
        }else{
            self.free_mem.get_mut(&free_len).unwrap().1.push(free_pos);
            let count = &mut self.free_mem.get_mut(&free_len).unwrap().0;
            *count += 1;
        }
    }
    fn free_mem_insert_empty_pos(&mut self, req_size: usize){
        if let btree_map::Entry::Vacant(e) = self.free_mem.entry(req_size) {
            e.insert((1, Vec::new()));
        }else{
            let count = &mut self.free_mem.get_mut(&req_size).unwrap().0;
            *count += 1;
        }
    }
    fn free_mem_remove_pos(&mut self, free_len: usize, index: usize){
        self.free_mem.get_mut(&free_len).unwrap().1.swap_remove(index);
        self.free_mem_remove_len(free_len)
    }
    fn free_mem_remove_len(&mut self, free_len: usize){
        let count = &mut self.free_mem.get_mut(&free_len).unwrap().0;
        *count -= 1;
        if *count == 0{
            self.free_mem.remove(&free_len);
        }
    }
    fn free_mem_len_decrease(&mut self, req_size: usize){
        if self.free_len >= req_size{
            self.free_len -= req_size;
        }else{
            self.free_len = 0;
        }
    }    
    pub fn free(&mut self, pos: usize, length: usize){
        self.free_mem.get_mut(&length).unwrap().1.push(pos);
        self.free_len += length;
        self.free_count += 1;
        if self.free_count > settings::MEMPOOL_FREE_COUNT_FOR_RESIZE{
            self.check_free_mem(0);
            self.free_count = 0;
        }
    }


    fn shrink_free_tail(
        &mut self,
        mut buff_len_bytes: usize,
        reserved_end: usize,
        has_req_mem: bool,
    ) -> usize {
        // Shrink tail blocks iteratively while possible.
        // We avoid iterating and mutating `self.free_mem` at the same time by working on snapshots.
        loop {
            let mut did_shrink = false;

            // Iterate from the end (largest free_len first) to maximize shrink per step.
            let free_lens: Vec<usize> = self.free_mem.keys().copied().collect();
            for free_len in free_lens.into_iter().rev() {
                let Some((_, positions)) = self.free_mem.get(&free_len) else {
                    continue;
                };
                if positions.is_empty() {
                    continue;
                }

                let shrink_candidate = positions.iter().enumerate().find_map(|(index, pos)| {
                    let ends_at_buffer = *pos + free_len == buff_len_bytes;
                    if !ends_at_buffer {
                        return None;
                    }
                    let aligned = *pos % settings::MEMPOOL_CHUNK_SIZE_BYTE == 0
                        && free_len % settings::MEMPOOL_CHUNK_SIZE_BYTE == 0;
                    if !aligned {
                        return None;
                    }
                    // Do not shrink away a region that intersects the reserved req block.
                    // It's safe to shrink only if the reserved block ends before this tail starts.
                    if has_req_mem && reserved_end > *pos {
                        return None;
                    }
                    Some((index, *pos))
                });

                if let Some((index, pos)) = shrink_candidate {
                    let new_chunk_len = pos / settings::MEMPOOL_CHUNK_SIZE_BYTE;
                    self.buff.truncate(new_chunk_len);

                    // Remove the exact position from the current map.
                    self.free_mem_remove_pos(free_len, index);
                    self.free_mem_len_decrease(free_len);

                    buff_len_bytes = self.buff.len() * settings::MEMPOOL_CHUNK_SIZE_BYTE;
                    did_shrink = true;
                }

                if did_shrink {
                    break;
                }
            }

            if !did_shrink {
                break;
            }
        }
        buff_len_bytes
    }

    #[cfg(test)]
    fn shrink_free_tail_for_test(&mut self) {
        let buff_len_bytes = self.buff.len() * settings::MEMPOOL_CHUNK_SIZE_BYTE;
        let _ = self.shrink_free_tail(buff_len_bytes, 0, false);
    }

    #[cfg(test)]
    pub fn debug_free_len(&self) -> usize {
        self.free_len
    }

    #[cfg(test)]
    pub fn debug_free_count(&self) -> usize {
        self.free_count
    }
  
    pub fn write_num<T>(&mut self, pos: usize, value: T)
    where T: ToBeBytes{
        let lpos = pos / settings::MEMPOOL_CHUNK_SIZE_BYTE;
        let offset = pos % settings::MEMPOOL_CHUNK_SIZE_BYTE;
                        
        if offset + std::mem::size_of::<T>() <= settings::MEMPOOL_CHUNK_SIZE_BYTE{
            let arr = &mut self.buff[lpos];
            arr[offset..offset + std::mem::size_of::<T>()].copy_from_slice(&value.to_be_bytes().as_ref());
        }else{
            let aleft = &mut self.buff[lpos];
            let left = settings::MEMPOOL_CHUNK_SIZE_BYTE - offset;
            aleft[offset..].copy_from_slice(&value.to_be_bytes().as_ref()[0..left]);

            let aright = &mut self.buff[lpos + 1];
            let right = std::mem::size_of::<T>() - left;
            aright[..right].copy_from_slice(&value.to_be_bytes().as_ref()[left..]);
        }
    } 
    pub fn write_array(&mut self, pos: usize, value: &[u8]){
        self.write_num(pos, value.len() as i32);
        self.write_data(pos + std::mem::size_of::<i32>(), value);
    }
    pub fn write_data(&mut self, pos: usize, value: &[u8]){
        let mut cpos = pos;
        let mut clen = 0;
        while clen < value.len() {
            let lpos = cpos / settings::MEMPOOL_CHUNK_SIZE_BYTE;
            let offset = cpos % settings::MEMPOOL_CHUNK_SIZE_BYTE;
        
            let arr = &mut self.buff[lpos]; // может не попасть в один 
            let wlen = (value.len() - clen).min(settings::MEMPOOL_CHUNK_SIZE_BYTE - offset);
            arr[offset..offset + wlen].copy_from_slice(&value[clen..clen + wlen]);

            cpos += settings::MEMPOOL_CHUNK_SIZE_BYTE - offset;
            clen += wlen;
        }
    }
   
    pub fn read_u64(&self, pos: usize)->u64{
        let lpos = pos / settings::MEMPOOL_CHUNK_SIZE_BYTE;
        let offset = pos % settings::MEMPOOL_CHUNK_SIZE_BYTE;
        
        if offset + std::mem::size_of::<u64>() <= settings::MEMPOOL_CHUNK_SIZE_BYTE{
            let arr = &self.buff[lpos];
            u64::from_be_bytes((&arr[offset..offset + std::mem::size_of::<u64>()]).try_into().unwrap())
        }else{
            let mut oarr= [0; 8];

            let aleft = &self.buff[lpos];
            let left = aleft.len() - offset;
            let right = std::mem::size_of::<u64>() - left;
            oarr[..left].copy_from_slice(&aleft[offset..]);

            let aright = &self.buff[lpos + 1];
            oarr[left..].copy_from_slice(&aright[..right]);

            u64::from_be_bytes(oarr)
        }
    }
    pub fn read_u32(&self, pos: usize)->u32{
        let lpos = pos / settings::MEMPOOL_CHUNK_SIZE_BYTE;
        let offset = pos % settings::MEMPOOL_CHUNK_SIZE_BYTE;
        
        if offset + std::mem::size_of::<u32>() <= settings::MEMPOOL_CHUNK_SIZE_BYTE{
            let arr = &self.buff[lpos];
            u32::from_be_bytes((&arr[offset..offset + std::mem::size_of::<u32>()]).try_into().unwrap())
        }else{
            let mut oarr= [0; 4];

            let aleft = &self.buff[lpos];
            let left = aleft.len() - offset;
            let right = std::mem::size_of::<u32>() - left;
            oarr[..left].copy_from_slice(&aleft[offset..]);

            let aright = &self.buff[lpos + 1];
            oarr[left..].copy_from_slice(&aright[..right]);

            u32::from_be_bytes(oarr)
        }
    }
    pub fn read_u8(&self, pos: usize)->u8{
        let lpos = pos / settings::MEMPOOL_CHUNK_SIZE_BYTE;
        let offset = pos % settings::MEMPOOL_CHUNK_SIZE_BYTE;
        
        let arr = &self.buff[lpos];
        arr[offset]
    }   
    pub fn read_data(&self, pos: usize, out: &mut[u8]){
        let mut cpos = pos;
        let mut clen = 0;
        while clen < out.len() {
            let lpos = cpos / settings::MEMPOOL_CHUNK_SIZE_BYTE;
            let offset = cpos % settings::MEMPOOL_CHUNK_SIZE_BYTE;
                    
            let arr = &self.buff[lpos]; // может не попасть в один 
            let wlen = (out.len() - clen).min(settings::MEMPOOL_CHUNK_SIZE_BYTE - offset);
            out[clen..clen + wlen].copy_from_slice(&arr[offset..offset + wlen]);

            cpos += settings::MEMPOOL_CHUNK_SIZE_BYTE - offset;
            clen += wlen;
        }
    }
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
impl ToBeBytes for u32 {
    type ByteArray = [u8; 4];
    fn to_be_bytes(&self) -> Self::ByteArray {
        u32::to_be_bytes(*self)
    }
}
impl ToBeBytes for u64 {
    type ByteArray = [u8; 8];
    fn to_be_bytes(&self) -> Self::ByteArray {
        u64::to_be_bytes(*self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn write_read_u32_across_chunk_boundary() {
        let mut mp = Mempool::new();
        let (pos, len) = mp.alloc(settings::MEMPOOL_CHUNK_SIZE_BYTE + 16);
        assert_eq!(len, settings::MEMPOOL_CHUNK_SIZE_BYTE + 16);

        let p = pos + settings::MEMPOOL_CHUNK_SIZE_BYTE - 2; // 2 bytes left in chunk
        let v: u32 = 0xA1B2_C3D4;
        mp.write_num(p, v);
        assert_eq!(mp.read_u32(p), v);
    }

    #[test]
    fn write_read_u64_across_chunk_boundary() {
        let mut mp = Mempool::new();
        let (pos, len) = mp.alloc(settings::MEMPOOL_CHUNK_SIZE_BYTE + 32);
        assert_eq!(len, settings::MEMPOOL_CHUNK_SIZE_BYTE + 32);

        let p = pos + settings::MEMPOOL_CHUNK_SIZE_BYTE - 3; // 3 bytes left in chunk
        let v: u64 = 0x0102_0304_0506_0708;
        mp.write_num(p, v);
        assert_eq!(mp.read_u64(p), v);
    }

    #[test]
    fn write_read_data_across_chunk_boundary() {
        let mut mp = Mempool::new();
        let data_len = 128;
        let (pos, _) = mp.alloc(settings::MEMPOOL_CHUNK_SIZE_BYTE + data_len);

        let start = pos + settings::MEMPOOL_CHUNK_SIZE_BYTE - 64;
        let data: Vec<u8> = (0..data_len as u8).collect();
        mp.write_data(start, &data);

        let mut out = vec![0u8; data_len];
        mp.read_data(start, &mut out);
        assert_eq!(out, data);
    }

    #[test]
    fn alloc_reuses_freed_block() {
        let mut mp = Mempool::new();
        let (p1, l1) = mp.alloc(256);
        let (p2, l2) = mp.alloc(256);
        assert_eq!(l1, 256);
        assert_eq!(l2, 256);
        assert_ne!(p1, p2);

        mp.free(p1, l1);
        let (p3, l3) = mp.alloc(256);
        assert_eq!(l3, 256);
        assert_eq!(p3, p1);
    }

    #[test]
    fn alloc_split_removes_original_segment_size() {
        let mut mp = Mempool::new();

        // Allocate one larger block and free it, creating a single free segment of that size.
        let (p, l) = mp.alloc(512);
        mp.free(p, l);

        // Alloc a smaller block from it -> split.
        let (_p2, l2) = mp.alloc(128);
        assert_eq!(l2, 128);

        // The original 512-size segment should no longer exist.
        // (It becomes one allocated 128 segment + one free 384 tail segment.)
        assert!(!mp.free_mem.contains_key(&512));
        assert!(mp.free_mem.contains_key(&128));
        assert!(mp.free_mem.contains_key(&384));
    }

    #[test]
    fn alloc_exact_keeps_size_bucket_but_consumes_position() {
        let mut mp = Mempool::new();

        let (p, l) = mp.alloc(256);
        assert_eq!(l, 256);
        mp.free(p, l);

        // We should have a bucket for 256 with exactly one position.
        assert!(mp.free_mem.contains_key(&256));
        assert_eq!(mp.free_mem.get(&256).unwrap().1.len(), 1);

        // Allocate exactly 256: consumes the position but bucket (count) remains as "empty size".
        let (p2, l2) = mp.alloc(256);
        assert_eq!(l2, 256);
        assert_eq!(p2, p);

        assert!(mp.free_mem.contains_key(&256));
        assert_eq!(mp.free_mem.get(&256).unwrap().1.len(), 0);
    }

    #[test]
    fn check_free_mem_shrinks_free_tail_when_large_enough() {
        let mut mp = Mempool::new();
        let chunk = settings::MEMPOOL_CHUNK_SIZE_BYTE;

        // Grow beyond resize threshold (in bytes).
        let total_mb = settings::MEMPOOL_OVER_SIZE_MB + 16;
        let total_bytes = total_mb * 1024 * 1024;
        let total_chunks = (total_bytes + chunk - 1) / chunk;

        let mut positions = Vec::with_capacity(total_chunks);
        for _ in 0..total_chunks {
            let (p, l) = mp.alloc(chunk);
            assert_eq!(l, chunk);
            positions.push(p);
        }
        assert_eq!(mp.buff.len(), total_chunks);

        // Free a big tail: >= 50% to exceed MEMPOOL_MIN_PERCENT_FOR_RESIZE (0.25).
        let free_tail_chunks = total_chunks / 2;
        let tail_start_ix = total_chunks - free_tail_chunks;
        for &p in &positions[tail_start_ix..] {
            mp.free(p, chunk);
        }

        let before_chunks = mp.buff.len();
        let _ = mp.check_free_mem(0);
        let after_chunks = mp.buff.len();

        assert_eq!(before_chunks - after_chunks, free_tail_chunks);
    }

    #[test]
    fn shrink_free_tail_can_shrink_multiple_steps_in_one_call() {
        let mut mp = Mempool::new();
        let chunk = settings::MEMPOOL_CHUNK_SIZE_BYTE;

        // Build an artificial free list with two tail blocks that can be shrunk one after another.
        // This targets the iterative shrink loop itself (not the coalescing logic).
        mp.buff = vec![vec![0; chunk]; 4];
        mp.free_len = 2 * chunk;
        mp.free_count = 0;
        mp.free_mem = BTreeMap::from([(chunk, (2, vec![2 * chunk, 3 * chunk]))]);

        mp.shrink_free_tail_for_test();

        // We should shrink 2 chunks: from 4 -> 2.
        assert_eq!(mp.buff.len(), 2);
        assert_eq!(mp.free_len, 0);
        assert!(mp.free_mem.get(&chunk).is_none());
    }

    #[test]
    fn check_free_mem_does_not_shrink_unaligned_tail() {
        let mut mp = Mempool::new();
        let chunk = settings::MEMPOOL_CHUNK_SIZE_BYTE;

        // Grow beyond resize threshold.
        let total_mb = settings::MEMPOOL_OVER_SIZE_MB + 16;
        let total_bytes = total_mb * 1024 * 1024;
        let total_chunks = (total_bytes + chunk - 1) / chunk;

        let mut positions = Vec::with_capacity(total_chunks);
        for _ in 0..total_chunks {
            let (p, l) = mp.alloc(chunk);
            assert_eq!(l, chunk);
            positions.push(p);
        }

        // Create a tail block that ends at buffer end but is NOT chunk-aligned:
        // free the last chunk (aligned), then allocate a small piece from it, leaving an unaligned free tail.
        let last = *positions.last().unwrap();
        mp.free(last, chunk);
        let (_p, _l) = mp.alloc(16); // takes from that free chunk, leaving endlen = chunk-16 at last+16

        let before_chunks = mp.buff.len();
        let _ = mp.check_free_mem(0);
        let after_chunks = mp.buff.len();

        // Should not shrink because tail free block is unaligned (pos % chunk != 0).
        assert_eq!(after_chunks, before_chunks);
    }

    #[test]
    fn check_free_mem_shrinks_but_keeps_reserved_req_at_tail_start() {
        let mut mp = Mempool::new();
        let chunk = settings::MEMPOOL_CHUNK_SIZE_BYTE;

        let total_mb = settings::MEMPOOL_OVER_SIZE_MB + 16;
        let total_bytes = total_mb * 1024 * 1024;
        let total_chunks = (total_bytes + chunk - 1) / chunk;

        let mut positions = Vec::with_capacity(total_chunks);
        for _ in 0..total_chunks {
            let (p, l) = mp.alloc(chunk);
            assert_eq!(l, chunk);
            positions.push(p);
        }
        assert_eq!(mp.buff.len(), total_chunks);

        // Free a big tail; then request exactly one chunk. The allocator inside
        // check_free_mem(req_size) may pick the tail as req_pos; shrinking must not remove req.
        let free_tail_chunks = total_chunks / 2;
        let tail_start_ix = total_chunks - free_tail_chunks;
        let tail_start_pos = positions[tail_start_ix];
        for &p in &positions[tail_start_ix..] {
            mp.free(p, chunk);
        }

        let got = mp.check_free_mem(chunk);
        let after_chunks = mp.buff.len();

        // We should keep exactly one chunk (the reserved req) at the tail start.
        assert_eq!(after_chunks, tail_start_ix + 1);
        assert_eq!(got, Some((tail_start_pos, chunk)));
    }

    #[test]
    fn new_mem_updates_free_len_for_endlen() {
        let mut mp = Mempool::new();
        let chunk = settings::MEMPOOL_CHUNK_SIZE_BYTE;

        let req = chunk + 16;
        let (_p, l) = mp.alloc(req);
        assert_eq!(l, req);

        // new_mem() should have created exactly one free tail block of size (chunk - 16).
        assert_eq!(mp.free_len, chunk - 16);
    }

    #[test]
    fn check_free_mem_coalesces_adjacent_frees_and_satisfies_request() {
        let mut mp = Mempool::new();
        let chunk = settings::MEMPOOL_CHUNK_SIZE_BYTE;

        // Make csz large enough so the internal "worth compressing" guard passes.
        // Allocate N chunks contiguously.
        let total_chunks = 20;
        let mut pos = Vec::with_capacity(total_chunks);
        for _ in 0..total_chunks {
            let (p, l) = mp.alloc(chunk);
            assert_eq!(l, chunk);
            pos.push(p);
        }
        for i in 1..total_chunks {
            assert_eq!(pos[i], pos[i - 1] + chunk);
        }

        // Free a large contiguous tail; it should coalesce into one big block.
        // Need enough free_len so the internal guard passes: (free_len - req) >= 0.2 * csz.
        let ix = 10;
        for i in ix..total_chunks {
            mp.free(pos[i], chunk);
        }

        // Request 1 chunk from that coalesced region.
        let got = mp.check_free_mem(chunk);
        assert_eq!(got, Some((pos[ix], chunk)));

        // Next allocation of 1 chunk should reuse the remainder at pos[ix] + chunk.
        let (p2, l2) = mp.alloc(chunk);
        assert_eq!(l2, chunk);
        assert_eq!(p2, pos[ix] + chunk);
    }

    #[test]
    fn check_free_mem_does_not_coalesce_when_not_adjacent() {
        let mut mp = Mempool::new();
        let chunk = settings::MEMPOOL_CHUNK_SIZE_BYTE;

        let total_chunks = 20;
        let mut pos = Vec::with_capacity(total_chunks);
        for _ in 0..total_chunks {
            let (p, l) = mp.alloc(chunk);
            assert_eq!(l, chunk);
            pos.push(p);
        }

        // Free many chunks, but only at even indices => none are adjacent.
        for i in (0..total_chunks).step_by(2) {
            mp.free(pos[i], chunk);
        }

        // No adjacency => no coalescing => check_free_mem should not find a merged block.
        // With a large request, alloc() can't satisfy it either, so we expect None.
        let got = mp.check_free_mem(2 * chunk);
        assert_eq!(got, None);
    }
}
