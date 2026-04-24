use crate::{print_error, settings};
use crate::mempool::Mempool;

use std::io::{Read, Write};
use std::time::Duration;
use std::sync::{Arc, Mutex};

// return: mem_pos, mem_alloc_length, is_shutdown
pub fn read_stream<T>(stream: &mut T, mempool: &Arc<Mutex<Mempool>>) -> (usize, usize, bool)
where
    T: Read,
{
    let mut buff = [0; settings::BYTESTREAM_READ_BUFFER_SIZE];
    let mut len_hdr = [0u8; 4];
    let mut hdr_offs: usize = 0;
    let msg_len: usize;
    let mem_pos: usize;
    let mem_alloc_length: usize;
    let mut mem_fill_length: usize = 0;

    // Phase 1: read 4-byte big-endian length header (u32).
    while hdr_offs < len_hdr.len() {
        match stream.read(&mut len_hdr[hdr_offs..]) {
            Ok(0) => {
                return (0, 0, true);
            }
            Ok(n) => {
                hdr_offs += n;
            }
            Err(e) => {
                let k = e.kind();
                if k == std::io::ErrorKind::Interrupted {
                    continue;
                }
                if k == std::io::ErrorKind::WouldBlock {
                    if hdr_offs == 0 {
                        return (0, 0, false);
                    }
                    // Busy-spin by request; yield to scheduler.
                    std::thread::sleep(Duration::from_millis(0));
                    continue;
                }
                print_error!(&format!("{}", k));
                return (0, 0, true);
            }
        }
    }

    msg_len = u32::from_be_bytes(len_hdr) as usize;
    if msg_len == 0 || msg_len > settings::BYTESTREAM_MAX_MESSAGE_SIZE {
        print_error!(&format!(
            "invalid bytestream length: {} (max {})",
            msg_len,
            settings::BYTESTREAM_MAX_MESSAGE_SIZE
        ));
        return (0, 0, true);
    }

    // Allocate message buffer in mempool.
    {
        let mut mp = mempool.lock().unwrap();
        (mem_pos, mem_alloc_length) = mp.alloc(msg_len);
        assert_eq!(msg_len, mem_alloc_length);
    }

    // Phase 2: read payload.
    while mem_fill_length < msg_len {
        let want = std::cmp::min(msg_len - mem_fill_length, buff.len());
        match stream.read(&mut buff[..want]) {
            Ok(0) => {
                // Stream closed mid-message: free and signal shutdown.
                mempool.lock().unwrap().free(mem_pos, mem_alloc_length);
                return (0, 0, true);
            }
            Ok(n) => {
                mempool.lock().unwrap().write_data(mem_pos + mem_fill_length, &buff[..n]);
                mem_fill_length += n;
            }
            Err(e) => {
                let k = e.kind();
                if k == std::io::ErrorKind::Interrupted {
                    continue;
                }
                if k == std::io::ErrorKind::WouldBlock {
                    // Busy-spin by request; yield to scheduler.
                    std::thread::sleep(Duration::from_millis(0));
                    continue;
                }
                print_error!(&format!("{}", k));
                mempool.lock().unwrap().free(mem_pos, mem_alloc_length);
                return (0, 0, true);
            }
        }
    }

    (mem_pos, mem_alloc_length, false)
}

pub fn write_stream<T>(stream: &mut T, mem_alloc_pos: usize, mem_alloc_length: usize, mempool: &Arc<Mutex<Mempool>>)->bool
where
    T: Write,
{    
    const BUFF_LEN: usize = settings::BYTESTREAM_WRITE_BUFFER_SIZE;
    let mut buff = [0; BUFF_LEN];
    buff[..std::mem::size_of::<u32>()].copy_from_slice((mem_alloc_length as u32).to_be_bytes().as_ref());
    let mut wsz: usize = 0;
    let mut offs: usize = std::mem::size_of::<u32>();
    let mut is_continue = false;
    let mess_size = mem_alloc_length;
    while wsz < mess_size{
        let endlen = std::cmp::min(mess_size - wsz + offs, BUFF_LEN);
        if !is_continue{
            if let Ok(mempool) = mempool.lock(){
                mempool.read_data(mem_alloc_pos + wsz, &mut buff[offs..endlen]);
            }           
        }
        match stream.write_all(&buff[..endlen]){
            Ok(_) => {
                wsz += endlen - offs;
                offs = 0;
                is_continue = false;
            },
            Err(err) => {
                let e = err.kind();
                if e == std::io::ErrorKind::WouldBlock || e == std::io::ErrorKind::Interrupted{
                    is_continue = true;
                    continue;
                }else{
                    print_error!(&format!("{}", e));
                    break;                  
                }
            },            
        }
    }
    wsz == mess_size
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Error, ErrorKind};

    fn mp() -> Arc<Mutex<Mempool>> {
        Arc::new(Mutex::new(Mempool::new()))
    }

    fn alloc_and_write(mp: &Arc<Mutex<Mempool>>, data: &[u8]) -> (usize, usize) {
        let (pos, len) = mp.lock().unwrap().alloc(data.len());
        assert_eq!(len, data.len());
        mp.lock().unwrap().write_data(pos, data);
        (pos, len)
    }

    fn read_from_mp(mp: &Arc<Mutex<Mempool>>, pos: usize, len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        mp.lock().unwrap().read_data(pos, &mut out);
        out
    }

    struct WouldBlockReader {
        data: Vec<u8>,
        pos: usize,
        plan: Vec<Result<usize, ErrorKind>>,
        step: usize,
    }

    impl WouldBlockReader {
        fn new(data: Vec<u8>, plan: Vec<Result<usize, ErrorKind>>) -> Self {
            Self { data, pos: 0, plan, step: 0 }
        }
    }

    impl Read for WouldBlockReader {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if self.step < self.plan.len() {
                let a = self.plan[self.step].clone();
                self.step += 1;
                match a {
                    Err(k) => return Err(Error::from(k)),
                    Ok(0) => return Ok(0),
                    Ok(max_n) => {
                        let n = max_n.min(out.len()).min(self.data.len().saturating_sub(self.pos));
                        if n == 0 {
                            return Ok(0);
                        }
                        out[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
                        self.pos += n;
                        return Ok(n);
                    }
                }
            }

            let n = out.len().min(self.data.len().saturating_sub(self.pos));
            if n == 0 {
                return Ok(0);
            }
            out[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    struct WouldBlockWriter {
        sink: Vec<u8>,
        plan: Vec<Result<(), ErrorKind>>,
        step: usize,
    }

    impl WouldBlockWriter {
        fn new(plan: Vec<Result<(), ErrorKind>>) -> Self {
            Self { sink: Vec::new(), plan, step: 0 }
        }
        fn into_inner(self) -> Vec<u8> {
            self.sink
        }
    }

    impl Write for WouldBlockWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.step < self.plan.len() {
                let a = self.plan[self.step].clone();
                self.step += 1;
                match a {
                    Err(k) => return Err(Error::from(k)),
                    Ok(()) => { /* proceed */ }
                }
            }
            self.sink.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct PartialWriter {
        sink: Vec<u8>,
        max_per_write: usize,
    }

    impl PartialWriter {
        fn new(max_per_write: usize) -> Self {
            Self {
                sink: Vec::new(),
                max_per_write,
            }
        }

        fn into_inner(self) -> Vec<u8> {
            self.sink
        }
    }

    impl Write for PartialWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let n = buf.len().min(self.max_per_write);
            self.sink.extend_from_slice(&buf[..n]);
            Ok(n)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn write_then_read_roundtrip_small() {
        let mp = mp();
        let data = b"hello world".to_vec();
        let (pos, len) = alloc_and_write(&mp, &data);

        let mut out = Vec::<u8>::new();
        {
            let mut cur = Cursor::new(&mut out);
            assert!(write_stream(&mut cur, pos, len, &mp));
        }

        let mut cur = Cursor::new(out);
        let (rpos, rlen, is_shutdown) = read_stream(&mut cur, &mp);
        assert!(!is_shutdown);
        assert_eq!(rlen, data.len());
        assert_eq!(read_from_mp(&mp, rpos, rlen), data);
    }

    #[test]
    fn write_then_read_roundtrip_large_over_buffer() {
        let mp = mp();
        let data_len = settings::BYTESTREAM_WRITE_BUFFER_SIZE * 3 + 123;
        let data: Vec<u8> = (0..data_len).map(|i| (i % 251) as u8).collect();
        let (pos, len) = alloc_and_write(&mp, &data);

        let mut out = Vec::<u8>::new();
        {
            let mut cur = Cursor::new(&mut out);
            assert!(write_stream(&mut cur, pos, len, &mp));
        }

        let mut cur = Cursor::new(out);
        let (rpos, rlen, is_shutdown) = read_stream(&mut cur, &mp);
        assert!(!is_shutdown);
        assert_eq!(rlen, data.len());
        assert_eq!(read_from_mp(&mp, rpos, rlen), data);
    }

    #[test]
    fn read_wouldblock_at_start_returns_retryable() {
        let mp = mp();
        let payload = b"abcd".to_vec();
        let mut msg = Vec::new();
        msg.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        msg.extend_from_slice(&payload);

        // WouldBlock before any bytes are available: should be retryable with no allocation.
        let mut r = WouldBlockReader::new(
            msg.clone(),
            vec![Err(ErrorKind::WouldBlock)],
        );
        let (p1, l1, s1) = read_stream(&mut r, &mp);
        assert_eq!((p1, l1, s1), (0, 0, false));

        let (p2, l2, s2) = read_stream(&mut r, &mp);
        assert!(!s2);
        assert_eq!(l2, payload.len());
        assert_eq!(read_from_mp(&mp, p2, l2), payload);
    }

    #[test]
    fn read_wouldblock_after_partial_header_still_completes() {
        let mp = mp();
        let payload = b"abcd".to_vec();
        let mut msg = Vec::new();
        msg.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        msg.extend_from_slice(&payload);

        // Read 2 bytes of header, then WouldBlock once, then the rest should be read by busy-spin.
        let mut r = WouldBlockReader::new(msg, vec![Ok(2), Err(ErrorKind::WouldBlock)]);
        let (p, l, s) = read_stream(&mut r, &mp);
        assert!(!s);
        assert_eq!(l, payload.len());
        assert_eq!(read_from_mp(&mp, p, l), payload);
    }

    #[test]
    fn read_wouldblock_mid_payload_still_completes_with_busy_spin() {
        let mp = mp();
        let payload: Vec<u8> = (0..10_000).map(|i| (i % 251) as u8).collect();
        let mut msg = Vec::new();
        msg.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        msg.extend_from_slice(&payload);

        let mut r = WouldBlockReader::new(msg, vec![Ok(4), Ok(100), Err(ErrorKind::WouldBlock)]);
        let (p, l, s) = read_stream(&mut r, &mp);
        assert!(!s);
        assert_eq!(l, payload.len());
        assert_eq!(read_from_mp(&mp, p, l), payload);
    }

    #[test]
    fn read_rejects_zero_message_length() {
        let mp = mp();
        let mut cur = Cursor::new(0u32.to_be_bytes().to_vec());
        let (p, l, s) = read_stream(&mut cur, &mp);
        assert_eq!((p, l, s), (0, 0, true));
    }

    #[test]
    fn read_eof_before_header_shutdown() {
        let mp = mp();
        let mut cur = Cursor::new(Vec::<u8>::new());
        let (p, l, s) = read_stream(&mut cur, &mp);
        assert_eq!((p, l, s), (0, 0, true));
    }

    #[test]
    fn read_eof_mid_payload_shutdown_and_frees() {
        let mp = mp();
        let payload = vec![1u8; 32];
        let mut msg = Vec::new();
        msg.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        msg.extend_from_slice(&payload[..10]); // truncated payload => EOF mid-message

        let mut cur = Cursor::new(msg);
        let (p, l, s) = read_stream(&mut cur, &mp);
        assert_eq!((p, l, s), (0, 0, true));
    }

    #[test]
    fn read_rejects_too_large_message_length() {
        let mp = mp();
        let too_big = (settings::BYTESTREAM_MAX_MESSAGE_SIZE as u32).saturating_add(1);
        let msg = too_big.to_be_bytes().to_vec(); // header only; should be rejected immediately
        let mut cur = Cursor::new(msg);
        let (p, l, s) = read_stream(&mut cur, &mp);
        assert_eq!((p, l, s), (0, 0, true));
    }

    #[test]
    fn write_stream_retries_on_interrupted() {
        let mp = mp();
        let data: Vec<u8> = (0..5000).map(|i| (i % 251) as u8).collect();
        let (pos, len) = alloc_and_write(&mp, &data);

        let mut w = WouldBlockWriter::new(vec![
            Err(ErrorKind::Interrupted),
            Ok(()),
        ]);
        assert!(write_stream(&mut w, pos, len, &mp));

        let out = w.into_inner();
        let mut cur = Cursor::new(out);
        let (rpos, rlen, is_shutdown) = read_stream(&mut cur, &mp);
        assert!(!is_shutdown);
        assert_eq!(rlen, data.len());
        assert_eq!(read_from_mp(&mp, rpos, rlen), data);
    }

    #[test]
    fn write_stream_works_with_partial_writes() {
        let mp = mp();
        let data: Vec<u8> = (0..2048).map(|i| (i % 251) as u8).collect();
        let (pos, len) = alloc_and_write(&mp, &data);

        let expected_len = 4 + data.len();
        let mut expected = Vec::with_capacity(expected_len);
        expected.extend_from_slice(&(data.len() as u32).to_be_bytes());
        expected.extend_from_slice(&data);

        let mut w = PartialWriter::new(17);
        let ok = write_stream(&mut w, pos, len, &mp);
        assert!(ok);
        assert_eq!(w.into_inner(), expected);
    }
}