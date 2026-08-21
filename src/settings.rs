use std::sync::atomic::{AtomicUsize, Ordering};

/// Reserved topic for broker-internal events (client connect/disconnect, subscribe/unsubscribe).
pub const INTERNAL_CHANNEL_TOPIC: &str = "__#internal_channel";

pub const WRITE_BUFFER_CAPASITY: usize = 64 * 1024;
pub const READ_BUFFER_CAPASITY: usize = 64 * 1024;
/// Flush read messages to the receive thread (and wake it) every N messages so
/// a large burst does not delay delivery until the whole stream drain finishes.
pub const LISTENER_RECEIVE_FLUSH_BATCH: usize = 100;
pub const BYTESTREAM_READ_BUFFER_SIZE: usize = 8 * 1024;
pub const BYTESTREAM_WRITE_BUFFER_SIZE: usize = 8 * 1024;
/// Default max framed message size (also initial value of [`max_message_size`]).
pub const BYTESTREAM_MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024;
pub const EPOLL_LISTEN_EVENTS_COUNT: usize = 128;
pub const CHECK_AVAILABLE_STREAM_TIMEOUT_MS: u64 = 10*1000;  //10sec
pub const UPDATE_LAST_MESS_NUMBER_TIMEOUT_MS: u64 = 1000;    //1s
pub const BYTESTREAM_WOULD_BLOCK_TIMEOUT_MS: u64 = 10*1000;  //10sec
pub const SENDER_THREAD_WAIT_TIMEOUT_MS: u64 = 100;
pub const LISTENER_THREAD_WAIT_TIMEOUT_MS: u64 = 100;
/// Backoff when the sender loop has no writable work (avoids tight lock contention).
pub const SENDER_THREAD_IDLE_BACKOFF_MS: u64 = 1;
/// Default zstd threshold (also initial value of [`compress_threshold`]).
pub const MIN_SIZE_DATA_FOR_COMPRESS_BYTE: usize = 1024*1024;
pub const DATA_COMPRESS_LEVEL: i32 = 0; // A level of `0` uses zstd's default (currently `3`).
pub const MEMPOOL_MIN_PERCENT_FOR_COMPRESS: f32 = 0.2;
pub const MEMPOOL_FREE_COUNT_FOR_RESIZE: usize = 1000000;
pub const MEMPOOL_CHUNK_SIZE_BYTE: usize = 256 * 1024;

// Shrink mempool backing storage (truncate tail) when a large, chunk-aligned
// free block exists at the very end of the buffer.
pub const MEMPOOL_MIN_PERCENT_FOR_RESIZE: f32 = 0.25;
pub const MEMPOOL_OVER_SIZE_MB: usize = 64;

static MAX_MESSAGE_SIZE: AtomicUsize = AtomicUsize::new(BYTESTREAM_MAX_MESSAGE_SIZE);
static COMPRESS_THRESHOLD: AtomicUsize = AtomicUsize::new(MIN_SIZE_DATA_FOR_COMPRESS_BYTE);
/// 0 = unlimited (default).
static MAX_SEND_QUEUE: AtomicUsize = AtomicUsize::new(0);

pub fn max_message_size() -> usize {
    MAX_MESSAGE_SIZE.load(Ordering::Relaxed)
}

/// Returns false if `bytes == 0`.
pub fn set_max_message_size(bytes: usize) -> bool {
    if bytes == 0 {
        return false;
    }
    MAX_MESSAGE_SIZE.store(bytes, Ordering::Relaxed);
    true
}

pub fn compress_threshold() -> usize {
    COMPRESS_THRESHOLD.load(Ordering::Relaxed)
}

/// Returns false if `bytes == 0`.
pub fn set_compress_threshold(bytes: usize) -> bool {
    if bytes == 0 {
        return false;
    }
    COMPRESS_THRESHOLD.store(bytes, Ordering::Relaxed);
    true
}

/// Max in-memory sender messages per peer slot. `0` = unlimited.
pub fn max_send_queue() -> usize {
    MAX_SEND_QUEUE.load(Ordering::Relaxed)
}

pub fn set_max_send_queue(n: usize) -> bool {
    MAX_SEND_QUEUE.store(n, Ordering::Relaxed);
    true
}

#[cfg(test)]
static LIMITS_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Serialize tests that mutate process-global size / compress / queue limits.
#[cfg(test)]
pub fn test_limits_lock() -> std::sync::MutexGuard<'static, ()> {
    LIMITS_TEST_LOCK
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}
