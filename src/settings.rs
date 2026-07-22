/// Reserved topic for broker-internal events (client connect/disconnect, subscribe/unsubscribe).
pub const INTERNAL_CHANNEL_TOPIC: &str = "__#internal_channel";

pub const WRITE_BUFFER_CAPASITY: usize = 64 * 1024;
pub const READ_BUFFER_CAPASITY: usize = 64 * 1024;
pub const BYTESTREAM_READ_BUFFER_SIZE: usize = 8 * 1024;
pub const BYTESTREAM_WRITE_BUFFER_SIZE: usize = 8 * 1024;
pub const BYTESTREAM_MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024;
pub const EPOLL_LISTEN_EVENTS_COUNT: usize = 128;
pub const CHECK_AVAILABLE_STREAM_TIMEOUT_MS: u64 = 10*1000;  //10sec
pub const UPDATE_LAST_MESS_NUMBER_TIMEOUT_MS: u64 = 1000;    //1s
pub const BYTESTREAM_WOULD_BLOCK_TIMEOUT_MS: u64 = 10*1000;  //10sec
pub const SENDER_THREAD_WAIT_TIMEOUT_MS: u64 = 100;
pub const LISTENER_THREAD_WAIT_TIMEOUT_MS: u64 = 100;
/// Backoff when the sender loop has no writable work (avoids tight lock contention).
pub const SENDER_THREAD_IDLE_BACKOFF_MS: u64 = 1;
pub const MIN_SIZE_DATA_FOR_COMPRESS_BYTE: usize = 1024*1024;
pub const DATA_COMPRESS_LEVEL: i32 = 0; // A level of `0` uses zstd's default (currently `3`).
pub const MEMPOOL_MIN_PERCENT_FOR_COMPRESS: f32 = 0.2;
pub const MEMPOOL_FREE_COUNT_FOR_RESIZE: usize = 1000000;
pub const MEMPOOL_CHUNK_SIZE_BYTE: usize = 256 * 1024;

// Shrink mempool backing storage (truncate tail) when a large, chunk-aligned
// free block exists at the very end of the buffer.
pub const MEMPOOL_MIN_PERCENT_FOR_RESIZE: f32 = 0.25;
pub const MEMPOOL_OVER_SIZE_MB: usize = 64;