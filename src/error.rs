//! Sync API error codes (C `LNR_OK` / `LNR_ERR_*`). Detail text still goes to stderr via `print_error!`.

/// Stable `i32` values shared with [`include/liner.h`](../../include/liner.h).
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    Ok = 0,
    NotRunning = 1,
    AlreadyRunning = 2,
    SelfTopic = 3,
    InternalTopic = 4,
    NoAddr = 5,
    Bind = 6,
    Store = 7,
    InvalidArg = 8,
    ClearWhileRunning = 9,
    /// Listener startup after TCP bind (mio poll/register/waker or topic_key).
    Startup = 10,
}

impl ErrorCode {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

impl Default for ErrorCode {
    fn default() -> Self {
        ErrorCode::Ok
    }
}
