//! Process-global error log sink (optional). Default remains stderr via `eprintln!`.

use crate::UData;
use std::sync::{Mutex, OnceLock};

pub type LogCbackIntern =
    extern "C" fn(message: *const i8, udata: *mut libc::c_void);

struct LogSink {
    cb: Option<LogCbackIntern>,
    udata: UData,
}

fn log_sink() -> &'static Mutex<LogSink> {
    static SINK: OnceLock<Mutex<LogSink>> = OnceLock::new();
    SINK.get_or_init(|| {
        Mutex::new(LogSink {
            cb: None,
            udata: UData::null(),
        })
    })
}

/// Serialize tests that install/clear the process-global log hook.
#[cfg(test)]
pub fn test_log_hook_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

/// Install or clear the global log callback. `None` restores stderr-only behavior.
pub fn set_log_cb(cb: Option<LogCbackIntern>, udata: UData) {
    if let Ok(mut sink) = log_sink().lock() {
        sink.cb = cb;
        sink.udata = udata;
    }
}

/// Emit an error line: hook if set, otherwise `eprintln!`.
pub fn emit_error_line(line: &str) {
    let (cb, udata) = match log_sink().lock() {
        Ok(sink) => (sink.cb, sink.udata.0),
        Err(_) => {
            eprintln!("{}", line);
            return;
        }
    };
    if let Some(cb) = cb {
        if let Ok(c) = std::ffi::CString::new(line) {
            cb(c.as_ptr(), udata);
            return;
        }
    }
    eprintln!("{}", line);
}
