//! Status / error callback kinds and a thread-safe emitter for FFI callbacks.

use crate::UData;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Peer registered / `run` (internal `client_connected`), related topic only.
pub const LNR_PEER_CONNECTED: i32 = 1;
/// Peer teardown (internal `client_disconnected`), related topic only.
pub const LNR_PEER_DISCONNECTED: i32 = 2;
/// Peer `subscribe` (internal `subscribed`), related topic only.
pub const LNR_PEER_SUBSCRIBED: i32 = 3;
/// Peer `unsubscribe` (internal `unsubscribed`), related topic only.
pub const LNR_PEER_UNSUBSCRIBED: i32 = 4;
/// Sender: TCP connect fail or stream close for a local route.
pub const LNR_SENDER_ROUTE_LOST: i32 = 5;
/// Sender: background store / DB error on send-reconnect / persist paths.
pub const LNR_SENDER_STORE_ERROR: i32 = 6;
/// Sender: background write/flush failure after an accepted send.
pub const LNR_SENDER_SEND_ERROR: i32 = 7;
/// Listener: background store / DB error on ack / lookup paths.
pub const LNR_LISTENER_STORE_ERROR: i32 = 8;

/// Keys into the status detail message map ([`status_msg_templates`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StatusMsg {
    GetLastMessNumber,
    LoadMessagesForSender,
    TcpConnectFailed,
    WriteFailed,
    FlushFailed,
    StreamClosed,
    GetLastMessNumberConnKey,
    SaveMessagesFromSender,
    SetLastMessNumberFromListener,
    GetLastMessNumberForListener,
    GetSenderTopicByConnectionKey,
}

/// Template strings for [`StatusMsg`]. Placeholders are `{}` in order of `args`.
fn status_msg_templates() -> &'static HashMap<StatusMsg, &'static str> {
    static MAP: OnceLock<HashMap<StatusMsg, &'static str>> = OnceLock::new();
    MAP.get_or_init(|| {
        HashMap::from([
            (
                StatusMsg::GetLastMessNumber,
                "get_last_mess_number_for_sender: {}",
            ),
            (
                StatusMsg::LoadMessagesForSender,
                "load_messages_for_sender {}: {}",
            ),
            (
                StatusMsg::TcpConnectFailed,
                "tcp connect failed: {} {}",
            ),
            (StatusMsg::WriteFailed, "write failed: {}"),
            (StatusMsg::FlushFailed, "flush failed: {} {} {}"),
            (StatusMsg::StreamClosed, "stream closed: {}"),
            (
                StatusMsg::GetLastMessNumberConnKey,
                "get_last_mess_number_for_sender connection_key {}",
            ),
            (
                StatusMsg::SaveMessagesFromSender,
                "save_messages_from_sender connection_key {}: {}",
            ),
            (
                StatusMsg::SetLastMessNumberFromListener,
                "set_last_mess_number_from_listener connection_key {}: {}",
            ),
            (
                StatusMsg::GetLastMessNumberForListener,
                "get_last_mess_number_for_listener: {}",
            ),
            (
                StatusMsg::GetSenderTopicByConnectionKey,
                "get_sender_topic conn_key {}: {}",
            ),
        ])
    })
}

fn render_status_msg(msg: StatusMsg, args: &[&str]) -> String {
    let Some(mut template) = status_msg_templates().get(&msg).copied() else {
        return String::new();
    };
    let mut out = String::with_capacity(template.len().saturating_add(32));
    for arg in args {
        if let Some(i) = template.find("{}") {
            out.push_str(&template[..i]);
            out.push_str(arg);
            template = &template[i + 2..];
        } else {
            break;
        }
    }
    out.push_str(template);
    out
}

pub type StatusCbackIntern = extern "C" fn(
    kind: i32,
    topic: *const i8,
    peer: *const i8,
    message: *const i8,
    udata: *mut libc::c_void,
);

struct StatusEmitterInner {
    cb: Option<StatusCbackIntern>,
    udata: UData,
}

struct StatusEmitterShared {
    inner: Mutex<StatusEmitterInner>,
    /// Lock-free fast path when no user callback is registered.
    enabled: AtomicBool,
}

/// Shared between `Client` and background sender threads.
///
/// [`Clone`] is a single `Arc` bump (not a deep copy).
#[derive(Clone)]
pub struct StatusEmitter {
    shared: Arc<StatusEmitterShared>,
}

impl StatusEmitter {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(StatusEmitterShared {
                inner: Mutex::new(StatusEmitterInner {
                    cb: None,
                    udata: UData::null(),
                }),
                enabled: AtomicBool::new(false),
            }),
        }
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.shared.enabled.load(Ordering::Relaxed)
    }

    pub fn set_callback(&self, cb: Option<StatusCbackIntern>, udata: UData) {
        self.shared.enabled.store(cb.is_some(), Ordering::Relaxed);
        if let Ok(mut inner) = self.shared.inner.lock() {
            inner.cb = cb;
            inner.udata = udata;
        }
    }

    /// Emit with a raw detail string (e.g. internal-channel event name).
    pub fn emit(&self, kind: i32, topic: &str, peer: &str, message: &str) {
        if !self.is_enabled() {
            return;
        }
        self.emit_locked(kind, topic, peer, message);
    }

    /// Emit using a mapped template ([`StatusMsg`]); formats only when a callback is set.
    pub fn emit_msg(&self, kind: i32, topic: &str, peer: &str, msg: StatusMsg, args: &[&str]) {
        if !self.is_enabled() {
            return;
        }
        let message = render_status_msg(msg, args);
        self.emit_locked(kind, topic, peer, &message);
    }

    fn emit_locked(&self, kind: i32, topic: &str, peer: &str, message: &str) {
        let Ok(inner) = self.shared.inner.lock() else {
            return;
        };
        let Some(cb) = inner.cb else {
            return;
        };
        // Empty strings: borrow static NUL, avoid three heap CString allocs on sparse fields.
        let topic_owned;
        let peer_owned;
        let message_owned;
        let topic_p = if topic.is_empty() {
            empty_cstr().as_ptr()
        } else {
            topic_owned = CString::new(topic).unwrap_or_default();
            topic_owned.as_ptr()
        };
        let peer_p = if peer.is_empty() {
            empty_cstr().as_ptr()
        } else {
            peer_owned = CString::new(peer).unwrap_or_default();
            peer_owned.as_ptr()
        };
        let message_p = if message.is_empty() {
            empty_cstr().as_ptr()
        } else {
            message_owned = CString::new(message).unwrap_or_default();
            message_owned.as_ptr()
        };
        cb(kind, topic_p, peer_p, message_p, inner.udata.0);
    }
}

fn empty_cstr() -> &'static CStr {
    // SAFETY: single trailing NUL, no interior NUL.
    unsafe { CStr::from_bytes_with_nul_unchecked(b"\0") }
}

impl Default for StatusEmitter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_replaces_placeholders_in_order() {
        assert_eq!(
            render_status_msg(StatusMsg::SaveMessagesFromSender, &["7", "busy"]),
            "save_messages_from_sender connection_key 7: busy"
        );
        assert_eq!(
            render_status_msg(StatusMsg::StreamClosed, &["127.0.0.1:1"]),
            "stream closed: 127.0.0.1:1"
        );
    }
}
