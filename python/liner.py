from __future__ import absolute_import
import ctypes


lib_ = None

# Status callback kinds (match include/liner.h)
PEER_CONNECTED = 1
PEER_DISCONNECTED = 2
PEER_SUBSCRIBED = 3
PEER_UNSUBSCRIBED = 4
SENDER_ROUTE_LOST = 5
SENDER_STORE_ERROR = 6
SENDER_SEND_ERROR = 7
LISTENER_STORE_ERROR = 8
SENDER_BUSY = 9

# Sync last-error codes (match include/liner.h)
OK = 0
ERR_NOT_RUNNING = 1
ERR_ALREADY_RUNNING = 2
ERR_SELF_TOPIC = 3
ERR_INTERNAL_TOPIC = 4
ERR_NO_ADDR = 5
ERR_BIND = 6
ERR_STORE = 7
ERR_INVALID_ARG = 8
ERR_CLEAR_WHILE_RUNNING = 9
ERR_STARTUP = 10
ERR_BUSY = 11

def version() -> str:
    if not lib_:
        raise Exception('lib not load')
    pfun = lib_.lnr_version
    pfun.restype = ctypes.c_char_p
    pfun.argtypes = ()
    raw = pfun()
    return raw.decode("utf-8") if raw else ""

def set_log_callback(log_cback):
    """Process-global log callback: ``fn(message: str)``. Pass ``None`` to restore stderr."""
    global lib_, _logCBack
    if not lib_:
        raise Exception('lib not load')
    LogCb = ctypes.CFUNCTYPE(None, ctypes.c_char_p, ctypes.c_void_p)
    pfun = lib_.lnr_set_log_cb
    pfun.restype = ctypes.c_bool
    pfun.argtypes = (LogCb, ctypes.c_void_p)
    if log_cback is None:
        _logCBack = None
        pfun.argtypes = (ctypes.c_void_p, ctypes.c_void_p)
        return pfun(None, None)

    def c_cb(msg, _udata):
        m = msg.decode("utf-8") if msg else ""
        log_cback(m)

    _logCBack = LogCb(c_cb)
    return pfun(_logCBack, None)


def set_max_message_size(bytes_: int) -> bool:
    if not lib_:
        raise Exception('lib not load')
    pfun = lib_.lnr_set_max_message_size
    pfun.restype = ctypes.c_bool
    pfun.argtypes = (ctypes.c_size_t,)
    return pfun(bytes_)


def get_max_message_size() -> int:
    if not lib_:
        raise Exception('lib not load')
    pfun = lib_.lnr_get_max_message_size
    pfun.restype = ctypes.c_size_t
    pfun.argtypes = ()
    return int(pfun())


def set_compress_threshold(bytes_: int) -> bool:
    if not lib_:
        raise Exception('lib not load')
    pfun = lib_.lnr_set_compress_threshold
    pfun.restype = ctypes.c_bool
    pfun.argtypes = (ctypes.c_size_t,)
    return pfun(bytes_)


def get_compress_threshold() -> int:
    if not lib_:
        raise Exception('lib not load')
    pfun = lib_.lnr_get_compress_threshold
    pfun.restype = ctypes.c_size_t
    pfun.argtypes = ()
    return int(pfun())


def set_max_send_queue(n: int) -> bool:
    if not lib_:
        raise Exception('lib not load')
    pfun = lib_.lnr_set_max_send_queue
    pfun.restype = ctypes.c_bool
    pfun.argtypes = (ctypes.c_size_t,)
    return pfun(n)


def get_max_send_queue() -> int:
    if not lib_:
        raise Exception('lib not load')
    pfun = lib_.lnr_get_max_send_queue
    pfun.restype = ctypes.c_size_t
    pfun.argtypes = ()
    return int(pfun())


_logCBack = None


def loadLib(path : str):
  global lib_
  lib_ = ctypes.CDLL(path)
  
class Client:
    """Thin ctypes wrapper over ``include/liner.h`` (Redis constructor in ``__init__``).

    ``send_to`` / ``send_all`` forward ``at_least_once_delivery`` to ``lnr_send_to`` / ``lnr_send_all``.
    Default is ``True``. If each peer uses its **own** SQLite file (no shared catalog), use ``False``
    for cross-peer sends — see ``docs/using-sqlite.md``.
    """
    def __init__(self,
               uniqName: str,
               topic: str,
               localhost: str,
               redisPath: str
               ):
        if not lib_:
            raise Exception('lib not load')
        
        c_redisPath = redisPath.encode("utf-8")
        c_uniqName = uniqName.encode("utf-8")
        c_topic = topic.encode("utf-8")
        c_localhost = localhost.encode("utf-8")
        
        pfun = lib_.lnr_new_client_redis
        pfun.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p)
        pfun.restype = ctypes.c_void_p
        self.hClient_ = ctypes.c_void_p(pfun(c_uniqName, c_topic, c_localhost, c_redisPath))
    
        if not self.hClient_:
            raise Exception('error init client, check redisPath') 

    @classmethod
    def new_sqlite(
        cls,
        uniqName: str,
        topic: str,
        localhost: str,
        sqlite_path: str,
        receivers_json: str = "",
    ):
        """SQLite-backed client (``lnr_new_client_sqlite``). Use one shared ``sqlite_path`` for cooperating peers."""
        global lib_
        if not lib_:
            raise Exception('lib not load')
        inst = cls.__new__(cls)
        pfun = lib_.lnr_new_client_sqlite
        pfun.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p)
        pfun.restype = ctypes.c_void_p
        inst.hClient_ = ctypes.c_void_p(
            pfun(
                uniqName.encode("utf-8"),
                topic.encode("utf-8"),
                localhost.encode("utf-8"),
                sqlite_path.encode("utf-8"),
                receivers_json.encode("utf-8"),
            )
        )
        if not inst.hClient_:
            raise Exception('error init sqlite client, check sqlite_path / receivers_json')
        return inst

    @classmethod
    def new_postgres(
        cls,
        uniqName: str,
        topic: str,
        localhost: str,
        postgres_url: str,
    ):
        """PostgreSQL-backed client (``lnr_new_client_postgres``; library must be built with ``--features postgres``)."""
        global lib_
        if not lib_:
            raise Exception('lib not load')
        if not hasattr(lib_, 'lnr_new_client_postgres'):
            raise Exception('lib built without postgres support (rebuild with --features postgres)')
        inst = cls.__new__(cls)
        pfun = lib_.lnr_new_client_postgres
        pfun.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p)
        pfun.restype = ctypes.c_void_p
        inst.hClient_ = ctypes.c_void_p(
            pfun(
                uniqName.encode("utf-8"),
                topic.encode("utf-8"),
                localhost.encode("utf-8"),
                postgres_url.encode("utf-8"),
            )
        )
        if not inst.hClient_:
            raise Exception('error init postgres client, check postgres_url')
        return inst
                 
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()    
  
    def run(self, receive_cback)->bool:
        """
        :param ucb: def func(to: str, from: str, data: bytes)
        """
            
        def c_rcb(to: ctypes.c_char_p, from_: ctypes.c_char_p, data: ctypes.c_void_p, dlen: ctypes.c_size_t, udata: ctypes.c_void_p):
            data = ctypes.string_at(data, dlen)
            receive_cback(to.decode("utf-8"), from_.decode("utf-8"), data)
      
        recvCBackType = ctypes.CFUNCTYPE(None, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p)    
        self.recvCBack_ = recvCBackType(c_rcb)

        pfun = lib_.lnr_run
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, recvCBackType, ctypes.c_void_p)
        return pfun(self.hClient_, self.recvCBack_, ctypes.c_void_p())

    def set_status_callback(self, status_cback)->bool:
        """Register status/background-error callback: ``fn(kind: int, topic: str, peer: str, message: str)``.

        Pass ``None`` to clear. Peer events are filtered to related topics (sent/subscribed/refreshed).
        Kind constants: module-level ``PEER_CONNECTED`` .. ``LISTENER_STORE_ERROR``.
        """
        StatusCBackType = ctypes.CFUNCTYPE(
            None, ctypes.c_int, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p
        )
        pfun = lib_.lnr_set_status_cb
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, StatusCBackType, ctypes.c_void_p)

        if status_cback is None:
            self.statusCBack_ = None
            # NULL function pointer clears the callback (Option::None on Rust side).
            pfun.argtypes = (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p)
            return pfun(self.hClient_, None, None)

        def c_scb(kind: ctypes.c_int, topic: ctypes.c_char_p, peer: ctypes.c_char_p,
                  message: ctypes.c_char_p, udata: ctypes.c_void_p):
            t = topic.decode("utf-8") if topic else ""
            p = peer.decode("utf-8") if peer else ""
            m = message.decode("utf-8") if message else ""
            status_cback(int(kind), t, p, m)

        self.statusCBack_ = StatusCBackType(c_scb)
        return pfun(self.hClient_, self.statusCBack_, ctypes.c_void_p())

    def last_error_code(self) -> int:
        """Last sync API error code (``OK`` / ``ERR_*``). Detail text via stderr / log callback / ``last_error_message``."""
        pfun = lib_.lnr_last_error_code
        pfun.restype = ctypes.c_int
        pfun.argtypes = (ctypes.c_void_p,)
        return int(pfun(self.hClient_))

    def last_error_message(self) -> str:
        """Bare detail string for the last sync failure (empty when OK)."""
        pfun = lib_.lnr_last_error_message
        pfun.restype = ctypes.c_char_p
        pfun.argtypes = (ctypes.c_void_p,)
        raw = pfun(self.hClient_)
        return raw.decode("utf-8") if raw else ""

    def list_addresses(self, topic: str):
        """Return ``[(addr, unique_name), ...]`` from the store catalog for ``topic``."""
        out = []
        AddrCb = ctypes.CFUNCTYPE(None, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p)

        def c_cb(addr, name, _udata):
            a = addr.decode("utf-8") if addr else ""
            n = name.decode("utf-8") if name else ""
            out.append((a, n))

        cb = AddrCb(c_cb)
        pfun = lib_.lnr_list_addresses
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, ctypes.c_char_p, AddrCb, ctypes.c_void_p)
        if not pfun(self.hClient_, topic.encode("utf-8"), cb, None):
            return None
        return out

    def pending_count(self) -> int:
        """Offline queue depth for this sender; ``-1`` on error."""
        pfun = lib_.lnr_pending_count
        pfun.restype = ctypes.c_longlong
        pfun.argtypes = (ctypes.c_void_p,)
        return int(pfun(self.hClient_))

    def pending_by_peer(self):
        """Return ``[(addr, topic, unique_name, count), ...]`` or ``None`` on error."""
        out = []
        PendingCb = ctypes.CFUNCTYPE(
            None, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_longlong, ctypes.c_void_p
        )

        def c_cb(addr, topic, name, count, _udata):
            out.append((
                addr.decode("utf-8") if addr else "",
                topic.decode("utf-8") if topic else "",
                name.decode("utf-8") if name else "",
                int(count),
            ))

        cb = PendingCb(c_cb)
        pfun = lib_.lnr_pending_by_peer
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, PendingCb, ctypes.c_void_p)
        if not pfun(self.hClient_, cb, None):
            return None
        return out

    def set_advertise_addr(self, addr)->bool:
        """Publish ``addr`` to the store catalog instead of the bind string. Call before ``run``.

        Pass ``None`` or ``""`` to clear. Fails with ``ERR_ALREADY_RUNNING`` while running.
        """
        pfun = lib_.lnr_set_advertise_addr
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, ctypes.c_char_p)
        if addr is None:
            return pfun(self.hClient_, None)
        return pfun(self.hClient_, addr.encode("utf-8"))

    def stop(self)->bool:
        """Stop listener/sender and unregister (idempotent). Allows ``clear_*`` / ``run`` again."""
        pfun = lib_.lnr_stop
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p,)
        return pfun(self.hClient_)

    def is_running(self)->bool:
        pfun = lib_.lnr_is_running
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p,)
        return pfun(self.hClient_)

    def advertise_addr(self):
        """Configured advertise string; ``None`` if never set / cleared."""
        pfun = lib_.lnr_advertise_addr
        pfun.restype = ctypes.c_char_p
        pfun.argtypes = (ctypes.c_void_p,)
        raw = pfun(self.hClient_)
        return raw.decode("utf-8") if raw else None

    def bound_listen_addr(self):
        """Last successful bind address (kept after ``stop``); ``None`` if never run."""
        pfun = lib_.lnr_bound_listen_addr
        pfun.restype = ctypes.c_char_p
        pfun.argtypes = (ctypes.c_void_p,)
        raw = pfun(self.hClient_)
        return raw.decode("utf-8") if raw else None

    def published_addr(self):
        """Catalog address while registered; ``None`` after ``stop``."""
        pfun = lib_.lnr_published_addr
        pfun.restype = ctypes.c_char_p
        pfun.argtypes = (ctypes.c_void_p,)
        raw = pfun(self.hClient_)
        return raw.decode("utf-8") if raw else None

    def send_to(self, to_topic: str, data: bytearray, at_least_once_delivery: bool = True) -> bool:
        """``at_least_once_delivery``: same as C API; default ``True``. Use ``False`` for isolated per-process SQLite."""
        c_to_topic = to_topic.encode("utf-8")
        c_at_least_once_delivery = ctypes.c_bool(at_least_once_delivery)
        c_dlen = ctypes.c_size_t(len(data))
        c_data = ctypes.c_char * len(data)
   
        pfun = lib_.lnr_send_to
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_bool)
        return pfun(self.hClient_, c_to_topic, c_data.from_buffer_copy(data), c_dlen, c_at_least_once_delivery)
    
    def send_all(self, to_topic: str, data: bytearray, at_least_once_delivery: bool = True) -> bool:
        """Same third-argument semantics as :meth:`send_to`."""
        c_to_topic = to_topic.encode("utf-8")
        c_at_least_once_delivery = ctypes.c_bool(at_least_once_delivery)
        c_dlen = ctypes.c_size_t(len(data))
        c_data = ctypes.c_char * len(data)
   
        pfun = lib_.lnr_send_all
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_bool)
        return pfun(self.hClient_, c_to_topic, c_data.from_buffer_copy(data), c_dlen, c_at_least_once_delivery)
    
    def subscribe(self, to_topic: str)->bool:
        c_to_topic = to_topic.encode("utf-8")
        
        pfun = lib_.lnr_subscribe
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, ctypes.c_char_p)
        return pfun(self.hClient_, c_to_topic)
    
    def unsubscribe(self, to_topic: str)->bool:
        c_to_topic = to_topic.encode("utf-8")
        
        pfun = lib_.lnr_unsubscribe
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, ctypes.c_char_p)
        return pfun(self.hClient_, c_to_topic)
    
    def refresh_address_topic(self, to_topic: str)->bool:
        c_to_topic = to_topic.encode("utf-8")
        
        pfun = lib_.lnr_refresh_address_topic
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, ctypes.c_char_p)
        return pfun(self.hClient_, c_to_topic)
    
    def clear_stored_messages(self)->bool:
        pfun = lib_.lnr_clear_stored_messages
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p,)
        return pfun(self.hClient_)
    
    def clear_addresses_of_topic(self)->bool:
        pfun = lib_.lnr_clear_addresses_of_topic
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p,)
        return pfun(self.hClient_)
    
    def close(self):
        if (self.hClient_):
            pfun = lib_.lnr_delete_client
            pfun.argtypes = (ctypes.c_void_p,)
            pfun(self.hClient_)
