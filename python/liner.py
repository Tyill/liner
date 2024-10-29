from __future__ import absolute_import
import ctypes


lib_ = None

def loadLib(path : str):
  global lib_
  lib_ = ctypes.CDLL(path)
  
class Client: 
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
        
        pfun = lib_.ln_new_client
        pfun.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p)
        pfun.restype = ctypes.c_void_p
        self.hClient_ = ctypes.c_void_p(pfun(c_uniqName, c_topic, c_localhost, c_redisPath))
    
        pfun = lib_.ln_has_client
        pfun.argtypes = (ctypes.c_void_p,)
        if not pfun(ctypes.byref(self.hClient_)):
            raise Exception('error init client, check redisPath') 
                 
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if (self.hClient_):
            pfun = lib_.ln_delete_client
            pfun.argtypes = (ctypes.c_void_p,)
            pfun(self.hClient_)
  
    def run(self, receive_cback)->bool:
        """
        :param ucb: def func(to: str, from: str, data: bytes)
        """
            
        def c_rcb(to: ctypes.c_char_p, from_: ctypes.c_char_p, data: ctypes.c_void_p, dlen: ctypes.c_size_t, udata: ctypes.c_void_p):
            data = ctypes.string_at(data, dlen)
            receive_cback(str(to), str(from_), data)
      
        recvCBackType = ctypes.CFUNCTYPE(None, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p)    
        self.recvCBack_ = recvCBackType(c_rcb)

        pfun = lib_.ln_run
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, recvCBackType, ctypes.c_void_p)
        return pfun(ctypes.byref(self.hClient_), self.recvCBack_, ctypes.c_void_p())
    
    def send_to(self, to_topic: str, data: bytearray, at_least_once_delivery: bool = True)->bool:
        c_to_topic = to_topic.encode("utf-8")
        c_at_least_once_delivery = ctypes.c_bool(at_least_once_delivery)
        c_dlen = ctypes.c_size_t(len(data))
        c_data = ctypes.c_char * len(data)
   
        pfun = lib_.ln_send_to
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_bool)
        return pfun(ctypes.byref(self.hClient_), c_to_topic, c_data.from_buffer_copy(data), c_dlen, c_at_least_once_delivery)
    
    def send_all(self, to_topic: str, data: bytearray, at_least_once_delivery: bool = True)->bool:
        c_to_topic = to_topic.encode("utf-8")
        c_at_least_once_delivery = ctypes.c_bool(at_least_once_delivery)
        c_dlen = ctypes.c_size_t(len(data))
        c_data = ctypes.c_char * len(data)
   
        pfun = lib_.ln_send_all
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_bool)
        return pfun(ctypes.byref(self.hClient_), c_to_topic, c_data.from_buffer_copy(data), c_dlen, c_at_least_once_delivery)
    
    def subscribe(self, to_topic: str)->bool:
        c_to_topic = to_topic.encode("utf-8")
        
        pfun = lib_.ln_subscribe
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, ctypes.c_char_p)
        return pfun(ctypes.byref(self.hClient_), c_to_topic)
    
    def unsubscribe(self, to_topic: str)->bool:
        c_to_topic = to_topic.encode("utf-8")
        
        pfun = lib_.ln_unsubscribe
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p, ctypes.c_char_p)
        return pfun(ctypes.byref(self.hClient_), c_to_topic)
    
    def clear_stored_messages(self)->bool:
        pfun = lib_.ln_clear_stored_messages
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p,)
        return pfun(ctypes.byref(self.hClient_))
    
    def clear_addresses_of_topic(self)->bool:
        pfun = lib_.ln_clear_addresses_of_topic
        pfun.restype = ctypes.c_bool
        pfun.argtypes = (ctypes.c_void_p,)
        return pfun(ctypes.byref(self.hClient_))