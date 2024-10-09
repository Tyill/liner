import os
module_path = os.path.dirname(os.path.abspath(__file__))

import liner 

liner.loadLib(module_path + "/../target/release/libliner.so")

hClient1 = liner.Client("unique1", "topic1", "redis://127.0.0.1/")
hClient2 = liner.Client("unique2", "topic2", "redis://127.0.0.1/")

def receive_cback1(to: str, from_: str, data, dlen):
    pass

def receive_cback2(to: str, from_: str, data: bytes, dlen: int):
    print(to, from_, data.decode("utf-8"), dlen)
    pass

if not hClient1.run("localhost:2255", receive_cback1):
   raise Exception('error no run, check localhost')  

if not hClient2.run("localhost:2256", receive_cback2):
   raise Exception('error no run, check localhost')  

b = bytearray(b'hello world!')

if not hClient1.send_to("topic2", b, len(b), True):
   raise Exception('error send_to')  

pass