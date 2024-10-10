import os
import time

import liner 

module_path = os.path.dirname(os.path.abspath(__file__))
liner.loadLib(module_path + "/../target/release/libliner.so")


def receive_cback1(to: str, from_: str, data):
    pass

MESS_SEND_COUNT = 10000
MESS_SIZE = 100
SEND_CYCLE_COUNT = 30

receive_count = 0
send_begin = time.time()
send_end = time.time()

def receive_cback2(to: str, from_: str, data: bytes):

    global receive_count     
    global send_end     
    receive_count += 1

    if receive_count == MESS_SEND_COUNT:
        receive_count = 0
        print(f"receive_from {round((time.time() - send_end)* 1000, 3)} ms")
    
def func(): 
    hClient1 = liner.Client("unique1", "topic1", "redis://127.0.0.1/")
    hClient2 = liner.Client("unique2", "topic2", "redis://127.0.0.1/")

    if not hClient1.run("localhost:2255", receive_cback1):
        raise Exception('error no run, check localhost')  

    if not hClient2.run("localhost:2256", receive_cback2):
        raise Exception('error no run, check localhost')  

    b = bytearray(MESS_SIZE)

    global send_end     
    for i in range(SEND_CYCLE_COUNT):
        send_begin = time.time()
        for j in range(MESS_SEND_COUNT):
            if not hClient1.send_to("topic2", b, len(b), True):
                raise Exception('error send_to')  
        send_end = time.time()
        print("send_to", round((send_end - send_begin) * 1000, 3), "ms")
    
        time.sleep(1)
    print("end")

func()