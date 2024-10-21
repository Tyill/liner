import os
import time

import liner 

module_path = os.path.dirname(os.path.abspath(__file__))
liner.loadLib(module_path + "/../target/release/libliner.so")

MESS_SEND_COUNT = 1
MESS_SIZE = 100
SEND_CYCLE_COUNT = 10

receive_count = 0
send_begin = time.time()
send_end = time.time()


def receive_cback1(to: str, from_: str, data):
    pass

def receive_cback2(to: str, from_: str, data: bytes):

    global receive_count     
    global send_end     
    receive_count += 1
    if receive_count == MESS_SEND_COUNT:
        receive_count = 0
        print(f"receive_from {round((time.time() - send_end)* 1000, 3)} ms data: {data}")
    
def func(): 
    hClient1 = liner.Client("client1", "topic_client1", "localhost:2255", "redis://localhost/")
    hClient2 = liner.Client("client2", "topic_client2", "localhost:2256", "redis://localhost/")

    hClient1.clear_stored_messages()
    hClient2.clear_stored_messages()

    hClient1.clear_addresses_of_topic()
    hClient2.clear_addresses_of_topic()

    if not hClient1.run(receive_cback1):
        raise Exception('hClient1 error no run')  

    if not hClient2.run(receive_cback2):
        raise Exception('hClient2 error no run')  

    #b = bytearray(MESS_SIZE)

    b = b'hello world'

    global send_end     
    for i in range(SEND_CYCLE_COUNT):
        send_begin = time.time()
        for j in range(MESS_SEND_COUNT):
            if not hClient1.send_to("topic_client2", b, len(b), True):
                raise Exception('error send_to')  
        send_end = time.time()
        print("send_to", round((send_end - send_begin) * 1000, 3), "ms")
    
        time.sleep(1)
    print("end")

func()