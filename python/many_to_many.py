import os
import time

import liner 

module_path = os.path.dirname(os.path.abspath(__file__))
liner.loadLib(module_path + "/../target/release/libliner_broker.so")

MESS_SEND_COUNT = 10
MESS_SIZE = 100
SEND_CYCLE_COUNT = 10

receive_count_client1 = 0
receive_count_client2 = 0
send_begin = time.time()
send_end = time.time()

def receive_cback1(to: str, from_: str, data):
    global receive_count_client2     
    receive_count_client2 += 1

def receive_cback2(to: str, from_: str, data: bytes):
    global receive_count_client1     
    receive_count_client1 += 1

def receive_cback_server(to: str, from_: str, data: bytes):
    pass  
    
def func(): 
    hClient1 = liner.Client("client1", "topic_client", "localhost:2255", "redis://localhost/")
    hClient2 = liner.Client("client2", "topic_client", "localhost:2256", "redis://localhost/")
    hServer1 = liner.Client("server1", "topic_server1", "localhost:2257", "redis://localhost/")
    hServer2 = liner.Client("server2", "topic_server2", "localhost:2258", "redis://localhost/")

    hServer1.clear_stored_messages()
    hServer2.clear_stored_messages()
    hClient1.clear_stored_messages()
    hClient2.clear_stored_messages()

    hServer1.clear_addresses_of_topic()
    hServer2.clear_addresses_of_topic()
    hClient1.clear_addresses_of_topic()
    hClient2.clear_addresses_of_topic()

    if not hClient1.run(receive_cback1):
        raise Exception('hClient1 error no run')  

    if not hClient2.run(receive_cback2):
        raise Exception('hClient2 error no run')  
    
    if not hServer1.run(receive_cback_server):
        raise Exception('hServer1 error no run') 

    if not hServer2.run(receive_cback_server):
        raise Exception('hServer2 error no run')  

    b = bytearray(MESS_SIZE)

    global send_end     
    for i in range(SEND_CYCLE_COUNT):
        send_begin = time.time()
        for j in range(MESS_SEND_COUNT):
            if not hServer1.send_all("topic_client", b):
                raise Exception('error send_all')
        for j in range(MESS_SEND_COUNT):
            if not hServer2.send_all("topic_client", b):
                raise Exception('error send_all')  
        send_end = time.time()
        print("send_to", round((send_end - send_begin) * 1000, 3), "ms")
    
        time.sleep(1)
    global receive_count_client1, receive_count_client2
    print("receive_count_client1", receive_count_client1)
    print("receive_count_client2", receive_count_client2)
    print("end")

func()