import os
import sys
import subprocess
import asyncio
import time

module_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, module_path + "/..")

from python import liner 

module_path = os.path.dirname(os.path.abspath(__file__))
liner.loadLib(module_path + "/../target/release/libliner_broker.so")

loop = asyncio.new_event_loop()
hClient2 = None  

async def send_to_topic1():
    global hClient2
    hClient2.send_to("topic1", b'hello')

    await asyncio.sleep(1)
    asyncio.run_coroutine_threadsafe(send_to_topic1(), loop)

async def run_topic1():
    await asyncio.sleep(5)
    subprocess.Popen([binPath,
                      '--client-name=client1',
                      '--client-topic=topic1',
                      '--client-addr=localhost:2255'])
        

if __name__ == "__main__":
    
    prPath = os.path.expanduser("~") + '/projects/rust/liner/'
    binPath = prPath + 'test/client_process.py'

    hClient2 = liner.Client('client2', 'topic2', 'localhost:2256', "redis://localhost/")
    hClient2.clear_addresses_of_topic()
    hClient2.clear_stored_messages()
       
    def receive_cback2(to: str, from_: str, data_):
        print(f"client2 receive_from {from_}, data: {data_}")

    hClient2.run(receive_cback2)
        
    asyncio.run_coroutine_threadsafe(send_to_topic1(), loop)

    asyncio.run_coroutine_threadsafe(run_topic1(), loop)
    
    loop.run_forever()

       

    
