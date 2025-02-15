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
    
if __name__ == "__main__":
    
    prPath = os.path.expanduser("~") + '/projects/rust/liner/'
    binPath = prPath + 'test/client_object.py'

    c1Proc = subprocess.Popen([binPath,
                                    '--client-name=client1',
                                    '--client-topic=topic1',
                                    '--client-addr=localhost:2255'])
    
    time.sleep(3)

    c1Proc.kill()
    
    hClient2 = liner.Client('client2', 'topic2', 'localhost:2256', "redis://localhost/")
       
    def receive_cback2(to: str, from_: str, data_):
        print(f"client2 receive_from {from_}, data: {data_}")
       
    hClient2.run(receive_cback2)

    hClient2.send_to("topic1", b'hello')

    c1Proc = subprocess.Popen([binPath,
                                    '--client-name=client1',
                                    '--client-topic=topic1',
                                    '--client-addr=localhost:2255'])

    loop = asyncio.new_event_loop()
    loop.run_forever()