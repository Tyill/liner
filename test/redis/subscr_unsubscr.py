import os
from pathlib import Path
import sys
import subprocess
import asyncio
import time

module_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, str(Path(module_path).resolve().parent.parent.parent))

from python import liner 

module_path = os.path.dirname(os.path.abspath(__file__))
liner.loadLib(str(Path(module_path).resolve().parent.parent.parent / "target/release/libliner_broker.so"))
    
if __name__ == "__main__":
    
    binPath = str(Path(__file__).resolve().parent / "client_process.py")

    c1Proc = subprocess.Popen([binPath,
                                    '--client-name=client1',
                                    '--client-topic=topic1',
                                    '--client-addr=localhost:2255',
                                    '--subscr-topic=topic_subscr'])
    
    time.sleep(3)
    
    hClient2 = liner.Client('client2', 'topic2', 'localhost:2256', "redis://localhost/")
       
    def receive_cback2(to: str, from_: str, data_):
        print(f"client2 receive_from {from_}, data: {data_}")
       
    hClient2.run(receive_cback2)

    hClient2.send_to("topic_subscr", b'hello')

    time.sleep(3)

    c1Proc.kill()

    time.sleep(3)

    c1Proc = subprocess.Popen([binPath,
                                    '--client-name=client1',
                                    '--client-topic=topic1',
                                    '--client-addr=localhost:2255',
                                    '--unsubscr-topic=topic_subscr'])
    time.sleep(3)
    hClient2.send_to("topic_subscr", b'hello')

    loop = asyncio.new_event_loop()
    loop.run_forever()