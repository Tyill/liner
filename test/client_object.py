#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import argparse
import sys
import asyncio

module_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, module_path + "/..")

from python import liner 

module_path = os.path.dirname(os.path.abspath(__file__))
liner.loadLib(module_path + "/../target/release/libliner_broker.so")
    
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--client-name', help='name of client')
    parser.add_argument('--client-topic', help='topic of client')
    parser.add_argument('--client-addr', help='addr of client')
    parser.add_argument('--subscr-topic', help='topic for subscr', default='')
    parser.add_argument('--unsubscr-topic', help='topic for unsubscr', default='')

    args = parser.parse_args()

    hClient1 = liner.Client(args.client_name, args.client_topic, args.client_addr, "redis://localhost/")
   
    def receive_cback1(to: str, from_: str, data_):
        print(f"{args.client_name} receive_from {from_}, data: {data_}")
        hClient1.send_to(from_, data_)

    if len(args.subscr_topic):
        hClient1.subscribe(args.subscr_topic)
    
    if len(args.unsubscr_topic):
        hClient1.unsubscribe(args.unsubscr_topic)

    hClient1.run(receive_cback1)
    
    loop = asyncio.new_event_loop()
    loop.run_forever()