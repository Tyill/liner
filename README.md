# liner

Redis based message serverless broker.  
Data transfer via TCP.

Python example:
``` Python

def foo():
    client1 = liner.Client("client1", "topic_client", "localhost:2255", "redis://localhost/")
    client2 = liner.Client("client2", "topic_client", "localhost:2256", "redis://localhost/")
    server = liner.Client("server", "topic_server", "localhost:2257", "redis://localhost/")
    
    client1.run(receive_cback1)
    client2.run(receive_cback2)
    server.run(receive_server)
    
    b = b'hello world'
    server.send_all("topic_client", b, len(b), True)
    

def receive_cback1(to: str, from_: str, data: bytes):
    print(f"receive_from {from_}, data: {data}")

def receive_cback2(to: str, from_: str, data: bytes):
    print(f"receive_from {from_}, data: {data}")

def receive_server(to: str, from_: str, data: bytes):
    print(f"receive_from {from_}, data: {data}")
    
```

### Features

 - high speed transmission of multiple messages ([benchmark](https://github.com/Tyill/liner/blob/main/benchmark))

 - delivery guarantee: at least once delivery (using redis db)

 - message size is not predetermined and is not limited

 - easy api: create client, run client and send data to 

 - interface for Python and CPP
 
### Build (only linux)
 - install [Rust and Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html)
 - install [Redis](https://redis.io/docs/latest/operate/oss_and_stack/install/install-redis/)
 - download this repo
 - while in the folder repo, execute in console: `cargo build`
 
### Examples of use

One to one: [Python](https://github.com/Tyill/liner/blob/main/python/one_to_one.py) / [CPP](https://github.com/Tyill/liner/blob/main/cpp/one_to_one.cpp) / [Rust](https://github.com/Tyill/liner/blob/main/rust/one_to_one.rs)

<p float="left">
 <img src="docs/one_to_one.gif" 
  width="500" height="150" alt="lorem">
</p>

One to one for many: [Python](https://github.com/Tyill/liner/blob/main/python/one_to_one_for_many.py) / [CPP](https://github.com/Tyill/liner/blob/main/cpp/one_to_one_for_many.cpp) / [Rust](https://github.com/Tyill/liner/blob/main/rust/one_to_one_for_many.rs)
<p float="left">
 <img src="docs/one_to_one_for_many.gif" 
  width="500" height="200" alt="lorem">
</p>

One to many: [Python](https://github.com/Tyill/liner/blob/main/python/one_to_many.py) / [CPP](https://github.com/Tyill/liner/blob/main/cpp/one_to_many.cpp) / [Rust](https://github.com/Tyill/liner/blob/main/rust/one_to_many.rs)
<p float="left">
 <img src="docs/one_to_many.gif" 
  width="500" height="200" alt="lorem">
</p>

Many to many: [Python](https://github.com/Tyill/liner/blob/main/python/many_to_many.py) / [CPP](https://github.com/Tyill/liner/blob/main/cpp/many_to_many.cpp) / [Rust](https://github.com/Tyill/liner/blob/main/rust/many_to_many.rs)
<p float="left">
 <img src="docs/many_to_many.gif" 
  width="500" height="200" alt="lorem">
</p>

Producer-consumer: [Python](https://github.com/Tyill/liner/blob/main/python/producer_consumer.py) / [CPP](https://github.com/Tyill/liner/blob/main/cpp/producer_consumer.cpp) / [Rust](https://github.com/Tyill/liner/blob/main/rust/producer_consumer.rs)
<p float="left">
 <img src="docs/producer_consumer.gif" 
  width="500" height="200" alt="lorem">
</p>

### [Benchmark](https://github.com/Tyill/liner/blob/main/benchmark)

### [Tests](https://github.com/Tyill/liner/blob/main/test)

### [Docs](https://docs.rs/liner_broker/1.0.5/liner_broker/)

### License
Licensed under an [MIT-2.0]-[license](LICENSE).

