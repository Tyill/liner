# liner

Redis based message serverless broker.  

**ON DEVELOPMENT STAGE**

### Example of use for CPP

```cpp


#include "../include/liner.h"

#include <ctime>
#include <iostream>
#include <chrono>
#include <thread>

void cb1(char* to, char* from,  char* data, size_t data_size){
 //  std::cout << "cb1" << to << from << data << data_size << std::endl;
}

const int MESS_SEND_COUNT = 10000;
const int MESS_SIZE = 100;
const int SEND_CYCLE_COUNT = 30;

int receive_count = 0;
clock_t send_begin = clock();
clock_t send_end = clock();

void cb2(char* to, char* from,  char* data, size_t data_size){
     
    receive_count += 1;

    if (receive_count == MESS_SEND_COUNT){
        receive_count = 0;
        std::cout << "receive_from " << 1000.0 * (clock() - send_end) / CLOCKS_PER_SEC << " ms" << std::endl;
    }
}

int main(int argc, char* argv[])
{  
    auto hclient1 = ln_new_client("unique1", "topic1", "redis://127.0.0.1/");
    auto hclient2 = ln_new_client("unique2", "topic2", "redis://127.0.0.1/");
 
    bool ok = ln_run(&hclient1, "localhost:2255", cb1);
    ln_run(&hclient2, "localhost:2256", cb2);
 
    char data[MESS_SIZE];
    for (int i = 0; i < SEND_CYCLE_COUNT; ++i){
        send_begin = clock();
        for (int j = 0; j < MESS_SEND_COUNT; ++j){
            ln_send_to(&hclient1, "topic2", data, sizeof(data), TRUE);
        }
        send_end = clock();
        std::cout << "send_to " << 1000.0 * (send_end - send_begin) / CLOCKS_PER_SEC << " ms" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    ln_delete_client(hclient1);
    ln_delete_client(hclient2);
}

```

### [Tests](https://github.com/Tyill/liner/blob/main/src/test.rs)


### License
Licensed under an [MIT-2.0]-[license](LICENSE).

