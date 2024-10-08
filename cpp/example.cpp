
#include "../include/liner.h"

#include <ctime>
#include <iostream>
#include <chrono>
#include <thread>

void cb1(char* to, char* from,  char* data, size_t data_size){
 //  std::cout << "cb1" << to << from << uuid << timestamp << data << data_size << std::endl;
}

void cb2(char* to, char* from,  char* data, size_t data_size){
 //  std::cout << "cb2" << to << from << data << data_size << std::endl;
}

int main(int argc, char* argv[])
{  
    auto hclient1 = ln_new_client("unique1", "redis://127.0.0.1/");
    auto hclient2 = ln_new_client("unique2", "redis://127.0.0.1/");
 
    bool ok = ln_run(&hclient1, "topic1", "localhost:2255", cb1);
    ln_run(&hclient2, "topic2", "localhost:2256", cb2);
 
    char data[100];
    for (int i = 0; i < 30; ++i){
        auto bt = clock();
        for (int j = 0; j < 10000; ++j){
            ln_send_to(&hclient1, "topic2", data, 100, TRUE);
        }
        std::cout << "send_to " << 1000.0 * (clock() - bt) / CLOCKS_PER_SEC << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    ln_delete_client(hclient1);
    ln_delete_client(hclient2);
}