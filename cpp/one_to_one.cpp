
#include "../include/liner.h"

#include <ctime>
#include <iostream>
#include <chrono>
#include <thread>

const int MESS_SEND_COUNT = 10000;
const int MESS_SIZE = 1024;
const int SEND_CYCLE_COUNT = 30;

int receive_count = 0;
clock_t send_begin = clock();
clock_t send_end = clock();

void cb1(char* to, char* from,  char* data, size_t data_size, void* udata){
 //  std::cout << "cb1" << to << from << data << data_size << std::endl;
}

void cb2(char* to, char* from,  char* data, size_t data_size, void* udata){
     
    receive_count += 1;

    if (receive_count == MESS_SEND_COUNT){
        receive_count = 0;
        std::cout << "receive_from " << 1000.0 * (clock() - send_end) / CLOCKS_PER_SEC << " ms" << std::endl;
    }
}

int main(int argc, char* argv[])
{  
    auto hclient1 = lnr_new_client("client1", "topic_client1", "localhost:2255", "redis://localhost/");
    auto hclient2 = lnr_new_client("client2", "topic_client2", "localhost:2256", "redis://localhost/");
 
    lnr_run(hclient1, cb1, nullptr);
    lnr_run(hclient2, cb2, nullptr);
 
    char data[MESS_SIZE];
    for (int i = 0; i < SEND_CYCLE_COUNT; ++i){
        send_begin = clock();
        for (int j = 0; j < MESS_SEND_COUNT; ++j){
            lnr_send_to(hclient1, "topic_client2", data, sizeof(data), TRUE);
        }
        send_end = clock();
        std::cout << "send_to " << 1000.0 * (send_end - send_begin) / CLOCKS_PER_SEC << " ms" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    lnr_delete_client(hclient1);
    lnr_delete_client(hclient2);
}