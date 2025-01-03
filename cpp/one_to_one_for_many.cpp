
#include "../include/liner.h"

#include <ctime>
#include <iostream>
#include <chrono>
#include <thread>


const int MESS_SEND_COUNT = 10;
const int MESS_SIZE = 100;
const int SEND_CYCLE_COUNT = 10;

int receive_count_1 = 0, receive_count_2 = 0, receive_count_3 = 0;
clock_t send_begin = clock();
clock_t send_end = clock();

void cb_server(char* to, char* from,  char* data, size_t data_size, void* udata){
 //  std::cout << "cb1" << to << from << data << data_size << std::endl;
}

void cb_client1(char* to, char* from,  char* data, size_t data_size, void* udata){
     
    ++receive_count_1;
    std::cout << "client1 " << to << " receive_count_1 "  << receive_count_1 << std::endl;
}

void cb_client2(char* to, char* from,  char* data, size_t data_size, void* udata){
     
    ++receive_count_2;
    std::cout << "client2 " << to << " receive_count_2 "  << receive_count_2 << std::endl;
}

void cb_client3(char* to, char* from,  char* data, size_t data_size, void* udata){
     
    ++receive_count_3;
    std::cout << "client3 " << to << " receive_count_3 "  << receive_count_3 << std::endl;
}

int main(int argc, char* argv[])
{  
    auto hclient1 = ln_new_client("client1", "topic_client", "localhost:2255", "redis://localhost/");
    auto hclient2 = ln_new_client("client2", "topic_client", "localhost:2256", "redis://localhost/");
    auto hclient3 = ln_new_client("client3", "topic_client", "localhost:2257", "redis://localhost/");
    auto hserver1 = ln_new_client("server1", "topic_server", "localhost:2258", "redis://localhost/");
 
    ln_run(hclient1, cb_client1, nullptr);
    ln_run(hclient2, cb_client2, nullptr);
    ln_run(hclient3, cb_client3, nullptr);

    ln_run(hserver1, cb_server, nullptr);
 
    char data[MESS_SIZE];
    for (int i = 0; i < SEND_CYCLE_COUNT; ++i){
        send_begin = clock();
        for (int j = 0; j < MESS_SEND_COUNT; ++j){
            ln_send_to(hserver1, "topic_client", data, sizeof(data), TRUE);
        }
        send_end = clock();
        std::cout << "send_to " << 1000.0 * (send_end - send_begin) / CLOCKS_PER_SEC << " ms" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    ln_delete_client(hclient1);
    ln_delete_client(hclient2);
    ln_delete_client(hclient3);
    
    ln_delete_client(hserver1);

    std::cout << "client1 receive_count_1 " << receive_count_1 << std::endl;
    std::cout << "client2 receive_count_2 " << receive_count_2 << std::endl;
    std::cout << "client3 receive_count_3 " << receive_count_3 << std::endl;
}