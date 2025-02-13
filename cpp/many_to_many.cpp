
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
    auto hclient1 = lnr_new_client("client1", "topic_client", "localhost:2255", "redis://localhost/");
    auto hclient2 = lnr_new_client("client2", "topic_client", "localhost:2256", "redis://localhost/");
    auto hclient3 = lnr_new_client("client3", "topic_client", "localhost:2257", "redis://localhost/");
    auto hserver1 = lnr_new_client("server1", "topic_server1", "localhost:2258", "redis://localhost/");
    auto hserver2 = lnr_new_client("server2", "topic_server2", "localhost:2259", "redis://localhost/");
    auto hserver3 = lnr_new_client("server3", "topic_server3", "localhost:2260", "redis://localhost/");
 
    lnr_run(hclient1, cb_client1, nullptr);
    lnr_run(hclient2, cb_client2, nullptr);
    lnr_run(hclient3, cb_client3, nullptr);

    lnr_run(hserver1, cb_server, nullptr);
    lnr_run(hserver2, cb_server, nullptr);
    lnr_run(hserver3, cb_server, nullptr);
 
    char data[MESS_SIZE];
    for (int i = 0; i < SEND_CYCLE_COUNT; ++i){
        send_begin = clock();
        for (int j = 0; j < MESS_SEND_COUNT; ++j){
            lnr_send_all(hserver1, "topic_client", data, sizeof(data), TRUE);
        }
        for (int j = 0; j < MESS_SEND_COUNT; ++j){
            lnr_send_all(hserver2, "topic_client", data, sizeof(data), TRUE);
        }
        for (int j = 0; j < MESS_SEND_COUNT; ++j){
            lnr_send_all(hserver3, "topic_client", data, sizeof(data), TRUE);
        }
        send_end = clock();
        std::cout << "send_to " << 1000.0 * (send_end - send_begin) / CLOCKS_PER_SEC << " ms" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    lnr_delete_client(hclient1);
    lnr_delete_client(hclient2);
    lnr_delete_client(hclient3);
    
    lnr_delete_client(hserver1);
    lnr_delete_client(hserver2);
    lnr_delete_client(hserver3);

    std::cout << "client1 receive_count_1 " << receive_count_1 << std::endl;
    std::cout << "client2 receive_count_2 " << receive_count_2 << std::endl;
    std::cout << "client3 receive_count_3 " << receive_count_3 << std::endl;
}