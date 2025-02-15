
#include "liner_broker.h"

#include <ctime>
#include <iostream>
#include <chrono>
#include <thread>

const int MESS_SEND_COUNT = 10000;
const int MESS_SIZE = 1024;
const int SEND_CYCLE_COUNT = 30;

int main(int argc, char* argv[])
{  
    auto client1 = LinerBroker("client1", "topic_client1", "localhost:2255", "redis://localhost/");
    auto client2 = LinerBroker("client2", "topic_client2", "localhost:2256", "redis://localhost/");
 
    int receive_count = 0;
    clock_t send_begin = clock();
    clock_t send_end = clock();

    client1.run([](const std::string& to, const std::string& from, const std::string& data){});
    client2.run([&receive_count, &send_end](const std::string& to, const std::string& from, const std::string& data){
        receive_count += 1;
        if (receive_count == MESS_SEND_COUNT){
            receive_count = 0;
            std::cout << "receive_from " << 1000.0 * (clock() - send_end) / CLOCKS_PER_SEC << " ms" << std::endl;
        }
    });
    
    char data[MESS_SIZE];
    for (int i = 0; i < SEND_CYCLE_COUNT; ++i){
        send_begin = clock();
        for (int j = 0; j < MESS_SEND_COUNT; ++j){
            client1.sendTo("topic_client2", data);
        }
        send_end = clock();
        std::cout << "send_to " << 1000.0 * (send_end - send_begin) / CLOCKS_PER_SEC << " ms" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }    
}