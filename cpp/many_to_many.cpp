
#include "liner_broker.h"

#include <ctime>
#include <iostream>
#include <chrono>
#include <thread>


const int MESS_SEND_COUNT = 10;
const int MESS_SIZE = 100;
const int SEND_CYCLE_COUNT = 10;

int main(int argc, char* argv[])
{      
    auto client1 = LinerBroker("client1", "topic_client", "localhost:2255", "redis://localhost/");
    auto client2 = LinerBroker("client2", "topic_client", "localhost:2256", "redis://localhost/");
    auto client3 = LinerBroker("client3", "topic_client", "localhost:2257", "redis://localhost/");
    auto server1 = LinerBroker("server1", "topic_server1", "localhost:2258", "redis://localhost/");
    auto server2 = LinerBroker("server2", "topic_server2", "localhost:2259", "redis://localhost/");
    auto server3 = LinerBroker("server3", "topic_server3", "localhost:2260", "redis://localhost/");
 
    int receive_count_1 = 0, receive_count_2 = 0, receive_count_3 = 0;
    clock_t send_begin = clock();
    clock_t send_end = clock();

    client1.run([&receive_count_1](const std::string& to, const std::string& from, const std::string& data){
        ++receive_count_1;
        std::cout << "client1 " << to << " receive_count_1 "  << receive_count_1 << std::endl;
    });
    client2.run([&receive_count_2](const std::string& to, const std::string& from, const std::string& data){
        ++receive_count_2;
        std::cout << "client2 " << to << " receive_count_2 "  << receive_count_2 << std::endl;
    });
    client3.run([&receive_count_3](const std::string& to, const std::string& from, const std::string& data){
        ++receive_count_3;
        std::cout << "client3 " << to << " receive_count_3 "  << receive_count_3 << std::endl;
    });

    server1.run([](const std::string& to, const std::string& from, const std::string& data){});
    server2.run([](const std::string& to, const std::string& from, const std::string& data){});
    server3.run([](const std::string& to, const std::string& from, const std::string& data){});
 
    char data[MESS_SIZE];
    for (int i = 0; i < SEND_CYCLE_COUNT; ++i){
        send_begin = clock();
        for (int j = 0; j < MESS_SEND_COUNT; ++j){
            server1.sendAll("topic_client", data);
        }
        for (int j = 0; j < MESS_SEND_COUNT; ++j){
            server2.sendAll("topic_client", data);
        }
        for (int j = 0; j < MESS_SEND_COUNT; ++j){
            server2.sendAll("topic_client", data);
        }
        send_end = clock();
        std::cout << "send_to " << 1000.0 * (send_end - send_begin) / CLOCKS_PER_SEC << " ms" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
   
    std::cout << "client1 receive_count_1 " << receive_count_1 << std::endl;
    std::cout << "client2 receive_count_2 " << receive_count_2 << std::endl;
    std::cout << "client3 receive_count_3 " << receive_count_3 << std::endl;
}