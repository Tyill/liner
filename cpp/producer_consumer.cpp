
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

    int receive_count_1 = 0, receive_count_2 = 0, receive_count_3 = 0;
    int receive_count_subscr_1 = 0, receive_count_subscr_2 = 0, receive_count_subscr_3 = 0;
    clock_t send_begin = clock();
    clock_t send_end = clock();

    server1.clear_stored_messages();
    client1.clear_stored_messages();
    client2.clear_stored_messages();
    client3.clear_stored_messages();

    server1.clear_addresses_of_topic();
    client1.clear_addresses_of_topic();
    client2.clear_addresses_of_topic();
    client3.clear_addresses_of_topic();
   
    client1.subscribe("topic_for_subscr");
    client2.subscribe("topic_for_subscr");
    client3.subscribe("topic_for_subscr");

    client1.run([&receive_count_1, &receive_count_subscr_1](const std::string& to, const std::string& from, const std::string& data){
        if (to == "topic_client1"){
            ++receive_count_1;
            std::cout << "client1 " << to << " receive_count_1 "  << receive_count_1 << std::endl;
        }
        else if (to == "topic_for_subscr"){
            ++receive_count_subscr_1;
            std::cout << "client1 " << to << " receive_count_subscr_1 "  << receive_count_subscr_1 << std::endl;
        }
    });
    client2.run([&receive_count_2, &receive_count_subscr_2](const std::string& to, const std::string& from, const std::string& data){
        if (to == "topic_client2"){
            ++receive_count_2;
            std::cout << "client2 " << to << " receive_count_2 "  << receive_count_2 << std::endl;
        }
        else if (to == "topic_for_subscr"){
            ++receive_count_subscr_2;
            std::cout << "client2 " << to << " receive_count_subscr_2 "  << receive_count_subscr_2 << std::endl;
        }
    });
    client3.run([&receive_count_3, &receive_count_subscr_3](const std::string& to, const std::string& from, const std::string& data){
        if (to == "topic_client3"){
            ++receive_count_3;
            std::cout << "client3 " << to << " receive_count_3 "  << receive_count_3 << std::endl;
        }
        else if (to == "topic_for_subscr"){
            ++receive_count_subscr_3;
            std::cout << "client3 " << to << " receive_count_subscr_3 "  << receive_count_subscr_3 << std::endl;
        }
    });
    server1.run([](const std::string& to, const std::string& from, const std::string& data){});
  
    char data[MESS_SIZE];
    for (int i = 0; i < SEND_CYCLE_COUNT; ++i){
        send_begin = clock();
        for (int j = 0; j < MESS_SEND_COUNT; ++j){
            server1.sendTo("topic_client1", data);
            server1.sendTo("topic_client2", data);
            server1.sendTo("topic_client3", data);
           // server1.sendTo("topic_for_subscr", data);
        }
        send_end = clock();
        std::cout << "send_to " << 1000.0 * (send_end - send_begin) / CLOCKS_PER_SEC << " ms" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    std::cout << "client1 receive_count_1 " << receive_count_1 << " receive_count_subscr_1 " << receive_count_subscr_1 << std::endl;
    std::cout << "client2 receive_count_2 " << receive_count_2 << " receive_count_subscr_2 " << receive_count_subscr_2 << std::endl;
    std::cout << "client3 receive_count_3 " << receive_count_3 << " receive_count_subscr_3 " << receive_count_subscr_3 << std::endl;
}