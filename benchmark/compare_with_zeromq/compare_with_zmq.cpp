#include <iostream>
#include <zmq_addon.hpp>
#include <ctime>
#include <chrono>
#include <thread>

int main()
{
    zmq::context_t ctx;
    zmq::socket_t sock1(ctx, zmq::socket_type::push);
    zmq::socket_t sock2(ctx, zmq::socket_type::pull);
    sock1.bind("tcp://127.0.0.1:*");
    const std::string last_endpoint =
        sock1.get(zmq::sockopt::last_endpoint);
    std::cout << "Connecting to "
              << last_endpoint << std::endl;
    sock2.connect(last_endpoint);

    std::vector<zmq::const_buffer> send_msgs;
    char mess[1024];
    for (int i = 0; i < 10000; ++i){
        send_msgs.push_back(zmq::str_buffer(mess));
    }
    for (int i = 0; i < 10; ++i){
        auto send_begin = clock();
    
        if (!zmq::send_multipart(sock1, send_msgs))
            return 1;

        std::vector<zmq::message_t> recv_msgs;
        const auto ret = zmq::recv_multipart(
            sock2, std::back_inserter(recv_msgs));
        if (!ret)
            return 1;
        
        auto receive_end = clock();
        std::cout << "send_to " << 1000.0 * (receive_end - send_begin) / CLOCKS_PER_SEC << " ms" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    return 0;
}