#include "redis_push_client.h"
#include <stdexcept>

PushClient::PushClient(const std::string& host, int port) {
    client.connect(host, port, [](const std::string& host, std::size_t port, cpp_redis::client::connect_state status) {
        if (status == cpp_redis::client::connect_state::dropped) {
            std::cerr << "client disconnected from " << host << ":" << port << std::endl;
        }
    });
}

void PushClient::push(const std::string& queueName, const std::string& data) {
    if (queueName.empty()) {
        throw std::invalid_argument("Invalid queue name provided");
    }

    client.rpush(queueName, {data}, [](const cpp_redis::reply& reply) {
        if (reply.is_error()) { 
            std::cerr << "Error pushing data" << std::endl; 
        }
    });
    client.sync_commit();
}

void PushClient::disconnect() {
    client.disconnect();
}
