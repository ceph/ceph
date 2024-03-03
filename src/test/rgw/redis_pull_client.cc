#include "redis_pull_client.h"

PullClient::PullClient(const std::string& host, int port) {
    client.connect(host, port, [](const std::string& host, std::size_t port, cpp_redis::client::connect_state status) {
        if (status == cpp_redis::client::connect_state::dropped) {
            std::cerr << "client disconnected from " << host << ":" << port << std::endl;
        }
    });
}

std::string PullClient::pull(const std::string& queueName) {
    std::string data;
    client.blpop({queueName}, 0, [&data](const cpp_redis::reply& reply) {
        if (!reply.is_error() && reply.is_array()) {
            const auto& fetchedData = reply.as_array();
            data = fetchedData[1].as_string(); // Assuming data is at index 1
        }
    });
    client.sync_commit();
    return data;
}

void PullClient::disconnect() {
    client.disconnect();
}
