#ifndef PUSHCLIENT_H
#define PUSHCLIENT_H

#include <cpp_redis/cpp_redis>
#include <string>

class PushClient {
public:
    PushClient(const std::string& host, int port);
    void push(const std::string& queueName, const std::string& data);
    void disconnect();
private:
    cpp_redis::client client;
};

#endif // PUSHCLIENT_H
