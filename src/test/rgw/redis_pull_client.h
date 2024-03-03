#ifndef PULLCLIENT_H
#define PULLCLIENT_H

#include <cpp_redis/cpp_redis>
#include <string>

class PullClient {
public:
    PullClient(const std::string& host, int port);
    std::string pull(const std::string& queueName);
    void disconnect();
private:
    cpp_redis::client client;
};

#endif // PULLCLIENT_H
