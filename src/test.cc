#include <iostream>


#include "dm_clock_srv.h"


struct Request {
    int client;
    uint32_t op;
    char data[32];
};


int main(int argc, char* argv[]) {
    dmc::ClientDB<int> client_db;

    client_db.put(0, dmc::ClientInfo(1.0, 100.0, 250.0));
    client_db.put(1, dmc::ClientInfo(2.0, 100.0, 250.0));
    client_db.put(2, dmc::ClientInfo(2.0, 100.0, 250.0));
    client_db.put(3, dmc::ClientInfo(3.0,  50.0,   0.0));

    auto c0 = client_db.find(0);
    std::cout << "client 0: " << c0 << std::endl;

    auto c3 = client_db.find(3);
    std::cout << "client 0: " << c3 << std::endl;

    auto c6 = client_db.find(6);
    std::cout << "client 0: " << c6 << std::endl;


    dmc::ClientQueue<int,Request> client_queue;
    Request r0a = {0, 1, "foo"};
    Request r0b = {0, 2, "bar"};
    Request r0c = {0, 3, "baz"};
    Request r0d = {0, 4, "blast"};

    client_queue.append(r0a);
    client_queue.append(r0b);
    client_queue.append(r0c);
    client_queue.append(r0d);

    while (!client_queue.empty()) {
        auto e = client_queue.peek_front();
        std::cout << e->second.op << std::endl;
        client_queue.pop();
    }
}
