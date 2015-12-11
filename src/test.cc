// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <memory>
#include <iostream>

#include "dm_clock_srv.h"


struct Request {
  int client;
  uint32_t op;
  std::string data;

  Request(int c, uint32_t o, const char* d) :
    Request(c, o, std::string(d))
  {
    // empty
  }

  Request(int c, uint32_t o, std::string d) :
    client(c), op(o), data(d)
  {
    // empty
  }
};


typedef std::unique_ptr<Request> RequestRef;


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


  dmc::ClientQueue<Request> client_queue;

  RequestRef r0a(new Request(0, 1, "foo"));
  client_queue.push(std::move(r0a));

  client_queue.push(RequestRef( new Request(0, 2, "bar")));
  client_queue.push(RequestRef( new Request(0, 3, "baz")));
  client_queue.push(RequestRef( new Request(0, 4, "bazzzzz")));

  while (!client_queue.empty()) {
    auto e = client_queue.peek();
    std::cout << e->request.get()->op << std::endl;
    client_queue.pop();
  }
}
