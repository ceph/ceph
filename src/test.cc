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

  Request(const Request& r) :
    Request(r.client, r.op, r.data)
  {
    // empty
  }
};


typedef std::unique_ptr<Request> RequestRef;


dmc::ClientInfo getClientInfo(int c) {
  static dmc::ClientInfo info[] = {
    {1.0, 100.0, 250.0},
    {2.0, 100.0, 250.0},
    {2.0, 100.0, 250.0},
    {3.0,  50.0,   0.0},
  };

  if (c < sizeof info / sizeof info[0]) {
    return info[c];
  } else {
    return info[0]; // first item is default item
  }
}


int main(int argc, char* argv[]) {

  std::cout.precision(17);
  std::cout << "now: " << dmc::getTime() << std::endl;
  std::cout << "now: " << dmc::getTime() << std::endl;

#if 0
  dmc::ClientDB<int> client_db;

  client_db.put(0, dmc::ClientInfo(1.0, 100.0, 250.0));
  client_db.put(1, dmc::ClientInfo(2.0, 100.0, 250.0));
  client_db.put(2, dmc::ClientInfo(2.0, 100.0, 250.0));
  client_db.put(3, dmc::ClientInfo(3.0,  50.0,   0.0));

  int ca[] = {0, 3, 6};
  for (int c = 0; c < sizeof ca / sizeof ca[0]; ++c) {
    auto cli = ca[c];
    auto cl = client_db.find(cli);
    std::cout << "client " << cli << ": ";
    if (cl) {
      std::cout << *cl;
    } else {
      std::cout << "undefined";
    }

    std::cout << std::endl;
  }

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
#endif


  dmc::ClientQueue<Request> cq;

  auto f = std::function<dmc::ClientInfo(int)>(getClientInfo);
  
  dmc::PriorityQueue<int,Request> priorityQueue(f);

  priorityQueue.test();

  std::cout << "done" << std::endl;
}
