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

  dmc::ClientQueue<Request> cq(getClientInfo(0));

  auto f = std::function<dmc::ClientInfo(int)>(getClientInfo);
  
  dmc::PriorityQueue<int,Request> priorityQueue(f);

  priorityQueue.test();
  priorityQueue.addRequest(Request(0, 17, "foobar"), 0, dmc::getTime());

  std::cout << "done" << std::endl;
}
