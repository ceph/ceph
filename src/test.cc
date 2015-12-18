// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <unistd.h>

#include <memory>
#include <iostream>

#include "dm_clock_srv.h"
#include "test_srv.h"


namespace dmc = crimson::dmclock;


TestServer* testServer;



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
  std::cout << "getClientInfo called" << std::endl;

  static dmc::ClientInfo info[] = {
    {1.0, 100.0, 250.0},
    {2.0, 100.0, 250.0},
    {2.0,  50.0, 250.0},
    {3.0,  50.0,   0.0},
  };

  if (c < sizeof info / sizeof info[0]) {
    return info[c];
  } else {
    return info[0]; // first item is default item
  }
}


bool canHandleReq() {
  return testServer->hasAvailThread();
}


void handleReq(std::unique_ptr<Request>&& request_ref,
	       std::function<void()> callback) {
  std::unique_ptr<Request> req(std::move(request_ref));
  int client = req->client;
  uint32_t op = req->op;
  // std:: cout << "scheduling " << client << std::endl;
  
  testServer->post(0.1,
		   [=] {
		     callback();
		     std:: cout << "finished " << client << " / " << op <<
		       std::endl;
		   });
}


int main(int argc, char* argv[]) {
  std::cout.precision(17);
  std::cout << "now: " << dmc::getTime() << std::endl;
  std::cout << "now: " << dmc::getTime() << std::endl;

  auto f1 = std::function<dmc::ClientInfo(int)>(getClientInfo);
  auto f2 = std::function<bool()>(canHandleReq);
  auto f3 = std::function<void(std::unique_ptr<Request>&&,
			       std::function<void()>)>(handleReq);

  testServer = new TestServer(5);

  dmc::PriorityQueue<int,Request> priorityQueue(f1, f2, f3);

  std::cout << "queue created" << std::endl;

  for (int i = 0; i < 1000; ++i) {
    int client = i % 4;
    priorityQueue.addRequest(Request(client, i, "foobar"),
			     client,
			     dmc::getTime());
  }

  std::cout << "request added" << std::endl;

  priorityQueue.markAsIdle(1);

  sleep(60);
  delete testServer;

  std::cout << "done" << std::endl;
}
