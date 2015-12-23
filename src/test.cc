// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <unistd.h>

#include <memory>
#include <iostream>

#include "test_server.h"
#include "test_client.h"


using namespace std::placeholders;


TestServer* testServer;


typedef std::unique_ptr<TestRequest> TestRequestRef;

std::mutex cout_mtx;
typedef typename std::lock_guard<std::mutex> Guard;


#define COUNT(array) (sizeof(array) / sizeof(array[0]))


static dmc::ClientInfo client_info[] = {
  {1.0, 100.0, 250.0},
  {2.0, 100.0, 250.0},
  {2.0,  50.0, 100.0},
  {3.0,  50.0,   0.0},
};


static int client_goals[] = {150, 150, 150, 150}; // in IOPS


dmc::ClientInfo getClientInfo(int c) {
  {
    Guard g(cout_mtx);
    std::cout << "getClientInfo called" << std::endl;
  }

  if (c < COUNT(client_info)) {
    return info[c];
  } else {
    return info[0]; // first item is default item
  }
}


#if 0
bool canHandleReq() {
  return testServer->hasAvailThread();
}


void handleReq(std::unique_ptr<TestRequest>&& request_ref,
	       std::function<void()> callback) {
  std::unique_ptr<TestRequest> req(std::move(request_ref));
  int client = req->client;
  uint32_t op = req->op;

  testServer->post(0.1,
		   [=] {
		     callback();
		     Guard g(cout_mtx);
		     std::cout << "finished " << client << " / " <<
		       op << std::endl;
		   });
}

#endif


int main(int argc, char* argv[]) {
  auto client_info_f = std::function<dmc::ClientInfo(int)>(getClientInfo);
#if 0
  auto f2 = std::function<bool()>(canHandleReq);
  auto f3 = std::function<void(std::unique_ptr<TestRequest>&&,
			       std::function<void()>)>(handleReq);
#endif

  assert(COUNT(client_info) == COUNT(client_goals));

  TestServer server(300, 7);

  TestClient** clients = new TestClient*[clientCount()];
  for (int i = 0; i < COUNT(client_info); ++i) {
    clients[i] = new TestClient(i,
				std::bind(&TestServer::post, &testServer, _1, _2),
				client_goals[i] * 60,
				4);
				
  }

  

  for (int i = 0; i < COUNT(client_info); ++i) {
    clients[i]->waitForDone();
    delete clients[i];
  }
  delete[] clients;

  {
    Guard g(cout_mtx);
    std::cout << "done" << std::endl;
  }
}
