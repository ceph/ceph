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

namespace dmc = crimson::dmclock;


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

  assert(c < COUNT(client_info));
  return client_info[c];
}


int main(int argc, char* argv[]) {
  assert(COUNT(client_info) == COUNT(client_goals));
  const int client_count = COUNT(client_info);

  auto client_info_f = std::function<dmc::ClientInfo(int)>(getClientInfo);

  TestServer server(300, 7, client_info_f);

  TestClient** clients = new TestClient*[client_count];
  for (int i = 0; i < client_count; ++i) {
    clients[i] = new TestClient(i,
				std::bind(&TestServer::post, &server, _1),
				client_goals[i] * 60,
				client_goals[i],
				4);
  }

  // clients are now running

  // wait for all clients to finish
  for (int i = 0; i < client_count; ++i) {
    clients[i]->waitForDone();
    delete clients[i];
  }
  delete[] clients;

  {
    Guard g(cout_mtx);
    std::cout << "done" << std::endl;
  }
}
