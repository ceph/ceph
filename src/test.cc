// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <unistd.h>

#include <memory>
#include <chrono>
#include <iostream>
#include <map>

#include "test_recs.h"
#include "test_server.h"
#include "test_client.h"


using namespace std::placeholders;

namespace dmc = crimson::dmclock;
namespace chrono = std::chrono;


TestServer* testServer;


typedef std::unique_ptr<TestRequest> TestRequestRef;

std::mutex cout_mtx;
typedef typename std::lock_guard<std::mutex> Guard;

using ClientMap = std::map<ClientId,TestClient*>;

#define COUNT(array) (sizeof(array) / sizeof(array[0]))


static const int goal_secs_to_run = 30;

static const int server_ops = 150;
static const int server_threads = 7;

static const int client_outstanding_ops = 10;

static std::map<std::string,std::pair<dmc::ClientInfo,int>> client_info = {
  {"alpha", {{ 1.0, 50.0, 200.0 }, 100 }},
  {"bravo", {{ 2.0, 50.0, 200.0 }, 100 }},
};


#if 0
static dmc::ClientInfo client_info_array[] = {
  // as of C++ 11 this will invoke the constructor with three doubles
  // as parameters
  {1.0, 50.0, 200.0},
  {2.0, 50.0, 200.0},
  // {1.0, 50.0, 0.0},
  // {2.0, 50.0, 0.0},
  // {2.0, 50.0, 0.0},
};

static std::map<ClientId,dmc::ClientInfo> client_info_map;


static int client_goals[] = {
  100,
  100,
  // 40,
  // 80,
  // 80,
}; // in IOPS
#endif


dmc::ClientInfo getClientInfo(const ClientId& c) {
  auto it = client_info.find(c);
  assert(client_info.end() != it);
  return it->second.first;
}


void send_response(ClientMap& clients,
		   ClientId client_id,
		   const TestResponse& resp,
		   const dmc::RespParams<ServerId>& resp_params) {
  clients[client_id]->receiveResponse(resp, resp_params);
}


int main(int argc, char* argv[]) {
  std::cout << "simulation started" << std::endl;

  const TestClient::TimePoint early_time = TestClient::now();
  const chrono::seconds skip_amount(2); // skip first 2 secondsd of data
  const chrono::seconds measure_unit(5); // calculate in groups of 5 seconds
  const chrono::seconds report_unit(1); // unit to output reports in

  ClientMap clients;

  auto client_info_f = std::function<dmc::ClientInfo(ClientId)>(getClientInfo);
  TestServer::ClientRespFunc client_response_f =
    std::bind(&send_response, std::ref(clients), _1, _2, _3);

  TestServer server(0,
		    server_ops, server_threads,
		    client_info_f, client_response_f);

  for (auto i = client_info.begin(); i != client_info.end(); ++i) {
    std::string name = i->first;
    int goal = i->second.second;
    clients[name] =
      new TestClient(name,
		     std::bind(&TestServer::post, &server, _1, _2),
		     goal * goal_secs_to_run,
		     goal,
		     client_outstanding_ops);
  }

  // clients are now running

  for (auto i = clients.begin(); i != clients.end(); ++i) {
    i->second->waitUntilDone();
  }

  const TestClient::TimePoint late_time = TestClient::now();
  TestClient::TimePoint latest_start = early_time;
  TestClient::TimePoint earliest_finish = late_time;
  TestClient::TimePoint latest_finish = early_time;
  
  // all clients are done
  for (auto i = clients.begin(); i != clients.end(); ++i) {
    auto start = i->second->getOpTimes().front();
    auto end = i->second->getOpTimes().back();

    if (start > latest_start) { latest_start = start; }
    if (end < earliest_finish) { earliest_finish = end; }
    if (end > latest_finish) { latest_finish = end; }
  }

  const auto start_edge = latest_start + skip_amount;

  for (auto i = clients.begin(); i != clients.end(); ++i) {
    auto it = i->second->getOpTimes().begin();
    const auto end = i->second->getOpTimes().end();
    while (it != end && *it < start_edge) { ++it; }

    for (auto time_edge = start_edge + measure_unit;
	 time_edge < latest_finish;
	 time_edge += measure_unit) {
      int count = 0;
      for (; it != end && *it < time_edge; ++count, ++it) { /* empty */ }
      double ops_per_second = double(count) / (measure_unit / report_unit);
      std::cout << "client " << i->first << ": " << ops_per_second << 
	" ops per second." << std::endl;
    }
  }

  // clean up

  for (auto i = clients.begin(); i != clients.end(); ++i) {
    delete i->second;
    i->second = nullptr;
  }

  std::cout << "simulation complete" << std::endl;
}
