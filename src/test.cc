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


int main(int argc, char* argv[]) {
  using TestRequestRef = std::unique_ptr<TestRequest> ;
  using ClientMap = std::map<ClientId,TestClient*>;
  using ServerMap = std::map<ServerId,TestServer*>;

  std::cout << "simulation started" << std::endl;

  // simulation params

  const int goal_secs_to_run = 30;
  const TestClient::TimePoint early_time = TestClient::now();
  const chrono::seconds skip_amount(2); // skip first 2 secondsd of data
  const chrono::seconds measure_unit(5); // calculate in groups of 5 seconds
  const chrono::seconds report_unit(1); // unit to output reports in

  // server params

#if 0
  const int server_ops = 150;
  const int server_threads = 7;
#endif

  // name -> (server iops, server threads)
  const std::map<ServerId,std::pair<int,int>> server_info = {
    {0.0, { 150, 7 }},
    {1.0, { 150, 7 }},
  };


  // client params

  const int client_outstanding_ops = 10;

  // id -> (client_info, goal iops)
  const std::map<ClientId,std::pair<dmc::ClientInfo,int>> client_info = {
    {"alpha", {{ 1.0, 50.0, 200.0 }, 100 }},
    {"bravo", {{ 2.0, 50.0, 200.0 }, 100 }},
  };


  // construct servers

  auto client_info_f =
    [&client_info](const ClientId& c) -> dmc::ClientInfo {
    auto it = client_info.find(c);
    assert(client_info.end() != it);
    return it->second.first;
  };

  ClientMap clients;

  TestServer::ClientRespFunc client_response_f =
    [&clients](ClientId client_id,
	       const TestResponse& resp,
	       const dmc::RespParams<ServerId>& resp_params) {
    clients[client_id]->receiveResponse(resp, resp_params);
  };

  ServerMap servers;
  for (auto const &i : server_info) {
    const ServerId& id = i.first;
    const int& iops = i.second.first;
    const int& threads = i.second.second;

    servers[id] =
      new TestServer(id, iops, threads, client_info_f, client_response_f);
  }

#if 0
  TestServer server(0,
		    server_ops, server_threads,
		    client_info_f, client_response_f);
#endif

  auto a_server = servers.find(0.0);
  assert(servers.end() != a_server);

  // construct clients

  for (auto i = client_info.begin(); i != client_info.end(); ++i) {
    ClientId name = i->first;
    int goal = i->second.second;
    clients[name] =
      new TestClient(name,
		     std::bind(&TestServer::post, a_server->second, _1, _2),
		     goal * goal_secs_to_run,
		     goal,
		     client_outstanding_ops);
  }

  // clients are now running; wait for all to finish

  for (auto i = clients.begin(); i != clients.end(); ++i) {
    i->second->waitUntilDone();
  }

  // compute and display stats

  const TestClient::TimePoint late_time = TestClient::now();
  TestClient::TimePoint latest_start = early_time;
  TestClient::TimePoint earliest_finish = late_time;
  TestClient::TimePoint latest_finish = early_time;
  
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
    clients.erase(i);
  }

  std::cout << "simulation complete" << std::endl;
}
