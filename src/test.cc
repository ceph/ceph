// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <unistd.h>

#include <memory>
#include <chrono>
#include <map>
#include <random>
#include <iostream>
#include <iomanip>

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

  // name -> (server iops, server threads)
  const std::map<ServerId,std::pair<int,int>> server_info = {
    {0, { 75, 7 }},
    {1, { 75, 7 }},
#if 0
    {2, { 75, 7 }},
    {3, { 75, 7 }},
    {4, { 75, 7 }},
    {5, { 75, 7 }},
    {6, { 75, 7 }},
    {7, { 75, 7 }},
#endif
  };

  // client params

  const int client_outstanding_ops = 10;

  // id -> (client_info, goal iops)
  const std::map<ClientId,std::pair<dmc::ClientInfo,int>> client_info = {
    {"c1", {{ 1.0, 50.0, 200.0 }, 100 }},
    {"c2", {{ 2.0, 50.0, 200.0 }, 100 }},
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
    clients[client_id]->receive_response(resp, resp_params);
  };

  std::vector<ServerId> server_ids;

  ServerMap servers;
  for (auto const &i : server_info) {
    const ServerId& id = i.first;
    const int& iops = i.second.first;
    const int& threads = i.second.second;

    server_ids.push_back(id);
    servers[id] =
      new TestServer(id, iops, threads, client_info_f, client_response_f);
  }

  // construct clients

  // lambda to choose a server based on a seed; called by client
  auto server_rotate_f = [&server_ids](uint64_t seed) -> const ServerId& {
    int index = seed % server_ids.size();
    return server_ids[index];
  };

  std::default_random_engine
    srv_rand(std::chrono::system_clock::now().time_since_epoch().count());

  // lambda to choose a server randomly
  auto server_random_f =
    [&server_ids, &srv_rand] (uint64_t seed) -> const ServerId& {
    int index = srv_rand() % server_ids.size();
    return server_ids[index];
  };

  // lambda to always choose the first server
  auto server_0_f = [server_ids] (uint64_t seed) -> const ServerId& {
    return server_ids[0];
  };

  // lambda to post a request to the identified server; called by client
  auto server_post_f = [&servers](const ServerId& server,
				  const TestRequest& request,
				  const dmc::ReqParams<ClientId>& req_params) {
    auto i = servers.find(server);
    assert(servers.end() != i);
    i->second->post(request, req_params);
  };

  for (auto const &i : client_info) {
    ClientId name = i.first;
    int goal = i.second.second;
    clients[name] = new TestClient(name,
				   server_post_f,
				   server_0_f,
				   goal * goal_secs_to_run,
				   goal,
				   client_outstanding_ops);
  }

  // clients are now running; wait for all to finish

  for (auto const &i : clients) {
    i.second->wait_until_done();
  }

  // compute and display stats

  const TestClient::TimePoint late_time = TestClient::now();
  TestClient::TimePoint latest_start = early_time;
  TestClient::TimePoint earliest_finish = late_time;
  TestClient::TimePoint latest_finish = early_time;

  for (auto const &i : clients) {
    auto start = i.second->get_op_times().front();
    auto end = i.second->get_op_times().back();

    if (start > latest_start) { latest_start = start; }
    if (end < earliest_finish) { earliest_finish = end; }
    if (end > latest_finish) { latest_finish = end; }
  }

  const auto start_edge = latest_start + skip_amount;

  std::map<ClientId,std::vector<double>> ops_data;

  for (auto const &i : clients) {
    auto it = i.second->get_op_times().begin();
    const auto end = i.second->get_op_times().end();
    while (it != end && *it < start_edge) { ++it; }

    for (auto time_edge = start_edge + measure_unit;
	 time_edge < latest_finish;
	 time_edge += measure_unit) {
      int count = 0;
      for (; it != end && *it < time_edge; ++count, ++it) { /* empty */ }
      double ops_per_second = double(count) / (measure_unit / report_unit);
      ops_data[i.first].push_back(ops_per_second);
    }
  }

  const int head_w = 12;
  const int data_w = 8;
  const int data_prec = 2;

  std::cout << std::setw(head_w) << "client:";
  for (auto const &i : clients) {
    std::cout << std::setw(data_w) << i.first;
  }
  std::cout << std::setw(data_w) << "total" << std::endl;

  {
    bool has_data;
    int i = 0;
    do {
      std::string line_header = "t_" + std::to_string(i) + ":";
      std::cout << std::setw(head_w) << line_header;
      has_data = false;
      double total = 0.0;
      for (auto const &c : clients) {
	double data = 0.0;
	if (i < ops_data[c.first].size()) {
	  data = ops_data[c.first][i];
	  has_data = true;
	}
	total += data;
	std::cout << std::setw(data_w) << std::setprecision(data_prec) <<
	  std::fixed << data;
      }
      std::cout << std::setw(data_w) << std::setprecision(data_prec) <<
	std::fixed << total << std::endl;
      ++i;
    } while(has_data);
  }
  
  // report how many ops were done by reservation and proportion for
  // each client

  std::cout << std::setw(head_w) << "res_ops:";
  for (auto const &c : clients) {
    std::cout << std::setw(data_w) << c.second->get_res_count();
  }
  std::cout << std::endl;

  std::cout << std::setw(head_w) << "prop_ops:";
  for (auto const &c : clients) {
    std::cout << std::setw(data_w) << c.second->get_prop_count();
  }
  std::cout << std::endl;


  for (auto const &s : servers) {
    std::cout << "server:" << s.first <<
      ", res_ops:" << s.second->get_res_count() <<
      ", prop_ops:" << s.second->get_prop_count() << std::endl;
  }


  // clean up clients then servers

  for (auto i = clients.begin(); i != clients.end(); ++i) {
    delete i->second;
    i->second = nullptr;
  }

  for (auto i = servers.begin(); i != servers.end(); ++i) {
    delete i->second;
    i->second = nullptr;
  }

  std::cout << "simulation complete" << std::endl;
}
